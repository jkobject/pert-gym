"""Lazy compatibility loader for logical pert-gym datasets and collections.

The public boundary presents one dataset as ``obs`` + ``X`` + one verified,
dataset-level ``var`` regardless of whether its payload is the logical sparse
Zarr v1 surface, the GCS-native sparse Zarr surface, or a legacy small H5AD
triplet. Opening a dataset reads metadata only. Matrix and obs payloads are
materialized one selected block at a time.
"""

from __future__ import annotations

import hashlib
import io
import json
import posixpath
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence, TypeAlias, cast

import anndata as ad
import fsspec
import numpy as np
import pandas as pd
import zarr
from scipy import sparse

from pert_gym.gcs_native_sparse_zarr import (
    FORMAT as GCS_NATIVE_FORMAT,
)
from pert_gym.gcs_native_sparse_zarr import (
    GIB,
    PRODUCTION_BLOCK_MAX_BYTES,
    PRODUCTION_BLOCK_MIN_BYTES,
    PRODUCTION_BLOCK_TARGET_BYTES,
    plan_production_blocks,
)
from pert_gym.logical_sparse_zarr import (
    _matrix_components,
    _sha256_array,
    shared_var_identity,
)
from pert_gym.sparse_zarr_contract import (
    LEGACY_FORMAT,
    SURFACE_FORMAT,
    LogicalSparseSurface,
    load_compatible_surface,
)

CompressedMatrix: TypeAlias = sparse.csr_matrix | sparse.csc_matrix


@dataclass(frozen=True)
class LogicalBatch:
    """One bounded materialization with authoritative obs order and shared var."""

    dataset: str
    start: int
    end: int
    block_indexes: tuple[int, ...]
    X: CompressedMatrix
    obs: pd.DataFrame
    var: pd.DataFrame


@dataclass(frozen=True)
class _BlockSpec:
    index: int
    production_block_index: int
    start: int
    end: int
    matrix_key: str
    obs_key: str
    checksums: Mapping[str, str]
    obs_generation: str = ""


class LogicalMatrixView:
    """Backed-like row-slice view; it never retains prior materialized blocks."""

    def __init__(self, dataset: "LogicalDataset") -> None:
        self._dataset = dataset

    @property
    def shape(self) -> tuple[int, int]:
        return self._dataset.shape

    @property
    def format(self) -> str:
        return self._dataset.sparse_format

    def __getitem__(self, selection: object) -> CompressedMatrix:
        columns: object = slice(None)
        rows = selection
        if isinstance(selection, tuple):
            if len(selection) != 2:
                raise IndexError("logical matrix expects X[rows] or X[rows, columns]")
            rows, columns = selection
        if not isinstance(rows, slice):
            raise TypeError("logical matrix supports contiguous row slices only")
        result = self._dataset.read(rows=rows).X
        return result[:, columns]


class LogicalObsView:
    """Lazy observation view matching the logical matrix row coordinates."""

    def __init__(self, dataset: "LogicalDataset") -> None:
        self._dataset = dataset

    def __len__(self) -> int:
        return self._dataset.shape[0]

    def __getitem__(self, selection: slice) -> pd.DataFrame:
        return self._dataset.read_obs(rows=selection)

    @property
    def iloc(self) -> "LogicalObsView":
        return self


class LogicalDataset:
    """Public lazy view over one immutable logical dataset revision."""

    def __init__(
        self,
        *,
        name: str,
        manifest: Mapping[str, Any],
        filesystem: Any,
        root: str,
    ) -> None:
        self.name = name
        self._manifest = dict(manifest)
        self._fs = filesystem
        self._root = root.rstrip("/")
        self._var: pd.DataFrame | None = None
        self._legacy = self._manifest.get("format") == LEGACY_FORMAT
        self._gcs_native = self._manifest.get("format") == GCS_NATIVE_FORMAT
        self._surface: LogicalSparseSurface | None = None

        if self._gcs_native:
            (
                self._shape,
                self._nnz,
                self._sparse_format,
                self._blocks,
                self._production_block_count,
            ) = _normalize_gcs_native_manifest(self._manifest)
            var = _require_mapping(self._manifest, "var")
            self._var_key = _nonempty_string(var.get("key"), "var.key")
            self._var_generation = str(var.get("generation", ""))
            self._var_identity = {
                key: _nonempty_string(var.get(key), f"var.{key}")
                for key in ("index_sha256", "frame_sha256", "schema_fingerprint")
            }
        else:
            self._surface = load_compatible_surface(self._manifest)
            self._shape = self._surface.shape
            self._nnz = self._surface.nnz
            self._sparse_format = self._surface.sparse_format
            self._blocks = tuple(
                _BlockSpec(
                    index=index,
                    production_block_index=index,
                    start=chunk.start,
                    end=chunk.end,
                    matrix_key=chunk.key,
                    obs_key=str(chunk.obs["key"]),
                    checksums=chunk.checksums,
                )
                for index, chunk in enumerate(self._surface.chunks)
            )
            self._production_block_count = len(self._blocks)
            self._var_key = str(self._surface.shared_var["key"])
            self._var_generation = ""
            if self._surface.format == SURFACE_FORMAT:
                self._var_identity = {
                    key: str(self._surface.shared_var[key])
                    for key in ("index_sha256", "frame_sha256", "schema_fingerprint")
                }
            else:
                self._var_identity = None

        self._var_resolved_key = self._resolve_shared_var_key()

        self.X = LogicalMatrixView(self)
        self.obs = LogicalObsView(self)

    @property
    def shape(self) -> tuple[int, int]:
        return self._shape

    @property
    def nnz(self) -> int:
        return self._nnz

    @property
    def sparse_format(self) -> str:
        return self._sparse_format

    @property
    def block_count(self) -> int:
        return self._production_block_count

    @property
    def var(self) -> pd.DataFrame:
        if self._var is None:
            frame = _read_parquet(
                self._fs,
                self._var_resolved_key,
                generation=self._var_generation,
            )
            if len(frame) != self.shape[1]:
                raise ValueError("shared var row count does not match dataset width")
            if self._var_identity is not None:
                observed = shared_var_identity(
                    frame,
                    schema_fingerprint=self._var_identity["schema_fingerprint"],
                )
                if (
                    observed.index_sha256.lower()
                    != self._var_identity["index_sha256"].lower()
                    or observed.frame_sha256.lower()
                    != self._var_identity["frame_sha256"].lower()
                ):
                    raise ValueError(
                        "shared var identity does not match manifest full hashes"
                    )
            self._var = frame
        return self._var

    def _resolve_key(self, key: str) -> str:
        if self._gcs_native:
            return key.strip("/")
        if not self._root:
            return key
        return posixpath.join(self._root, key)

    def _resolve_shared_var_key(self) -> str:
        if self._gcs_native or self._legacy:
            return self._resolve_key(self._var_key)
        candidate = self._resolve_key(self._var_key)
        if self._fs.exists(candidate):
            return candidate
        ancestor = self._root
        for _ in range(32):
            parent = posixpath.dirname(ancestor)
            if parent == ancestor:
                break
            candidate = posixpath.join(parent, self._var_key)
            if self._fs.exists(candidate):
                return candidate
            ancestor = parent
        raise FileNotFoundError(
            f"shared var {self._var_key!r} is not reachable from manifest root {self._root!r}"
        )

    def _read_block(
        self, block: _BlockSpec, *, local_start: int, local_end: int
    ) -> tuple[CompressedMatrix, pd.DataFrame]:
        if self._legacy:
            matrix = _read_legacy_matrix(
                self._fs,
                self._resolve_key(block.matrix_key),
                sparse_format=self.sparse_format,
                rows=slice(local_start, local_end),
            )
        else:
            matrix = _read_zarr_matrix(
                self._fs, self._resolve_key(block.matrix_key), self.sparse_format
            )
            if block.checksums:
                for name, values in zip(
                    ("data_sha256", "indices_sha256", "indptr_sha256"),
                    _matrix_components(matrix),
                    strict=True,
                ):
                    expected = block.checksums[name]
                    if _sha256_array(values).lower() != expected.lower():
                        raise ValueError(
                            f"matrix checksum mismatch for block {block.index}:{name}"
                        )
        obs = _read_parquet(
            self._fs,
            self._resolve_key(block.obs_key),
            generation=block.obs_generation,
        )
        expected_matrix_rows = (
            local_end - local_start if self._legacy else block.end - block.start
        )
        if matrix.shape != (expected_matrix_rows, self.shape[1]) or len(obs) != (
            block.end - block.start
        ):
            raise ValueError(f"block {block.index} matrix/obs shape mismatch")
        return matrix, obs

    def iter_blocks(
        self,
        *,
        blocks: Sequence[int] | None = None,
        rows: slice | None = None,
    ) -> Iterator[LogicalBatch]:
        """Yield selected physical blocks, each bounded to the requested row slice."""
        start, end = _normalize_rows(rows, self.shape[0])
        selected = _normalize_block_indexes(blocks, self.block_count)
        for block in self._blocks:
            if block.production_block_index not in selected:
                continue
            overlap_start = max(start, block.start)
            overlap_end = min(end, block.end)
            if overlap_start >= overlap_end:
                continue
            local_start = overlap_start - block.start
            local_end = overlap_end - block.start
            matrix, obs = self._read_block(
                block, local_start=local_start, local_end=local_end
            )
            yield LogicalBatch(
                dataset=self.name,
                start=overlap_start,
                end=overlap_end,
                block_indexes=(block.production_block_index,),
                X=(
                    matrix.copy()
                    if self._legacy
                    else matrix[local_start:local_end].copy()
                ),
                obs=obs.iloc[local_start:local_end].copy(),
                var=self.var,
            )

    def read(
        self,
        *,
        rows: slice | None = None,
        blocks: Sequence[int] | None = None,
    ) -> LogicalBatch:
        """Materialize one bounded, contiguous batch.

        GCS-native reads may cover at most one measured production block. The
        older logical surface has no measured block metadata, so it retains the
        small compatibility allowance for at most one chunk's row capacity,
        including a slice crossing one chunk boundary. Larger selections must
        use :meth:`iter_blocks`.
        """
        if rows is None and blocks is None:
            raise ValueError(
                "read() requires a bounded row or block selection; use iter_blocks() to stream the dataset"
            )
        row_start, row_end = _normalize_rows(rows, self.shape[0])
        selected_blocks = _normalize_block_indexes(blocks, self.block_count)
        if blocks is not None and selected_blocks:
            first_block = min(selected_blocks)
            last_block = max(selected_blocks)
            if selected_blocks != set(range(first_block, last_block + 1)):
                raise ValueError(
                    "read() requires contiguous blocks for one LogicalBatch; use iter_blocks() for non-contiguous selections"
                )
        if (
            self.block_count > 1
            and (row_start, row_end) == (0, self.shape[0])
            and selected_blocks == set(range(self.block_count))
        ):
            raise ValueError(
                "full-dataset materialization is forbidden; use iter_blocks() to stream logical blocks"
            )
        overlapping = tuple(
            block
            for block in self._blocks
            if block.production_block_index in selected_blocks
            and max(row_start, block.start) < min(row_end, block.end)
        )
        overlapping_blocks = {block.production_block_index for block in overlapping}
        if self._gcs_native and len(overlapping_blocks) > 1:
            raise ValueError(
                "bounded selection exceeds the read() limit of 1 measured production block; use iter_blocks() to stream logical blocks"
            )
        if not self._gcs_native and overlapping:
            materialized_rows = sum(
                min(row_end, block.end) - max(row_start, block.start)
                for block in overlapping
            )
            maximum_chunk_rows = max(block.end - block.start for block in self._blocks)
            if len(overlapping) > 2 or materialized_rows > maximum_chunk_rows:
                raise ValueError(
                    "bounded selection exceeds one logical chunk's row capacity; use iter_blocks() to stream logical blocks"
                )
        batches = list(self.iter_blocks(blocks=blocks, rows=rows))
        constructor = (
            sparse.csr_matrix if self.sparse_format == "csr" else sparse.csc_matrix
        )
        if not batches:
            matrix: CompressedMatrix = constructor((0, self.shape[1]))
            obs = pd.DataFrame()
            start = end = _normalize_rows(rows, self.shape[0])[0]
        else:
            matrix = sparse.vstack(
                [batch.X for batch in batches], format=self.sparse_format
            )
            obs = pd.concat([batch.obs for batch in batches], axis=0)
            start, end = batches[0].start, batches[-1].end
        if end - start != matrix.shape[0] or matrix.shape[0] != len(obs):
            raise ValueError(
                "LogicalBatch row coordinates do not match materialized rows"
            )
        return LogicalBatch(
            dataset=self.name,
            start=start,
            end=end,
            block_indexes=tuple(
                dict.fromkeys(
                    index for batch in batches for index in batch.block_indexes
                )
            ),
            X=matrix,
            obs=obs,
            var=self.var,
        )

    def read_obs(
        self,
        *,
        rows: slice | None = None,
        blocks: Sequence[int] | None = None,
    ) -> pd.DataFrame:
        """Read bounded obs sidecars without materializing matrix payloads."""
        if rows is None and blocks is None:
            raise ValueError(
                "read_obs() requires a bounded row or block selection; stream explicit logical blocks instead"
            )
        start, end = _normalize_rows(rows, self.shape[0])
        selected = _normalize_block_indexes(blocks, self.block_count)
        frames: list[pd.DataFrame] = []
        for block in self._blocks:
            if block.production_block_index not in selected:
                continue
            overlap_start = max(start, block.start)
            overlap_end = min(end, block.end)
            if overlap_start >= overlap_end:
                continue
            obs = _read_parquet(
                self._fs,
                self._resolve_key(block.obs_key),
                generation=block.obs_generation,
            )
            if len(obs) != block.end - block.start:
                raise ValueError(f"block {block.index} obs shape mismatch")
            frames.append(
                obs.iloc[overlap_start - block.start : overlap_end - block.start].copy()
            )
        return pd.concat(frames, axis=0) if frames else pd.DataFrame()


class LogicalCollection:
    """Lazy named collection; dataset selection does not dereference payloads."""

    def __init__(
        self,
        datasets: Mapping[str, object],
        *,
        filesystem: Any | None = None,
    ) -> None:
        if not datasets or any(not str(name) for name in datasets):
            raise ValueError("logical collection requires non-empty dataset names")
        self._sources = dict(datasets)
        self._filesystem = filesystem
        self._opened: dict[str, LogicalDataset] = {}

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._sources)

    def __getitem__(self, name: str) -> LogicalDataset:
        if name not in self._sources:
            raise KeyError(name)
        if name not in self._opened:
            self._opened[name] = open_logical_dataset(
                self._sources[name], filesystem=self._filesystem, name=name
            )
        return self._opened[name]

    def select(self, names: Sequence[str]) -> "LogicalCollection":
        missing = [name for name in names if name not in self._sources]
        if missing:
            raise KeyError(f"unknown logical datasets: {missing}")
        return LogicalCollection(
            {name: self._sources[name] for name in names}, filesystem=self._filesystem
        )


def open_logical_dataset(
    source: object,
    *,
    root: str | Path | None = None,
    filesystem: Any | None = None,
    name: str | None = None,
) -> LogicalDataset:
    """Open manifest metadata locally or remotely without loading obs/X/var payloads."""
    manifest: Mapping[str, Any]
    resolved_root: str
    fs = filesystem
    if isinstance(source, Mapping):
        manifest = cast(Mapping[str, Any], source)
        if fs is None:
            fs = fsspec.filesystem("file")
        resolved_root = str(root or "")
    elif isinstance(source, (str, Path)):
        source_value = str(source)
        if fs is None:
            fs, manifest_key = fsspec.core.url_to_fs(source_value)
        else:
            manifest_key = source_value
        with fs.open(manifest_key, "rb") as handle:
            manifest = json.load(handle)
        if "manifest_key" in manifest and "manifest_sha256" in manifest:
            promoted_key = _nonempty_string(
                manifest.get("manifest_key"), "promotion.manifest_key"
            )
            expected_hash = _nonempty_string(
                manifest.get("manifest_sha256"), "promotion.manifest_sha256"
            )
            with fs.open(promoted_key, "rb") as handle:
                promoted_bytes = handle.read()
            if (
                hashlib.sha256(promoted_bytes).hexdigest().lower()
                != expected_hash.lower()
            ):
                raise ValueError(
                    "promoted manifest hash does not match promotion marker"
                )
            promoted = json.loads(promoted_bytes)
            if not isinstance(promoted, Mapping):
                raise ValueError("promoted manifest must be an object")
            manifest = promoted
            manifest_key = promoted_key
        resolved_root = (
            str(root) if root is not None else posixpath.dirname(manifest_key)
        )
    else:
        raise TypeError("source must be a manifest mapping or manifest path/URI")
    dataset_name = name or str(manifest.get("logical_key") or "logical-dataset")
    return LogicalDataset(
        name=dataset_name,
        manifest=manifest,
        filesystem=fs,
        root=resolved_root,
    )


def _normalize_gcs_native_manifest(
    manifest: Mapping[str, Any],
) -> tuple[tuple[int, int], int, str, tuple[_BlockSpec, ...], int]:
    if manifest.get("format") != GCS_NATIVE_FORMAT:
        raise ValueError("unsupported GCS-native format")
    shape_raw = manifest.get("shape")
    if not isinstance(shape_raw, list) or len(shape_raw) != 2:
        raise ValueError("GCS-native shape must be [n_obs, n_vars]")
    shape = (
        _nonnegative_int(shape_raw[0], "shape[0]"),
        _nonnegative_int(shape_raw[1], "shape[1]"),
    )
    nnz = _nonnegative_int(manifest.get("nnz"), "nnz")
    sparse_format = str(manifest.get("sparse_format"))
    if sparse_format not in {"csr", "csc"}:
        raise ValueError("GCS-native sparse_format must be csr or csc")
    source = _require_mapping(manifest, "source")
    source_start = _nonnegative_int(source.get("row_start"), "source.row_start")
    source_end = _nonnegative_int(source.get("row_end"), "source.row_end")
    if source_end - source_start != shape[0]:
        raise ValueError("GCS-native source bounds must match shape")
    raw_chunks = manifest.get("chunks")
    if not isinstance(raw_chunks, list):
        raise ValueError("GCS-native chunks must be a list")
    chunks_without_blocks: list[dict[str, object]] = []
    expected = source_start
    for index, raw in enumerate(raw_chunks):
        if not isinstance(raw, Mapping):
            raise ValueError(f"chunks[{index}] must be an object")
        start = _nonnegative_int(raw.get("start"), f"chunks[{index}].start")
        end = _nonnegative_int(raw.get("end"), f"chunks[{index}].end")
        if start != expected or end <= start:
            raise ValueError("GCS-native chunks must contiguously tile source rows")
        checksums = _require_mapping(raw, "checksums")
        compressed_bytes = _nonnegative_int(
            raw.get("compressed_bytes"), f"chunks[{index}].compressed_bytes"
        )
        chunks_without_blocks.append(
            {
                "index": index,
                "start": start - source_start,
                "end": end - source_start,
                "matrix_key": _nonempty_string(raw.get("matrix_key"), "matrix_key"),
                "obs_key": _nonempty_string(raw.get("obs_key"), "obs_key"),
                "checksums": {
                    key: _nonempty_string(checksums.get(key), f"checksums.{key}")
                    for key in ("data_sha256", "indices_sha256", "indptr_sha256")
                },
                "obs_generation": str(raw.get("obs_generation", "")),
                "compressed_bytes": compressed_bytes,
            }
        )
        expected = end
    if expected != source_end or (shape[0] and not chunks_without_blocks):
        raise ValueError("GCS-native chunk denominator mismatch")
    raw_blocks = manifest.get("blocks")
    if not isinstance(raw_blocks, list):
        raise ValueError("GCS-native blocks must be a list")
    policy = _require_mapping(manifest, "production_block_policy")
    minimum_bytes = _nonnegative_int(
        policy.get("minimum_bytes"), "production_block_policy.minimum_bytes"
    )
    target_bytes = _nonnegative_int(
        policy.get("target_bytes"), "production_block_policy.target_bytes"
    )
    maximum_bytes = _nonnegative_int(
        policy.get("maximum_bytes"), "production_block_policy.maximum_bytes"
    )
    try:
        expected_block_ranges = plan_production_blocks(
            [
                _nonnegative_int(
                    chunk["compressed_bytes"], f"chunks[{index}].compressed_bytes"
                )
                for index, chunk in enumerate(chunks_without_blocks)
            ],
            min_bytes=minimum_bytes,
            target_bytes=target_bytes,
            max_bytes=maximum_bytes,
        )
    except ValueError as error:
        raise ValueError(
            "GCS-native production block layout violates measured policy"
        ) from error
    observed_block_ranges: list[tuple[int, int]] = []
    for raw_block in raw_blocks:
        if not isinstance(raw_block, Mapping):
            raise ValueError("GCS-native blocks must contain objects")
        indexes = raw_block.get("chunk_indexes")
        if not isinstance(indexes, list) or not indexes:
            raise ValueError("GCS-native block chunk_indexes must be non-empty")
        observed_block_ranges.append((indexes[0], indexes[-1] + 1))
    if tuple(observed_block_ranges) != expected_block_ranges:
        raise ValueError("GCS-native production block layout violates measured policy")
    manifest_var = _require_mapping(manifest, "var")
    physical_to_production: dict[int, int] = {}
    expected_chunk_index = 0
    for block_index, raw_block in enumerate(raw_blocks):
        if not isinstance(raw_block, Mapping):
            raise ValueError(f"blocks[{block_index}] must be an object")
        if raw_block.get("index") != block_index:
            raise ValueError("GCS-native block indexes must be contiguous")
        chunk_indexes = raw_block.get("chunk_indexes")
        if not isinstance(chunk_indexes, list) or not chunk_indexes:
            raise ValueError("GCS-native block chunk_indexes must be non-empty")
        expected_indexes = list(
            range(expected_chunk_index, expected_chunk_index + len(chunk_indexes))
        )
        if chunk_indexes != expected_indexes:
            raise ValueError("GCS-native blocks must contiguously tile chunks")
        expected_block_start = source_start + _nonnegative_int(
            chunks_without_blocks[expected_indexes[0]]["start"], "chunk.start"
        )
        expected_block_end = source_start + _nonnegative_int(
            chunks_without_blocks[expected_indexes[-1]]["end"], "chunk.end"
        )
        if (
            raw_block.get("start") != expected_block_start
            or raw_block.get("end") != expected_block_end
        ):
            raise ValueError("GCS-native block row bounds do not match its chunks")
        measured = sum(
            _nonnegative_int(
                chunks_without_blocks[index]["compressed_bytes"],
                f"chunks[{index}].compressed_bytes",
            )
            for index in expected_indexes
        )
        if raw_block.get("compressed_bytes") != measured:
            raise ValueError("GCS-native block compressed byte measurement mismatch")
        if raw_block.get("var") != manifest_var:
            raise ValueError("GCS-native block must reference manifest shared var")
        for index in expected_indexes:
            physical_to_production[index] = block_index
        expected_chunk_index += len(chunk_indexes)
    if expected_chunk_index != len(chunks_without_blocks):
        raise ValueError("GCS-native block denominator mismatch")
    blocks = tuple(
        _BlockSpec(
            index=_nonnegative_int(raw["index"], "chunk.index"),
            production_block_index=physical_to_production[
                _nonnegative_int(raw["index"], "chunk.index")
            ],
            start=_nonnegative_int(raw["start"], "chunk.start"),
            end=_nonnegative_int(raw["end"], "chunk.end"),
            matrix_key=str(raw["matrix_key"]),
            obs_key=str(raw["obs_key"]),
            checksums=cast(Mapping[str, str], raw["checksums"]),
            obs_generation=str(raw["obs_generation"]),
        )
        for raw in chunks_without_blocks
    )
    return shape, nnz, sparse_format, blocks, len(raw_blocks)


def _read_zarr_matrix(fs: Any, key: str, sparse_format: str) -> CompressedMatrix:
    store = zarr.storage.FSStore(key, fs=fs, mode="r", check=False)
    try:
        group = zarr.open_group(store=store, mode="r")
        constructor = sparse.csr_matrix if sparse_format == "csr" else sparse.csc_matrix
        return constructor(
            (
                np.asarray(group["data"]),
                np.asarray(group["indices"]),
                np.asarray(group["indptr"]),
            ),
            shape=tuple(group.attrs["shape"]),
        )
    finally:
        store.close()


def _read_legacy_matrix(
    fs: Any, key: str, *, sparse_format: str, rows: slice
) -> CompressedMatrix:
    if getattr(fs, "protocol", "file") not in {"file", ("file", "local")}:
        raise ValueError(
            "remote legacy H5AD requires an explicit local cache; sparse Zarr is required for large remote datasets"
        )
    source = ad.read_h5ad(key, backed="r")
    try:
        matrix_source = cast(Any, source.X)
        matrix = matrix_source[rows]
        if isinstance(matrix, np.ndarray):
            constructor = (
                sparse.csr_matrix if sparse_format == "csr" else sparse.csc_matrix
            )
            return constructor(matrix)
        if sparse_format == "csr" and sparse.isspmatrix_csr(matrix):
            return sparse.csr_matrix(matrix, copy=True)
        if sparse_format == "csc" and sparse.isspmatrix_csc(matrix):
            return sparse.csc_matrix(matrix, copy=True)
        raise TypeError("legacy H5AD sparse orientation does not match its manifest")
    finally:
        source.file.close()


def _read_parquet(fs: Any, key: str, *, generation: str = "") -> pd.DataFrame:
    if generation:
        if not bool(getattr(fs, "version_aware", False)):
            raise ValueError(
                "filesystem cannot enforce manifest-pinned object generation"
            )
        if "#" in key:
            raise ValueError("object key must not contain a generation fragment")
        pinned_key = f"{key}#{generation}"
        info = fs.info(pinned_key)
        if str(info.get("generation", "")) != generation:
            raise ValueError(
                "generation-qualified object resolved a different generation"
            )
        with fs.open(pinned_key, "rb") as handle:
            return pd.read_parquet(handle)
    with fs.open(key, "rb") as handle:
        return pd.read_parquet(io.BytesIO(handle.read()))


def _normalize_rows(rows: slice | None, n_obs: int) -> tuple[int, int]:
    if rows is None:
        return 0, n_obs
    if not isinstance(rows, slice) or rows.step not in (None, 1):
        raise TypeError("rows must be one contiguous slice with step 1")
    start, end, _ = rows.indices(n_obs)
    return start, end


def _normalize_block_indexes(blocks: Sequence[int] | None, count: int) -> set[int]:
    if blocks is None:
        return set(range(count))
    selected = set(blocks)
    if any(
        not isinstance(index, int)
        or isinstance(index, bool)
        or index < 0
        or index >= count
        for index in selected
    ):
        raise IndexError("block selection is outside the logical dataset")
    return selected


def _require_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = value.get(key)
    if not isinstance(nested, Mapping):
        raise ValueError(f"{key} must be an object")
    return nested


def _nonnegative_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _nonempty_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value
