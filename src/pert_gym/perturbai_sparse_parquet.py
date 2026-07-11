"""Streaming adapter for PerturbAI sparse-row Parquet sources.

The adapter deliberately writes only local, append-only logical sparse-Zarr
candidates. Publication remains delegated to ``publish_candidate`` so its
journal and recovery guarantees stay the sole remote-write path.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from scipy import sparse

from pert_gym.logical_sparse_zarr import (
    DEFAULT_MAX_ROWS,
    DEFAULT_MIN_ROWS,
    write_logical_sparse_revision,
)

REQUESTER_PAYS_PROJECT = "jkobject-1549353370965"
_SOURCE_STEM = re.compile(r"^(?P<family>WB\d+_\d+_\d+_part-)(?P<part>\d+)$")
_REQUIRED_COLUMNS = {"cell_id", "genes", "expressions"}
_CSR_STORAGE_DTYPE = np.dtype(np.int32)
_CSR_STORAGE_MAX = np.iinfo(_CSR_STORAGE_DTYPE).max


@dataclass(frozen=True)
class PerturbAISource:
    """One immutable sparse-row parquet object selected from the source commit."""

    stem: str
    source_uri: str
    source_commit: str
    source_object_id: str
    local_path: Path


def requester_pays_storage_options(billing_project: str) -> dict[str, object]:
    """Return the only allowed GCS requester-pays configuration for this adapter."""
    if billing_project != REQUESTER_PAYS_PROJECT:
        raise ValueError(
            f"billing project must be {REQUESTER_PAYS_PROJECT!r} for requester-pays"
        )
    return {"project": billing_project, "requester_pays": True}


def _source_part(source: PerturbAISource) -> tuple[str, int]:
    match = _SOURCE_STEM.fullmatch(source.stem)
    if match is None:
        raise ValueError(f"invalid PerturbAI source stem: {source.stem!r}")
    return match["family"], int(match["part"])


def validate_perturbai_sources(
    sources: Sequence[PerturbAISource],
) -> tuple[PerturbAISource, ...]:
    """Require one ordered, contiguous, immutable source-stem family."""
    result = tuple(sources)
    if not result:
        raise ValueError("at least one PerturbAI source is required")
    parsed = [_source_part(source) for source in result]
    stems = [source.stem for source in result]
    if len(set(stems)) != len(stems):
        raise ValueError("duplicate PerturbAI source stems")
    families = {family for family, _part in parsed}
    if len(families) != 1:
        raise ValueError("PerturbAI sources must belong to one stem family")
    parts = [part for _family, part in parsed]
    if parts != sorted(parts):
        raise ValueError("PerturbAI source parts are out of order")
    if parts != list(range(parts[0], parts[-1] + 1)):
        raise ValueError("PerturbAI source parts are missing")
    for source in result:
        if (
            not source.source_uri
            or not source.source_commit
            or not source.source_object_id
            or not source.local_path.is_file()
        ):
            raise ValueError(
                f"source identity or local payload is incomplete: {source.stem}"
            )
    return result


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _validated_sparse_vector(
    raw: object, *, sequence_name: str, value_name: str, upper_bound: int
) -> np.ndarray:
    """Validate one source list before casting to the adapter's int32 storage.

    CSR indices and raw-count values are stored as int32, so source values must
    be finite, integer-valued, non-negative, and no greater than ``upper_bound``.
    This prevents lossy coercion and int32 wraparound during candidate creation.
    """
    if raw is None or isinstance(raw, (str, bytes, bytearray)):
        raise ValueError(f"{sequence_name} must be a one-dimensional sequence")
    values = np.asarray(raw, dtype=object)
    if values.ndim != 1:
        raise ValueError(f"{sequence_name} must be a one-dimensional sequence")
    result = np.empty(len(values), dtype=_CSR_STORAGE_DTYPE)
    for index, value in enumerate(values):
        if isinstance(value, (list, tuple, np.ndarray)):
            raise ValueError(f"{sequence_name} must be a one-dimensional sequence")
        if isinstance(value, (bool, np.bool_)):
            raise ValueError(f"{value_name} must not be boolean")
        if not isinstance(value, (int, float, np.integer, np.floating)):
            raise ValueError(f"{value_name} must be numeric")
        if isinstance(value, (int, np.integer)):
            integer_value = int(value)
        else:
            float_value = float(value)
            if not np.isfinite(float_value):
                raise ValueError(f"{value_name} must be finite")
            if not float_value.is_integer():
                raise ValueError(f"{value_name} must be integer-valued")
            integer_value = int(float_value)
        if integer_value < 0 or integer_value > upper_bound:
            raise ValueError(f"{value_name} outside supported range 0..{upper_bound}")
        result[index] = integer_value
    return result


def _validated_cell_ids(frame: pd.DataFrame) -> pd.Index:
    """Return scalar, non-null, non-blank cell identities without null coercion."""
    cells: list[str] = []
    for raw in frame["cell_id"]:
        null = pd.isna(raw)
        if isinstance(null, (bool, np.bool_)) and null:
            raise ValueError("cell_id must be non-null")
        if not isinstance(null, (bool, np.bool_)):
            raise ValueError("cell_id must be a scalar identity")
        cell = str(raw)
        if not cell.strip():
            raise ValueError("cell_id must be non-blank")
        cells.append(cell)
    return pd.Index(cells, name="cell_id")


def _csr_from_frame(frame: pd.DataFrame, n_vars: int) -> sparse.csr_matrix:
    """Build an int32 CSR batch after validating every sparse source value."""
    missing = _REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"source parquet missing columns: {sorted(missing)}")
    if n_vars < 0 or n_vars > _CSR_STORAGE_MAX:
        raise ValueError(f"number of variables exceeds int32 index bound: {n_vars}")
    indptr = np.empty(len(frame) + 1, dtype=np.int64)
    indptr[0] = 0
    rows: list[tuple[np.ndarray, np.ndarray]] = []
    for genes, expressions in zip(frame["genes"], frame["expressions"], strict=True):
        gene_ids = _validated_sparse_vector(
            genes,
            sequence_name="genes",
            value_name="gene token",
            upper_bound=n_vars - 1,
        )
        values = _validated_sparse_vector(
            expressions,
            sequence_name="expressions",
            value_name="expression value",
            upper_bound=_CSR_STORAGE_MAX,
        )
        if len(gene_ids) != len(values):
            raise ValueError("sparse genes/expressions length mismatch")
        if len(gene_ids) != len(np.unique(gene_ids)):
            raise ValueError("duplicate gene token in sparse row")
        rows.append((gene_ids, values))
        indptr[len(rows)] = indptr[len(rows) - 1] + len(gene_ids)
    nnz = int(indptr[-1])
    indices = np.empty(nnz, dtype=_CSR_STORAGE_DTYPE)
    data = np.empty(nnz, dtype=_CSR_STORAGE_DTYPE)
    offset = 0
    for gene_ids, values in rows:
        end = offset + len(gene_ids)
        indices[offset:end] = gene_ids
        data[offset:end] = values
        offset = end
    return sparse.csr_matrix((data, indices, indptr), shape=(len(frame), n_vars))


class _SparseParquetMatrix:
    format = "csr"

    def __init__(
        self, sources: Sequence[PerturbAISource], n_vars: int, batch_rows: int
    ) -> None:
        if batch_rows <= 0:
            raise ValueError("parquet_batch_rows must be positive")
        self.sources = tuple(sources)
        self.n_vars = n_vars
        self.batch_rows = batch_rows
        self.row_ends: list[int] = []
        self.nnz = 0
        self.max_batch_rows = 0
        total = 0
        for source in self.sources:
            rows, nnz, max_rows = self._scan(source)
            total += rows
            self.row_ends.append(total)
            self.nnz += nnz
            self.max_batch_rows = max(self.max_batch_rows, max_rows)
        self.shape = (total, n_vars)

    def _batches(self, source: PerturbAISource) -> Iterator[pd.DataFrame]:
        parquet = pq.ParquetFile(source.local_path)
        for batch in parquet.iter_batches(batch_size=self.batch_rows):
            frame = batch.to_pandas()
            self.max_batch_rows = max(self.max_batch_rows, len(frame))
            yield frame

    def _scan(self, source: PerturbAISource) -> tuple[int, int, int]:
        rows = 0
        nnz = 0
        max_rows = 0
        seen_cells: set[str] = set()
        for frame in self._batches(source):
            matrix = _csr_from_frame(frame, self.n_vars)
            cells = _validated_cell_ids(frame)
            if not cells.is_unique or set(cells) & seen_cells:
                raise ValueError(f"cell_id is not unique in source {source.stem}")
            seen_cells.update(cells)
            rows += len(frame)
            nnz += matrix.nnz
            max_rows = max(max_rows, len(frame))
        return rows, nnz, max_rows

    def __getitem__(self, selection: slice) -> sparse.csr_matrix:
        if not isinstance(selection, slice) or selection.step not in (None, 1):
            raise TypeError("PerturbAI adapter supports contiguous row slices only")
        start, end, _ = selection.indices(self.shape[0])
        if start >= end:
            return sparse.csr_matrix((0, self.n_vars), dtype=np.int32)
        pieces: list[sparse.csr_matrix] = []
        source_start = 0
        for source, source_end in zip(self.sources, self.row_ends, strict=True):
            overlap_start = max(start, source_start)
            overlap_end = min(end, source_end)
            if overlap_start < overlap_end:
                cursor = source_start
                for frame in self._batches(source):
                    batch_end = cursor + len(frame)
                    take_start = max(overlap_start, cursor) - cursor
                    take_end = min(overlap_end, batch_end) - cursor
                    if take_start < take_end:
                        pieces.append(
                            _csr_from_frame(
                                frame.iloc[take_start:take_end], self.n_vars
                            )
                        )
                    cursor = batch_end
            source_start = source_end
        return sparse.vstack(pieces, format="csr")


def _build_obs(
    matrix: _SparseParquetMatrix, sources: Sequence[PerturbAISource]
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    seen_cells: set[str] = set()
    for source in sources:
        for raw in matrix._batches(source):
            obs = raw.drop(columns=["genes", "expressions"]).copy()
            obs.index = _validated_cell_ids(obs)
            if not obs.index.is_unique or set(obs.index) & seen_cells:
                raise ValueError("cell_id is not unique across selected sources")
            seen_cells.update(obs.index)
            obs["dataset"] = "perturbai/wholebrain_crispr_atlas"
            obs["source_file"] = f"data/{source.stem}.parquet"
            obs["source_commit"] = source.source_commit
            obs["source_object_id"] = source.source_object_id
            obs["x_semantics"] = "raw_counts"
            frames.append(obs)
    return pd.concat(frames, axis=0) if frames else pd.DataFrame()


def build_perturbai_revision(
    *,
    root: Path,
    logical_key: str,
    revision: str,
    sources: Sequence[PerturbAISource],
    var: pd.DataFrame,
    schema_fingerprint: str,
    ingestion_run_id: str,
    max_rss_bytes: int = 4 * 1024**3,
    min_rows: int = DEFAULT_MIN_ROWS,
    max_rows: int = DEFAULT_MAX_ROWS,
    parquet_batch_rows: int = 10_000,
    stop_after_chunks: int | None = None,
) -> dict[str, object]:
    """Convert ordered source rows to one append-only local logical revision."""
    ordered = validate_perturbai_sources(sources)
    matrix = _SparseParquetMatrix(ordered, len(var), parquet_batch_rows)
    obs = _build_obs(matrix, ordered)
    source_rows: list[dict[str, object]] = []
    start = 0
    for source, end in zip(ordered, matrix.row_ends, strict=True):
        source_rows.append(
            {
                "stem": source.stem,
                "uri": source.source_uri,
                "commit": source.source_commit,
                "object_id": source.source_object_id,
                "sha256": _sha256_file(source.local_path),
                "row_range": [start, end],
                "max_batch_rows": matrix.max_batch_rows,
            }
        )
        start = end
    source_identity = {"kind": "perturbai-sparse-parquet/v1", "sources": source_rows}
    return write_logical_sparse_revision(
        root=root,
        logical_key=logical_key,
        revision=revision,
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint=schema_fingerprint,
        source_uri="perturbai://sparse-parquet-source-family",
        source_checksum=f"sha256-file-bytes/v1:{_canonical_sha256(source_identity)}",
        source_identity=source_identity,
        ingestion_run_id=ingestion_run_id,
        max_rss_bytes=max_rss_bytes,
        min_rows=min_rows,
        max_rows=max_rows,
        stop_after_chunks=stop_after_chunks,
    )
