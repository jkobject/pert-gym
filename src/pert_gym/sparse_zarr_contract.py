"""Validation and compatibility helpers for logical sparse Zarr surfaces.

This module intentionally operates on JSON-like manifests rather than opening a
store. Writers own I/O; readers use this boundary to reject incomplete or
ambiguous logical datasets before dereferencing an artifact.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

SURFACE_FORMAT = "pert-gym.logical-sparse-zarr"
SURFACE_VERSION = 1
LEGACY_FORMAT = "pert-gym.triplet-h5ad"
LEGACY_VERSION = 1
_CHECKSUM_FIELDS = ("data_sha256", "indices_sha256", "indptr_sha256")
_PROVENANCE_STRING_FIELDS = (
    "source_uri",
    "source_checksum",
    "ingestion_run_id",
    "writer_version",
)


@dataclass(frozen=True)
class ChunkRange:
    """A half-open, row-major logical slice of a sparse matrix."""

    key: str
    start: int
    end: int
    nnz: int
    shape: tuple[int, int]
    dtype: str
    checksums: Mapping[str, str]
    obs: Mapping[str, Any]


@dataclass(frozen=True)
class LogicalSparseSurface:
    """Normalized view consumed by loaders independent of producer version."""

    format: str
    version: int
    shape: tuple[int, int]
    nnz: int
    sparse_format: str
    chunks: tuple[ChunkRange, ...]
    shared_var: Mapping[str, str]


def _require(mapping: Mapping[str, Any], key: str) -> Any:
    try:
        return mapping[key]
    except KeyError as error:
        raise ValueError(
            f"logical surface is missing required field {key!r}"
        ) from error


def _nonnegative_int(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _shape(value: Any, field: str) -> tuple[int, int]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or len(value) != 2
    ):
        raise ValueError(f"{field} must be a two-item sequence [n_obs, n_vars]")
    return (
        _nonnegative_int(value[0], f"{field}[0]"),
        _nonnegative_int(value[1], f"{field}[1]"),
    )


def _as_chunk_ranges(
    chunks: Sequence[Mapping[str, Any]], n_vars: int
) -> tuple[ChunkRange, ...]:
    result: list[ChunkRange] = []
    for index, chunk in enumerate(chunks):
        if not isinstance(chunk, Mapping):
            raise ValueError(f"chunks[{index}] must be an object")
        key = _nonempty_string(_require(chunk, "key"), f"chunks[{index}].key")
        start = _nonnegative_int(_require(chunk, "start"), f"chunks[{index}].start")
        end = _nonnegative_int(_require(chunk, "end"), f"chunks[{index}].end")
        nnz = _nonnegative_int(_require(chunk, "nnz"), f"chunks[{index}].nnz")
        if end <= start:
            raise ValueError(f"chunks[{index}] must have end > start")

        shape = _shape(_require(chunk, "shape"), f"chunks[{index}].shape")
        if shape != (end - start, n_vars):
            raise ValueError(
                f"chunks[{index}].shape must equal [end - start, manifest n_vars]"
            )
        dtype = _nonempty_string(_require(chunk, "dtype"), f"chunks[{index}].dtype")

        checksums = _require(chunk, "checksums")
        if not isinstance(checksums, Mapping):
            raise ValueError(f"chunks[{index}].checksums must be an object")
        for name in _CHECKSUM_FIELDS:
            _nonempty_string(
                _require(checksums, name), f"chunks[{index}].checksums.{name}"
            )

        obs = _require(chunk, "obs")
        if not isinstance(obs, Mapping):
            raise ValueError(f"chunks[{index}].obs must be an object")
        obs_key = _nonempty_string(_require(obs, "key"), f"chunks[{index}].obs.key")
        provenance = _require(obs, "provenance")
        if not isinstance(provenance, Mapping):
            raise ValueError(f"chunks[{index}].obs.provenance must be an object")
        for name in _PROVENANCE_STRING_FIELDS:
            _nonempty_string(
                _require(provenance, name), f"chunks[{index}].obs.provenance.{name}"
            )
        source_start = _nonnegative_int(
            _require(provenance, "source_row_start"),
            f"chunks[{index}].obs.provenance.source_row_start",
        )
        source_end = _nonnegative_int(
            _require(provenance, "source_row_end"),
            f"chunks[{index}].obs.provenance.source_row_end",
        )
        if source_end - source_start != end - start:
            raise ValueError(
                f"chunks[{index}] source row interval must match its logical row count"
            )

        result.append(
            ChunkRange(
                key=key,
                start=start,
                end=end,
                nnz=nnz,
                shape=shape,
                dtype=dtype,
                checksums={name: checksums[name] for name in _CHECKSUM_FIELDS},
                obs={"key": obs_key, "provenance": dict(provenance)},
            )
        )
    return tuple(result)


def validate_logical_sparse_surface(
    manifest: Mapping[str, Any],
) -> LogicalSparseSurface:
    """Validate fail-closed v1 invariants and normalize a logical surface."""
    if _require(manifest, "format") != SURFACE_FORMAT:
        raise ValueError(
            f"unsupported logical surface format: {manifest.get('format')!r}"
        )
    if _require(manifest, "version") != SURFACE_VERSION:
        raise ValueError(
            f"unsupported logical surface version: {manifest.get('version')!r}"
        )

    shape = _shape(_require(manifest, "shape"), "shape")
    n_obs, n_vars = shape
    nnz = _nonnegative_int(_require(manifest, "nnz"), "nnz")
    sparse_format = _require(manifest, "sparse_format")
    if sparse_format not in {"csr", "csc"}:
        raise ValueError("sparse_format must be 'csr' or 'csc'")

    chunks_raw = _require(manifest, "chunks")
    if not isinstance(chunks_raw, Sequence) or isinstance(chunks_raw, (str, bytes)):
        raise ValueError("chunks must be a sequence")
    chunks = _as_chunk_ranges(chunks_raw, n_vars)
    if n_obs == 0 and chunks:
        raise ValueError("zero-row surfaces must not declare chunks")
    if n_obs > 0 and not chunks:
        raise ValueError("non-empty surfaces must declare chunks")

    expected_start = 0
    for chunk in chunks:
        if chunk.start != expected_start:
            raise ValueError(
                f"chunk coverage is not contiguous: expected start {expected_start}, got {chunk.start}"
            )
        expected_start = chunk.end
    if expected_start != n_obs:
        raise ValueError(
            f"chunk denominator mismatch: covered {expected_start}, declared {n_obs}"
        )
    if sum(chunk.nnz for chunk in chunks) != nnz:
        raise ValueError("chunk nnz sum must equal manifest nnz")

    shared_var = _require(manifest, "shared_var")
    if not isinstance(shared_var, Mapping):
        raise ValueError("shared_var must be an object")
    for key in ("key", "index_sha256", "frame_sha256", "schema_fingerprint"):
        _nonempty_string(_require(shared_var, key), f"shared_var.{key}")

    return LogicalSparseSurface(
        format=SURFACE_FORMAT,
        version=SURFACE_VERSION,
        shape=shape,
        nnz=nnz,
        sparse_format=sparse_format,
        chunks=chunks,
        shared_var={
            key: shared_var[key]
            for key in ("key", "index_sha256", "frame_sha256", "schema_fingerprint")
        },
    )


def load_compatible_surface(manifest: Mapping[str, Any]) -> LogicalSparseSurface:
    """Load v1 logical sparse manifests and fail-closed legacy triplet metadata."""
    if manifest.get("format") == SURFACE_FORMAT:
        return validate_logical_sparse_surface(manifest)
    if manifest.get("format") != LEGACY_FORMAT:
        raise ValueError(f"unsupported surface format: {manifest.get('format')!r}")
    if _require(manifest, "version") != LEGACY_VERSION:
        raise ValueError(
            f"unsupported legacy surface version: {manifest.get('version')!r}"
        )

    n_obs = _nonnegative_int(_require(manifest, "n_obs"), "n_obs")
    n_vars = _nonnegative_int(_require(manifest, "n_vars"), "n_vars")
    nnz = _nonnegative_int(_require(manifest, "nnz"), "nnz")
    sparse_format = _require(manifest, "sparse_format")
    if sparse_format not in {"csr", "csc"}:
        raise ValueError("legacy sparse_format must be 'csr' or 'csc'")
    x_key = _nonempty_string(_require(manifest, "x_key"), "x_key")
    obs_key = _nonempty_string(_require(manifest, "obs_key"), "obs_key")
    var_key = _nonempty_string(_require(manifest, "var_key"), "var_key")
    chunks: tuple[ChunkRange, ...]
    if n_obs == 0:
        chunks = ()
    else:
        chunks = (
            ChunkRange(
                key=x_key,
                start=0,
                end=n_obs,
                nnz=nnz,
                shape=(n_obs, n_vars),
                dtype="legacy-unspecified",
                checksums={},
                obs={"key": obs_key, "provenance": {}},
            ),
        )
    return LogicalSparseSurface(
        format=LEGACY_FORMAT,
        version=LEGACY_VERSION,
        shape=(n_obs, n_vars),
        nnz=nnz,
        sparse_format=sparse_format,
        chunks=chunks,
        shared_var={"key": var_key},
    )


def balanced_row_chunks(n_obs: int, target_rows: int) -> tuple[tuple[int, int], ...]:
    """Return contiguous chunks whose row counts differ by at most one."""
    if n_obs < 0 or target_rows <= 0:
        raise ValueError("n_obs must be >= 0 and target_rows must be > 0")
    if n_obs == 0:
        return ()
    n_chunks = (n_obs + target_rows - 1) // target_rows
    base, remainder = divmod(n_obs, n_chunks)
    chunks: list[tuple[int, int]] = []
    start = 0
    for index in range(n_chunks):
        end = start + base + (1 if index < remainder else 0)
        chunks.append((start, end))
        start = end
    return tuple(chunks)


def adaptive_target_rows(
    *,
    n_obs: int,
    n_vars: int,
    nnz: int,
    max_rss_bytes: int,
    min_rows: int,
    max_rows: int,
    bytes_per_nnz: int = 12,
    materialization_factor: float = 3.0,
) -> int:
    """Estimate a safe row target from sparsity, width, and RSS budget."""
    if n_obs <= 0 or n_vars <= 0 or max_rss_bytes <= 0:
        raise ValueError("n_obs, n_vars, and max_rss_bytes must be positive")
    if min_rows <= 0 or max_rows < min_rows:
        raise ValueError("invalid row bounds")
    if nnz < 0 or bytes_per_nnz <= 0 or materialization_factor < 1:
        raise ValueError("invalid sparse memory parameters")

    mean_nnz_per_row = max(1.0, nnz / n_obs)
    bytes_per_row = (mean_nnz_per_row * bytes_per_nnz + 8) * materialization_factor
    width_floor = n_vars * 8
    available = max(1, max_rss_bytes - width_floor)
    estimate = int(available // bytes_per_row)
    return max(min_rows, min(max_rows, estimate, n_obs))
