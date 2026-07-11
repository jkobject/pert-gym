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


@dataclass(frozen=True)
class ChunkRange:
    """A half-open, row-major logical slice of a sparse matrix."""

    key: str
    start: int
    end: int
    nnz: int


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
    obs: Mapping[str, str]
    provenance: Mapping[str, Any]


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


def _as_chunk_ranges(chunks: Sequence[Mapping[str, Any]]) -> tuple[ChunkRange, ...]:
    result: list[ChunkRange] = []
    for index, chunk in enumerate(chunks):
        key = _require(chunk, "key")
        if not isinstance(key, str) or not key:
            raise ValueError(f"chunks[{index}].key must be a non-empty string")
        start = _nonnegative_int(_require(chunk, "start"), f"chunks[{index}].start")
        end = _nonnegative_int(_require(chunk, "end"), f"chunks[{index}].end")
        nnz = _nonnegative_int(_require(chunk, "nnz"), f"chunks[{index}].nnz")
        if end <= start:
            raise ValueError(f"chunks[{index}] must have end > start")
        result.append(ChunkRange(key=key, start=start, end=end, nnz=nnz))
    return tuple(result)


def validate_logical_sparse_surface(
    manifest: Mapping[str, Any],
) -> LogicalSparseSurface:
    """Validate v1 invariants and return a normalized logical sparse surface.

    Chunk intervals must be contiguous and exactly cover the declared row
    denominator.  This makes partial ingestion and a silently dropped tail
    impossible to represent as a valid logical dataset.
    """

    if _require(manifest, "format") != SURFACE_FORMAT:
        raise ValueError(
            f"unsupported logical surface format: {manifest.get('format')!r}"
        )
    if _require(manifest, "version") != SURFACE_VERSION:
        raise ValueError(
            f"unsupported logical surface version: {manifest.get('version')!r}"
        )

    shape_raw = _require(manifest, "shape")
    if (
        not isinstance(shape_raw, Sequence)
        or isinstance(shape_raw, (str, bytes))
        or len(shape_raw) != 2
    ):
        raise ValueError("shape must be a two-item sequence [n_obs, n_vars]")
    n_obs = _nonnegative_int(shape_raw[0], "shape[0]")
    n_vars = _nonnegative_int(shape_raw[1], "shape[1]")
    nnz = _nonnegative_int(_require(manifest, "nnz"), "nnz")
    sparse_format = _require(manifest, "sparse_format")
    if sparse_format not in {"csr", "csc"}:
        raise ValueError("sparse_format must be 'csr' or 'csc'")

    chunks_raw = _require(manifest, "chunks")
    if not isinstance(chunks_raw, Sequence) or isinstance(chunks_raw, (str, bytes)):
        raise ValueError("chunks must be a sequence")
    chunks = _as_chunk_ranges(chunks_raw)
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
    for key in ("key", "index_sha256", "frame_sha256"):
        value = _require(shared_var, key)
        if not isinstance(value, str) or not value:
            raise ValueError(f"shared_var.{key} must be a non-empty string")

    obs = _require(manifest, "obs")
    if not isinstance(obs, Mapping) or not isinstance(_require(obs, "key"), str):
        raise ValueError("obs.key must be a string")
    provenance = _require(manifest, "provenance")
    if not isinstance(provenance, Mapping):
        raise ValueError("provenance must be an object")

    return LogicalSparseSurface(
        format=SURFACE_FORMAT,
        version=SURFACE_VERSION,
        shape=(n_obs, n_vars),
        nnz=nnz,
        sparse_format=sparse_format,
        chunks=chunks,
        shared_var=shared_var,
        obs=obs,
        provenance=provenance,
    )


def load_compatible_surface(manifest: Mapping[str, Any]) -> LogicalSparseSurface:
    """Load v1 logical sparse manifests and normalize legacy triplet metadata.

    Legacy triplets are intentionally represented as one chunk. They cannot
    claim shared-var deduplication unless a migration writes the v1 hashes.
    """

    if manifest.get("format") == SURFACE_FORMAT:
        return validate_logical_sparse_surface(manifest)
    if manifest.get("format") != LEGACY_FORMAT:
        raise ValueError(f"unsupported surface format: {manifest.get('format')!r}")

    n_obs = _nonnegative_int(_require(manifest, "n_obs"), "n_obs")
    n_vars = _nonnegative_int(_require(manifest, "n_vars"), "n_vars")
    nnz = _nonnegative_int(_require(manifest, "nnz"), "nnz")
    if n_obs == 0:
        chunks: tuple[ChunkRange, ...] = ()
    else:
        chunks = (
            ChunkRange(key=_require(manifest, "x_key"), start=0, end=n_obs, nnz=nnz),
        )
    return LogicalSparseSurface(
        format=LEGACY_FORMAT,
        version=int(manifest.get("version", 1)),
        shape=(n_obs, n_vars),
        nnz=nnz,
        sparse_format=str(manifest.get("sparse_format", "csr")),
        chunks=chunks,
        shared_var={"key": _require(manifest, "var_key")},
        obs={"key": _require(manifest, "obs_key")},
        provenance=dict(manifest.get("provenance", {})),
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
    """Estimate a safe row target from sparsity, width, and RSS budget.

    The estimate charges data+index bytes per nonzero plus one indptr entry per
    row, then applies a materialization safety factor. `n_vars` participates as
    a lower bound on per-chunk index metadata, so ultra-wide matrices do not
    receive an unsafe, purely row-based target.
    """

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
