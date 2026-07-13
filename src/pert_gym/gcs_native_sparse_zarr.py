"""Bounded, requester-pays GCS-native sparse-Zarr candidate writer.

This module deliberately publishes no Lamin records and never mutates a source,
``main`` prefix, or public prefix.  It writes immutable chunks below one
caller-supplied temporary revision prefix, verifies them over the remote object
store, then writes an immutable manifest and a separate promotion marker last.
There is no rename-based atomicity assumption: consumers must resolve a
promotion marker and then its manifest.
"""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence, cast

import fsspec
import numpy as np
import pandas as pd
import zarr
from scipy import sparse

from pert_gym.logical_sparse_zarr import (
    DEFAULT_MAX_ROWS,
    DEFAULT_MIN_ROWS,
    CompressedMatrix,
    _assert_source_readback_parity,
    _materialize_rows,
    _matrix_components,
    _sha256_array,
    _source_nnz,
    _source_sparse_format,
    shared_var_identity,
)
from pert_gym.perturbai_sparse_parquet import (
    requester_pays_storage_options,
)
from pert_gym.sparse_zarr_contract import adaptive_target_rows, balanced_row_chunks

DEFAULT_CACHE_CAP_BYTES = 20 * 1024**3
DEFAULT_CACHE_SAFETY_RESERVE_BYTES = 20 * 1024**3
FORMAT = "pert-gym.gcs-native-logical-sparse-zarr/v1"
GIB = 1024**3
PRODUCTION_BLOCK_MIN_BYTES = 2 * GIB
PRODUCTION_BLOCK_TARGET_BYTES = 5 * GIB // 2
PRODUCTION_BLOCK_MAX_BYTES = 3 * GIB


class GCSNativeWriterError(RuntimeError):
    """A remote candidate cannot be safely continued or promoted."""


@dataclass(frozen=True)
class GCSNativeMetrics:
    peak_rss_bytes: int
    bytes_written: int
    bytes_read: int
    chunk_count: int
    cache_cap_bytes: int
    cache_bytes_after_cleanup: int


def requester_pays_gcs_filesystem(billing_project: str) -> Any:
    """Create the only permitted requester-pays GCS filesystem configuration."""
    return fsspec.filesystem(
        "gcs", version_aware=True, **requester_pays_storage_options(billing_project)
    )


def assert_cache_budget(
    cache_dir: Path,
    *,
    cache_cap_bytes: int = DEFAULT_CACHE_CAP_BYTES,
    safety_reserve_bytes: int = DEFAULT_CACHE_SAFETY_RESERVE_BYTES,
) -> None:
    """Fail before a cache request could consume the VM's safe free-disk headroom."""
    cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if cache_cap_bytes <= 0:
        raise ValueError("cache_cap_bytes must be positive")
    if safety_reserve_bytes < 0:
        raise ValueError("safety_reserve_bytes must be non-negative")
    free = shutil.disk_usage(cache_dir).free
    if cache_cap_bytes > free - safety_reserve_bytes:
        raise GCSNativeWriterError(
            "requested local cache cap exceeds safe headroom: "
            f"cap={cache_cap_bytes}, free={free}, reserve={safety_reserve_bytes}"
        )


def _cache_bytes(cache_dir: Path) -> int:
    if not cache_dir.exists():
        return 0
    return sum(path.stat().st_size for path in cache_dir.rglob("*") if path.is_file())


def cleanup_cache(cache_dir: Path, *, cache_cap_bytes: int) -> int:
    """Remove only this run's cache directory and prove no cache payload remains."""
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    remaining = _cache_bytes(cache_dir)
    if remaining or remaining > cache_cap_bytes:
        raise GCSNativeWriterError("cache cleanup verification failed")
    return remaining


def _path(prefix: str, *parts: str) -> str:
    prefix = prefix.strip("/")
    if not prefix or ".." in Path(prefix).parts:
        raise ValueError("GCS prefix must be a non-empty relative path")
    return "/".join((prefix, *parts))


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_exclusive(fs: Any, key: str, data: bytes) -> dict[str, str | int]:
    """Write a single immutable object; never replace a same-name object."""
    if fs.exists(key):
        raise GCSNativeWriterError(
            f"refusing overwrite of immutable remote object: {key}"
        )
    try:
        with fs.open(key, "xb") as handle:
            handle.write(data)
    except (FileExistsError, OSError) as error:
        raise GCSNativeWriterError(
            f"remote immutable write failed for {key}"
        ) from error
    info = fs.info(key)
    if int(info.get("size", -1)) != len(data):
        raise GCSNativeWriterError(f"remote object size mismatch for {key}")
    return {
        "key": key,
        "generation": str(info.get("generation", "")),
        "size": len(data),
    }


def _read_bytes(fs: Any, key: str) -> bytes:
    with fs.open(key, "rb") as handle:
        return handle.read()


def _write_remote_matrix(
    fs: Any, key: str, matrix: CompressedMatrix, sparse_format: str
) -> int:
    """Write one Zarr group directly to object storage without an archive/spool file."""
    if fs.exists(key):
        raise GCSNativeWriterError(f"refusing overwrite of immutable chunk: {key}")
    store = zarr.storage.FSStore(key, fs=fs, mode="w", check=False)
    group = zarr.open_group(store=store, mode="w")
    group.attrs.update(
        {
            "sparse_format": sparse_format,
            "shape": list(matrix.shape),
            "nnz": int(matrix.nnz),
        }
    )
    for name, values in zip(
        ("data", "indices", "indptr"), _matrix_components(matrix), strict=True
    ):
        group.create_dataset(
            name, data=values, chunks=(max(1, min(len(values), 65_536)),)
        )
    store.close()
    return sum(int(item.get("size", 0)) for item in fs.find(key, detail=True).values())


def _read_remote_matrix(fs: Any, key: str, sparse_format: str) -> CompressedMatrix:
    store = zarr.storage.FSStore(key, fs=fs, mode="r", check=False)
    group = zarr.open_group(store=store, mode="r")
    constructor = sparse.csr_matrix if sparse_format == "csr" else sparse.csc_matrix
    result = constructor(
        (
            np.asarray(group["data"]),
            np.asarray(group["indices"]),
            np.asarray(group["indptr"]),
        ),
        shape=tuple(group.attrs["shape"]),
    )
    store.close()
    return result


def _write_parquet(fs: Any, key: str, frame: pd.DataFrame) -> dict[str, str | int]:
    buffer = io.BytesIO()
    frame.to_parquet(buffer)
    return _write_exclusive(fs, key, buffer.getvalue())


def _read_parquet(fs: Any, key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(_read_bytes(fs, key)))


def plan_production_blocks(
    compressed_chunk_bytes: Sequence[int],
    *,
    min_bytes: int = PRODUCTION_BLOCK_MIN_BYTES,
    target_bytes: int = PRODUCTION_BLOCK_TARGET_BYTES,
    max_bytes: int = PRODUCTION_BLOCK_MAX_BYTES,
) -> tuple[tuple[int, int], ...]:
    """Group measured compressed physical chunks into bounded logical blocks."""
    if min_bytes <= 0 or not min_bytes <= target_bytes <= max_bytes:
        raise ValueError("block policy must satisfy 0 < min <= target <= max")
    sizes = tuple(compressed_chunk_bytes)
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in sizes
    ):
        raise ValueError("compressed chunk sizes must be non-negative integers")
    if not sizes:
        return ()
    if any(value > max_bytes for value in sizes):
        raise ValueError("one measured chunk exceeds the production block ceiling")
    if sum(sizes) <= max_bytes:
        return ((0, len(sizes)),)

    suffix_bytes = [0] * (len(sizes) + 1)
    for index in range(len(sizes) - 1, -1, -1):
        suffix_bytes[index] = suffix_bytes[index + 1] + sizes[index]
    result: list[tuple[int, int]] = []
    start = 0
    while start < len(sizes):
        end = start
        size = 0
        while end < len(sizes) and size + sizes[end] <= target_bytes:
            size += sizes[end]
            end += 1
        if end == start:
            size = sizes[end]
            end += 1
        while size < min_bytes and end < len(sizes) and size + sizes[end] <= max_bytes:
            size += sizes[end]
            end += 1
        remaining = suffix_bytes[end]
        if remaining and remaining < min_bytes and size + remaining <= max_bytes:
            end = len(sizes)
            size += remaining
        if end < len(sizes) and size < min_bytes:
            raise ValueError(
                "measured chunk sizes cannot satisfy production block policy"
            )
        result.append((start, end))
        start = end
    return tuple(result)


def _checkpoint_identity(
    *,
    logical_key: str,
    revision: str,
    source_uri: str,
    source_generation: str,
    source_row_start: int,
    source_row_end: int,
    shape: tuple[int, int],
    sparse_format: str,
    var: pd.DataFrame,
    schema_fingerprint: str,
) -> dict[str, object]:
    identity = shared_var_identity(var, schema_fingerprint=schema_fingerprint)
    return {
        "logical_key": logical_key,
        "revision": revision,
        "source_uri": source_uri,
        "source_generation": source_generation,
        "source_row_start": source_row_start,
        "source_row_end": source_row_end,
        "shape": list(shape),
        "sparse_format": sparse_format,
        "var_index_sha256": identity.index_sha256,
        "var_frame_sha256": identity.frame_sha256,
        "schema_fingerprint": schema_fingerprint,
    }


def _load_plan(
    fs: Any,
    key: str,
    identity: Mapping[str, object],
    chunks: tuple[tuple[int, int], ...],
    production_block_policy: Mapping[str, int],
) -> dict[str, object]:
    if not fs.exists(key):
        plan: dict[str, object] = {
            "format": FORMAT,
            "identity": dict(identity),
            "planned_chunks": [list(x) for x in chunks],
            "production_block_policy": dict(production_block_policy),
        }
        _write_exclusive(fs, key, _json_bytes(plan))
        return plan
    plan = json.loads(_read_bytes(fs, key))
    if (
        plan.get("identity") != dict(identity)
        or plan.get("planned_chunks") != [list(x) for x in chunks]
        or plan.get("production_block_policy") != dict(production_block_policy)
    ):
        raise GCSNativeWriterError(
            "remote plan identity mismatch; refusing resume drift"
        )
    return plan


def write_gcs_native_sparse_revision(
    *,
    fs: Any,
    staging_prefix: str,
    logical_key: str,
    revision: str,
    matrix: object,
    obs: pd.DataFrame,
    var: pd.DataFrame,
    source_uri: str,
    source_generation: str,
    source_row_start: int | None,
    source_row_end: int | None,
    schema_fingerprint: str,
    ingestion_run_id: str,
    cache_dir: Path,
    cache_cap_bytes: int = DEFAULT_CACHE_CAP_BYTES,
    max_rss_bytes: int = 4 * 1024**3,
    cache_safety_reserve_bytes: int = DEFAULT_CACHE_SAFETY_RESERVE_BYTES,
    min_rows: int = DEFAULT_MIN_ROWS,
    max_rows: int = DEFAULT_MAX_ROWS,
    stop_after_chunks: int | None = None,
    production_block_min_bytes: int = PRODUCTION_BLOCK_MIN_BYTES,
    production_block_target_bytes: int = PRODUCTION_BLOCK_TARGET_BYTES,
    production_block_max_bytes: int = PRODUCTION_BLOCK_MAX_BYTES,
) -> tuple[dict[str, object], GCSNativeMetrics]:
    """Write bounded CSR/CSC tranches directly to a versioned temporary GCS prefix.

    The caller must provide a backed/range-readable matrix (for example an HDF5
    backed sparse object opened from GCS).  Only ``matrix[start:end]`` is ever
    materialized. ``stop_after_chunks`` is test-only fault injection.
    """
    assert_cache_budget(
        cache_dir,
        cache_cap_bytes=cache_cap_bytes,
        safety_reserve_bytes=cache_safety_reserve_bytes,
    )
    if not source_uri.startswith("gs://") or not source_generation:
        raise ValueError(
            "source_uri must be gs:// and source_generation must be non-empty"
        )
    if (
        not isinstance(source_row_start, int)
        or isinstance(source_row_start, bool)
        or not isinstance(source_row_end, int)
        or isinstance(source_row_end, bool)
        or source_row_start < 0
        or source_row_end <= source_row_start
    ):
        raise ValueError(
            "source row bounds must be non-negative, explicit, and non-empty"
        )
    if not ingestion_run_id:
        raise ValueError("ingestion_run_id must be non-empty")
    shape = getattr(matrix, "shape", None)
    if not isinstance(shape, tuple) or len(shape) != 2 or shape != (len(obs), len(var)):
        raise ValueError("matrix shape must match obs and var")
    if source_row_end - source_row_start != shape[0]:
        raise ValueError("source row bounds must exactly match matrix and obs rows")
    sparse_format = _source_sparse_format(matrix)
    nnz = _source_nnz(matrix)
    target_rows = adaptive_target_rows(
        n_obs=shape[0],
        n_vars=shape[1],
        nnz=nnz,
        max_rss_bytes=max_rss_bytes,
        min_rows=min_rows,
        max_rows=max_rows,
    )
    chunks = balanced_row_chunks(shape[0], target_rows) if shape[0] else ()
    candidate_prefix = _path(
        staging_prefix, logical_key, "temporary-revisions", revision
    )
    plan_key = _path(candidate_prefix, "plan.json")
    identity = _checkpoint_identity(
        logical_key=logical_key,
        revision=revision,
        source_uri=source_uri,
        source_generation=source_generation,
        source_row_start=source_row_start,
        source_row_end=source_row_end,
        shape=shape,
        sparse_format=sparse_format,
        var=var,
        schema_fingerprint=schema_fingerprint,
    )
    production_block_policy = {
        "minimum_bytes": production_block_min_bytes,
        "target_bytes": production_block_target_bytes,
        "maximum_bytes": production_block_max_bytes,
    }
    _load_plan(fs, plan_key, identity, chunks, production_block_policy)
    if fs.exists(_path(candidate_prefix, "manifest.json")):
        raise GCSNativeWriterError(
            "remote candidate is already completed; choose a new revision"
        )
    completed = {
        index
        for index in range(len(chunks))
        if fs.exists(
            _path(candidate_prefix, "chunk-records", f"chunk_{index:06d}.json")
        )
    }
    records: list[dict[str, object]] = []
    bytes_written = 0
    bytes_read = 0
    peak_rss = 0
    for index, (start, end) in enumerate(chunks):
        matrix_key = _path(candidate_prefix, "chunks", f"chunk_{index:06d}.zarr")
        obs_key = _path(candidate_prefix, "obs", f"chunk_{index:06d}.parquet")
        record_key = _path(candidate_prefix, "chunk-records", f"chunk_{index:06d}.json")
        source_chunk = _materialize_rows(matrix, start, end, sparse_format)
        source_obs = obs.iloc[start:end]
        bytes_read += sum(values.nbytes for values in _matrix_components(source_chunk))
        peak_rss = max(
            peak_rss, sum(values.nbytes for values in _matrix_components(source_chunk))
        )
        if index not in completed:
            if fs.exists(record_key) or fs.exists(matrix_key) or fs.exists(obs_key):
                raise GCSNativeWriterError(
                    f"orphan or partial remote chunk {index}; refusing overwrite"
                )
            matrix_compressed_bytes = _write_remote_matrix(
                fs, matrix_key, source_chunk, sparse_format
            )
            bytes_written += matrix_compressed_bytes
            obs_object = _write_parquet(fs, obs_key, source_obs)
            bytes_written += int(obs_object["size"])
            remote = _read_remote_matrix(fs, matrix_key, sparse_format)
            remote_obs = _read_parquet(fs, obs_key)
            _assert_source_readback_parity(source_chunk, remote, source_obs, remote_obs)
            checksums = {
                name: _sha256_array(values)
                for name, values in zip(
                    ("data_sha256", "indices_sha256", "indptr_sha256"),
                    _matrix_components(remote),
                    strict=True,
                )
            }
            record = {
                "index": index,
                "start": source_row_start + start,
                "end": source_row_start + end,
                "matrix_key": matrix_key,
                "obs_key": obs_key,
                "obs_generation": obs_object["generation"],
                "compressed_bytes": matrix_compressed_bytes,
                "obs_compressed_bytes": int(obs_object["size"]),
                "checksums": checksums,
                "source_generation": source_generation,
            }
            record_object = _write_exclusive(fs, record_key, _json_bytes(record))
            bytes_written += int(record_object["size"])
            completed.add(index)
            if stop_after_chunks is not None and len(completed) >= stop_after_chunks:
                raise GCSNativeWriterError(
                    f"intentional interruption after remote chunk {index}"
                )
        record = json.loads(_read_bytes(fs, record_key))
        if record.get("source_generation") != source_generation:
            raise GCSNativeWriterError(f"chunk source generation mismatch: {index}")
        measured_bytes = record.get("compressed_bytes")
        if (
            not isinstance(measured_bytes, int)
            or isinstance(measured_bytes, bool)
            or measured_bytes < 0
        ):
            raise GCSNativeWriterError(
                f"chunk compressed byte measurement missing: {index}"
            )
        records.append(record)
    var_key = _path(candidate_prefix, "var.parquet")
    if fs.exists(var_key):
        raise GCSNativeWriterError(
            "orphan shared var already exists under candidate prefix"
        )
    var_object = _write_parquet(fs, var_key, var)
    remote_var = _read_parquet(fs, var_key)
    if shared_var_identity(
        remote_var, schema_fingerprint=schema_fingerprint
    ) != shared_var_identity(var, schema_fingerprint=schema_fingerprint):
        raise GCSNativeWriterError("remote shared var readback identity mismatch")
    var_identity = shared_var_identity(var, schema_fingerprint=schema_fingerprint)
    var_manifest: dict[str, object] = {
        "key": var_key,
        "generation": var_object["generation"],
        "index_sha256": var_identity.index_sha256,
        "frame_sha256": var_identity.frame_sha256,
        "schema_fingerprint": var_identity.schema_fingerprint,
    }
    block_ranges = plan_production_blocks(
        [cast(int, record["compressed_bytes"]) for record in records],
        min_bytes=production_block_min_bytes,
        target_bytes=production_block_target_bytes,
        max_bytes=production_block_max_bytes,
    )
    production_blocks = [
        {
            "index": block_index,
            "start": records[first]["start"],
            "end": records[last - 1]["end"],
            "chunk_indexes": list(range(first, last)),
            "compressed_bytes": sum(
                cast(int, records[index]["compressed_bytes"])
                for index in range(first, last)
            ),
            "var": dict(var_manifest),
        }
        for block_index, (first, last) in enumerate(block_ranges)
    ]
    manifest: dict[str, object] = {
        "format": FORMAT,
        "logical_key": logical_key,
        "revision": revision,
        "candidate_prefix": candidate_prefix,
        "source": {
            "uri": source_uri,
            "generation": source_generation,
            "row_start": source_row_start,
            "row_end": source_row_end,
        },
        "shape": list(shape),
        "nnz": nnz,
        "sparse_format": sparse_format,
        "ingestion_run_id": ingestion_run_id,
        "chunks": records,
        "production_block_policy": production_block_policy,
        "blocks": production_blocks,
        "var": var_manifest,
    }
    manifest_key = _path(candidate_prefix, "manifest.json")
    manifest_object = _write_exclusive(fs, manifest_key, _json_bytes(manifest))
    metrics = GCSNativeMetrics(
        peak_rss_bytes=peak_rss,
        bytes_written=bytes_written
        + int(var_object["size"])
        + int(manifest_object["size"]),
        bytes_read=bytes_read,
        chunk_count=len(records),
        cache_cap_bytes=cache_cap_bytes,
        cache_bytes_after_cleanup=cleanup_cache(
            cache_dir, cache_cap_bytes=cache_cap_bytes
        ),
    )
    return manifest, metrics


def promote_gcs_native_revision(
    *,
    fs: Any,
    staging_prefix: str,
    logical_key: str,
    revision: str,
    manifest: Mapping[str, object],
) -> dict[str, object]:
    """Publish the one immutable promotion marker after an explicit remote readback."""
    candidate_prefix = _path(
        staging_prefix, logical_key, "temporary-revisions", revision
    )
    manifest_key = _path(candidate_prefix, "manifest.json")
    if _sha256_bytes(_read_bytes(fs, manifest_key)) != _sha256_bytes(
        _json_bytes(manifest)
    ):
        raise GCSNativeWriterError("manifest readback mismatch; refusing promotion")
    marker = {
        "format": FORMAT,
        "revision": revision,
        "manifest_key": manifest_key,
        "manifest_sha256": _sha256_bytes(_read_bytes(fs, manifest_key)),
        "promoted_at": time.time(),
    }
    marker_key = _path(staging_prefix, logical_key, "promotions", f"{revision}.json")
    _write_exclusive(fs, marker_key, _json_bytes(marker))
    return {**marker, "promotion_key": marker_key}


def register_gcs_prefix_with_lamin(*, ln: Any, prefix_uri: str) -> Any:
    """Fail closed unless this Lamin client explicitly supports prefix references.

    Generic ``ln.Artifact(path)`` registers a file and is not a safe substitute:
    packaging a prefix into a local tar violates the no-local-duplicate contract.
    """
    register = getattr(ln, "register_gcs_prefix", None)
    if not callable(register):
        raise GCSNativeWriterError(
            "Lamin API limitation: this client exposes no register_gcs_prefix(prefix_uri) "
            "API; refusing to create a local tar duplicate for a GCS prefix"
        )
    return register(prefix_uri)
