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
import math
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

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
    _frame_sha256,
    _index_sha256,
    _materialize_rows,
    _matrix_components,
    _peak_rss_bytes,
    _sha256_array,
    _source_nnz,
    _source_sparse_format,
    shared_var_identity,
)
from pert_gym.perturbai_sparse_parquet import (
    requester_pays_storage_options,
)
from pert_gym.sparse_zarr_contract import adaptive_target_rows

DEFAULT_CACHE_CAP_BYTES = 20 * 1024**3
DEFAULT_CACHE_SAFETY_RESERVE_BYTES = 20 * 1024**3
DEFAULT_MIN_BLOCK_BYTES = 2 * 1024**3
DEFAULT_MAX_BLOCK_BYTES = 3 * 1024**3
FORMAT = "pert-gym.gcs-native-logical-sparse-zarr/v1"


class GCSNativeWriterError(RuntimeError):
    """A remote candidate cannot be safely continued or promoted."""


class BlockPlanConflict(GCSNativeWriterError):
    """Measured byte and RSS evidence cannot satisfy the production contract."""

    def __init__(self, message: str, evidence: Mapping[str, object]) -> None:
        super().__init__(message)
        self.evidence = dict(evidence)


@dataclass(frozen=True)
class GCSNativeMetrics:
    peak_rss_bytes: int
    bytes_written: int
    bytes_read: int
    chunk_count: int
    cache_cap_bytes: int
    cache_bytes_after_cleanup: int


def _validate_block_thresholds(
    *, min_block_bytes: int, max_block_bytes: int, max_rss_bytes: int
) -> None:
    if min_block_bytes <= 0:
        raise ValueError("min_block_bytes must be positive")
    if max_block_bytes < min_block_bytes:
        raise ValueError("max_block_bytes must be at least min_block_bytes")
    if max_rss_bytes <= 0:
        raise ValueError("max_rss_bytes must be positive")


def validate_measured_block(
    *,
    start: int,
    end: int,
    n_obs: int,
    measured_bytes: int,
    measured_peak_rss_bytes: int,
    min_block_bytes: int = DEFAULT_MIN_BLOCK_BYTES,
    max_block_bytes: int = DEFAULT_MAX_BLOCK_BYTES,
    max_rss_bytes: int,
    explicit_exception: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Validate one completed block from remote bytes and OS high-water RSS."""
    _validate_block_thresholds(
        min_block_bytes=min_block_bytes,
        max_block_bytes=max_block_bytes,
        max_rss_bytes=max_rss_bytes,
    )
    if start < 0 or end <= start or end > n_obs:
        raise ValueError("measured block interval is invalid")
    violations: list[str] = []
    if measured_bytes < min_block_bytes:
        violations.append("bytes_below_minimum")
    if measured_bytes > max_block_bytes:
        violations.append("bytes_above_maximum")
    if measured_peak_rss_bytes > max_rss_bytes:
        violations.append("rss_above_maximum")

    exception: dict[str, object] | None = None
    byte_violations = {
        "bytes_below_minimum",
        "bytes_above_maximum",
    }.intersection(violations)
    if byte_violations and explicit_exception:
        exception = {"kind": "explicit", **dict(explicit_exception)}
        violations = [item for item in violations if item not in byte_violations]
    elif violations == ["bytes_below_minimum"] and start == 0 and end == n_obs:
        exception = {"kind": "whole_dataset_below_minimum"}
        violations.clear()
    elif violations == ["bytes_below_minimum"] and start > 0 and end == n_obs:
        exception = {"kind": "final_tail_below_minimum"}
        violations.clear()

    evidence: dict[str, object] = {
        "interval": [start, end],
        "measured_bytes": measured_bytes,
        "measured_peak_rss_bytes": measured_peak_rss_bytes,
        "thresholds": {
            "min_block_bytes": min_block_bytes,
            "max_block_bytes": max_block_bytes,
            "max_rss_bytes": max_rss_bytes,
        },
        "violations": violations,
    }
    if exception is not None:
        evidence["exception"] = exception
    if violations:
        raise BlockPlanConflict("measured block contract violation", evidence)
    return {"status": "accepted", **evidence}


def calibrated_block_plan(
    *,
    identity: Mapping[str, object],
    n_obs: int,
    probe_rows: int,
    measured_bytes: int,
    measured_peak_rss_bytes: int,
    min_block_bytes: int = DEFAULT_MIN_BLOCK_BYTES,
    max_block_bytes: int = DEFAULT_MAX_BLOCK_BYTES,
    max_rss_bytes: int,
    max_rows: int,
    calibration_objects: list[Mapping[str, object]],
    explicit_exception: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Choose immutable intervals only after an identity-bound measured probe."""
    _validate_block_thresholds(
        min_block_bytes=min_block_bytes,
        max_block_bytes=max_block_bytes,
        max_rss_bytes=max_rss_bytes,
    )
    if n_obs <= 0 or not 0 < probe_rows <= n_obs:
        raise ValueError("calibration probe rows must be within the dataset")
    if measured_bytes <= 0 or measured_peak_rss_bytes <= 0 or max_rows <= 0:
        raise ValueError("calibration measurements and max_rows must be positive")

    rows_required_for_min = math.ceil(probe_rows * min_block_bytes / measured_bytes)
    rows_allowed_by_bytes = probe_rows * max_block_bytes // measured_bytes
    rows_allowed_by_rss = max(1, probe_rows * max_rss_bytes // measured_peak_rss_bytes)
    estimated_dataset_bytes = math.ceil(measured_bytes * n_obs / probe_rows)
    small_dataset = estimated_dataset_bytes < min_block_bytes
    if measured_peak_rss_bytes > max_rss_bytes:
        evidence = {
            "kind": "calibration_rss_conflict",
            "measured_peak_rss_bytes": measured_peak_rss_bytes,
            "max_rss_bytes": max_rss_bytes,
            "thresholds": {
                "min_block_bytes": min_block_bytes,
                "max_block_bytes": max_block_bytes,
                "max_rss_bytes": max_rss_bytes,
            },
        }
        raise BlockPlanConflict("calibration exceeded hard RSS ceiling", evidence)
    whole_small_hard_limit_conflict = small_dataset and n_obs > min(
        max_rows, rows_allowed_by_rss
    )
    byte_target_conflict = (
        not small_dataset
        and not explicit_exception
        and rows_required_for_min
        > min(n_obs, max_rows, rows_allowed_by_bytes, rows_allowed_by_rss)
    )
    if whole_small_hard_limit_conflict or byte_target_conflict:
        evidence = {
            "kind": "byte_rss_conflict",
            "whole_dataset_below_minimum": small_dataset,
            "rows_required_for_min_bytes": rows_required_for_min,
            "rows_allowed_by_rss": rows_allowed_by_rss,
            "rows_allowed_by_max_bytes": rows_allowed_by_bytes,
            "max_rows": max_rows,
            "measured_bytes": measured_bytes,
            "measured_peak_rss_bytes": measured_peak_rss_bytes,
            "thresholds": {
                "min_block_bytes": min_block_bytes,
                "max_block_bytes": max_block_bytes,
                "max_rss_bytes": max_rss_bytes,
            },
        }
        raise BlockPlanConflict(
            "calibration cannot satisfy byte target and RSS ceiling", evidence
        )

    exception: dict[str, object] | None = None
    if small_dataset:
        chosen_rows = n_obs
        exception = (
            {
                "kind": "explicit",
                "automatic_basis": "whole_dataset_below_minimum",
                **dict(explicit_exception),
            }
            if explicit_exception
            else {"kind": "whole_dataset_below_minimum"}
        )
    else:
        target_bytes = (min_block_bytes + max_block_bytes) // 2
        chosen_rows = max(1, math.ceil(probe_rows * target_bytes / measured_bytes))
        if explicit_exception:
            chosen_rows = min(n_obs, max_rows, rows_allowed_by_rss, chosen_rows)
            exception = {"kind": "explicit", **dict(explicit_exception)}
        else:
            chosen_rows = min(
                n_obs,
                max_rows,
                rows_allowed_by_bytes,
                rows_allowed_by_rss,
                chosen_rows,
            )

    chunks = [
        [start, min(n_obs, start + chosen_rows)]
        for start in range(0, n_obs, chosen_rows)
    ]
    plan: dict[str, object] = {
        "format": f"{FORMAT}.block-plan/v2",
        "identity": dict(identity),
        "calibration": {
            "identity": dict(identity),
            "probe_interval": [0, probe_rows],
            "measured_bytes": measured_bytes,
            "measured_peak_rss_bytes": measured_peak_rss_bytes,
            "objects": [dict(item) for item in calibration_objects],
        },
        "thresholds": {
            "min_block_bytes": min_block_bytes,
            "max_block_bytes": max_block_bytes,
            "max_rss_bytes": max_rss_bytes,
        },
        "chosen_rows": chosen_rows,
        "max_rows": max_rows,
        "planned_chunks": chunks,
    }
    if exception is not None:
        plan["exception"] = exception
    return plan


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


def _required_int(value: Mapping[str, object], key: str) -> int:
    result = value.get(key)
    if not isinstance(result, int) or isinstance(result, bool):
        raise GCSNativeWriterError(f"remote evidence field {key} must be an integer")
    return result


def _required_mapping_list(
    value: Mapping[str, object], key: str
) -> list[Mapping[str, object]]:
    result = value.get(key)
    if not isinstance(result, list):
        raise GCSNativeWriterError(
            f"remote evidence field {key} must be a list of objects"
        )
    mappings: list[Mapping[str, object]] = []
    for item in result:
        if not isinstance(item, dict):
            raise GCSNativeWriterError(
                f"remote evidence field {key} must be a list of objects"
            )
        normalized: dict[str, object] = {}
        for item_key, item_value in item.items():
            if not isinstance(item_key, str):
                raise GCSNativeWriterError(
                    f"remote evidence field {key} object keys must be strings"
                )
            normalized[item_key] = item_value
        mappings.append(normalized)
    return mappings


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


def _checkpoint_identity(
    *,
    logical_key: str,
    revision: str,
    source_uri: str,
    source_generation: str,
    source_checksum: str,
    source_row_start: int,
    source_row_end: int,
    shape: tuple[int, int],
    sparse_format: str,
    var: pd.DataFrame,
    schema_fingerprint: str,
    candidate_metadata: Mapping[str, object] | None,
) -> dict[str, object]:
    identity = shared_var_identity(var, schema_fingerprint=schema_fingerprint)
    return {
        "logical_key": logical_key,
        "revision": revision,
        "source_uri": source_uri,
        "source_generation": source_generation,
        "source_checksum": source_checksum,
        "source_row_start": source_row_start,
        "source_row_end": source_row_end,
        "shape": list(shape),
        "sparse_format": sparse_format,
        "var_index_sha256": identity.index_sha256,
        "var_frame_sha256": identity.frame_sha256,
        "schema_fingerprint": schema_fingerprint,
        "candidate_metadata": dict(candidate_metadata or {}),
    }


def _load_plan(
    fs: Any,
    key: str,
    expected: Mapping[str, object],
) -> dict[str, object]:
    if not fs.exists(key):
        plan = dict(expected)
        _write_exclusive(fs, key, _json_bytes(plan))
        return plan
    plan = json.loads(_read_bytes(fs, key))
    if plan != dict(expected):
        raise GCSNativeWriterError(
            "remote plan identity mismatch; immutable plan refuses resume drift"
        )
    return plan


def _remote_object_evidence(fs: Any, key: str) -> list[dict[str, object]]:
    details = fs.find(key, detail=True)
    items = (
        details.items()
        if isinstance(details, dict)
        else ((item, fs.info(item)) for item in details)
    )
    return [
        {
            "key": object_key,
            "generation": str(info.get("generation", "")),
            "size": int(info.get("size", 0)),
        }
        for object_key, info in sorted(items)
    ]


def _calibration_pair(fs: Any, candidate_prefix: str) -> tuple[str, str, int]:
    attempt = 0
    while True:
        prefix = _path(candidate_prefix, "calibration", f"attempt_{attempt:06d}")
        matrix_key = _path(prefix, "matrix.zarr")
        obs_key = _path(prefix, "obs.parquet")
        if fs.exists(matrix_key) == fs.exists(obs_key):
            return matrix_key, obs_key, attempt
        attempt += 1


def _load_or_measure_calibration(
    *,
    fs: Any,
    candidate_prefix: str,
    matrix: object,
    obs: pd.DataFrame,
    sparse_format: str,
    probe_rows: int,
    identity: Mapping[str, object],
    peak_rss_reader: Callable[[], int],
) -> dict[str, object]:
    record_key = _path(candidate_prefix, "calibration", "probe.json")
    if fs.exists(record_key):
        record = json.loads(_read_bytes(fs, record_key))
        if record.get("identity") != dict(identity) or record.get("probe_interval") != [
            0,
            probe_rows,
        ]:
            raise GCSNativeWriterError(
                "calibration identity mismatch; immutable revision refuses resume drift"
            )
        return record

    source_chunk = _materialize_rows(matrix, 0, probe_rows, sparse_format)
    source_obs = obs.iloc[:probe_rows]
    matrix_key, obs_key, attempt = _calibration_pair(fs, candidate_prefix)
    if not fs.exists(matrix_key):
        _write_remote_matrix(fs, matrix_key, source_chunk, sparse_format)
        _write_parquet(fs, obs_key, source_obs)
    remote = _read_remote_matrix(fs, matrix_key, sparse_format)
    remote_obs = _read_parquet(fs, obs_key)
    _assert_source_readback_parity(source_chunk, remote, source_obs, remote_obs)
    objects = _remote_object_evidence(fs, matrix_key) + [
        {
            "key": obs_key,
            "generation": str(fs.info(obs_key).get("generation", "")),
            "size": int(fs.info(obs_key).get("size", 0)),
        }
    ]
    record = {
        "format": f"{FORMAT}.calibration/v1",
        "identity": dict(identity),
        "probe_interval": [0, probe_rows],
        "attempt": attempt,
        "objects": objects,
        "measured_bytes": sum(_required_int(item, "size") for item in objects),
        "measured_peak_rss_bytes": peak_rss_reader(),
    }
    _write_exclusive(fs, record_key, _json_bytes(record))
    return record


def _resumable_chunk_keys(
    fs: Any, candidate_prefix: str, index: int
) -> tuple[str, str, int | None]:
    """Choose append-only keys after an interrupted pre-record chunk write.

    A VM interruption can leave the canonical matrix or obs key without the
    immutable chunk record. Those objects are never deleted or overwritten. A
    complete pair is returned for parity validation and adoption; a partial pair
    is retained and the next unused recovery attempt is selected.
    """
    matrix_key = _path(candidate_prefix, "chunks", f"chunk_{index:06d}.zarr")
    obs_key = _path(candidate_prefix, "obs", f"chunk_{index:06d}.parquet")
    if fs.exists(matrix_key) == fs.exists(obs_key):
        return matrix_key, obs_key, None

    attempt = 0
    while True:
        attempt_prefix = _path(
            candidate_prefix,
            "recovery",
            f"chunk_{index:06d}",
            f"attempt_{attempt:06d}",
        )
        recovery_matrix_key = _path(attempt_prefix, "matrix.zarr")
        recovery_obs_key = _path(attempt_prefix, "obs.parquet")
        if fs.exists(recovery_matrix_key) == fs.exists(recovery_obs_key):
            return recovery_matrix_key, recovery_obs_key, attempt
        attempt += 1


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
    source_checksum: str,
    source_row_start: int | None,
    source_row_end: int | None,
    schema_fingerprint: str,
    ingestion_run_id: str,
    cache_dir: Path,
    candidate_metadata: Mapping[str, object] | None = None,
    cache_cap_bytes: int = DEFAULT_CACHE_CAP_BYTES,
    max_rss_bytes: int = 4 * 1024**3,
    min_block_bytes: int = DEFAULT_MIN_BLOCK_BYTES,
    max_block_bytes: int = DEFAULT_MAX_BLOCK_BYTES,
    block_size_exception: Mapping[str, object] | None = None,
    cache_safety_reserve_bytes: int = DEFAULT_CACHE_SAFETY_RESERVE_BYTES,
    min_rows: int = DEFAULT_MIN_ROWS,
    max_rows: int = DEFAULT_MAX_ROWS,
    peak_rss_reader: Callable[[], int] = _peak_rss_bytes,
    stop_after_chunks: int | None = None,
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
    digest = source_checksum.removeprefix("sha256-file-bytes/v1:")
    if len(digest) != 64 or any(
        char not in "0123456789abcdefABCDEF" for char in digest
    ):
        raise ValueError(
            "source_checksum must use sha256-file-bytes/v1:<64-hex-digest>"
        )
    try:
        json.dumps(candidate_metadata or {}, sort_keys=True, allow_nan=False)
        json.dumps(block_size_exception or {}, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise ValueError(
            "candidate metadata and exceptions must be finite JSON data"
        ) from error
    _validate_block_thresholds(
        min_block_bytes=min_block_bytes,
        max_block_bytes=max_block_bytes,
        max_rss_bytes=max_rss_bytes,
    )
    shape = getattr(matrix, "shape", None)
    if not isinstance(shape, tuple) or len(shape) != 2 or shape != (len(obs), len(var)):
        raise ValueError("matrix shape must match obs and var")
    if source_row_end - source_row_start != shape[0]:
        raise ValueError("source row bounds must exactly match matrix and obs rows")
    sparse_format = _source_sparse_format(matrix)
    nnz = _source_nnz(matrix)
    probe_rows = adaptive_target_rows(
        n_obs=shape[0],
        n_vars=shape[1],
        nnz=nnz,
        max_rss_bytes=max_rss_bytes,
        min_rows=min_rows,
        max_rows=max_rows,
    )
    candidate_prefix = _path(
        staging_prefix, logical_key, "temporary-revisions", revision
    )
    plan_key = _path(candidate_prefix, "plan.json")
    identity = _checkpoint_identity(
        logical_key=logical_key,
        revision=revision,
        source_uri=source_uri,
        source_generation=source_generation,
        source_checksum=source_checksum,
        source_row_start=source_row_start,
        source_row_end=source_row_end,
        shape=shape,
        sparse_format=sparse_format,
        var=var,
        schema_fingerprint=schema_fingerprint,
        candidate_metadata=candidate_metadata,
    )
    manifest_key = _path(candidate_prefix, "manifest.json")
    failure_key = _path(candidate_prefix, "failure.json")
    conflict_key = _path(candidate_prefix, "calibration", "conflict.json")
    if fs.exists(failure_key) or fs.exists(conflict_key):
        raise GCSNativeWriterError(
            "revision contains terminal failure evidence; choose a new revision"
        )
    thresholds = {
        "min_block_bytes": min_block_bytes,
        "max_block_bytes": max_block_bytes,
        "max_rss_bytes": max_rss_bytes,
    }
    if fs.exists(plan_key):
        plan = json.loads(_read_bytes(fs, plan_key))
        plan_exception = plan.get("exception")
        explicit_plan_exception = (
            isinstance(plan_exception, dict)
            and plan_exception.get("kind") == "explicit"
        )
        explicit_exception_mismatch = block_size_exception is not None and (
            not explicit_plan_exception
            or any(
                plan_exception.get(key) != value
                for key, value in block_size_exception.items()
            )
        )
        if (
            plan.get("format") != f"{FORMAT}.block-plan/v2"
            or plan.get("identity") != identity
            or plan.get("thresholds") != thresholds
            or plan.get("max_rows") != max_rows
            or explicit_exception_mismatch
            or (block_size_exception is None and explicit_plan_exception)
        ):
            raise GCSNativeWriterError(
                "remote plan identity mismatch; immutable plan refuses resume drift"
            )
        calibration_key = _path(candidate_prefix, "calibration", "probe.json")
        if not fs.exists(calibration_key):
            raise GCSNativeWriterError(
                "immutable plan has no calibration evidence; choose a new revision"
            )
        calibration = json.loads(_read_bytes(fs, calibration_key))
        if plan.get("calibration") != {
            "identity": calibration.get("identity"),
            "probe_interval": calibration.get("probe_interval"),
            "measured_bytes": calibration.get("measured_bytes"),
            "measured_peak_rss_bytes": calibration.get("measured_peak_rss_bytes"),
            "objects": calibration.get("objects"),
        }:
            raise GCSNativeWriterError(
                "immutable plan calibration evidence mismatch; choose a new revision"
            )
    else:
        if fs.exists(_path(candidate_prefix, "chunk-records")):
            raise GCSNativeWriterError(
                "production chunks exist without a calibrated immutable plan"
            )
        calibration = _load_or_measure_calibration(
            fs=fs,
            candidate_prefix=candidate_prefix,
            matrix=matrix,
            obs=obs,
            sparse_format=sparse_format,
            probe_rows=probe_rows,
            identity=identity,
            peak_rss_reader=peak_rss_reader,
        )
        try:
            plan = calibrated_block_plan(
                identity=identity,
                n_obs=shape[0],
                probe_rows=probe_rows,
                measured_bytes=_required_int(calibration, "measured_bytes"),
                measured_peak_rss_bytes=_required_int(
                    calibration, "measured_peak_rss_bytes"
                ),
                min_block_bytes=min_block_bytes,
                max_block_bytes=max_block_bytes,
                max_rss_bytes=max_rss_bytes,
                max_rows=max_rows,
                calibration_objects=_required_mapping_list(calibration, "objects"),
                explicit_exception=block_size_exception,
            )
        except BlockPlanConflict as error:
            _write_exclusive(
                fs,
                conflict_key,
                _json_bytes(
                    {
                        "status": "failed",
                        "identity": identity,
                        "evidence": error.evidence,
                        "ended_at": time.time(),
                    }
                ),
            )
            raise
        plan = _load_plan(fs, plan_key, plan)
    raw_chunks = plan.get("planned_chunks")
    if not isinstance(raw_chunks, list):
        raise GCSNativeWriterError("immutable plan chunks are malformed")
    parsed_chunks: list[tuple[int, int]] = []
    for interval in raw_chunks:
        if not isinstance(interval, list) or len(interval) != 2:
            raise GCSNativeWriterError("immutable plan chunks are malformed")
        start, end = interval
        if (
            not isinstance(start, int)
            or isinstance(start, bool)
            or not isinstance(end, int)
            or isinstance(end, bool)
        ):
            raise GCSNativeWriterError("immutable plan chunks are malformed")
        parsed_chunks.append((start, end))
    chunks = tuple(parsed_chunks)
    if not chunks:
        raise GCSNativeWriterError("immutable plan contains no production chunks")
    if fs.exists(manifest_key):
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
        chunk_started = time.monotonic()
        record_key = _path(candidate_prefix, "chunk-records", f"chunk_{index:06d}.json")
        source_chunk = _materialize_rows(matrix, start, end, sparse_format)
        source_obs = obs.iloc[start:end]
        bytes_read += sum(values.nbytes for values in _matrix_components(source_chunk))
        peak_rss = max(peak_rss, peak_rss_reader())
        if index not in completed:
            if fs.exists(record_key):
                raise GCSNativeWriterError(
                    f"chunk record appeared after resume preflight: {index}"
                )
            matrix_key, obs_key, recovery_attempt = _resumable_chunk_keys(
                fs, candidate_prefix, index
            )
            matrix_exists = fs.exists(matrix_key)
            obs_exists = fs.exists(obs_key)
            if matrix_exists != obs_exists:
                raise GCSNativeWriterError(
                    f"selected partial recovery pair for chunk {index}"
                )
            if matrix_exists:
                chunk_bytes_written = 0
                obs_object = fs.info(obs_key)
            else:
                chunk_bytes_written = _write_remote_matrix(
                    fs, matrix_key, source_chunk, sparse_format
                )
                obs_object = _write_parquet(fs, obs_key, source_obs)
                chunk_bytes_written += int(obs_object["size"])
                bytes_written += chunk_bytes_written
            stored_bytes = sum(
                _required_int(item, "size")
                for item in _remote_object_evidence(fs, matrix_key)
            ) + int(obs_object["size"])
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
            measured_peak_rss = peak_rss_reader()
            record: dict[str, object] = {
                "index": index,
                "start": source_row_start + start,
                "end": source_row_start + end,
                "shape": [end - start, shape[1]],
                "nnz": int(remote.nnz),
                "dtype": str(remote.dtype),
                "matrix_key": matrix_key,
                "obs_key": obs_key,
                "obs_generation": obs_object["generation"],
                "obs_index_sha256": _index_sha256(remote_obs),
                "obs_frame_sha256": _frame_sha256(remote_obs),
                "checksums": checksums,
                "source_generation": source_generation,
                "source_checksum": source_checksum,
                "runtime_seconds": time.monotonic() - chunk_started,
                "peak_rss_bytes": measured_peak_rss,
                "bytes_written": stored_bytes,
                "bytes_read": sum(
                    values.nbytes for values in _matrix_components(remote)
                ),
            }
            if recovery_attempt is not None:
                record["recovery_attempt"] = recovery_attempt
            try:
                record["block_validation"] = validate_measured_block(
                    start=start,
                    end=end,
                    n_obs=shape[0],
                    measured_bytes=stored_bytes,
                    measured_peak_rss_bytes=measured_peak_rss,
                    min_block_bytes=min_block_bytes,
                    max_block_bytes=max_block_bytes,
                    max_rss_bytes=max_rss_bytes,
                    explicit_exception=block_size_exception,
                )
            except BlockPlanConflict as error:
                record["block_validation"] = {
                    "status": "failed",
                    **error.evidence,
                }
                record_object = _write_exclusive(fs, record_key, _json_bytes(record))
                bytes_written += int(record_object["size"])
                _write_exclusive(
                    fs,
                    failure_key,
                    _json_bytes(
                        {
                            "status": "failed",
                            "identity": identity,
                            "failed_chunk": index,
                            "evidence": error.evidence,
                            "ended_at": time.time(),
                            "requires_new_revision": True,
                        }
                    ),
                )
                raise
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
        if record.get("source_checksum") != source_checksum:
            raise GCSNativeWriterError(f"chunk source checksum mismatch: {index}")
        validation = record.get("block_validation")
        if not isinstance(validation, dict) or validation.get("status") != "accepted":
            raise GCSNativeWriterError(
                f"chunk {index} lacks accepted measured block evidence; choose a new revision"
            )
        records.append(record)
    var_key = _path(candidate_prefix, "var.parquet")
    if fs.exists(var_key):
        var_object = fs.info(var_key)
    else:
        var_object = _write_parquet(fs, var_key, var)
    remote_var = _read_parquet(fs, var_key)
    var_identity = shared_var_identity(var, schema_fingerprint=schema_fingerprint)
    if (
        shared_var_identity(remote_var, schema_fingerprint=schema_fingerprint)
        != var_identity
    ):
        raise GCSNativeWriterError("remote shared var readback identity mismatch")
    manifest: dict[str, object] = {
        "format": FORMAT,
        "logical_key": logical_key,
        "revision": revision,
        "candidate_prefix": candidate_prefix,
        "source": {
            "uri": source_uri,
            "generation": source_generation,
            "checksum": source_checksum,
            "row_start": source_row_start,
            "row_end": source_row_end,
        },
        "shape": list(shape),
        "nnz": nnz,
        "sparse_format": sparse_format,
        "ingestion_run_id": ingestion_run_id,
        "chunks": records,
        "block_contract": plan,
        "var": {
            "key": var_key,
            "generation": var_object["generation"],
            "index_sha256": var_identity.index_sha256,
            "frame_sha256": var_identity.frame_sha256,
            "schema_fingerprint": var_identity.schema_fingerprint,
        },
        "candidate_metadata": dict(candidate_metadata or {}),
    }
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
