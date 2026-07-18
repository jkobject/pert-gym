"""Bounded, requester-pays GCS-native sparse-Zarr candidate writer.

This module deliberately publishes no Lamin records and never mutates a source,
``main`` prefix, or public prefix.  It writes immutable chunks below one
caller-supplied temporary revision prefix, verifies them over the remote object
store, then writes an immutable manifest and a separate promotion marker last.
There is no rename-based atomicity assumption: consumers must resolve a
promotion marker and then its manifest.
"""

from __future__ import annotations

import ctypes
import gc
import hashlib
import io
import json
import math
import shutil
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, cast

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
    _read_matrix,
    _sha256_array,
    _source_nnz,
    _source_sparse_format,
    _write_matrix,
    shared_var_identity,
)
from pert_gym.perturbai_sparse_parquet import (
    requester_pays_storage_options,
)
from pert_gym.sparse_zarr_contract import (
    adaptive_component_chunk_length,
    adaptive_target_rows,
    validate_target_object_bytes,
)

DEFAULT_CACHE_CAP_BYTES = 20 * 1024**3
DEFAULT_CACHE_SAFETY_RESERVE_BYTES = 20 * 1024**3
DEFAULT_MIN_BLOCK_BYTES = 2 * 1024**3
DEFAULT_MAX_BLOCK_BYTES = 3 * 1024**3
DEFAULT_TARGET_OBJECT_BYTES = 64 * 1024**2
DEFAULT_MAX_CANDIDATE_OBJECTS = 10_000
DEFAULT_MAX_REQUEST_COST_EUR = 1.0
DEFAULT_MAX_RECOVERY_ATTEMPTS = 1
CLASS_A_EUR_PER_1000 = 0.005689
CLASS_B_EUR_PER_1000 = 0.00045512
FORMAT = "pert-gym.gcs-native-logical-sparse-zarr/v1"


def _release_block_memory() -> None:
    """Return unreachable per-block buffers to Python and, on glibc, the OS."""
    gc.collect()
    if not sys.platform.startswith("linux"):
        return
    try:
        malloc_trim = ctypes.CDLL(None).malloc_trim
        malloc_trim.argtypes = [ctypes.c_size_t]
        malloc_trim.restype = ctypes.c_int
        malloc_trim(0)
    except (AttributeError, OSError):
        pass


class GCSNativeWriterError(RuntimeError):
    """A remote candidate cannot be safely continued or promoted."""


class BlockPlanConflict(GCSNativeWriterError):
    """Measured byte and RSS evidence cannot satisfy the production contract."""

    def __init__(self, message: str, evidence: Mapping[str, object]) -> None:
        super().__init__(message)
        self.evidence = dict(evidence)


class OperationBudgetExceeded(GCSNativeWriterError):
    """Runtime object/request counters exceeded the approved forecast envelope."""

    def __init__(self, evidence: Mapping[str, object]) -> None:
        super().__init__("runtime GCS operation counters exceeded approved forecast")
        self.evidence = dict(evidence)


def _component_object_upper(
    *,
    elements: int,
    itemsize: int,
    logical_blocks: int,
    target_object_bytes: int,
    legacy_fixed_chunk_elements: int | None,
) -> int:
    if elements == 0:
        return 0
    if legacy_fixed_chunk_elements is not None:
        return math.ceil(elements / legacy_fixed_chunk_elements) + logical_blocks - 1
    return math.ceil(elements * itemsize / target_object_bytes) + logical_blocks - 1


def forecast_gcs_operation_cost(
    *,
    n_obs: int,
    n_vars: int,
    nnz: int,
    data_dtype: np.dtype[Any],
    index_dtype: np.dtype[Any],
    indptr_dtype: np.dtype[Any],
    sparse_format: str,
    logical_blocks: int,
    calibration_rows: int,
    target_object_bytes: int = DEFAULT_TARGET_OBJECT_BYTES,
    legacy_fixed_chunk_elements: int | None = None,
    max_recovery_attempts: int = DEFAULT_MAX_RECOVERY_ATTEMPTS,
    max_retry_nnz: int | None = None,
    max_retry_rows: int | None = None,
    readback_passes: int = 1,
    monthly_free_class_a_remaining: int = 0,
) -> dict[str, object]:
    """Forecast a conservative candidate object/request envelope before writes.

    The per-dataset default deliberately applies no monthly free tier. A
    portfolio caller may pass the one shared remaining balance and must carry
    ``monthly_free_class_a_remaining_after`` into the next forecast.
    """
    if n_obs <= 0 or n_vars <= 0 or nnz < 0:
        raise ValueError("forecast dimensions must be positive and nnz non-negative")
    if logical_blocks <= 0 or not 0 < calibration_rows <= n_obs:
        raise ValueError("logical_blocks and calibration_rows must be positive")
    if sparse_format not in {"csr", "csc"}:
        raise ValueError("sparse_format must be csr or csc")
    if max_recovery_attempts < 0 or readback_passes < 1:
        raise ValueError(
            "max_recovery_attempts must be non-negative and readback_passes positive"
        )
    if monthly_free_class_a_remaining < 0:
        raise ValueError("monthly free-tier balance must be non-negative")
    if legacy_fixed_chunk_elements is not None and legacy_fixed_chunk_elements <= 0:
        raise ValueError("legacy_fixed_chunk_elements must be positive")
    if target_object_bytes <= 0:
        raise ValueError("target_object_bytes must be positive")
    retry_rows = n_obs if max_retry_rows is None else max_retry_rows
    retry_nnz = (
        min(nnz, retry_rows * n_vars) if max_retry_nnz is None else max_retry_nnz
    )
    if not 0 <= retry_nnz <= nnz or not 0 < retry_rows <= n_obs:
        raise ValueError("retry nnz/rows bounds must fit within the dataset")

    data_itemsize = int(np.dtype(data_dtype).itemsize)
    index_itemsize = int(np.dtype(index_dtype).itemsize)
    indptr_itemsize = int(np.dtype(indptr_dtype).itemsize)
    production_components = {
        "data": _component_object_upper(
            elements=nnz,
            itemsize=data_itemsize,
            logical_blocks=logical_blocks,
            target_object_bytes=target_object_bytes,
            legacy_fixed_chunk_elements=legacy_fixed_chunk_elements,
        ),
        "indices": _component_object_upper(
            elements=nnz,
            itemsize=index_itemsize,
            logical_blocks=logical_blocks,
            target_object_bytes=target_object_bytes,
            legacy_fixed_chunk_elements=legacy_fixed_chunk_elements,
        ),
        "indptr": _component_object_upper(
            elements=(
                n_obs + logical_blocks
                if sparse_format == "csr"
                else logical_blocks * (n_vars + 1)
            ),
            itemsize=indptr_itemsize,
            logical_blocks=logical_blocks,
            target_object_bytes=target_object_bytes,
            legacy_fixed_chunk_elements=legacy_fixed_chunk_elements,
        ),
    }
    # Density can be arbitrarily skewed. A proportional estimate is not an upper
    # bound for the leading calibration interval, so cap only by the dataset nnz
    # and the maximum number of entries that interval can physically contain.
    calibration_nnz = min(nnz, calibration_rows * n_vars)
    calibration_indptr = calibration_rows + 1 if sparse_format == "csr" else n_vars + 1
    calibration_components = {
        "data": _component_object_upper(
            elements=calibration_nnz,
            itemsize=data_itemsize,
            logical_blocks=1,
            target_object_bytes=target_object_bytes,
            legacy_fixed_chunk_elements=legacy_fixed_chunk_elements,
        ),
        "indices": _component_object_upper(
            elements=calibration_nnz,
            itemsize=index_itemsize,
            logical_blocks=1,
            target_object_bytes=target_object_bytes,
            legacy_fixed_chunk_elements=legacy_fixed_chunk_elements,
        ),
        "indptr": _component_object_upper(
            elements=calibration_indptr,
            itemsize=indptr_itemsize,
            logical_blocks=1,
            target_object_bytes=target_object_bytes,
            legacy_fixed_chunk_elements=legacy_fixed_chunk_elements,
        ),
    }
    # Per production block: Zarr metadata (<=7), obs, immutable record, and
    # operation checkpoint. Calibration is local-only, but remains in the packet
    # as evidence and request accounting for its eventual plan serialization.
    production_matrix_objects = sum(production_components.values()) + logical_blocks * 7
    obs_objects = logical_blocks
    record_objects = logical_blocks
    checkpoint_objects = logical_blocks
    production_objects = (
        production_matrix_objects + obs_objects + record_objects + checkpoint_objects
    )
    calibration_objects = 0
    fixed_objects = 3  # immutable plan, shared var, and manifest
    # Each bounded recovery can duplicate at most the largest exact planned
    # block. Callers must supply that block's nnz/rows after local planning.
    retry_components = {
        "data": _component_object_upper(
            elements=retry_nnz,
            itemsize=data_itemsize,
            logical_blocks=1,
            target_object_bytes=target_object_bytes,
            legacy_fixed_chunk_elements=legacy_fixed_chunk_elements,
        ),
        "indices": _component_object_upper(
            elements=retry_nnz,
            itemsize=index_itemsize,
            logical_blocks=1,
            target_object_bytes=target_object_bytes,
            legacy_fixed_chunk_elements=legacy_fixed_chunk_elements,
        ),
        "indptr": _component_object_upper(
            elements=retry_rows + 1 if sparse_format == "csr" else n_vars + 1,
            itemsize=indptr_itemsize,
            logical_blocks=1,
            target_object_bytes=target_object_bytes,
            legacy_fixed_chunk_elements=legacy_fixed_chunk_elements,
        ),
    }
    retry_objects = max_recovery_attempts * (sum(retry_components.values()) + 8)
    candidate_objects = (
        production_objects + calibration_objects + fixed_objects + retry_objects
    )
    list_requests = logical_blocks * (max_recovery_attempts + 2) + 6
    class_a_requests = candidate_objects + list_requests
    class_b_requests = (
        candidate_objects * (readback_passes + 1) + logical_blocks * 8 + 20
    )
    free_applied = min(class_a_requests, monthly_free_class_a_remaining)
    chargeable_class_a = class_a_requests - free_applied
    request_cost_eur = (
        chargeable_class_a * CLASS_A_EUR_PER_1000
        + class_b_requests * CLASS_B_EUR_PER_1000
    ) / 1000
    return {
        "format": f"{FORMAT}.operation-forecast/v1",
        "basis": "upper_bound",
        "target_object_bytes": target_object_bytes,
        "legacy_fixed_chunk_elements": legacy_fixed_chunk_elements,
        "logical_blocks": logical_blocks,
        "calibration_rows": calibration_rows,
        "calibration_nnz_upper_bound": calibration_nnz,
        "max_recovery_attempts": max_recovery_attempts,
        "max_retry_nnz": retry_nnz,
        "max_retry_rows": retry_rows,
        "readback_passes": readback_passes,
        "component_objects": {
            "production": production_components,
            "calibration": calibration_components,
            "retry": retry_components,
        },
        "layout_objects": {
            "calibration_local_only": sum(calibration_components.values()) + 7,
            "production_matrix": production_matrix_objects,
            "obs": obs_objects,
            "records": record_objects,
            "checkpoints": checkpoint_objects,
            "plan": 1,
            "var": 1,
            "manifest_or_failure": 1,
            "production": production_objects,
            "calibration": calibration_objects,
            "fixed": fixed_objects,
            "retries": retry_objects,
        },
        "candidate_objects": candidate_objects,
        "class_a_requests": class_a_requests,
        "class_b_requests": class_b_requests,
        "list_requests": list_requests,
        "monthly_free_class_a_applied": free_applied,
        "monthly_free_class_a_remaining_after": (
            monthly_free_class_a_remaining - free_applied
        ),
        "pricing": {
            "currency": "EUR",
            "class_a_eur_per_1000": CLASS_A_EUR_PER_1000,
            "class_b_eur_per_1000": CLASS_B_EUR_PER_1000,
            "free_tier_scope": "shared_monthly_balance; per-dataset default is zero",
        },
        "request_cost_eur": request_cost_eur,
    }


def validate_gcs_operation_budget(
    forecast: Mapping[str, object],
    *,
    exception: Mapping[str, object] | None = None,
    launch_context: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Fail closed above object/cost caps unless a reviewed task budget covers both."""
    candidate_objects = forecast.get("candidate_objects")
    request_cost = forecast.get("request_cost_eur")
    if not isinstance(candidate_objects, int) or isinstance(candidate_objects, bool):
        raise GCSNativeWriterError("operation forecast candidate_objects is invalid")
    if not isinstance(request_cost, (int, float)) or isinstance(request_cost, bool):
        raise GCSNativeWriterError("operation forecast request_cost_eur is invalid")
    violations = []
    if candidate_objects > DEFAULT_MAX_CANDIDATE_OBJECTS:
        violations.append("candidate_objects")
    if float(request_cost) > DEFAULT_MAX_REQUEST_COST_EUR:
        violations.append("request_cost_eur")
    if not violations:
        return {
            "status": "accepted",
            "limits": {
                "max_candidate_objects": DEFAULT_MAX_CANDIDATE_OBJECTS,
                "max_request_cost_eur": DEFAULT_MAX_REQUEST_COST_EUR,
            },
            "violations": [],
        }
    required = {
        "task_id",
        "reviewed_by",
        "reason",
        "max_candidate_objects",
        "max_request_cost_eur",
    }
    if exception is None or not required.issubset(exception):
        raise GCSNativeWriterError(
            "GCS operation budget exceeded without a task-scoped reviewed exception"
        )
    if any(
        not isinstance(exception[field], str) or not exception[field]
        for field in ("task_id", "reviewed_by", "reason")
    ):
        raise GCSNativeWriterError(
            "reviewed exception identity and reason must be non-empty"
        )
    if not isinstance(launch_context, Mapping) or any(
        exception[field] != launch_context.get(field)
        for field in ("task_id", "reviewed_by")
    ):
        raise GCSNativeWriterError(
            "reviewed exception identity does not match immutable launch context"
        )
    exception_objects = exception["max_candidate_objects"]
    exception_cost = exception["max_request_cost_eur"]
    if (
        not isinstance(exception_objects, int)
        or isinstance(exception_objects, bool)
        or not isinstance(exception_cost, (int, float))
        or isinstance(exception_cost, bool)
        or candidate_objects > exception_objects
        or float(request_cost) > float(exception_cost)
    ):
        raise GCSNativeWriterError(
            "GCS operation forecast exceeds reviewed exception budget"
        )
    return {
        "status": "accepted_exception",
        "limits": {
            "max_candidate_objects": DEFAULT_MAX_CANDIDATE_OBJECTS,
            "max_request_cost_eur": DEFAULT_MAX_REQUEST_COST_EUR,
        },
        "violations": violations,
        "exception": dict(exception),
    }


def runtime_operation_checkpoint(
    *,
    forecast: Mapping[str, object],
    logical_checkpoint: int,
    actual: Mapping[str, object],
    reserved: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Count candidate objects/requests at a durable block boundary.

    The object count includes the checkpoint object that the caller will write.
    Request counts are conservative upper bounds using the same accounting basis
    as the preflight forecast; no per-dataset free tier is applied.
    """
    fields = ("candidate_objects", "class_a_requests", "class_b_requests")
    forecast_values: dict[str, int] = {}
    actual_values: dict[str, int] = {}
    reserved_values: dict[str, int] = {}
    for field in fields:
        forecast_value = forecast.get(field)
        actual_value = actual.get(field)
        reserved_value = (reserved or {}).get(field, 0)
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (forecast_value, actual_value, reserved_value)
        ):
            raise GCSNativeWriterError(
                f"runtime checkpoint {field} counters are invalid"
            )
        forecast_values[field] = cast(int, forecast_value)
        actual_values[field] = cast(int, actual_value)
        reserved_values[field] = cast(int, reserved_value)
    if logical_checkpoint < 0:
        raise ValueError("logical_checkpoint must be non-negative")
    projected_final = {
        field: actual_values[field] + reserved_values[field] for field in fields
    }
    allowed = {field: math.floor(forecast_values[field] * 1.2) for field in fields}
    violations = [field for field in fields if projected_final[field] > allowed[field]]
    request_cost_eur = (
        actual_values["class_a_requests"] * CLASS_A_EUR_PER_1000
        + actual_values["class_b_requests"] * CLASS_B_EUR_PER_1000
    ) / 1000
    projected_cost_eur = (
        projected_final["class_a_requests"] * CLASS_A_EUR_PER_1000
        + projected_final["class_b_requests"] * CLASS_B_EUR_PER_1000
    ) / 1000
    return {
        "format": f"{FORMAT}.operation-checkpoint/v1",
        "status": "exceeded" if violations else "accepted",
        "logical_checkpoint": logical_checkpoint,
        "actual": {**actual_values, "request_cost_eur": request_cost_eur},
        "reserved": reserved_values,
        "projected_final": {
            **projected_final,
            "request_cost_eur": projected_cost_eur,
        },
        "allowed": {**allowed, "overage_fraction": 0.2},
        "violations": violations,
    }


@dataclass(frozen=True)
class GCSNativeMetrics:
    peak_rss_bytes: int
    bytes_written: int
    bytes_read: int
    chunk_count: int
    cache_cap_bytes: int
    cache_bytes_after_cleanup: int
    actual_operations: Mapping[str, int]


@dataclass
class GCSOperationCounter:
    """Conservative cumulative GCS request counter shared by source and target I/O."""

    candidate_objects: int = 0
    class_a_requests: int = 0
    class_b_requests: int = 0
    _prepaid_class_a_requests: int = 0
    _prepaid_class_b_requests: int = 0

    def count_class_a(self, requests: int = 1) -> None:
        consumed = min(requests, self._prepaid_class_a_requests)
        self._prepaid_class_a_requests -= consumed
        self.class_a_requests += requests - consumed

    def count_class_b(self, requests: int = 1) -> None:
        consumed = min(requests, self._prepaid_class_b_requests)
        self._prepaid_class_b_requests -= consumed
        self.class_b_requests += requests - consumed

    def add_cumulative_floor(self, floor: Mapping[str, int]) -> None:
        self.class_a_requests += floor["class_a_requests"]
        self.class_b_requests += floor["class_b_requests"]

    def prepay(self, *, class_a_requests: int, class_b_requests: int) -> None:
        self.class_a_requests += class_a_requests
        self.class_b_requests += class_b_requests
        self._prepaid_class_a_requests += class_a_requests
        self._prepaid_class_b_requests += class_b_requests

    def snapshot(self) -> dict[str, int]:
        return {
            "candidate_objects": self.candidate_objects,
            "class_a_requests": self.class_a_requests,
            "class_b_requests": self.class_b_requests,
        }


class _CountingFileSystem:
    """Transparent fsspec proxy counting every invoked remote operation."""

    def __init__(self, backend: Any, counter: GCSOperationCounter) -> None:
        self._backend = backend
        self.operation_counter = counter

    def __getattr__(self, name: str) -> Any:
        return getattr(self._backend, name)

    def _strip_protocol(self, path: str) -> str:
        return self._backend._strip_protocol(path)

    def get_mapper(self, root: str = "", **kwargs: Any) -> Any:
        return fsspec.mapping.FSMap(root, self, **kwargs)

    def exists(self, path: str, **kwargs: object) -> bool:
        self.operation_counter.count_class_b()
        return bool(self._backend.exists(path, **kwargs))

    def info(self, path: str, **kwargs: object) -> dict[str, object]:
        self.operation_counter.count_class_b()
        return dict(self._backend.info(path, **kwargs))

    def find(self, path: str, **kwargs: object) -> Any:
        self.operation_counter.count_class_a()
        return self._backend.find(path, **kwargs)

    def ls(self, path: str, **kwargs: object) -> Any:
        self.operation_counter.count_class_a()
        return self._backend.ls(path, **kwargs)

    def open(self, path: str, mode: str = "rb", **kwargs: object) -> Any:
        if any(flag in mode for flag in "wax+"):
            self.operation_counter.count_class_a()
            self.operation_counter.candidate_objects += 1
        else:
            self.operation_counter.count_class_b()
        return self._backend.open(path, mode=mode, **kwargs)

    def pipe_file(self, path: str, value: bytes, **kwargs: object) -> Any:
        self.operation_counter.count_class_a()
        self.operation_counter.candidate_objects += 1
        return self._backend.pipe_file(path, value, **kwargs)

    def pipe(self, path: Any, value: bytes | None = None, **kwargs: object) -> Any:
        object_count = len(path) if isinstance(path, dict) else 1
        self.operation_counter.count_class_a(object_count)
        self.operation_counter.candidate_objects += object_count
        return self._backend.pipe(path, value=value, **kwargs)

    def cat_file(self, path: str, **kwargs: object) -> bytes:
        self.operation_counter.count_class_b()
        return self._backend.cat_file(path, **kwargs)

    def cat(self, path: Any, **kwargs: object) -> Any:
        request_count = len(path) if isinstance(path, list) else 1
        self.operation_counter.count_class_b(request_count)
        return self._backend.cat(path, **kwargs)


def count_gcs_operations(fs: Any, counter: GCSOperationCounter) -> Any:
    """Return one non-nesting fsspec proxy bound to ``counter``."""
    if isinstance(fs, _CountingFileSystem):
        if fs.operation_counter is not counter:
            raise GCSNativeWriterError("counted filesystem uses a different operation counter")
        return fs
    return _CountingFileSystem(fs, counter)


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
    whole_small_active_constraints = [
        constraint
        for constraint, violated in (
            ("max_rows", n_obs > max_rows),
            ("rss_row_ceiling", n_obs > rows_allowed_by_rss),
        )
        if small_dataset and violated
    ]
    if whole_small_active_constraints:
        evidence = {
            "kind": "whole_small_hard_limit_conflict",
            "active_constraints": whole_small_active_constraints,
            "whole_dataset_below_minimum": True,
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
        active_constraints = ", ".join(whole_small_active_constraints)
        raise BlockPlanConflict(
            "whole dataset below byte minimum exceeds hard constraints: "
            f"{active_constraints}",
            evidence,
        )

    byte_target_conflict = (
        not small_dataset
        and not explicit_exception
        and rows_required_for_min
        > min(n_obs, max_rows, rows_allowed_by_bytes, rows_allowed_by_rss)
    )
    if byte_target_conflict:
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
            "target_object_bytes": identity.get("target_object_bytes"),
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


def _source_component_dtypes(
    matrix: object, *, probe_rows: int, sparse_format: str
) -> tuple[np.dtype[Any], np.dtype[Any], np.dtype[Any]]:
    direct = tuple(
        getattr(matrix, name, None) for name in ("data", "indices", "indptr")
    )
    if all(isinstance(value, np.ndarray) for value in direct):
        data, indices, indptr = direct
        assert isinstance(data, np.ndarray)
        assert isinstance(indices, np.ndarray)
        assert isinstance(indptr, np.ndarray)
        return np.dtype(data.dtype), np.dtype(indices.dtype), np.dtype(indptr.dtype)
    declared = tuple(
        getattr(matrix, name, None)
        for name in ("data_dtype", "index_dtype", "indptr_dtype")
    )
    if all(value is not None for value in declared):
        return np.dtype(declared[0]), np.dtype(declared[1]), np.dtype(declared[2])
    probe = _materialize_rows(matrix, 0, probe_rows, sparse_format)
    try:
        data, indices, indptr = _matrix_components(probe)
        return np.dtype(data.dtype), np.dtype(indices.dtype), np.dtype(indptr.dtype)
    finally:
        del probe
        _release_block_memory()


def _source_interval_nnz(
    matrix: object, *, start: int, end: int, sparse_format: str
) -> int:
    """Read an exact planned-block nnz bound without retaining its payload."""
    block_nnz = getattr(matrix, "block_nnz", None)
    if callable(block_nnz):
        return int(block_nnz(start, end))
    indptr = getattr(matrix, "indptr", None)
    if sparse_format == "csr" and indptr is not None:
        return int(indptr[end]) - int(indptr[start])
    block = _materialize_rows(matrix, start, end, sparse_format)
    try:
        return int(block.nnz)
    finally:
        del block
        _release_block_memory()


def _planned_retry_bounds(
    matrix: object,
    intervals: Sequence[object],
    *,
    sparse_format: str,
) -> tuple[int, int]:
    parsed: list[tuple[int, int]] = []
    for interval in intervals:
        if (
            not isinstance(interval, list)
            or len(interval) != 2
            or not all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in interval
            )
        ):
            raise GCSNativeWriterError("immutable plan chunks are malformed")
        parsed.append((cast(int, interval[0]), cast(int, interval[1])))
    return (
        max(
            _source_interval_nnz(
                matrix, start=start, end=end, sparse_format=sparse_format
            )
            for start, end in parsed
        ),
        max(end - start for start, end in parsed),
    )


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
    fs: Any,
    key: str,
    matrix: CompressedMatrix,
    sparse_format: str,
    *,
    target_object_bytes: int,
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
            name,
            data=values,
            chunks=(
                adaptive_component_chunk_length(
                    length=len(values),
                    dtype=values.dtype,
                    target_object_bytes=target_object_bytes,
                ),
            ),
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
    target_object_bytes: int,
    max_recovery_attempts: int,
    launch_context: Mapping[str, object],
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
        "target_object_bytes": target_object_bytes,
        "max_recovery_attempts": max_recovery_attempts,
        "launch_context": dict(launch_context),
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


def _measure_calibration_locally(
    *,
    cache_dir: Path,
    matrix: object,
    obs: pd.DataFrame,
    sparse_format: str,
    probe_rows: int,
    identity: Mapping[str, object],
    peak_rss_reader: Callable[[], int],
    target_object_bytes: int,
) -> dict[str, object]:
    """Measure/read back a probe locally before any candidate-prefix write."""
    probe_dir = cache_dir / "calibration-probe"
    if probe_dir.exists():
        shutil.rmtree(probe_dir)
    probe_dir.mkdir(mode=0o700, parents=True)
    matrix_path = probe_dir / "matrix.zarr"
    obs_path = probe_dir / "obs.parquet"
    source_chunk = _materialize_rows(matrix, 0, probe_rows, sparse_format)
    source_obs = obs.iloc[:probe_rows]
    _write_matrix(
        matrix_path,
        source_chunk,
        sparse_format,
        target_object_bytes=target_object_bytes,
    )
    source_obs.to_parquet(obs_path)
    remote = _read_matrix(matrix_path, sparse_format)
    remote_obs = pd.read_parquet(obs_path)
    _assert_source_readback_parity(source_chunk, remote, source_obs, remote_obs)
    objects = [
        {
            "key": str(path.relative_to(probe_dir)),
            "generation": "local-calibration",
            "size": path.stat().st_size,
            "sha256": _sha256_bytes(path.read_bytes()),
        }
        for path in sorted(path for path in probe_dir.rglob("*") if path.is_file())
    ]
    record = {
        "format": f"{FORMAT}.calibration/v1",
        "identity": dict(identity),
        "probe_interval": [0, probe_rows],
        "target_object_bytes": target_object_bytes,
        "objects": objects,
        "measured_bytes": sum(_required_int(item, "size") for item in objects),
        "measured_peak_rss_bytes": peak_rss_reader(),
    }
    del source_chunk, source_obs, remote, remote_obs
    _release_block_memory()
    return record


def _resumable_chunk_keys(
    fs: Any, candidate_prefix: str, index: int, *, max_recovery_attempts: int
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
    while attempt < max_recovery_attempts:
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
    raise GCSNativeWriterError(
        f"chunk {index} exhausted the reviewed recovery-attempt budget"
    )


def _verify_completed_chunk(
    *,
    fs: Any,
    record: Mapping[str, object],
    source_chunk: CompressedMatrix,
    source_obs: pd.DataFrame,
    sparse_format: str,
    index: int,
) -> None:
    matrix_key = record.get("matrix_key")
    obs_key = record.get("obs_key")
    expected_objects = record.get("matrix_objects")
    if (
        not isinstance(matrix_key, str)
        or not isinstance(obs_key, str)
        or not isinstance(expected_objects, list)
    ):
        raise GCSNativeWriterError(
            f"completed chunk {index} lacks immutable object evidence"
        )
    actual_objects = _remote_object_evidence(fs, matrix_key)
    if actual_objects != expected_objects:
        raise GCSNativeWriterError(
            f"completed chunk object generation mismatch: {index}"
        )
    obs_info = fs.info(obs_key)
    if str(obs_info.get("generation", "")) != record.get("obs_generation"):
        raise GCSNativeWriterError(f"completed chunk obs generation mismatch: {index}")
    try:
        remote = _read_remote_matrix(fs, matrix_key, sparse_format)
        remote_obs = _read_parquet(fs, obs_key)
        checksums = {
            name: _sha256_array(values)
            for name, values in zip(
                ("data_sha256", "indices_sha256", "indptr_sha256"),
                _matrix_components(remote),
                strict=True,
            )
        }
        if checksums != record.get("checksums"):
            raise GCSNativeWriterError(f"completed chunk payload mismatch: {index}")
        if _index_sha256(remote_obs) != record.get("obs_index_sha256") or _frame_sha256(
            remote_obs
        ) != record.get("obs_frame_sha256"):
            raise GCSNativeWriterError(f"completed chunk obs mismatch: {index}")
        _assert_source_readback_parity(source_chunk, remote, source_obs, remote_obs)
    except GCSNativeWriterError:
        raise
    except Exception as error:
        raise GCSNativeWriterError(
            f"completed chunk payload mismatch: {index}"
        ) from error
    finally:
        if "remote" in locals():
            del remote
        if "remote_obs" in locals():
            del remote_obs
        _release_block_memory()


def _cumulative_request_counts(
    value: object, *, evidence_key: str
) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise GCSNativeWriterError(f"{evidence_key} cumulative counters are missing")
    counters = cast(Mapping[str, object], value)
    result: dict[str, int] = {}
    for field in ("class_a_requests", "class_b_requests"):
        field_value = counters.get(field)
        if (
            not isinstance(field_value, int)
            or isinstance(field_value, bool)
            or field_value < 0
        ):
            raise GCSNativeWriterError(f"{evidence_key} cumulative counters are invalid")
        result[field] = field_value
    return result


def _load_cumulative_operation_floor(
    fs: Any, candidate_prefix: str, identity: Mapping[str, object]
) -> dict[str, int]:
    floor = {"class_a_requests": 0, "class_b_requests": 0}
    evidence_prefixes = (
        _path(candidate_prefix, "operation-attempts"),
        _path(candidate_prefix, "operation-checkpoints"),
    )
    for evidence_prefix in evidence_prefixes:
        for key in sorted(fs.find(evidence_prefix)):
            evidence = json.loads(_read_bytes(fs, key))
            if not isinstance(evidence, dict) or evidence.get("identity") != identity:
                raise GCSNativeWriterError(
                    "operation checkpoint identity mismatch; immutable resume refused"
                )
            counts = _cumulative_request_counts(
                evidence.get("cumulative", evidence.get("actual")),
                evidence_key=key,
            )
            for field, count in counts.items():
                floor[field] = max(floor[field], count)
    return floor


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
    launch_context: Mapping[str, object],
    cache_dir: Path,
    forecast_logical_blocks: int,
    candidate_metadata: Mapping[str, object] | None = None,
    cache_cap_bytes: int = DEFAULT_CACHE_CAP_BYTES,
    max_rss_bytes: int = 4 * 1024**3,
    min_block_bytes: int = DEFAULT_MIN_BLOCK_BYTES,
    max_block_bytes: int = DEFAULT_MAX_BLOCK_BYTES,
    block_size_exception: Mapping[str, object] | None = None,
    cache_safety_reserve_bytes: int = DEFAULT_CACHE_SAFETY_RESERVE_BYTES,
    min_rows: int = DEFAULT_MIN_ROWS,
    max_rows: int = DEFAULT_MAX_ROWS,
    target_object_bytes: int = DEFAULT_TARGET_OBJECT_BYTES,
    max_recovery_attempts: int = DEFAULT_MAX_RECOVERY_ATTEMPTS,
    operation_cost_exception: Mapping[str, object] | None = None,
    operation_counter: GCSOperationCounter | None = None,
    peak_rss_reader: Callable[[], int] = _peak_rss_bytes,
    stop_after_chunks: int | None = None,
) -> tuple[dict[str, object], GCSNativeMetrics]:
    """Write bounded CSR/CSC tranches directly to a versioned temporary GCS prefix.

    The caller must provide a backed/range-readable matrix (for example an HDF5
    backed sparse object opened from GCS).  Only ``matrix[start:end]`` is ever
    materialized. ``stop_after_chunks`` is test-only fault injection.
    """
    validate_target_object_bytes(target_object_bytes)
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
    if forecast_logical_blocks <= 0:
        raise ValueError("forecast_logical_blocks must be positive")
    if max_recovery_attempts < 0:
        raise ValueError("max_recovery_attempts must be non-negative")
    if any(
        not isinstance(launch_context.get(field), str) or not launch_context.get(field)
        for field in ("task_id", "reviewed_by")
    ):
        raise ValueError("launch_context must bind non-empty task_id and reviewed_by")
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
        json.dumps(operation_cost_exception or {}, sort_keys=True, allow_nan=False)
        json.dumps(launch_context, sort_keys=True, allow_nan=False)
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
    component_dtypes = _source_component_dtypes(
        matrix, probe_rows=probe_rows, sparse_format=sparse_format
    )
    if operation_counter is None:
        operation_counter = (
            fs.operation_counter
            if isinstance(fs, _CountingFileSystem)
            else GCSOperationCounter()
        )
    fs = count_gcs_operations(fs, operation_counter)
    candidate_prefix = _path(
        staging_prefix, logical_key, "temporary-revisions", revision
    )
    operation_counter.candidate_objects = len(fs.find(candidate_prefix))
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
        target_object_bytes=target_object_bytes,
        max_recovery_attempts=max_recovery_attempts,
        launch_context=launch_context,
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
        calibration = plan.get("calibration")
        if not isinstance(calibration, dict) or calibration.get("identity") != identity:
            raise GCSNativeWriterError(
                "immutable plan calibration evidence mismatch; choose a new revision"
            )
    else:
        if fs.exists(_path(candidate_prefix, "chunk-records")):
            raise GCSNativeWriterError(
                "production chunks exist without a calibrated immutable plan"
            )
        calibration = _measure_calibration_locally(
            cache_dir=cache_dir,
            matrix=matrix,
            obs=obs,
            sparse_format=sparse_format,
            probe_rows=probe_rows,
            identity=identity,
            peak_rss_reader=peak_rss_reader,
            target_object_bytes=target_object_bytes,
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
        except BlockPlanConflict:
            # Preserve zero remote writes until block count and budget accept.
            raise
        raw_planned_chunks = plan.get("planned_chunks")
        if not isinstance(raw_planned_chunks, list) or not raw_planned_chunks:
            raise GCSNativeWriterError("calibrated plan has no production chunks")
        if len(raw_planned_chunks) != forecast_logical_blocks:
            raise GCSNativeWriterError(
                "calibration does not match reviewed logical block count; "
                "choose a new reviewed launch packet"
            )
        max_retry_nnz, max_retry_rows = _planned_retry_bounds(
            matrix, raw_planned_chunks, sparse_format=sparse_format
        )
        operation_forecast = forecast_gcs_operation_cost(
            n_obs=shape[0],
            n_vars=shape[1],
            nnz=nnz,
            data_dtype=component_dtypes[0],
            index_dtype=component_dtypes[1],
            indptr_dtype=component_dtypes[2],
            sparse_format=sparse_format,
            logical_blocks=len(raw_planned_chunks),
            calibration_rows=probe_rows,
            target_object_bytes=target_object_bytes,
            max_recovery_attempts=max_recovery_attempts,
            max_retry_nnz=max_retry_nnz,
            max_retry_rows=max_retry_rows,
        )
        operation_budget = validate_gcs_operation_budget(
            operation_forecast,
            exception=operation_cost_exception,
            launch_context=launch_context,
        )
        plan["operation_forecast"] = operation_forecast
        plan["operation_budget"] = operation_budget
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
    if len(chunks) != forecast_logical_blocks:
        raise GCSNativeWriterError(
            "immutable plan does not match reviewed logical block count"
        )
    max_retry_nnz, max_retry_rows = _planned_retry_bounds(
        matrix, raw_chunks, sparse_format=sparse_format
    )
    operation_forecast = forecast_gcs_operation_cost(
        n_obs=shape[0],
        n_vars=shape[1],
        nnz=nnz,
        data_dtype=component_dtypes[0],
        index_dtype=component_dtypes[1],
        indptr_dtype=component_dtypes[2],
        sparse_format=sparse_format,
        logical_blocks=len(chunks),
        calibration_rows=probe_rows,
        target_object_bytes=target_object_bytes,
        max_recovery_attempts=max_recovery_attempts,
        max_retry_nnz=max_retry_nnz,
        max_retry_rows=max_retry_rows,
    )
    operation_budget = validate_gcs_operation_budget(
        operation_forecast,
        exception=operation_cost_exception,
        launch_context=launch_context,
    )
    if plan.get("operation_forecast") != operation_forecast:
        raise GCSNativeWriterError(
            "immutable plan operation forecast mismatch; choose a new revision"
        )
    if plan.get("operation_budget") != operation_budget:
        raise GCSNativeWriterError(
            "immutable plan operation budget mismatch; choose a new revision"
        )
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
    for completed_index in completed:
        operation_checkpoint_key = _path(
            candidate_prefix,
            "operation-checkpoints",
            f"chunk_{completed_index:06d}.json",
        )
        if not fs.exists(operation_checkpoint_key):
            raise GCSNativeWriterError(
                f"completed chunk {completed_index} lacks operation checkpoint"
            )
    cumulative_floor = _load_cumulative_operation_floor(fs, candidate_prefix, identity)
    operation_counter.add_cumulative_floor(cumulative_floor)
    per_attempt_class_a_reserve = max(
        1,
        math.ceil(
            _required_int(operation_forecast, "class_a_requests") / len(chunks)
        ),
    )
    per_attempt_class_b_reserve = max(
        1,
        math.ceil(
            _required_int(operation_forecast, "class_b_requests") / len(chunks)
        ),
    )
    operation_counter.prepay(
        class_a_requests=per_attempt_class_a_reserve,
        class_b_requests=per_attempt_class_b_reserve,
    )
    attempt_prefix = _path(candidate_prefix, "operation-attempts")
    attempt_index = len(fs.find(attempt_prefix))
    attempt_key = _path(attempt_prefix, f"attempt_{attempt_index:06d}.json")
    # Prepay the immutable evidence write so a crash cannot omit its own Class A
    # request from the cumulative value serialized in that evidence object.
    operation_counter.prepay(class_a_requests=1, class_b_requests=0)
    attempt_evidence = {
        "format": f"{FORMAT}.operation-attempt/v1",
        "attempt": attempt_index,
        "identity": identity,
        "cumulative_floor": cumulative_floor,
        "prepaid_until_next_durable_checkpoint": {
            "class_a_requests": per_attempt_class_a_reserve,
            "class_b_requests": per_attempt_class_b_reserve,
        },
        "cumulative": operation_counter.snapshot(),
    }
    _write_exclusive(fs, attempt_key, _json_bytes(attempt_evidence))
    attempt_checkpoint = runtime_operation_checkpoint(
        forecast=operation_forecast,
        logical_checkpoint=0,
        actual=operation_counter.snapshot(),
    )
    if attempt_checkpoint["status"] != "accepted":
        raise OperationBudgetExceeded(attempt_checkpoint)
    recovery_attempts_used = 0
    for completed_index in completed:
        completed_record = json.loads(
            _read_bytes(
                fs,
                _path(
                    candidate_prefix,
                    "chunk-records",
                    f"chunk_{completed_index:06d}.json",
                ),
            )
        )
        completed_recovery = completed_record.get("recovery_attempt")
        if isinstance(completed_recovery, int) and not isinstance(
            completed_recovery, bool
        ):
            recovery_attempts_used += completed_recovery + 1
    if recovery_attempts_used > max_recovery_attempts:
        raise GCSNativeWriterError(
            "completed chunks exceed the reviewed recovery-attempt budget"
        )
    records: list[dict[str, object]] = []
    bytes_written = 0
    bytes_read = 0
    peak_rss = 0
    for index, (start, end) in enumerate(chunks):
        resuming_completed = index in completed
        chunk_started = time.monotonic()
        record_key = _path(candidate_prefix, "chunk-records", f"chunk_{index:06d}.json")
        source_chunk = _materialize_rows(matrix, start, end, sparse_format)
        source_obs = obs.iloc[start:end]
        remote = None
        remote_obs = None
        bytes_read += sum(values.nbytes for values in _matrix_components(source_chunk))
        peak_rss = max(peak_rss, peak_rss_reader())
        if index not in completed:
            if fs.exists(record_key):
                raise GCSNativeWriterError(
                    f"chunk record appeared after resume preflight: {index}"
                )
            matrix_key, obs_key, recovery_attempt = _resumable_chunk_keys(
                fs,
                candidate_prefix,
                index,
                max_recovery_attempts=max_recovery_attempts,
            )
            if recovery_attempt is not None:
                required_attempts = recovery_attempt + 1
                if recovery_attempts_used + required_attempts > max_recovery_attempts:
                    raise GCSNativeWriterError(
                        "candidate exhausted the reviewed recovery-attempt budget"
                    )
                recovery_attempts_used += required_attempts
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
                    fs,
                    matrix_key,
                    source_chunk,
                    sparse_format,
                    target_object_bytes=target_object_bytes,
                )
                obs_object = _write_parquet(fs, obs_key, source_obs)
                chunk_bytes_written += int(obs_object["size"])
                bytes_written += chunk_bytes_written
            matrix_objects = _remote_object_evidence(fs, matrix_key)
            stored_bytes = sum(
                _required_int(item, "size") for item in matrix_objects
            ) + _required_int(obs_object, "size")
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
                "matrix_objects": matrix_objects,
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
            operation_checkpoint_key = _path(
                candidate_prefix,
                "operation-checkpoints",
                f"chunk_{index:06d}.json",
            )
            # The immutable checkpoint must include the request that persists it.
            operation_counter.prepay(class_a_requests=1, class_b_requests=0)
            operation_checkpoint = runtime_operation_checkpoint(
                forecast=operation_forecast,
                logical_checkpoint=index,
                actual=operation_counter.snapshot(),
                reserved={
                    "candidate_objects": 3,
                    "class_a_requests": 3,
                    "class_b_requests": 3,
                },
            )
            operation_checkpoint["identity"] = identity
            operation_checkpoint["cumulative"] = operation_counter.snapshot()
            checkpoint_object = _write_exclusive(
                fs,
                operation_checkpoint_key,
                _json_bytes(operation_checkpoint),
            )
            bytes_written += int(checkpoint_object["size"])
            if operation_checkpoint["status"] != "accepted":
                _write_exclusive(
                    fs,
                    failure_key,
                    _json_bytes(
                        {
                            "status": "failed",
                            "identity": identity,
                            "failed_chunk": index,
                            "evidence": operation_checkpoint,
                            "ended_at": time.time(),
                            "requires_new_revision": True,
                        }
                    ),
                )
                raise OperationBudgetExceeded(operation_checkpoint)
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
        if resuming_completed:
            _verify_completed_chunk(
                fs=fs,
                record=record,
                source_chunk=source_chunk,
                source_obs=source_obs,
                sparse_format=sparse_format,
                index=index,
            )
        validation = record.get("block_validation")
        if not isinstance(validation, dict) or validation.get("status") != "accepted":
            raise GCSNativeWriterError(
                f"chunk {index} lacks accepted measured block evidence; choose a new revision"
            )
        operation_checkpoint_key = _path(
            candidate_prefix,
            "operation-checkpoints",
            f"chunk_{index:06d}.json",
        )
        operation_checkpoint = json.loads(_read_bytes(fs, operation_checkpoint_key))
        if operation_checkpoint.get("identity") != identity:
            raise GCSNativeWriterError(
                f"chunk {index} operation checkpoint identity mismatch"
            )
        if operation_checkpoint.get("status") != "accepted":
            if not fs.exists(failure_key):
                _write_exclusive(
                    fs,
                    failure_key,
                    _json_bytes(
                        {
                            "status": "failed",
                            "identity": identity,
                            "failed_chunk": index,
                            "evidence": operation_checkpoint,
                            "ended_at": time.time(),
                            "requires_new_revision": True,
                        }
                    ),
                )
            raise OperationBudgetExceeded(operation_checkpoint)
        records.append(record)
        del source_chunk, source_obs, remote, remote_obs
        _release_block_memory()
    final_operation_checkpoint = runtime_operation_checkpoint(
        forecast=operation_forecast,
        logical_checkpoint=len(chunks),
        actual=operation_counter.snapshot(),
        reserved={
            "candidate_objects": 2,
            "class_a_requests": 2,
            "class_b_requests": 6,
        },
    )
    final_operation_checkpoint["identity"] = identity
    final_operation_checkpoint["cumulative"] = operation_counter.snapshot()
    if final_operation_checkpoint["status"] != "accepted":
        raise OperationBudgetExceeded(final_operation_checkpoint)
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
        "operation_forecast": operation_forecast,
        "operation_budget": operation_budget,
        "final_operation_checkpoint": final_operation_checkpoint,
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
    actual_operations = operation_counter.snapshot()
    metrics = GCSNativeMetrics(
        peak_rss_bytes=peak_rss,
        bytes_written=bytes_written
        + _required_int(var_object, "size")
        + _required_int(manifest_object, "size"),
        bytes_read=bytes_read,
        chunk_count=len(records),
        cache_cap_bytes=cache_cap_bytes,
        cache_bytes_after_cleanup=cleanup_cache(
            cache_dir, cache_cap_bytes=cache_cap_bytes
        ),
        actual_operations=actual_operations,
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
