"""Fail-closed semantic parity checks for pandas Parquet round trips."""

from __future__ import annotations

import hashlib
import io
import json
from typing import Any

import numpy as np
import pandas as pd
from pandas.api.types import is_integer_dtype, is_object_dtype

_NORMALIZATION_RULE = "category[int]->identical-integer-dtype/v1"


def parquet_bytes(frame: pd.DataFrame) -> bytes:
    """Serialize a frame using the writer's production Parquet path."""
    buffer = io.BytesIO()
    frame.to_parquet(buffer)
    return buffer.getvalue()


def read_parquet_bytes(payload: bytes) -> pd.DataFrame:
    """Deserialize bytes using the writer's production Parquet path."""
    return pd.read_parquet(io.BytesIO(payload))


def _python_scalar(value: Any) -> Any:
    return value.item() if isinstance(value, np.generic) else value


def _dtype_map(frame: pd.DataFrame) -> dict[str, str]:
    return {str(column): str(dtype) for column, dtype in frame.dtypes.items()}


def _values_identical(source: pd.Series, readback: pd.Series) -> bool:
    if not source.equals(readback):
        return False
    if not is_object_dtype(source.dtype):
        return True
    for expected, actual, is_null in zip(
        source.tolist(), readback.tolist(), source.isna().tolist(), strict=True
    ):
        if not is_null and type(expected) is not type(actual):
            return False
    return True


def _integer_category_domain(series: pd.Series) -> list[Any] | None:
    if not isinstance(series.dtype, pd.CategoricalDtype):
        return None
    categories = series.cat.categories
    if not is_integer_dtype(categories.dtype):
        return None
    return [_python_scalar(value) for value in sorted(categories.tolist())]


def _allowlisted_category_normalization(
    source: pd.Series, readback: pd.Series, column: object
) -> dict[str, Any] | None:
    domain = _integer_category_domain(source)
    if domain is None or source.isna().any() or readback.isna().any():
        return None

    decoded_dtype = str(source.cat.categories.dtype)
    if str(readback.dtype) != decoded_dtype:
        return None

    readback_domain = [
        _python_scalar(value) for value in sorted(readback.unique().tolist())
    ]
    if domain != readback_domain:
        return None

    decoded = source.astype(source.cat.categories.dtype)
    if not np.array_equal(decoded.to_numpy(), readback.to_numpy()):
        return None

    return {
        "column": str(column),
        "source_dtype": str(source.dtype),
        "readback_dtype": str(readback.dtype),
        "decoded_dtype": decoded_dtype,
        "decoded_domain": domain,
        "rule": _NORMALIZATION_RULE,
    }


def _semantic_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy(deep=False)
    for column in normalized.columns:
        series = normalized[column]
        domain = _integer_category_domain(series)
        if domain is not None and not series.isna().any():
            normalized[column] = series.astype(series.cat.categories.dtype)
    return normalized


def semantic_frame_sha256(frame: pd.DataFrame) -> str:
    """Hash ordered values and schema after the sole reviewed normalization."""
    normalized = _semantic_frame(frame)
    value_hashes = pd.util.hash_pandas_object(
        normalized, index=True, categorize=True
    ).to_numpy(dtype=np.uint64)
    index = normalized.index
    schema = {
        "columns": [repr(column) for column in normalized.columns],
        "dtypes": [str(dtype) for dtype in normalized.dtypes],
        "index_class": type(index).__name__,
        "index_names": [repr(name) for name in index.names],
        "index_dtypes": (
            [str(level.dtype) for level in index.levels]
            if isinstance(index, pd.MultiIndex)
            else [str(index.dtype)]
        ),
        "normalization_rule": _NORMALIZATION_RULE,
    }
    payload = np.ascontiguousarray(value_hashes).tobytes(order="C") + json.dumps(
        schema, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def parquet_frame_parity(
    source: pd.DataFrame, readback: pd.DataFrame
) -> dict[str, Any]:
    """Return a diagnostic verdict without weakening any non-allowlisted drift."""
    mismatches: list[str] = []
    normalizations: list[dict[str, Any]] = []

    if source.shape != readback.shape:
        mismatches.append(f"shape_mismatch:{source.shape!r}->{readback.shape!r}")
    if list(source.columns) != list(readback.columns):
        mismatches.append("column_order_mismatch")
    if not source.index.identical(readback.index):
        mismatches.append("index_mismatch")
    if not source.columns.is_unique or not readback.columns.is_unique:
        mismatches.append("duplicate_columns_not_supported")

    can_compare_columns = (
        source.shape == readback.shape
        and list(source.columns) == list(readback.columns)
        and source.columns.is_unique
        and readback.columns.is_unique
    )
    if can_compare_columns:
        for column in source.columns:
            expected = source[column]
            actual = readback[column]
            null_masks_equal = np.array_equal(
                expected.isna().to_numpy(), actual.isna().to_numpy()
            )
            if not null_masks_equal:
                mismatches.append(f"null_mask_mismatch:{column}")

            if str(expected.dtype) == str(actual.dtype):
                if null_masks_equal and not _values_identical(expected, actual):
                    mismatches.append(f"value_mismatch:{column}")
                continue

            normalization = _allowlisted_category_normalization(
                expected, actual, column
            )
            if null_masks_equal and normalization is not None:
                normalizations.append(normalization)
                continue

            mismatches.append(
                f"dtype_mismatch:{column}:{expected.dtype}->{actual.dtype}"
            )

    source_hash = semantic_frame_sha256(source)
    readback_hash = semantic_frame_sha256(readback)
    if not mismatches and source_hash != readback_hash:
        mismatches.append("semantic_hash_mismatch")

    return {
        "passed": not mismatches,
        "mismatches": mismatches,
        "source_shape": list(source.shape),
        "readback_shape": list(readback.shape),
        "source_dtypes": _dtype_map(source),
        "readback_dtypes": _dtype_map(readback),
        "allowlisted_normalizations": normalizations,
        "semantic_sha256": {"source": source_hash, "readback": readback_hash},
    }


def assert_parquet_frame_parity(
    source: pd.DataFrame, readback: pd.DataFrame
) -> dict[str, Any]:
    """Return a passing verdict or raise with deterministic diagnostics."""
    verdict = parquet_frame_parity(source, readback)
    if not verdict["passed"]:
        raise ValueError(
            "Parquet frame parity failed: "
            + json.dumps(verdict, sort_keys=True, separators=(",", ":"))
        )
    return verdict
