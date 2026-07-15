from __future__ import annotations

import pandas as pd
import pytest

from pert_gym.parquet_frame_parity import (
    assert_parquet_frame_parity,
    parquet_bytes,
    parquet_frame_parity,
    read_parquet_bytes,
)


def safe_category_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "hash.ID": pd.Categorical([2, 6, 5], categories=[2, 5, 6]),
            "cluster_l1": pd.Categorical([2, 1, 0], categories=[0, 1, 2]),
            "score": [1.25, 2.5, 3.75],
        },
        index=pd.Index(["cell-a", "cell-b", "cell-c"], name="cell_id"),
    )


def assert_rejected(source: pd.DataFrame, readback: pd.DataFrame) -> dict[str, object]:
    verdict = parquet_frame_parity(source, readback)
    assert verdict["passed"] is False
    with pytest.raises(ValueError, match="Parquet frame parity failed"):
        assert_parquet_frame_parity(source, readback)
    return verdict


def test_actual_parquet_round_trip_allows_only_integer_category_decoding() -> None:
    source = safe_category_frame()
    readback = read_parquet_bytes(parquet_bytes(source))

    assert not readback.equals(source), "reproduce the old dtype-sensitive failure"
    verdict = assert_parquet_frame_parity(source, readback)

    assert verdict["passed"] is True
    assert verdict["source_dtypes"] == {
        "hash.ID": "category",
        "cluster_l1": "category",
        "score": "float64",
    }
    assert verdict["readback_dtypes"] == {
        "hash.ID": "int64",
        "cluster_l1": "int64",
        "score": "float64",
    }
    assert verdict["semantic_sha256"]["source"] == verdict["semantic_sha256"][
        "readback"
    ]
    assert verdict["allowlisted_normalizations"] == [
        {
            "column": "hash.ID",
            "source_dtype": "category",
            "readback_dtype": "int64",
            "decoded_dtype": "int64",
            "decoded_domain": [2, 5, 6],
            "rule": "category[int]->identical-integer-dtype/v1",
        },
        {
            "column": "cluster_l1",
            "source_dtype": "category",
            "readback_dtype": "int64",
            "decoded_dtype": "int64",
            "decoded_domain": [0, 1, 2],
            "rule": "category[int]->identical-integer-dtype/v1",
        },
    ]


def test_changed_value_fails_closed() -> None:
    source = safe_category_frame()
    readback = read_parquet_bytes(parquet_bytes(source))
    readback.loc["cell-b", "score"] = 2.75

    verdict = assert_rejected(source, readback)
    assert "value_mismatch:score" in verdict["mismatches"]


def test_null_mask_drift_fails_closed() -> None:
    source = safe_category_frame()
    readback = read_parquet_bytes(parquet_bytes(source))
    readback.loc["cell-a", "score"] = None

    verdict = assert_rejected(source, readback)
    assert "null_mask_mismatch:score" in verdict["mismatches"]


def test_reordered_rows_and_index_fail_closed() -> None:
    source = safe_category_frame()
    readback = read_parquet_bytes(parquet_bytes(source)).iloc[::-1]

    verdict = assert_rejected(source, readback)
    assert "index_mismatch" in verdict["mismatches"]


def test_reordered_columns_fail_closed() -> None:
    source = safe_category_frame()
    readback = read_parquet_bytes(parquet_bytes(source))[
        ["cluster_l1", "hash.ID", "score"]
    ]

    verdict = assert_rejected(source, readback)
    assert "column_order_mismatch" in verdict["mismatches"]


@pytest.mark.parametrize(
    "source_values,source_categories,readback_values",
    [
        ([1, 2, 1], [1, 2], [10, 20, 10]),
        ([1, 2, 1], [1, 2, 3], [1, 2, 1]),
    ],
    ids=["category-label-drift", "category-domain-drift"],
)
def test_category_label_or_domain_drift_fails_closed(
    source_values: list[int],
    source_categories: list[int],
    readback_values: list[int],
) -> None:
    source = pd.DataFrame(
        {"category": pd.Categorical(source_values, categories=source_categories)}
    )
    readback = pd.DataFrame(
        {"category": pd.Series(readback_values, dtype="int64")}
    )

    verdict = assert_rejected(source, readback)
    assert not verdict["allowlisted_normalizations"]


def test_numeric_precision_drift_fails_closed() -> None:
    source = pd.DataFrame({"value": pd.Series([1.5, 2.5], dtype="float64")})
    readback = source.astype({"value": "float32"})

    verdict = assert_rejected(source, readback)
    assert "dtype_mismatch:value:float64->float32" in verdict["mismatches"]


def test_non_allowlisted_dtype_drift_fails_closed() -> None:
    source = pd.DataFrame({"value": pd.Series([1, 2], dtype="int64")})
    readback = source.astype({"value": "object"})

    verdict = assert_rejected(source, readback)
    assert "dtype_mismatch:value:int64->object" in verdict["mismatches"]


def test_object_scalar_type_drift_fails_closed() -> None:
    source = pd.DataFrame({"value": pd.Series([1], dtype="object")})
    readback = pd.DataFrame({"value": pd.Series([True], dtype="object")})

    verdict = assert_rejected(source, readback)
    assert "value_mismatch:value" in verdict["mismatches"]


def test_index_name_drift_fails_closed() -> None:
    source = safe_category_frame()
    readback = read_parquet_bytes(parquet_bytes(source))
    readback.index.name = "different"

    verdict = assert_rejected(source, readback)
    assert "index_mismatch" in verdict["mismatches"]
