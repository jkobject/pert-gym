from __future__ import annotations

import importlib.util
from pathlib import Path

import anndata as ad
import numpy as np
import pandas as pd

SCRIPT = (
    Path(__file__).parents[1] / "artifacts/schema_audit/real_dataset_curation_20260723/"
    "cellarity_public_collection/t_9c09e453/inspect_sources.py"
)
SPEC = importlib.util.spec_from_file_location("cellarity_source_inspection", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
inspection = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(inspection)


def test_member_inventory_is_exact_dataset_denominator() -> None:
    assert len(inspection.MEMBERS) == 10
    assert sum(item["n_obs"] for item in inspection.MEMBERS) == 2_212_441
    assert len({item["prefix"] for item in inspection.MEMBERS}) == 10
    assert {item["accession"] for item in inspection.MEMBERS} == {
        "GSE305370",
        "GSE305979",
        "GSE306429",
    }


def test_hashes_are_order_sensitive_or_multiset_invariant_as_declared() -> None:
    left = pd.Index(["cell-b", "cell-a", "cell-c"])
    right = pd.Index(["cell-a", "cell-b", "cell-c"])
    assert inspection.ordered_sha256(left) != inspection.ordered_sha256(right)
    assert inspection.multiset_sha256(left) == inspection.multiset_sha256(right)
    assert inspection.multiset_sha256(left) != inspection.multiset_sha256(
        pd.Index(["cell-a", "cell-b", "cell-d"])
    )


def test_normalized_column_hash_preserves_order_and_missingness() -> None:
    source = pd.Series(["A", pd.NA, "B"], dtype="string")
    same = pd.Series(["A", None, "B"], dtype="object")
    reordered = pd.Series(["B", None, "A"], dtype="object")
    assert inspection.normalized_sha256(source) == inspection.normalized_sha256(same)
    assert inspection.normalized_sha256(source) != inspection.normalized_sha256(
        reordered
    )


def test_source_h5ad_is_inspected_one_column_at_a_time(tmp_path: Path) -> None:
    path = tmp_path / "source.h5ad"
    ad.AnnData(
        X=np.ones((3, 2)),
        obs=pd.DataFrame(
            {
                "cell_type": pd.Categorical(["T", "B", "T"]),
                "time": pd.Series([0, 1, 2], index=["c1", "c2", "c3"]),
            },
            index=["c1", "c2", "c3"],
        ),
        var=pd.DataFrame(index=["g1", "g2"]),
    ).write_h5ad(path)

    result = inspection.inspect_source_h5ad(path)

    assert result["obs_summary"]["rows"] == 3
    assert result["obs_summary"]["columns"]["cell_type"]["samples"] == ["T", "B"]
    assert result["obs_index"].tolist() == ["c1", "c2", "c3"]
    assert result["n_vars"] == 2
    assert result["var_index_unique"] is True
