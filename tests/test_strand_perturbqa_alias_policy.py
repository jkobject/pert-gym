"""Focused local-cache checks for the reviewed STRAND/PerturbQA alias policy.

The policy inputs are intentionally ignored local audit artifacts.  CI environments
without that metadata skip this integration test rather than downloading data.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BUILDER_PATH = (
    ROOT / "artifacts/schema_audit/build_model_ready_v2_strand_join_table_20260710.py"
)
POLICY_PATH = (
    ROOT
    / "artifacts/schema_audit/strand_perturbqa_guide_gap_classification_20260710_t_78160c62.json"
)
GUIDES_PATH = (
    ROOT / "artifacts/schema_audit/strand_perturbqa_guide_reference_20260708_v3.parquet"
)
CACHE_PATH = (
    ROOT
    / ".lamin-cache/lamindb/lamin-us-west-2/H7d9vxvceBoh/pert-gym/auxiliary/strand"
    / "perturbqa_mappings_20260703/k562_gw_mapping_full.json"
)

pytestmark = pytest.mark.skipif(
    not all(
        path.exists() for path in (BUILDER_PATH, POLICY_PATH, GUIDES_PATH, CACHE_PATH)
    ),
    reason="reviewed STRAND/PerturbQA local metadata fixtures are unavailable",
)


def _builder_module():
    spec = importlib.util.spec_from_file_location("strand_join_builder", BUILDER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reviewed_alias_policy_build_is_row_complete_and_excludes_elob() -> None:
    builder = _builder_module()

    rows, metadata = builder.build_table()

    counts = metadata["counts"]
    assert counts["unmatched_unique_perturbation_rows_before_resolution"] == 403
    assert counts["resolved_unique_perturbation_rows"] == 401
    assert counts["unresolved_unique_perturbation_rows_after_resolution"] == 2
    assert counts["reviewed_alias_policy_keys"] == 157
    assert counts["reviewed_alias_policy_per_dataset"] == {
        "hepg2": {"accepted_alias_rows": 53, "excluded_rows": 0},
        "jurkat": {"accepted_alias_rows": 66, "excluded_rows": 0},
        "k562": {"accepted_alias_rows": 216, "excluded_rows": 2},
        "rpe1": {"accepted_alias_rows": 66, "excluded_rows": 0},
    }
    excluded = metadata["loader_exclusions"]["unresolved_by_file"]
    assert {
        (mapping_file, item["perturbation"], item["classification"])
        for mapping_file, items in excluded.items()
        for item in items
    } == {
        ("k562-de.csv", "ELOB", "ambiguous_multi_target"),
        ("k562-dir.csv", "ELOB", "ambiguous_multi_target"),
    }
    assert not any(
        row["perturbqa_perturbation"] == "ELOB"
        and row["perturbqa_mapping_file"].startswith("k562-")
        for row in rows
    )


def test_reviewed_alias_policy_rejects_schema_and_sha_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = _builder_module()
    guides = builder.pd.read_parquet(builder.GUIDE_REFERENCE)

    monkeypatch.setattr(builder, "CLASSIFICATION_SHA256", "0" * 64)
    with pytest.raises(ValueError, match="SHA256"):
        builder._load_reviewed_alias_policy(guides)

    monkeypatch.setattr(
        builder, "CLASSIFICATION_SHA256", builder._sha256(builder.GAP_CLASSIFICATION)
    )
    monkeypatch.setattr(builder, "CLASSIFICATION_SCHEMA", "unexpected.v1")
    with pytest.raises(ValueError, match="schema"):
        builder._load_reviewed_alias_policy(guides)
