import json
from pathlib import Path

import pandas as pd
import pytest

from tools.depmap_chronos_contract import (
    CANONICAL_RESPONSE_DIRECTIONS,
    CHRONOS_SOURCE_ACCESSION,
    DEPMAP_26Q1_BASELINE_PREFIX,
    DEPMAP_26Q1_RELEASE,
    LOWER_MORE_DEPENDENT,
    annotate_raw_chronos_gene_effect,
    apply_release_locked_baseline_policy,
    validate_chronos_gene_effect_rows,
    write_chronos_coverage_artifact,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKED_IN_COVERAGE_ARTIFACT = (
    REPO_ROOT
    / "artifacts/schema_audit/model_ready_v2_depmap_chronos_coverage_20260711.json"
)


def test_raw_chronos_gene_effect_keeps_negative_values_and_canonical_direction():
    source = pd.DataFrame(
        {
            "model_id": [" ACH-000001 ", "ACH-000002"],
            "response_value": [-1.25, -0.11],
        }
    )

    annotated = annotate_raw_chronos_gene_effect(source)

    assert annotated["response_value"].tolist() == [-1.25, -0.11]
    assert annotated["response_direction"].tolist() == [
        LOWER_MORE_DEPENDENT,
        LOWER_MORE_DEPENDENT,
    ]
    assert annotated["model_id"].tolist() == ["ACH-000001", "ACH-000002"]
    assert annotated["depmap_id"].tolist() == annotated["model_id"].tolist()
    assert annotated["baseline_join_id"].tolist() == annotated["model_id"].tolist()
    validate_chronos_gene_effect_rows(annotated)

    invalid = annotated.assign(response_direction="unknown_direction")
    with pytest.raises(ValueError, match="Unknown response_direction"):
        validate_chronos_gene_effect_rows(invalid)
    assert LOWER_MORE_DEPENDENT in CANONICAL_RESPONSE_DIRECTIONS


def test_chronos_identity_normalization_does_not_require_dataframe_map(monkeypatch):
    annotated = annotate_raw_chronos_gene_effect(
        pd.DataFrame({"model_id": [" ACH-000001 "], "response_value": [-1.25]})
    )

    def fail_dataframe_map(*_args, **_kwargs):
        raise AssertionError("DataFrame.map is unavailable on pandas 2.0")

    monkeypatch.setattr(pd.DataFrame, "map", fail_dataframe_map, raising=False)

    validate_chronos_gene_effect_rows(annotated)


@pytest.mark.parametrize(
    "column",
    [
        "response_direction",
        "response_metric",
        "score_source",
        "response_transform",
    ],
)
def test_chronos_validator_rejects_null_required_metadata_per_row(column):
    annotated = annotate_raw_chronos_gene_effect(
        pd.DataFrame(
            {
                "model_id": ["ACH-000001", "ACH-000002"],
                "response_value": [-1.25, -0.11],
            }
        )
    )
    annotated.loc[1, column] = None

    with pytest.raises(ValueError, match=f"Chronos {column} must be non-null"):
        validate_chronos_gene_effect_rows(annotated)


def test_release_locked_join_marks_unmatched_without_aliasing_or_dropping():
    chronos = annotate_raw_chronos_gene_effect(
        pd.DataFrame(
            {
                "model_id": ["ACH-000001", "ACH-000002", "ACH-000003"],
                "response_value": [-1.25, -0.11, -0.02],
            }
        )
    )

    classified = apply_release_locked_baseline_policy(
        chronos,
        baseline_model_ids=["ACH-000001", "ACH-000003"],
    )

    assert classified["baseline_conditioned_promotion"].tolist() == [True, False, True]
    assert classified["baseline_join_status"].tolist() == [
        "matched_same_release",
        "unmatched_same_release",
        "matched_same_release",
    ]
    unmatched = classified.loc[classified["model_id"] == "ACH-000002"].iloc[0]
    assert unmatched["response_value"] == -0.11
    assert not unmatched["baseline_conditioned_promotion"]
    assert (
        unmatched["baseline_conditioned_exclusion_reason"]
        == "missing_exact_26Q1_baseline_ModelID"
    )

    mismatched_identity = chronos.copy()
    mismatched_identity.loc[0, "depmap_id"] = "ACH-999999"
    with pytest.raises(
        ValueError, match="must agree by exact normalized string equality"
    ):
        apply_release_locked_baseline_policy(
            mismatched_identity,
            baseline_model_ids=["ACH-000001"],
        )

    with pytest.raises(
        ValueError, match="Cross-release baseline substitution is forbidden"
    ):
        apply_release_locked_baseline_policy(
            chronos,
            baseline_model_ids=["ACH-000001"],
            baseline_release="DepMap Public 25Q2",
        )
    with pytest.raises(ValueError, match="release-locked"):
        apply_release_locked_baseline_policy(
            chronos,
            baseline_model_ids=["ACH-000001"],
            baseline_prefix="depmap_ccle/25q2",
        )


def test_coverage_artifact_is_deterministic_and_exposes_unmatched_sidecar(tmp_path):
    source_ids = [f"ACH-{index:06d}" for index in range(1208)]
    baseline_ids = source_ids[:1140]
    artifact_path = tmp_path / "chronos_coverage.json"

    written_path, sidecar_path = write_chronos_coverage_artifact(
        source_ids, baseline_ids, artifact_path
    )

    payload = json.loads(written_path.read_text(encoding="utf-8"))
    assert payload["source"]["accession"] == CHRONOS_SOURCE_ACCESSION
    assert payload["source"]["release"] == DEPMAP_26Q1_RELEASE
    assert payload["source"]["response_direction"] == LOWER_MORE_DEPENDENT
    assert payload["source"]["numeric_transform"] == "none"
    assert payload["baseline"]["lamin_prefix"] == DEPMAP_26Q1_BASELINE_PREFIX
    assert payload["coverage"] == {
        "source_model_ids": 1208,
        "matched_model_ids": 1140,
        "unmatched_model_ids": 68,
        "unmatched_model_ids_sidecar": "chronos_coverage_unmatched_model_ids.tsv",
    }
    sidecar_lines = sidecar_path.read_text(encoding="utf-8").splitlines()
    assert sidecar_lines[0] == "ModelID\treason"
    assert len(sidecar_lines) == 69
    assert sidecar_lines[1].startswith("ACH-001140\t")


def test_checked_in_coverage_decision_records_reviewed_26q1_counts():
    payload = json.loads(CHECKED_IN_COVERAGE_ARTIFACT.read_text(encoding="utf-8"))

    assert payload["source"] == {
        "accession": CHRONOS_SOURCE_ACCESSION,
        "numeric_transform": "none",
        "release": DEPMAP_26Q1_RELEASE,
        "response_direction": LOWER_MORE_DEPENDENT,
        "response_metric": "GeneEffect",
        "response_transform": "raw_Chronos_GeneEffect",
        "score_source": "Chronos",
    }
    assert payload["baseline"] == {
        "cross_release_policy": "forbidden_without_reviewed_versioned_mapping",
        "join_policy": "exact_normalized_ModelID_equality",
        "lamin_prefix": DEPMAP_26Q1_BASELINE_PREFIX,
        "release": DEPMAP_26Q1_RELEASE,
    }
    assert payload["coverage"] == {
        "matched_model_ids": 1140,
        "source_model_ids": 1208,
        "unmatched_model_ids": 68,
    }
    assert (
        payload["deterministic_unmatched_sidecar"]["unmatched_reason"]
        == "missing_exact_26Q1_baseline_ModelID"
    )
