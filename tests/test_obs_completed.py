from __future__ import annotations

import json
from pathlib import Path

from tools.score_obs_completed import (
    load_contract,
    score_manifest,
    write_result,
)

CONTRACT_PATH = Path("config/obs_completed_contract_v1.json")


def manifest_row(
    logical_dataset: str = "dataset/a", member: str = "dataset/a/obs.parquet"
) -> dict:
    return {
        "logical_dataset": logical_dataset,
        "artifact_key": member,
        "n_obs": "2",
        "modality": "scRNA-seq",
        "assay": "Perturb-seq",
        "x_semantics": "raw_counts",
        "control_availability": "strict_control_available",
    }


def complete_evidence(contract: dict, logical_dataset: str = "dataset/a") -> dict:
    fields = {
        field: {
            "state": "present",
            "source": field,
            "non_null_rows": 2,
            "total_rows": 2,
        }
        for field in contract["canonical_obs_columns"]
    }
    return {
        "logical_dataset": logical_dataset,
        "members": {
            "dataset/a/obs.parquet": {
                "fields": fields,
                "identity": {
                    "obs_uuid_present": True,
                    "obs_uuid_unique_within_member": True,
                    "obs_uuid_global_unique": True,
                    "original_obs_index_preserved": True,
                    "row_count_preserved": True,
                    "row_order_preserved": True,
                },
            }
        },
        "dataset_checks": {
            "modality_assay_x_semantics": True,
            "modality_required_fields": True,
            "control_semantics": True,
            "derived_applicability_declared": True,
            "derived_outputs_complete": True,
            "combination_semantics": True,
            "quality_flag": "ok",
            "citations": ["doi:10.example/example"],
            "provenance": ["source manifest v1"],
            "fabricated_values": False,
        },
        "var_ensembl_species": {
            "biological_features_total": 100,
            "stable_ensembl_id_features": 100,
            "correct_species_features": 100,
            "provenance": ["reviewed var audit v1"],
        },
    }


def test_complete_evidence_scores_true_with_explicit_denominators() -> None:
    contract = load_contract(CONTRACT_PATH)

    result = score_manifest([manifest_row()], [complete_evidence(contract)], contract)

    dataset = result["datasets"][0]
    assert dataset["OBS_COMPLETED"] == "true"
    assert dataset["failed_checks"] == []
    assert dataset["blocked_checks"] == []
    assert dataset["denominators"]["members_total"] == 1
    assert dataset["denominators"]["rows_manifest"] == 2
    assert dataset["denominators"]["canonical_fields_total"] == 42
    assert dataset["denominators"]["canonical_fields_applicable"] == 42
    assert dataset["denominators"]["canonical_fields_covered"] == 42
    assert dataset["VAR_ENSEMBL_SPECIES_COMPLETED"] == "true"


def test_missing_expected_field_is_false_but_not_applicable_is_excluded() -> None:
    contract = load_contract(CONTRACT_PATH)
    evidence = complete_evidence(contract)
    evidence["members"]["dataset/a/obs.parquet"]["fields"]["dose"] = {
        "state": "not_applicable",
        "source": "curation: no chemical dose",
    }
    evidence["members"]["dataset/a/obs.parquet"]["fields"]["organism"] = {
        "state": "missing",
        "source": "audit:v1",
    }

    dataset = score_manifest([manifest_row()], [evidence], contract)["datasets"][0]

    assert dataset["OBS_COMPLETED"] == "false"
    assert "fields.organism.missing" in dataset["failed_checks"]
    assert all("dose" not in item for item in dataset["failed_checks"])
    assert dataset["denominators"]["canonical_fields_applicable"] == 41
    assert dataset["denominators"]["canonical_fields_covered"] == 40


def test_not_applicable_field_requires_nonblank_provenance() -> None:
    contract = load_contract(CONTRACT_PATH)
    evidence = complete_evidence(contract)
    evidence["members"]["dataset/a/obs.parquet"]["fields"]["molecule_sequence"] = {
        "state": "not_applicable"
    }

    dataset = score_manifest([manifest_row()], [evidence], contract)["datasets"][0]

    assert dataset["OBS_COMPLETED"] == "false"
    assert "fields.molecule_sequence.missing_source" in dataset["failed_checks"]
    assert dataset["denominators"]["canonical_fields_applicable"] == 41


def test_alias_requires_provenance_and_full_non_null_coverage() -> None:
    contract = load_contract(CONTRACT_PATH)
    evidence = complete_evidence(contract)
    fields = evidence["members"]["dataset/a/obs.parquet"]["fields"]
    fields["cell_type"] = {
        "state": "alias_only",
        "source": "celltype",
        "non_null_rows": 1,
        "total_rows": 2,
    }
    fields["tissue_type"] = {
        "state": "alias_only",
        "source": "",
        "non_null_rows": 2,
        "total_rows": 2,
    }

    dataset = score_manifest([manifest_row()], [evidence], contract)["datasets"][0]

    assert dataset["OBS_COMPLETED"] == "false"
    assert "fields.cell_type.incomplete_coverage:1/2" in dataset["failed_checks"]
    assert "fields.tissue_type.missing_source" in dataset["failed_checks"]


def test_manifest_without_obs_evidence_is_blocked_not_missing() -> None:
    contract = load_contract(CONTRACT_PATH)
    rows = [
        manifest_row("dataset/a"),
        manifest_row("dataset/b", "dataset/b/obs.parquet"),
    ]

    result = score_manifest(rows, [], contract)

    assert result["summary"] == {
        "datasets_total": 2,
        "true": 0,
        "false": 0,
        "blocked": 2,
    }
    assert all(item["OBS_COMPLETED"] == "blocked" for item in result["datasets"])
    assert "member_evidence.missing" in result["datasets"][0]["blocked_checks"]


def test_cli_output_never_treats_storage_contract_terms_as_checks(
    tmp_path: Path,
) -> None:
    contract = load_contract(CONTRACT_PATH)
    result = score_manifest([manifest_row()], [complete_evidence(contract)], contract)
    rendered = json.dumps(result)

    for forbidden in contract["explicitly_excluded_criteria"]:
        assert forbidden not in result["datasets"][0]["checks"]
    assert "OBS_COMPLETED" in rendered


def test_contract_matches_authoritative_obs_field_correction() -> None:
    contract = load_contract(CONTRACT_PATH)
    forbidden = {
        "perturbation_target",
        "perturbation_target_id",
        "timepoint_unit",
        "model_ready",
        "loader_projectable",
        "harmonization_level",
        "duplicate_status",
        "guide_id",
    }

    assert len(contract["canonical_obs_columns"]) == 42
    assert forbidden.isdisjoint(contract["canonical_obs_columns"])
    assert forbidden.isdisjoint(contract["required_dataset_checks"])
    assert "guide_sequence" in contract["canonical_obs_columns"]
    assert "molecule_sequence" in contract["canonical_obs_columns"]
    assert "molecule_sequence" in contract["combination_suffix_fields"]


def test_var_verdict_is_separate_and_cannot_change_obs_verdict() -> None:
    contract = load_contract(CONTRACT_PATH)
    evidence = complete_evidence(contract)
    evidence["var_ensembl_species"]["stable_ensembl_id_features"] = 99

    result = score_manifest([manifest_row()], [evidence], contract)
    dataset = result["datasets"][0]

    assert dataset["OBS_COMPLETED"] == "true"
    assert dataset["VAR_ENSEMBL_SPECIES_COMPLETED"] == "false"
    assert dataset["var_ensembl_species_failed_checks"] == [
        "stable_ensembl_id_features.incomplete:99/100"
    ]
    assert dataset["var_ensembl_species_denominators"] == {
        "biological_features_total": 100,
        "correct_species_features": 100,
        "stable_ensembl_id_features": 99,
    }


def test_missing_var_evidence_is_blocked_without_blocking_obs() -> None:
    contract = load_contract(CONTRACT_PATH)
    evidence = complete_evidence(contract)
    del evidence["var_ensembl_species"]

    dataset = score_manifest([manifest_row()], [evidence], contract)["datasets"][0]

    assert dataset["OBS_COMPLETED"] == "true"
    assert dataset["VAR_ENSEMBL_SPECIES_COMPLETED"] == "blocked"
    assert dataset["var_ensembl_species_blocked_checks"] == [
        "var_ensembl_species.evidence_missing"
    ]


def test_var_provenance_rejects_blank_only_strings() -> None:
    contract = load_contract(CONTRACT_PATH)
    evidence = complete_evidence(contract)
    evidence["var_ensembl_species"]["provenance"] = ["   "]

    dataset = score_manifest([manifest_row()], [evidence], contract)["datasets"][0]

    assert dataset["OBS_COMPLETED"] == "true"
    assert dataset["VAR_ENSEMBL_SPECIES_COMPLETED"] == "false"
    assert dataset["var_ensembl_species_failed_checks"] == [
        "var_ensembl_species.provenance.empty"
    ]


def test_write_result_creates_output_parent_directory(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "score.json"

    write_result({"OBS_COMPLETED": "blocked"}, output)

    assert json.loads(output.read_text()) == {"OBS_COMPLETED": "blocked"}
