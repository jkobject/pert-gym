import json
from pathlib import Path

import pandas as pd
import pytest

from tools.depmap_genetic_dependencies import (
    BASELINE_RNA_CONTRACT,
    DEPMAP_DOWNLOADS_API,
    DEPMAP_GENETIC_RELEASE,
    ESSENTIALITY_OBS_REQUIRED_COLUMNS,
    ESSENTIALITY_VAR_REQUIRED_COLUMNS,
    SANGER_SCORE_AUX_CONTRACT,
    DepMapDownloadFile,
    depmap_matrix_to_long_table,
    depmap_matrix_to_obs_var,
    durable_manifest_entry,
    infer_depmap_readout_modality,
    parse_depmap_gene_label,
    redact_transient_download_url,
    validate_baseline_rna_obs_contract,
    validate_essentiality_obs_var_contract,
    write_obs_var,
    write_resolved_manifest,
)
from tools.plan_phase3_ingestion import DATASETS


def test_parse_depmap_gene_label():
    assert parse_depmap_gene_label("A1BG (1)") == ("A1BG", "1")
    assert parse_depmap_gene_label("MALFORMED") == ("MALFORMED", None)


def test_durable_manifest_redacts_transient_signed_url_fields(tmp_path: Path, monkeypatch):
    signed_url = (
        "https://storage.googleapis.com/depmap-external-downloads/path/CRISPRGeneEffect.csv"
        "?response-content-disposition=attachment%3B+filename%3D%22CRISPRGeneEffect.csv%22"
        "&GoogleAccessId=depmap-external-downloads@example.iam.gserviceaccount.com"
        "&Expires=1783441968"
        "&Signature=abc123"
        "&userProject=broad-achilles"
    )
    meta = DepMapDownloadFile(
        release=DEPMAP_GENETIC_RELEASE,
        filename="CRISPRGeneEffect.csv",
        url=signed_url,
        md5_hash="deadbeef",
        content_length=123,
        accessible=True,
    )

    assert redact_transient_download_url(signed_url) == (
        "https://storage.googleapis.com/depmap-external-downloads/path/CRISPRGeneEffect.csv"
    )
    entry = durable_manifest_entry(meta)
    assert entry == {
        "release": DEPMAP_GENETIC_RELEASE,
        "filename": "CRISPRGeneEffect.csv",
        "md5_hash": "deadbeef",
        "content_length": 123,
        "accessible": True,
        "download_api_url": DEPMAP_DOWNLOADS_API,
        "source_url_redacted": (
            "https://storage.googleapis.com/depmap-external-downloads/path/CRISPRGeneEffect.csv"
        ),
        "download_url_note": entry["download_url_note"],
    }
    assert "transient signed GCS URLs" in str(entry["download_url_note"])
    assert "url" not in entry

    monkeypatch.setattr(
        "tools.depmap_genetic_dependencies.resolve_depmap_files",
        lambda release, probe: {"CRISPRGeneEffect.csv": meta},
    )
    output = tmp_path / "manifest.json"
    write_resolved_manifest(output, probe=False)
    manifest_text = output.read_text(encoding="utf-8")
    for forbidden in ("Signature=", "GoogleAccessId=", "Expires=", "userProject="):
        assert forbidden not in manifest_text
    manifest = json.loads(manifest_text)
    assert manifest["download_api_url"] == DEPMAP_DOWNLOADS_API
    assert manifest["artifact_contract"] == "essentiality_obs_var_only"
    assert manifest["essentiality_expected_outputs"] == ["obs.parquet", "var.parquet"]
    assert "X.h5ad for dependency/gene-effect scores" in manifest["forbidden_outputs"]
    assert manifest["baseline_rna_contract"]["x_semantics"].startswith("RNA expression")
    assert manifest["files"]["CRISPRGeneEffect.csv"]["source_url_redacted"].endswith(
        "/CRISPRGeneEffect.csv"
    )


def test_depmap_matrix_to_obs_var_essentiality_contract(tmp_path: Path):
    matrix = tmp_path / "CRISPRGeneEffect.csv"
    matrix.write_text(
        ",A1BG (1),TP53 (7157)\n"
        "ACH-000001,-0.11,-1.25\n"
        "ACH-000002,0.05,-0.80\n",
        encoding="utf-8",
    )
    model = tmp_path / "Model.csv"
    model.write_text(
        "ModelID,CellLineName,OncotreeLineage,SangerModelID\n"
        "ACH-000001,NIH:OVCAR-3,Ovary/Fallopian Tube,SIDM00105\n"
        "ACH-000002,HL-60,Myeloid,SIDM00829\n",
        encoding="utf-8",
    )

    obs, var = depmap_matrix_to_obs_var(
        matrix,
        model_csv=model,
        score_column="effect_score",
        dataset_release=DEPMAP_GENETIC_RELEASE,
    )

    assert set(ESSENTIALITY_OBS_REQUIRED_COLUMNS).issubset(obs.columns)
    assert set(ESSENTIALITY_VAR_REQUIRED_COLUMNS).issubset(var.columns)
    assert len(obs) == 4
    assert len(var) == 2
    assert set(obs["cell_line"]) == {"NIH:OVCAR-3", "HL-60"}
    assert set(obs["model_id"]) == {"ACH-000001", "ACH-000002"}
    assert set(obs["baseline_join_id"]) == {"ACH-000001", "ACH-000002"}
    assert set(obs["perturbation_gene"]) == {"A1BG", "TP53"}
    assert set(obs["perturbation_gene_id"]) == {"1", "7157"}
    assert set(obs["perturbation_type"]) == {"CRISPRko"}
    assert set(obs["readout_modality"]) == {"essentiality"}
    assert set(obs["baseline_lamin_prefix"]) == {BASELINE_RNA_CONTRACT["lamin_prefix"]}
    assert set(var["gene_id_type"]) == {"NCBI Entrez Gene ID"}
    assert "X" not in obs.columns
    assert "expression" not in obs.columns

    tp53_ovcar = obs[
        (obs["cell_line"] == "NIH:OVCAR-3")
        & (obs["perturbation_gene"] == "TP53")
    ].iloc[0]
    assert tp53_ovcar["effect_score"] == -1.25
    assert tp53_ovcar["score"] == -1.25
    assert tp53_ovcar["score_type"] == "effect_score"


def test_depmap_matrix_rejects_malformed_gene_labels_explicitly(tmp_path: Path):
    matrix = tmp_path / "CRISPRGeneEffect.csv"
    matrix.write_text(
        ",A1BG (1),MALFORMED_GENE_LABEL\nACH-000001,-0.11,-1.25\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Malformed DepMap gene labels.*MALFORMED_GENE_LABEL",
    ):
        depmap_matrix_to_obs_var(matrix, score_column="GeneEffect")


def test_depmap_readout_modality_distinguishes_effect_and_dependency_scores(tmp_path: Path):
    assert infer_depmap_readout_modality("effect_score") == "essentiality"
    assert infer_depmap_readout_modality("dependency_score") == "dependency"

    dependency_matrix = tmp_path / "CRISPRGeneDependency.csv"
    dependency_matrix.write_text(",A1BG (1)\nACH-000001,0.02\n", encoding="utf-8")
    dependency_obs, _dependency_var = depmap_matrix_to_obs_var(
        dependency_matrix,
        score_column="dependency_score",
    )

    assert set(dependency_obs["readout_modality"]) == {"dependency"}
    assert "pooled_CRISPR_screen" not in set(dependency_obs["readout_modality"])


@pytest.mark.parametrize("score_column", ["GeneEffect", "CRISPRGeneEffect"])
def test_gene_effect_score_spellings_select_effect_assay(tmp_path: Path, score_column: str):
    matrix = tmp_path / "CRISPRGeneEffect.csv"
    matrix.write_text(",A1BG (1)\nACH-000001,-0.11\n", encoding="utf-8")

    obs, _var = depmap_matrix_to_obs_var(matrix, score_column=score_column)

    assert set(obs["assay"]) == {"Chronos_CRISPR_gene_effect"}


def test_no_fake_x_output_for_essentiality_converter(tmp_path: Path):
    matrix = tmp_path / "CRISPRGeneDependency.csv"
    matrix.write_text(",A1BG (1)\nACH-000001,0.02\n", encoding="utf-8")
    obs, var = depmap_matrix_to_obs_var(matrix, score_column="dependency_score")
    obs_path, var_path = write_obs_var(obs, var, tmp_path / "obs.csv", tmp_path / "var.csv")

    assert obs_path.name == "obs.csv"
    assert var_path.name == "var.csv"
    assert not (tmp_path / "X.h5ad").exists()
    reloaded = pd.read_csv(obs_path)
    assert reloaded.loc[0, "cell_line"] == "ACH-000001"
    assert reloaded.loc[0, "dependency_score"] == 0.02

    compatibility_obs = depmap_matrix_to_long_table(matrix, score_column="dependency_score")
    assert list(compatibility_obs.columns[: len(ESSENTIALITY_OBS_REQUIRED_COLUMNS)]) == list(
        ESSENTIALITY_OBS_REQUIRED_COLUMNS
    )


def test_validator_rejects_fake_x_output_for_essentiality(tmp_path: Path):
    matrix = tmp_path / "CRISPRGeneDependency.csv"
    matrix.write_text(",A1BG (1)\nACH-000001,0.02\n", encoding="utf-8")
    obs, var = depmap_matrix_to_obs_var(matrix, score_column="dependency_score")

    try:
        validate_essentiality_obs_var_contract(obs, var, expected_outputs=("obs.parquet", "X.h5ad", "var.parquet"))
    except ValueError as exc:
        assert "must not declare X.h5ad" in str(exc)
    else:  # pragma: no cover - should fail before here
        raise AssertionError("validator accepted fake X.h5ad essentiality outputs")


def test_baseline_rna_obs_contract_has_stable_join_ids_and_no_scores():
    obs = pd.DataFrame(
        {
            "baseline_join_id": ["ACH-000001"],
            "model_id": ["ACH-000001"],
            "depmap_id": ["ACH-000001"],
            "dataset_release": ["DepMap Public 26Q1"],
        }
    )
    validate_baseline_rna_obs_contract(obs)

    leaking = obs.assign(effect_score=[-1.0])
    try:
        validate_baseline_rna_obs_contract(leaking)
    except ValueError as exc:
        assert "must not contain essentiality score columns" in str(exc)
    else:  # pragma: no cover - should fail before here
        raise AssertionError("validator accepted score columns in baseline RNA obs")


@pytest.mark.parametrize(
    ("column", "invalid_value"),
    [
        ("baseline_join_id", None),
        ("model_id", "   "),
        ("depmap_id", "ACH-999999"),
    ],
)
def test_baseline_rna_obs_contract_rejects_invalid_or_inconsistent_ids(
    column: str,
    invalid_value: object,
):
    obs = pd.DataFrame(
        {
            "baseline_join_id": ["ACH-000001"],
            "model_id": ["ACH-000001"],
            "depmap_id": ["ACH-000001"],
        }
    )
    obs.loc[0, column] = invalid_value

    with pytest.raises(ValueError, match="Baseline RNA stable join fields"):
        validate_baseline_rna_obs_contract(obs)


def test_sanger_score_contract_matches_pr38_typed_aux_payload():
    assert SANGER_SCORE_AUX_CONTRACT["expected_outputs"] == (
        "obs.parquet",
        "X.h5ad(empty)",
        "var.parquet(empty)",
        "X_score.h5ad",
        "var_score.parquet",
    )
    assert SANGER_SCORE_AUX_CONTRACT["score_aux_keys"] == ("X_score", "var_score")
    assert "canonical X is intentionally empty" in SANGER_SCORE_AUX_CONTRACT["x_semantics"]


def test_phase3_plan_declares_essentiality_and_baseline_as_separate_contracts():
    depmap = next(dataset for dataset in DATASETS if dataset.name == "DepMap genetic dependencies")
    sanger = next(dataset for dataset in DATASETS if dataset.name == "Sanger SCORE CRISPR KO")
    dual_guide = next(dataset for dataset in DATASETS if dataset.name == "Sanger Dual-guide KO CRC")
    prism = next(dataset for dataset in DATASETS if dataset.name == "Broad PRISM Repurposing")
    gdsc = next(dataset for dataset in DATASETS if dataset.name == "Sanger GDSC")
    baseline = next(dataset for dataset in DATASETS if dataset.name == "DepMap CCLE")

    assert depmap.modality == "essentiality"
    assert depmap.expected_outputs == ["obs.parquet", "var.parquet", "source_manifest.json"]
    assert "X.h5ad" not in depmap.expected_outputs
    assert "obs+var" in depmap.notes
    assert "Sanger/Project Score" in depmap.notes

    assert sanger.expected_outputs == list(SANGER_SCORE_AUX_CONTRACT["expected_outputs"])
    assert sanger.modality == "essentiality"
    assert "X_score.h5ad" in sanger.expected_outputs
    assert "typed auxiliary X_score/var_score" in sanger.notes

    assert dual_guide.modality == "genetic_screen_counts"
    assert dual_guide.perturbation_axis.startswith("CRISPRko_dual_guide")
    assert dual_guide.expected_outputs == ["obs.parquet", "var.parquet", "source_manifest.json"]
    assert "X.h5ad" not in dual_guide.expected_outputs
    assert "count/read_count" in dual_guide.notes

    assert prism.modality == "drug_response"
    assert gdsc.modality == "drug_response"

    assert baseline.expected_outputs == ["obs.parquet", "X.h5ad", "var.parquet"]
    assert baseline.lamin_prefix == BASELINE_RNA_CONTRACT["lamin_prefix"]
    assert "RNA expression" in baseline.notes
    assert "no essentiality scores" in baseline.notes
    assert BASELINE_RNA_CONTRACT["join_fields"] == ("baseline_join_id", "model_id", "depmap_id")
    assert "dependency" not in BASELINE_RNA_CONTRACT["x_semantics"].split(",")[0]


def test_phase3_plan_uses_controlled_readout_modalities_not_vague_screen_labels():
    forbidden_modalities = {"screen", "bulk/sensitivity", "scRNA-seq or score matrix"}
    observed_modalities = {dataset.modality for dataset in DATASETS}

    assert observed_modalities.isdisjoint(forbidden_modalities)
