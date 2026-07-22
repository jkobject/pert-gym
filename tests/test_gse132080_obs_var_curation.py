from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
pytest.importorskip("anndata")

SCRIPT = (
    Path(__file__).parents[1]
    / "artifacts/schema_audit/real_dataset_curation_20260722/geo_GSE132080/t_79ff033e/curate_obs_var.py"
)
EVIDENCE = SCRIPT.parent
SOURCE_MANIFEST = EVIDENCE / "source_manifest.json"
DECISION_NOTEBOOK = EVIDENCE / "GSE132080_processing_decisions.ipynb"
SPEC = importlib.util.spec_from_file_location("gse132080_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
curation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curation)


def test_source_manifest_binds_publication_and_all_six_paired_geo_samples() -> None:
    manifest = json.loads(SOURCE_MANIFEST.read_text())
    assert manifest["publication"] == {
        "title": "Titrating gene expression using libraries of systematically attenuated CRISPR guide RNAs",
        "doi": "10.1038/s41587-019-0387-5",
        "pmid": "31932729",
        "pmc_url": "https://pmc.ncbi.nlm.nih.gov/articles/PMC7065968/",
    }
    assert [sample["accession"] for sample in manifest["samples"]] == [
        f"GSM38422{i:02d}" for i in range(7, 13)
    ]
    expression = [
        sample for sample in manifest["samples"] if sample["maps_to_obs_sample"]
    ]
    guide_barcodes = [
        sample for sample in manifest["samples"] if not sample["maps_to_obs_sample"]
    ]
    assert [sample["role"] for sample in expression] == [
        "single-cell expression library"
    ] * 3
    assert [sample["role"] for sample in guide_barcodes] == [
        "sgRNA barcode library"
    ] * 3
    assert [sample["paired_guide_barcode_accession"] for sample in expression] == [
        "GSM3842210",
        "GSM3842211",
        "GSM3842212",
    ]
    assert [sample["paired_expression_accession"] for sample in guide_barcodes] == [
        "GSM3842207",
        "GSM3842208",
        "GSM3842209",
    ]


def test_processing_decision_notebook_executes_postwrite_evidence_assertions() -> None:
    nbformat = pytest.importorskip("nbformat")
    notebook_client = pytest.importorskip("nbclient")
    notebook = nbformat.read(DECISION_NOTEBOOK, as_version=4)
    notebook_client.NotebookClient(
        notebook,
        timeout=60,
        resources={"metadata": {"path": str(SCRIPT.parents[5])}},
    ).execute(cwd=str(SCRIPT.parents[5]))


def test_frozen_inputs_match_card_hashes() -> None:
    manifest = curation.load_frozen_input_bindings()
    assert {item["uncompressed_sha256"] for item in manifest["inputs"]} == {
        "65388d3d575d99961e2f8fb62d35dd38366d50268068ff144445af6530b54a9b",
        "60530cc3a14fe28e1dbf06c9f62b3e993649750069287083e4351aea3f8318df",
    }


def test_source_guide_name_removes_only_the_redundant_target_prefix() -> None:
    assert (
        curation.source_guide_name("ALDOA_ALDOA_+_30077139.23-P1P2_00")
        == "ALDOA_+_30077139.23-P1P2_00"
    )
    assert curation.source_guide_name("neg_ctrl_non-targeting_00028") is None
    assert curation.source_guide_name("*") is None
    with pytest.raises(AssertionError, match="syntax"):
        curation.source_guide_name("unexpected")


def _obs_fixture() -> tuple[Any, dict[str, Any]]:
    index = pd.Index(["cell-a", "cell-b", "cell-c"])
    identities = pd.DataFrame(
        {
            "guide_identity": [
                "ALDOA_ALDOA_+_30077139.23-P1P2_00",
                "neg_ctrl_non-targeting_00028",
                "ATP5E_ATP5E_-_57607036.23-P1P2_00",
            ],
            "read_count": [100, 200, 300],
            "UMI_count": [10, 20, 30],
            "coverage": [10.0, 10.0, 10.0],
            "gemgroup": [1, 2, 3],
            "good_coverage": [True, False, True],
            "number_of_cells": [1, 1, 1],
        },
        index=index,
    )
    obs = identities.copy(deep=True)
    obs["perturbation"] = ["ALDOA", "non-targeting", "ATP5E"]
    obs["condition"] = ["test", "control", "test"]
    obs["perturbation_type"] = "CRISPRi"
    obs["disease"] = "Leukemia"
    obs["tissue_type"] = "leukemia cell"
    obs["organism"] = "Humans (Homo sapiens)"
    obs["is_control"] = [False, True, False]
    obs["cancer"] = True
    obs["cell_line"] = "unknown"
    obs["dataset"] = "GSE132080"
    obs["modality"] = "scRNA-seq"
    obs["assay"] = "Perturb-seq"
    obs["original_obs_index"] = index
    obs["original_obs_index_is_duplicated"] = False
    obs["obs_uuid"] = ["uuid-a", "uuid-b", "uuid-c"]
    guides = pd.DataFrame(
        {
            "sgRNA_name": [
                "ALDOA_+_30077139.23-P1P2_00",
                "ATP5E_-_57607036.23-P1P2_00",
            ],
            "sequence": ["A" * 20, "C" * 20],
            "gene": ["ALDOA", "ATP5E"],
            "gamma_day5": [-0.4, -0.2],
            "gamma_day10": [-0.3, -0.1],
            "relative_activity_day5": [1.0, 0.5],
            "relative_activity_day10": [1.0, 0.6],
        }
    )
    source = {
        "identities": identities,
        "guides": guides,
        "barcodes": index.append(pd.Index(["unassigned"])),
        "excluded_barcodes": pd.Index(["unassigned"]),
    }
    return obs, source


def test_curate_obs_materializes_exact_source_join_and_guide_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    obs, source = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    curated, receipt = curation.curate_obs(obs, source)
    assert receipt["join_mismatch_count"] == 0
    assert curated["sample"].tolist() == ["GSM3842207", "GSM3842208", "GSM3842209"]
    assert curated["cell_line"].unique().tolist() == ["K-562"]
    assert curated["organism"].unique().tolist() == ["Homo sapiens"]
    assert curated["guide_sequence"].tolist()[0] == "A" * 20
    assert pd.isna(curated["guide_sequence"].iloc[1])
    assert curated["guide_sequence"].tolist()[2] == "C" * 20
    assert curated["is_low_quality"].tolist() == [False, True, False]
    assert curated["timepoint"].unique().tolist() == [7200.0]
    assert curated["source_guide_gamma_day5"].tolist()[0] == -0.4
    assert pd.isna(curated["source_guide_gamma_day5"].iloc[1])
    dispositions = curation.field_dispositions(curated)
    assert set(dispositions) == set(curation.CANONICAL_OBS_FIELDS)
    assert dispositions["guide_sequence"]["disposition"] == "materialized_partial"
    assert dispositions["donor_id"]["disposition"] == "unknown"
    assert dispositions["dose"]["disposition"] == "not_applicable"


def test_obs_verifier_rejects_wrong_but_non_null_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    obs, source = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    expected, _ = curation.curate_obs(obs, source)
    actual = expected.copy(deep=True)
    actual["organism"] = "Mus musculus"
    with pytest.raises(AssertionError, match="semantic"):
        curation.verify_obs_semantics(actual, expected)


def test_obs_curation_replay_preserves_original_source_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    obs, source = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    first, _ = curation.curate_obs(obs, source)
    replay, receipt = curation.curate_obs(first, source)
    assert receipt["join_mismatch_count"] == 0
    pd.testing.assert_frame_equal(replay, first, check_categorical=True)
    assert replay["source_original_dataset"].unique().tolist() == ["GSE132080"]


def _var_fixture() -> tuple[Any, Any]:
    index = pd.Index(["DUP", "DUP", "UNIQUE"])
    stable = ["ENSG00000000001", "ENSG00000000002", "ENSG00000000003"]
    var = pd.DataFrame(
        {
            "pert_gym_original_var_index": index,
            "gene_id": stable,
            "ensembl_gene_id": stable,
            "stable_feature_id": stable,
            "stable_feature_id_source": "normalized_existing_gene_id",
            "stable_feature_id_mapping_status": "exact_stable_id",
        },
        index=index,
    )
    genes = pd.DataFrame({"ensembl_gene_id": stable, "gene_symbol": index})
    return var, genes


def test_var_curation_accepts_unique_ensembl_feature_index_with_duplicate_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    var, genes = _var_fixture()
    original = var.copy(deep=True)
    curated = curation.curate_var(var, genes)
    pd.testing.assert_frame_equal(curated.loc[:, original.columns], original)
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 3)
    verdict = curation.verify_var(curated, genes, curated.index)
    assert verdict["index_unique"] is False
    assert verdict["duplicate_index_rows"] == 2
    assert verdict["stable_feature_id_unique"] is True
    assert verdict["stable_id_or_index_uniqueness"] is True
    assert verdict["axis_order_parity"] is True


def test_var_verifier_rejects_duplicate_stable_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    var, genes = _var_fixture()
    curated = curation.curate_var(var, genes)
    curated.loc[curated.index[1], "stable_feature_id"] = "ENSG00000000001"
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 3)
    with pytest.raises(AssertionError, match="ENSG"):
        curation.verify_var(curated, genes, curated.index)


def test_var_verifier_rejects_x_axis_order_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    var, genes = _var_fixture()
    curated = curation.curate_var(var, genes)
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 3)
    with pytest.raises(AssertionError, match="VAR/X"):
        curation.verify_var(curated, genes, pd.Index(["DUP", "UNIQUE", "DUP"]))
