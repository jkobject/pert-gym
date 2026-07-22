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
    / "artifacts/schema_audit/real_dataset_curation_20260722/scperturb_datlinger17/t_60ced1f1/curate_obs_var.py"
)
EVIDENCE = SCRIPT.parent
SOURCE_MANIFEST = EVIDENCE / "source_manifest.json"
DECISION_NOTEBOOK = EVIDENCE / "datlinger17_processing_decisions.ipynb"
SPEC = importlib.util.spec_from_file_location("datlinger17_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
curation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curation)


def test_source_manifest_binds_publication_geo_samples_and_payload_hashes() -> None:
    manifest = json.loads(SOURCE_MANIFEST.read_text())
    assert manifest["publication"] == {
        "title": "Pooled CRISPR screening with single-cell transcriptome read-out",
        "doi": "10.1038/nmeth.4177",
        "pmid": "28099430",
        "pmc_url": "https://pmc.ncbi.nlm.nih.gov/articles/PMC5334791/",
    }
    assert [sample["accession"] for sample in manifest["samples"]] == [
        f"GSM24390{index:02d}" for index in range(80, 91)
    ]
    assert [
        (sample["condition"], sample["replicate"]) for sample in manifest["samples"]
    ] == [
        *(("stimulated", replicate) for replicate in range(1, 7)),
        *(("unstimulated", replicate) for replicate in range(1, 6)),
    ]
    assert {item["sha256"] for item in manifest["files"]} == {
        "3e0bb8554fdd6f732ec039e703f685631334e9c06029864e81594babc8def0af",
        "3acaf07ca5b5cb2fde9b957ae9e6f0b27a6df267b013ba9d818931b11ce54c44",
    }
    assert manifest["denominator_accounting"] == {
        "biological_datasets": 1,
        "logical_families": 1,
        "physical_members": 1,
        "source_and_accepted_observations": 5905,
        "source_features": 36722,
        "accepted_features": 24389,
        "guide_library_rows": 116,
        "source_control_rows": 1320,
        "geo_samples": 11,
    }


def test_frozen_inputs_match_card_hashes() -> None:
    manifest = curation.load_frozen_input_bindings()
    assert {item["uncompressed_sha256"] for item in manifest["inputs"]} == {
        "65388d3d575d99961e2f8fb62d35dd38366d50268068ff144445af6530b54a9b",
        "60530cc3a14fe28e1dbf06c9f62b3e993649750069287083e4351aea3f8318df",
    }
    assert curation.EXPECTED_OBS == {
        "uid": "sitiyL4128YBC8BS0004",
        "hash": "gU48Qsw1u6MbLvJMIshIiA",
    }


def test_source_matrix_uses_upstream_positional_obs_and_unique_var_rules() -> None:
    source = curation.load_sources(load_matrix=True)
    assert source["matrix"].index.equals(source["obs"].index)
    assert source["matrix"].columns.is_unique
    assert source["matrix"].shape == (5905, 36722)


def _obs_fixture() -> tuple[Any, dict[str, Any]]:
    index = pd.Index(["cell-a", "cell-b", "cell-c"])
    source_obs = pd.DataFrame(
        {
            "condition": ["stimulated", "unstimulated", "stimulated"],
            "replicate": ["1", "1", "2"],
            "grna": ["Tcrlibrary_JUND_2", "CTRL00320", "Tcrlibrary_BACH2_3"],
            "gene": ["JUND", "CTRL", "BACH2"],
        },
        index=index,
    )
    obs = pd.DataFrame(index=index)
    obs["condition"] = source_obs["condition"]
    obs["replicate"] = source_obs["replicate"]
    obs["perturbation"] = ["Tcrlibrary_JUND_2", "control", "Tcrlibrary_BACH2_3"]
    obs["perturbation_2"] = source_obs["condition"]
    obs["pert_target"] = ["JUND", "CTRL", "BACH2"]
    obs["perturbation_type"] = "CRISPR"
    obs["cell_type"] = "T cells"
    obs["cell_line"] = "Jurkat"
    obs["disease"] = "T-cell acute lymphoblastic leukemia"
    obs["tissue_type"] = "cell culture"
    obs["organism"] = "Humans (Homo sapiens)"
    obs["sex"] = "male"
    obs["dataset"] = "datlinger17"
    obs["source_accession"] = "datlinger17"
    obs["modality"] = "scRNA-seq"
    obs["assay"] = "CROP-seq"
    obs["ncounts"] = [100.0, 200.0, 300.0]
    obs["ngenes"] = [10, 20, 30]
    obs["percent_mito"] = [1.0, 2.0, 3.0]
    obs["percent_ribo"] = [4.0, 5.0, 6.0]
    obs["original_obs_index"] = index
    obs["original_obs_index_is_duplicated"] = False
    obs["obs_uuid"] = ["uuid-a", "uuid-b", "uuid-c"]
    guides = pd.DataFrame(
        {
            "gRNA_ID": ["Tcrlibrary_JUND_2", "CTRL00320", "Tcrlibrary_BACH2_3"],
            "Sequence": ["A" * 20, "C" * 20, "G" * 20],
        }
    )
    return obs, {"obs": source_obs, "guides": guides, "receipts": {}}


def test_curate_obs_materializes_exact_source_join_and_all_guide_sequences(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    obs, source = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    monkeypatch.setattr(
        curation,
        "SAMPLE_BY_CONDITION_REPLICATE",
        {
            ("stimulated", "1"): "GSM-A",
            ("unstimulated", "1"): "GSM-B",
            ("stimulated", "2"): "GSM-C",
        },
    )
    curated, receipt = curation.curate_obs(obs, source)
    assert receipt["join_mismatch_count"] == 0
    assert curated["sample"].tolist() == ["GSM-A", "GSM-B", "GSM-C"]
    assert curated["guide_id"].tolist() == source["obs"]["grna"].tolist()
    assert curated["guide_sequence"].tolist() == ["A" * 20, "C" * 20, "G" * 20]
    assert curated["is_control"].tolist() == [False, True, False]
    assert curated["perturbation_target"].tolist()[0] == "JUND"
    assert pd.isna(curated["perturbation_target"].iloc[1])
    assert curated["perturbation_type"].unique().tolist() == ["CRISPRko"]
    assert curated["timepoint"].unique().tolist() == [240.0]
    assert curated["is_baseline"].tolist() == [False, True, False]
    assert curated["x_semantics"].unique().tolist() == ["raw_counts"]
    dispositions = curation.field_dispositions(curated)
    assert set(dispositions) == set(curation.CANONICAL_OBS_FIELDS)
    assert dispositions["guide_sequence"]["disposition"] == "materialized_complete"
    assert dispositions["perturbation_target"]["disposition"] == "materialized_partial"
    assert dispositions["donor_id"]["disposition"] == "unknown"
    assert dispositions["dose"]["disposition"] == "not_applicable"


def test_curate_obs_uses_geo_accession_and_rejects_internal_slug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    obs, source = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    monkeypatch.setattr(
        curation,
        "SAMPLE_BY_CONDITION_REPLICATE",
        {
            ("stimulated", "1"): "GSM-A",
            ("unstimulated", "1"): "GSM-B",
            ("stimulated", "2"): "GSM-C",
        },
    )
    expected, receipt = curation.curate_obs(obs, source)
    assert expected["source_accession"].unique().tolist() == ["GSE92872"]
    assert expected["dataset"].unique().tolist() == ["scperturb/datlinger17"]
    assert expected["source_original_source_accession"].unique().tolist() == [
        "datlinger17"
    ]
    assert receipt["canonical_source_accession"] == "GSE92872"
    assert receipt["preserved_source_accession_rows"] == 3
    assert receipt["preserved_source_accession_values"] == ["datlinger17"]

    wrong = expected.copy(deep=True)
    wrong["source_accession"] = "datlinger17"
    assert curation.obs_matches_expected_semantics(expected, expected) is True
    assert curation.obs_matches_expected_semantics(wrong, expected) is False
    with pytest.raises(AssertionError, match="semantic"):
        curation.verify_obs_semantics(wrong, expected)


def test_obs_verifier_rejects_wrong_but_non_null_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    obs, source = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    monkeypatch.setattr(
        curation,
        "SAMPLE_BY_CONDITION_REPLICATE",
        {
            ("stimulated", "1"): "GSM-A",
            ("unstimulated", "1"): "GSM-B",
            ("stimulated", "2"): "GSM-C",
        },
    )
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
    monkeypatch.setattr(
        curation,
        "SAMPLE_BY_CONDITION_REPLICATE",
        {
            ("stimulated", "1"): "GSM-A",
            ("unstimulated", "1"): "GSM-B",
            ("stimulated", "2"): "GSM-C",
        },
    )
    first, _ = curation.curate_obs(obs, source)
    replay, receipt = curation.curate_obs(first, source)
    assert receipt["join_mismatch_count"] == 0
    pd.testing.assert_frame_equal(replay, first, check_categorical=True)
    assert replay["source_original_dataset"].unique().tolist() == ["datlinger17"]
    assert replay["source_original_perturbation_type"].unique().tolist() == ["CRISPR"]


def test_var_verifier_accepts_exact_unique_human_ensg_axis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = pd.Index(["GENE-A", "GENE-B", "GENE-C"])
    var = pd.DataFrame(
        {
            "stable_feature_id": [
                "ENSG00000000001",
                "ENSG00000000002",
                "ENSG00000000003",
            ]
        },
        index=index,
    )
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 3)
    verdict = curation.verify_var(var, index)
    assert verdict["stable_feature_id_unique"] is True
    assert verdict["wrong_species_rows"] == 0
    assert verdict["needs_revision"] is False


def test_var_verifier_rejects_axis_order_and_stable_id_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = pd.Index(["GENE-A", "GENE-B", "GENE-C"])
    var = pd.DataFrame(
        {
            "stable_feature_id": [
                "ENSG00000000001",
                "ENSG00000000001",
                "ENSG00000000003",
            ]
        },
        index=index,
    )
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 3)
    with pytest.raises(AssertionError, match="VAR/X"):
        curation.verify_var(var, index[::-1])
    with pytest.raises(AssertionError, match="ENSG"):
        curation.verify_var(var, index)


def test_processing_decision_notebook_executes_postwrite_evidence_assertions() -> None:
    nbformat = pytest.importorskip("nbformat")
    notebook_client = pytest.importorskip("nbclient")
    notebook = nbformat.read(DECISION_NOTEBOOK, as_version=4)
    notebook_client.NotebookClient(
        notebook,
        timeout=60,
        resources={"metadata": {"path": str(SCRIPT.parents[5])}},
    ).execute(cwd=str(SCRIPT.parents[5]))
