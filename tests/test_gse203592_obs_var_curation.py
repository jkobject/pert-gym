from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

pytest.importorskip("anndata")

SCRIPT = (
    Path(__file__).parents[1]
    / "artifacts/schema_audit/real_dataset_curation_20260723/geo_GSE203592/t_40b72cca/curate_obs_var.py"
)
EVIDENCE = SCRIPT.parent
SOURCE_MANIFEST = EVIDENCE / "source_manifest.json"
DECISION_NOTEBOOK = EVIDENCE / "GSE203592_processing_decisions.ipynb"

spec = importlib.util.spec_from_file_location("gse203592_curation", SCRIPT)
assert spec is not None and spec.loader is not None
curation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(curation)


def test_source_manifest_closes_search_without_inventing_guide_sequences() -> None:
    manifest = json.loads(SOURCE_MANIFEST.read_text())
    assert manifest["search_effort_complete"] is True
    assert manifest["dataset_id"] == "prism_collection/GSE203592"
    guide = manifest["explicit_unresolved_fields"]["guide_sequence"]
    assert guide["state"] == "unknown"
    assert "do not substitute" in guide["projection_guard"]
    assert manifest["reported_sources_not_claimed_inspected"]
    assert manifest["scope_guards"]["allowed_writes"] == [
        "append-only OBS revision for prism_collection/GSE203592/obs.parquet",
        "append-only VAR revision for prism_collection/GSE203592/var.parquet",
        "OBS to accepted X link",
        "accepted X to revised VAR link",
    ]


def test_mouse_mapping_uses_release_order_only_for_complete_make_unique_groups() -> (
    None
):
    symbols = pd.Index(["A", "B", "B.1", "C", "D", "Legit.1"])
    mapping = curation.map_mouse_features(
        symbols,
        {
            "A": ["ENSMUSG00000000001"],
            "B": ["ENSMUSG00000000002", "ENSMUSG00000000003"],
            "C": ["ENSMUSG00000000004", "ENSMUSG00000000005"],
            "Legit.1": ["ENSMUSG00000000006"],
        },
    )
    assert mapping["stable_feature_id"].tolist() == [
        "ENSMUSG00000000001",
        "ENSMUSG00000000002",
        "ENSMUSG00000000003",
        pd.NA,
        pd.NA,
        "ENSMUSG00000000006",
    ]
    assert mapping.loc["C", "stable_feature_id_mapping_status"] == (
        "ambiguous_release93_gene_name"
    )
    assert mapping.loc["D", "stable_feature_id_mapping_status"] == (
        "unmapped_release93_symbol"
    )


def obs_fixture() -> pd.DataFrame:
    index = pd.Index(["cell-a", "cell-b", "cell-c", "cell-d"])
    return pd.DataFrame(
        {
            "obs_uuid": ["uuid-a", "uuid-b", "uuid-c", "uuid-d"],
            "original_obs_index": index,
            "cell_barcode": index,
            "experiment_id": ["V35_M1A", "V35_M1A", "V41_M1L", "V41_M1L"],
            "orig_batch": ["V35", "V35", "V41", "V41"],
            "tissue_type": ["CD8+ tumor infiltrating T cells"] * 4,
            "disease": ["Colon Cancer"] * 4,
            "cell_line": ["unknown"] * 4,
            "organism": ["mouse"] * 4,
            "perturbation": [
                "ACTR5",
                "non-targeting",
                "multiple-targeting",
                "non-targeting",
            ],
            "orig_guide": ["ACTR5-1", "CTRL1-1", "zMulti-1", "zNone-1"],
            "is_control": [False, True, False, True],
            "condition": ["test", "control", "test", "control"],
            "nCount_RNA": [100.0, 200.0, 300.0, 400.0],
            "nFeature_RNA": [10, 20, 30, 40],
            "percent.mt": [1.0, 2.0, 3.0, 4.0],
        },
        index=index,
    )


def test_obs_curation_materializes_joinable_source_fields_and_explicit_unknowns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 4)
    original = obs_fixture()
    curated = curation.curate_obs(original, x_semantics="raw_counts")

    assert curated.index.equals(original.index)
    assert curated["sample"].tolist() == ["V35_M1A", "V35_M1A", "V41_M1L", "V41_M1L"]
    assert curated["batch"].tolist() == ["V35", "V35", "V41", "V41"]
    assert curated["cell_type"].unique().tolist() == ["CD8+ tumor infiltrating T cells"]
    assert curated["cell_line"].isna().all()
    assert curated["cell_line_state"].unique().tolist() == ["not_applicable"]
    assert curated["organism"].unique().tolist() == ["Mus musculus"]
    assert curated["guide_id"].tolist()[:2] == ["ACTR5-1", "CTRL1-1"]
    assert curated["guide_id"].isna().tolist() == [False, False, True, True]
    assert curated["guide_sequence"].isna().all()
    assert curated["guide_sequence_state"].unique().tolist() == ["unknown"]
    assert curated["perturbation_target"].tolist()[0] == "ACTR5"
    assert curated["perturbation_target"].isna().tolist() == [False, True, True, True]
    assert curated["control_type"].tolist() == [
        pd.NA,
        "non-targeting_sgRNA",
        pd.NA,
        "non-targeting_sgRNA",
    ]
    assert curated["timepoint"].unique().tolist() == [21600.0]
    assert curated["n_counts"].tolist() == [100.0, 200.0, 300.0, 400.0]
    assert curated["pct_mito"].tolist() == [1.0, 2.0, 3.0, 4.0]
    assert curated["x_semantics"].unique().tolist() == ["raw_counts"]
    assert curated["prior_canonical_organism"].unique().tolist() == ["mouse"]
    replay = curation.curate_obs(curated, x_semantics="raw_counts")
    pd.testing.assert_frame_equal(replay, curated)


def test_obs_curation_rejects_control_disagreement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 4)
    obs = obs_fixture()
    obs.loc["cell-a", "condition"] = "control"
    with pytest.raises(AssertionError, match="control semantics"):
        curation.curate_obs(obs, x_semantics=None)


def test_var_curation_replaces_wrong_species_mapping_and_preserves_prior_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbols = pd.Index(["A", "B", "B.1"])
    var = pd.DataFrame(
        {
            "stable_feature_id": ["ENSG00000000001", pd.NA, pd.NA],
            "ensembl_gene_id": ["ENSG00000000001", pd.NA, pd.NA],
            "stable_feature_id_source": ["wrong human source"] * 3,
            "stable_feature_id_mapping_status": ["wrong"] * 3,
            "stable_feature_id_candidate_count": [1, 0, 0],
        },
        index=symbols,
    )
    by_symbol = {
        "A": ["ENSMUSG00000000001"],
        "B": ["ENSMUSG00000000002", "ENSMUSG00000000003"],
    }
    curated = curation.curate_var(var, by_symbol)
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 3)
    verdict = curation.verify_var(curated, symbols)

    assert curated["stable_feature_id"].tolist() == [
        "ENSMUSG00000000001",
        "ENSMUSG00000000002",
        "ENSMUSG00000000003",
    ]
    assert curated["prior_stable_feature_id"].tolist()[0] == "ENSG00000000001"
    assert curated["organism"].unique().tolist() == ["Mus musculus"]
    assert curated["feature_index"].is_unique
    assert verdict["stable_feature_id_coverage"] == 1.0
    assert verdict["wrong_species_rows"] == 0


def test_writer_scope_is_append_only_and_collection_read_only() -> None:
    text = SCRIPT.read_text()
    assert 'revises=result["obs_artifact"]' in text
    assert 'revises=result["var_artifact"]' in text
    assert 'features.set_values({"X": result["x_artifact"]})' in text
    assert 'features.set_values({"var": var})' in text
    assert ".delete(" not in text
    assert "collection.save(" not in text.lower()
    assert "collection.artifacts.set(" not in text.lower()


def test_processing_decision_notebook_contains_executable_postwrite_assertions() -> (
    None
):
    notebook = json.loads(DECISION_NOTEBOOK.read_text())
    assert notebook["nbformat"] == 4
    sources = [
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    ]
    combined = "\n".join(sources)
    assert 'obs.features.get_values()["X"]' in combined
    assert 'x.features.get_values()["var"]' in combined
    assert "ENSMUSG" in combined
    assert "guide_sequence_state" in combined
    assert "Collection" in combined
