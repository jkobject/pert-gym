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
    / "artifacts/schema_audit/real_dataset_curation_20260722/geo_GSE150062/t_680f05a3/curate_obs_var.py"
)
EVIDENCE = SCRIPT.parent
SPEC = importlib.util.spec_from_file_location("gse150062_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
curation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curation)


def test_source_manifest_binds_publication_table_s5_and_author_reference() -> None:
    manifest = json.loads((EVIDENCE / "source_manifest.json").read_text())
    assert manifest["publication"] == {
        "title": "Dual genome-wide coding and lncRNA screens in neural induction of induced pluripotent stem cells",
        "doi": "10.1016/j.xgen.2022.100177",
        "pmid": "36381608",
        "pmc": "PMC9648144",
        "pmc_url": "https://pmc.ncbi.nlm.nih.gov/articles/PMC9648144/",
    }
    assert manifest["table_s5"]["rows"] == 78_393
    assert manifest["table_s5"]["sha256"] == curation.PMC_TABLE_SPEC[1]
    assert manifest["author_code"]["commit"] == curation.AUTHOR_COMMIT
    assert (
        manifest["author_code"]["reference_metadata_sha256"]
        == curation.AUTHOR_SPECS["unified_metadata.tsv.gz"][1]
    )
    assert len(manifest["perturbseq_samples"]["gene_expression"]) == 10
    assert len(manifest["perturbseq_samples"]["direct_guide_capture"]) == 10
    assert (
        "Do not coerce source-native LH identifiers to Ensembl IDs."
        in manifest["forbidden_inferences"]
    )


def test_cellranger_feature_id_sanitization_matches_geo_spellings() -> None:
    values = pd.Series(["5S_rRNA", "RP11-293G6__B.8", "XXyac-YX65C7_A.3"])
    assert curation.cellranger_feature_ids(values).tolist() == [
        "5S-rRNA",
        "RP11-293G6--B.8",
        "XXyac-YX65C7-A.3",
    ]


def _obs_fixture() -> tuple[Any, dict[str, Any]]:
    index = pd.Index(["cell-a", "cell-b", "cell-c"])
    table = pd.DataFrame(
        {
            "Cell barcode": index,
            "Guide identity": ["sgGENE1_1", "sgNTC_1", "sgLH00001_1"],
            "Guide target": ["GENE1", "Non-Targeting", "LH00001"],
            "Library": ["Coding", "Control", "lncRNA"],
            "RNA velocity trajectory": ["NSC", "Cell Cycle", "Non-NSC"],
            "Gene expression UMI": ["1000", "2000", "3000"],
            "Gene expression complexity": ["100", "200", "300"],
            "CRISPRi sgRNA UMI": ["10", "20", "30"],
            "Protospacer": ["A" * 20, "C" * 20, "G" * 20],
            "Batch": ["1", "2", "3"],
        },
        index=index,
    )
    obs = pd.DataFrame(
        {
            "guide": table["Guide identity"],
            "perturbation": table["Guide target"].replace(
                {"Non-Targeting": "non-targeting"}
            ),
            "library": table["Library"],
            "trajectory": table["RNA velocity trajectory"],
            "nCount_RNA": [1000, 2000, 3000],
            "nFeature_RNA": [100, 200, 300],
            "UMI_count": [10, 20, 30],
            "sample": ["diff_1", "diff_2", "diff_3"],
            "is_control": [False, True, False],
            "percent_mito": [1.0, 2.0, 3.0],
            "percent_ribo": [10.0, 20.0, 30.0],
            "original_obs_index": index,
            "original_obs_index_is_duplicated": False,
            "obs_uuid": ["uuid-a", "uuid-b", "uuid-c"],
        },
        index=index,
    )
    return obs, {"table": table}


def test_curate_obs_materializes_exact_table_s5_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    obs, source = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    curated, receipt = curation.curate_obs(obs, source)
    assert receipt["join_mismatch_count"] == 0
    assert curated["guide_sequence"].tolist() == ["A" * 20, "C" * 20, "G" * 20]
    assert curated["perturbation"].tolist() == ["GENE1", "non-targeting", "LH00001"]
    assert curated["is_control"].tolist() == [False, True, False]
    assert curated["trajectory_id"].tolist() == ["NSC", "Cell Cycle", "Non-NSC"]
    assert curated["timepoint"].unique().tolist() == [11_520.0]
    assert curated["n_counts"].tolist() == [1000, 2000, 3000]
    assert curated["is_low_quality"].tolist() == [False, False, False]
    dispositions = curation.field_dispositions(curated)
    assert set(dispositions) == set(curation.CANONICAL_OBS_FIELDS)
    assert dispositions["guide_sequence"]["disposition"] == "materialized_complete"
    assert dispositions["pseudotime"]["disposition"] == "unknown"
    assert dispositions["dose"]["disposition"] == "not_applicable"


def test_obs_join_rejects_wrong_but_non_null_guide_target() -> None:
    obs, source = _obs_fixture()
    obs.loc["cell-a", "perturbation"] = "WRONG"
    with pytest.raises(AssertionError, match="semantic mismatch"):
        curation.exact_source_join(obs, source["table"])


def test_obs_curation_replay_is_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    obs, source = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    first, _ = curation.curate_obs(obs, source)
    replay, receipt = curation.curate_obs(first, source)
    assert receipt["join_mismatch_count"] == 0
    pd.testing.assert_frame_equal(replay, first, check_categorical=True)
    assert replay["source_original_perturbation"].tolist() == [
        "GENE1",
        "non-targeting",
        "LH00001",
    ]


def _var_fixture() -> tuple[Any, dict[str, Any]]:
    genes = pd.Index(["GENE1", "LH00001", "AMBIG"])
    var = pd.DataFrame(
        {
            "stable_feature_id": ["ENSG99999999999", pd.NA, pd.NA],
            "stable_feature_id_source": "old mapping",
            "stable_feature_id_mapping_status": ["old", "unmapped", "unmapped"],
        },
        index=genes,
    )
    gene_rows = pd.DataFrame(
        {
            "feature_id": ["GENE1", "LH00001", "AMBIG", "AMBIG"],
            "gene_id": [
                "ENSG00000000001",
                "LH00001",
                "ENSG00000000002",
                "ENSG00000000003",
            ],
        }
    )
    display = pd.DataFrame(
        {"gene_name": ["GENE1", "custom lncRNA", "AMBIG"], "display_name": genes},
        index=genes,
    )
    return var, {"genes": genes, "gene_rows": gene_rows, "display": display}


def test_curate_var_preserves_source_native_lh_and_refuses_fabricated_ensembl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    var, source = _var_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 3)
    curated = curation.curate_var(var, source)
    assert curated["stable_feature_id"].tolist()[0] == "ENSG00000000001"
    assert pd.isna(curated["stable_feature_id"].iloc[1])
    assert curated["source_native_lh_id"].iloc[1] == "LH00001"
    assert pd.isna(curated["stable_feature_id"].iloc[2])
    assert curated["source_reference_gene_id_count"].tolist() == [1, 1, 2]
    assert curated["previous_stable_feature_id"].iloc[0] == "ENSG99999999999"
    monkeypatch.setattr(curation, "EXPECTED_STABLE_ENSG_COUNT", 1)
    monkeypatch.setattr(curation, "EXPECTED_SOURCE_LH_COUNT", 1)
    verdict = curation.verify_var(curated, source, curated.index)
    assert verdict["stable_ensembl_id_features"] == 1
    assert verdict["source_native_lh_features"] == 1
    assert verdict["other_nonpassing_features"] == 1
    assert verdict["feature_denominator"] == {
        "standard_ensembl_gene": 1,
        "source_native_custom_lh_not_applicable": 1,
        "unresolved_applicable_unknown": 1,
        "total": 3,
    }
    assert verdict["full_feature_ensembl_coverage"] is False
    assert verdict["var_ensembl_species_completed"] is True
    assert verdict["var_feature_identity_species_disposition_completed"] is True
    assert verdict["var_ensembl_species_completion_basis"] == (
        "source_bound_disposition_complete_not_full_coverage"
    )
    assert verdict["verdict"] == "accepted_partial_boundary"


@pytest.mark.parametrize("tampering", ["different_ensg", "moved_lh"])
def test_var_verifier_rejects_row_level_source_identity_tampering(
    monkeypatch: pytest.MonkeyPatch, tampering: str
) -> None:
    var, source = _var_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 3)
    monkeypatch.setattr(curation, "EXPECTED_STABLE_ENSG_COUNT", 1)
    monkeypatch.setattr(curation, "EXPECTED_SOURCE_LH_COUNT", 1)
    curated = curation.curate_var(var, source)
    if tampering == "different_ensg":
        curated.loc["GENE1", "stable_feature_id"] = "ENSG00000000009"
        curated.loc["GENE1", "ensembl_gene_id"] = "ENSG00000000009"
    else:
        curated.loc["LH00001", "source_native_lh_id"] = pd.NA
        curated.loc["AMBIG", "source_native_lh_id"] = "LH00001"
        curated.loc["LH00001", "feature_contract_class"] = (
            "unresolved_source_gene_feature"
        )
        curated.loc["AMBIG", "feature_contract_class"] = (
            "source_native_custom_lh_feature"
        )
        curated.loc["LH00001", "ensembl_id_state"] = "unknown"
        curated.loc["AMBIG", "ensembl_id_state"] = "not_applicable"
    with pytest.raises(AssertionError, match="source-bound row mismatch"):
        curation.verify_var(curated, source, curated.index)


def test_structural_collection_reuse_distinguishes_curated_obs_from_anchor() -> None:
    collections = {
        "pert-gym/canonical/example": {
            "uid": "collection-uid",
            "target_matches": [
                {
                    "key": "prism_collection/GSE150062/obs.parquet",
                    "uid": curation.EXPECTED_STRUCTURAL_COLLECTION_OBS_UID,
                }
            ],
        }
    }
    verdict = curation.verify_structural_collection_reuse(
        collections, curated_obs_uid="CkcQf1IYkOkbxKed0003"
    )
    assert verdict["structural_anchor_reused"] is True
    assert verdict["curated_obs_is_collection_anchor"] is False
    assert verdict["collection_mutation_in_scope"] is False
    assert verdict["verdict"] == "accepted_structural_reuse"


def test_structural_collection_reuse_rejects_false_current_anchor_claim() -> None:
    collections = {
        "pert-gym/canonical/example": {
            "uid": "collection-uid",
            "target_matches": [
                {
                    "key": "prism_collection/GSE150062/obs.parquet",
                    "uid": "CkcQf1IYkOkbxKed0003",
                }
            ],
        }
    }
    with pytest.raises(AssertionError, match="structural Collection anchor drift"):
        curation.verify_structural_collection_reuse(
            collections, curated_obs_uid="CkcQf1IYkOkbxKed0003"
        )


def test_var_verifier_rejects_x_axis_order_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    var, source = _var_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 3)
    curated = curation.curate_var(var, source)
    with pytest.raises(AssertionError, match="VAR/X"):
        curation.verify_var(curated, source, pd.Index(["LH00001", "GENE1", "AMBIG"]))


def test_processing_decision_notebook_executes_from_repo_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notebook_path = EVIDENCE / "GSE150062_processing_decisions.ipynb"
    notebook = json.loads(notebook_path.read_text())
    monkeypatch.chdir(Path(__file__).parents[1])
    namespace: dict[str, Any] = {}
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            exec("".join(cell["source"]), namespace)


def test_retry_receipts_bind_partial_var_and_zero_write_replay() -> None:
    mutation = json.loads((EVIDENCE / "mutation_retry_receipt.json").read_text())
    verification = json.loads(
        (EVIDENCE / "verification_retry_receipt.json").read_text()
    )
    assert mutation["canonical_sha256"] == (
        "dcc2b68f51f17bc115175ec24589ee79710d5f452cf083ac12c1f1f724baca7a"
    )
    assert verification["canonical_sha256"] == (
        "be105bf47d138337f65c75d61a5629bd00382cec7ade73ddbdb18fbfc843bc43"
    )
    assert mutation["writes"]["obs_revisions"] == 0
    assert mutation["writes"]["var_revisions"] == 1
    assert mutation["writes"]["artifacts"]["var"][0]["uid"] == "rRlvtvSEpbFnek7K0003"
    assert verification["replay_noop"] is True
    assert (
        verification["registry_counts"]["before"]
        == verification["registry_counts"]["after"]
    )
    assert verification["dataset_e2e_v3"]["complete"] is True
    assert verification["collection_contract"]["verdict"] == "accepted_structural_reuse"
    assert verification["member_after"]["var_verdict"]["feature_denominator"] == {
        "standard_ensembl_gene": 44_025,
        "source_native_custom_lh_not_applicable": 16_401,
        "unresolved_applicable_unknown": 71,
        "total": 60_497,
    }


def test_dashboard_reconciles_accepted_ledger_without_gse150062_credit() -> None:
    repo = Path(__file__).parents[1]
    todo = (repo / "TODO.md").read_text()
    status = (repo / "docs/project/current-status.md").read_text()
    for text in (todo, status):
        assert "9/70" in text
        assert "8/70" in text
        assert "GSE150062 remains pending independent acceptance" in text
        assert "t_3bb03773" in text
        assert "lhR6Ny3n8QcVeItH0003" in text
        assert "GJ1HqkBSHfDD1o4m0002" in text
        assert "t_2c228f48" in text
        assert "AYnivbGN3JCRzkN70001" in text
