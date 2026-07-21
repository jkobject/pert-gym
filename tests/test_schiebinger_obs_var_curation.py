from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
pytest.importorskip("anndata")

SCRIPT = (
    Path(__file__).parents[1]
    / "artifacts/schema_audit/real_dataset_curation_20260721/SchiebingerLander2019/t_fbb1d519/curate_obs_var.py"
)
SPEC = importlib.util.spec_from_file_location("schiebinger_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
curation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curation)


def test_frozen_inputs_match_card_hashes() -> None:
    manifest = curation.load_frozen_input_bindings()
    assert {item["uncompressed_sha256"] for item in manifest["inputs"]} == {
        "65388d3d575d99961e2f8fb62d35dd38366d50268068ff144445af6530b54a9b",
        "60530cc3a14fe28e1dbf06c9f62b3e993649750069287083e4351aea3f8318df",
    }


def test_source_age_to_minutes_preserves_terminal_ipsc_as_unknown() -> None:
    values = pd.Series(["D0", "D0.5", "D18", "iPSC", "iPSCs"], dtype="string")
    result = curation.source_age_to_minutes(values)
    assert result.iloc[:3].tolist() == [0.0, 720.0, 25_920.0]
    assert result.iloc[3:].isna().all()


def _fixture() -> tuple[object, object]:
    source_index = pd.Index(
        [
            "CELL_A-GSM1-SchiebingerLander2019_GSE115943",
            "CELL_B-GSM2-SchiebingerLander2019_GSE106340",
            "CELL_C-GSM3-SchiebingerLander2019_GSE106340",
        ]
    )
    source = pd.DataFrame(
        {
            "age": ["D0", "D2", "iPSCs"],
            "replicate": [1.0, np.nan, np.nan],
            "perturbation": ["control", "2i (LIF+MEKi+GSK3i)", pd.NA],
            "GSM": ["GSM1", "GSM2", "GSM3"],
            "cancer": [False] * 3,
            "organism": ["Mus musculus"] * 3,
            "disease": ["healthy"] * 3,
            "tissue_type": ["stem"] * 3,
            "perturbation_type": ["drug"] * 3,
            "celltype": ["mESCs"] * 3,
            "ncounts": [100, 200, 300],
            "ngenes": [10, 20, 30],
            "percent_mito": [1.0, 2.0, 3.0],
            "percent_ribo": [4.0, 5.0, 6.0],
            "nperts": [1, 1, 0],
            "_source_accession": ["GSE115943", "GSE106340", "GSE106340"],
        },
        index=source_index,
    )
    obs = source.drop(columns="_source_accession").copy()
    obs["original_obs_index"] = source_index.astype(str)
    obs["obs_uuid"] = ["uuid-a", "uuid-b", "uuid-c"]
    obs.index = source_index
    return obs, source


def test_curate_obs_materializes_source_join_and_temporal_semantics(monkeypatch) -> None:
    obs, source = _fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    curated, receipt = curation.curate_obs(obs, source)
    assert receipt["join_mismatch_count"] == 0
    assert curated["sample"].tolist() == ["GSM1", "GSM2", "GSM3"]
    assert curated["timepoint"].iloc[:2].tolist() == [0.0, 2880.0]
    assert pd.isna(curated["timepoint"].iloc[2])
    assert curated["is_baseline"].tolist() == [True, False, False]
    assert curated["cell_type"].tolist() == [
        "mouse embryonic fibroblast",
        "MEF-derived reprogramming cell",
        "induced pluripotent stem cell",
    ]
    assert curated["source_age_label"].tolist() == ["D0", "D2", "iPSCs"]
    assert curated["age"].isna().all()
    assert curated["is_control"].tolist()[:2] == [True, False]
    assert pd.isna(curated["is_control"].iloc[2])
    dispositions = curation.field_dispositions(curated)
    assert set(dispositions) == set(curation.CANONICAL_OBS_FIELDS)
    assert dispositions["sample"]["disposition"] == "materialized_complete"
    assert dispositions["timepoint"]["disposition"] == "materialized_partial"
    assert dispositions["age"]["disposition"] == "unknown"
    assert dispositions["guide_id"]["disposition"] == "not_applicable"


def _var_fixture() -> object:
    statuses = (
        ["mapped_exact_external_gene_name_unique"] * 2
        + ["ambiguous_multiple_ensembl_ids"]
        + ["unmapped_symbol"]
    )
    return pd.DataFrame(
        {
            "stable_feature_id": ["ENSMUSG00000000001", "ENSMUSG00000000002", pd.NA, pd.NA],
            "stable_feature_id_mapping_status": statuses,
            "stable_feature_id_candidate_count": [1, 1, 2, 0],
            "stable_feature_id_mapping_release": ["Ensembl 116"] * 4,
            "stable_feature_id_mapping_dataset": ["mmusculus_gene_ensembl"] * 4,
            "stable_feature_id_mapping_response_sha256": ["a" * 64] * 4,
        },
        index=pd.Index(["A", "B", "C", "D"]),
    )


def test_var_curation_preserves_unresolved_and_adds_mouse_contract(monkeypatch) -> None:
    original = _var_fixture()
    curated = curation.curate_var(original)
    pd.testing.assert_frame_equal(curated.loc[:, original.columns], original)
    assert curated["organism"].unique().tolist() == ["Mus musculus"]
    assert curated["stable_feature_id_namespace"].unique().tolist() == [
        "Ensembl stable gene ID"
    ]
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 4)
    monkeypatch.setattr(
        curation,
        "MAPPING_COUNTS",
        {
            "mapped_exact_external_gene_name_unique": 2,
            "ambiguous_multiple_ensembl_ids": 1,
            "unmapped_symbol": 1,
        },
    )
    verdict = curation.verify_var(curated, curated.index, curated.index)
    assert verdict["mapped_exact_unique"] == 2
    assert verdict["unresolved_preserved"] == 2


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda frame: frame.assign(organism="Homo sapiens"), "organism"),
        (
            lambda frame: frame.assign(
                stable_feature_id=["ENSMUSG00000000001", "ENSMUSG00000000001", pd.NA, pd.NA]
            ),
            "exact-unique",
        ),
        (
            lambda frame: frame.assign(
                stable_feature_id=["ENSMUSG00000000001", "ENSMUSG00000000002", "ENSMUSG00000000003", pd.NA]
            ),
            "unresolved symbols",
        ),
    ],
)
def test_var_verifier_rejects_wrong_non_null_semantics(monkeypatch, mutation, message) -> None:
    curated = curation.curate_var(_var_fixture())
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 4)
    monkeypatch.setattr(
        curation,
        "MAPPING_COUNTS",
        {
            "mapped_exact_external_gene_name_unique": 2,
            "ambiguous_multiple_ensembl_ids": 1,
            "unmapped_symbol": 1,
        },
    )
    with pytest.raises(AssertionError, match=message):
        curation.verify_var(mutation(curated), curated.index, curated.index)


def test_var_verifier_rejects_independent_x_axis_order_drift(monkeypatch) -> None:
    curated = curation.curate_var(_var_fixture())
    monkeypatch.setattr(curation, "EXPECTED_N_VARS", 4)
    with pytest.raises(AssertionError, match="feature-axis"):
        curation.verify_var(curated, curated.index, curated.index[::-1])
