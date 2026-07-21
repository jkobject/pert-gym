from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
pytest.importorskip("anndata")

SCRIPT = (
    Path(__file__).parents[1]
    / "artifacts/schema_audit/real_dataset_curation_20260721/scperturb_adamson16/t_f1d056cd/curate_obs_var.py"
)
SPEC = importlib.util.spec_from_file_location("adamson16_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
curation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curation)
HANDOFF = SCRIPT.with_name("integrated_handoff.json")


def test_table_s1_map_is_source_bound_and_resolves_single_and_combo_guides() -> None:
    by_vector, by_guide, payload = curation.load_table_s1()

    assert payload["row_count"] == 98
    assert by_vector["pDS353"] == "GGCTTGTTCGCTGGTGGCGT"
    assert curation.guide_sequences("OST4_pDS353", by_vector, by_guide) == [
        "GGCTTGTTCGCTGGTGGCGT"
    ]
    assert curation.guide_sequences("ATF6_PERK_IRE1_pMJ158", by_vector, by_guide) == [
        by_guide["ATF6"],
        by_guide["PERK"],
        by_guide["IRE1"],
    ]
    assert curation.guide_sequences("63(mod)_pBA580", by_vector, by_guide) == []


def test_field_dispositions_cover_every_canonical_field() -> None:
    frame = pd.DataFrame(
        {
            "dataset": ["scperturb/adamson16_component"],
            "guide_sequence": [pd.NA],
            "donor_id": ["unknown"],
        }
    )

    result = curation.field_dispositions(frame)

    assert set(result) == set(curation.CANONICAL_OBS_FIELDS)
    assert result["dataset"]["disposition"] == "materialized_complete"
    assert result["guide_sequence"]["disposition"] == "materialized_partial"
    assert result["donor_id"]["disposition"] == "unknown"
    assert result["dose"]["disposition"] == "not_applicable"


def test_var_curation_preserves_rows_and_binds_human_ensembl_namespace() -> None:
    frame = pd.DataFrame(
        {
            "stable_feature_id": ["ENSG00000123456", "ENSG00000123457"],
            "stable_feature_id_mapping_status": ["exact_stable_id"] * 2,
        },
        index=pd.Index(["GENE_A", "GENE_B"], name="gene_symbol"),
    )

    curated = curation.curate_var(frame)

    pd.testing.assert_frame_equal(curated.loc[:, frame.columns], frame)
    assert curated.index.equals(frame.index)
    assert curated["stable_feature_id_namespace"].unique().tolist() == [
        "Ensembl stable gene ID"
    ]
    assert curated["organism"].unique().tolist() == ["Homo sapiens"]
    assert curation.verify_var(curated, 2)["needs_revision"] is False


def test_curation_preserves_existing_columns_and_materializes_source_values(
    monkeypatch,
) -> None:
    index = pd.Index(["CELL_A", "CELL_B"], name="cell_barcode")
    obs = pd.DataFrame(
        {
            "pert_genetic": pd.Categorical(["OST4_pDS353", "*"]),
            "pert_target": pd.Categorical(["OST4", None]),
            "assay": pd.Categorical(["10x 3' v1", "10x 3' v1"]),
            "ncounts": np.array([100.0, 200.0], dtype=np.float32),
            "ngenes": [10, 20],
            "percent_mito": np.array([1.0, 2.0], dtype=np.float32),
            "percent_ribo": np.array([3.0, 4.0], dtype=np.float32),
            "obs_uuid": ["uuid-a", "uuid-b"],
            "original_obs_index": ["CELL_A", "CELL_B"],
            "read count": [5.0, np.nan],
            "UMI count": [2.0, np.nan],
        },
        index=index,
    )
    original = obs.copy(deep=True)
    joined = pd.DataFrame(index=index)

    def fake_join(frame, spec):
        del frame, spec
        return (
            pd.Series(["CELL_A-1", "CELL_B-2"], index=index),
            pd.Series(["1", "2"], index=index),
            joined,
            {"join_mismatch_count": 0},
        )

    monkeypatch.setattr(curation, "reproduce_scperturb_join", fake_join)
    by_vector, by_guide, _ = curation.load_table_s1()
    curated, receipt = curation.curate_obs(
        obs,
        "scperturb/adamson16_fixture",
        {"gsm": "GSMfixture", "sequencer": "Illumina", "n_obs": 2},
        by_vector,
        by_guide,
    )

    pd.testing.assert_frame_equal(curated.loc[:, original.columns], original)
    assert curated["cell_id"].tolist() == ["CELL_A-1", "CELL_B-2"]
    assert curated["batch"].tolist() == ["gemgroup_1", "gemgroup_2"]
    assert curated["guide_sequence"].tolist()[0] == "GGCTTGTTCGCTGGTGGCGT"
    assert pd.isna(curated["guide_sequence"].iloc[1])
    assert receipt["guide_sequence_known_rows"] == 1


def test_integrated_handoff_binds_live_receipts_and_noop_replay() -> None:
    handoff = json.loads(HANDOFF.read_text())

    assert handoff["status"] == "PASS"
    assert handoff["source_exhaustive"]["status"] is True
    assert handoff["source_exhaustive"]["source_join_mismatch_count"] == 0
    assert handoff["denominators"]["physical_members"] == 3
    assert handoff["denominators"]["observations"] == 86_111
    assert handoff["production_writes"]["obs_revisions"] == 3
    assert handoff["production_writes"]["var_revisions"] == 3
    assert handoff["replay"] == {
        "collection_writes": 0,
        "noop": True,
        "obs_revisions": 0,
        "status": "PASS",
        "var_revisions": 0,
    }
    for member in handoff["members"]:
        assert set(member["canonical_field_dispositions"]) == set(
            curation.CANONICAL_OBS_FIELDS
        )
        assert member["source_join"]["join_mismatch_count"] == 0
        assert member["var_verdict"]["needs_revision"] is False
        assert member["var_verdict"]["organism_values"] == ["Homo sapiens"]
    for evidence in handoff["evidence"].values():
        path = Path(__file__).parents[1] / evidence["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == evidence["sha256"]
