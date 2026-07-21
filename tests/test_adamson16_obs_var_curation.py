from __future__ import annotations

import hashlib
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
    / "artifacts/schema_audit/real_dataset_curation_20260721/scperturb_adamson16/t_f1d056cd/curate_obs_var.py"
)
SPEC = importlib.util.spec_from_file_location("adamson16_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
curation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curation)
HANDOFF = SCRIPT.with_name("integrated_handoff.json")
LOCAL_RECEIPT = SCRIPT.with_name("local_verifier_receipt.json")


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

    result = curation.field_dispositions(frame, frame.copy(deep=True))

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
    binding = {
        "expected_rows": 2,
        "ordered_index_sha256": curation.ordered_values_sha256(curated.index),
        "x": {"uid": "x-uid", "hash": "x-hash"},
        "var": {"uid": "var-uid", "hash": "var-hash"},
    }
    verdict = curation.verify_var(
        curated,
        binding,
        x_identity=binding["x"],
        var_identity=binding["var"],
    )
    assert verdict["needs_revision"] is False
    assert verdict["axis_order_parity"] is True


def _valid_var() -> Any:
    return pd.DataFrame(
        {
            "stable_feature_id": ["ENSG00000123456", "ENSG00000123457"],
            "stable_feature_id_namespace": ["Ensembl stable gene ID"] * 2,
            "stable_feature_id_mapping_status": ["exact_stable_id"] * 2,
            "organism": ["Homo sapiens"] * 2,
        },
        index=pd.Index(["GENE_A", "GENE_B"], name="gene_symbol"),
    )


def _axis_binding(frame: Any) -> dict[str, Any]:
    return {
        "expected_rows": len(frame),
        "ordered_index_sha256": curation.ordered_values_sha256(frame.index),
        "x": {"uid": "accepted-x", "hash": "accepted-x-hash"},
        "var": {"uid": "accepted-var", "hash": "accepted-var-hash"},
    }


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda frame: frame.assign(
                stable_feature_id=["NOT_ENSG", "ENSG00000123457"]
            ),
            "human ENSG",
        ),
        (
            lambda frame: frame.assign(stable_feature_id=[pd.NA, "ENSG00000123457"]),
            "human ENSG",
        ),
        (
            lambda frame: frame.assign(
                stable_feature_id=["ENSG00000123456", "ENSG00000123456"]
            ),
            "stable_feature_id uniqueness",
        ),
        (
            lambda frame: frame.assign(
                stable_feature_id_namespace=["Ensembl-ish", "Ensembl stable gene ID"]
            ),
            "namespace",
        ),
        (
            lambda frame: frame.assign(
                stable_feature_id_mapping_status=["mapped", "exact_stable_id"]
            ),
            "mapping status",
        ),
        (
            lambda frame: frame.assign(organism=["Mus musculus", "Homo sapiens"]),
            "organism",
        ),
        (
            lambda frame: frame.set_axis(["GENE_A", "GENE_A"]),
            "index uniqueness",
        ),
    ],
)
def test_var_verifier_rejects_semantically_invalid_feature_axes(
    mutation, message
) -> None:
    valid = _valid_var()
    invalid = mutation(valid.copy(deep=True))
    binding = _axis_binding(valid)

    with pytest.raises(AssertionError, match=message):
        curation.verify_var(
            invalid,
            binding,
            x_identity=binding["x"],
            var_identity=binding["var"],
        )


@pytest.mark.parametrize("drift", ["count", "order"])
def test_var_verifier_rejects_count_and_order_drift_from_independent_axis(
    drift,
) -> None:
    accepted = _valid_var()
    binding = _axis_binding(accepted)
    candidate = accepted.iloc[:1] if drift == "count" else accepted.iloc[::-1]

    with pytest.raises(AssertionError, match="VAR (row|feature order) drift"):
        curation.verify_var(
            candidate,
            binding,
            x_identity=binding["x"],
            var_identity=binding["var"],
        )


def test_var_verifier_rejects_unbound_x_or_var_artifact_identity() -> None:
    valid = _valid_var()
    binding = _axis_binding(valid)

    with pytest.raises(AssertionError, match="accepted X identity drift"):
        curation.verify_var(
            valid,
            binding,
            x_identity={"uid": "wrong", "hash": "accepted-x-hash"},
            var_identity=binding["var"],
        )


def test_curation_preserves_existing_columns_and_materializes_source_values(
    monkeypatch,
) -> None:
    index = pd.Index(["CELL_A", "CELL_B"], name="cell_barcode")
    obs = pd.DataFrame(
        {
            "pert_genetic": pd.Categorical(["OST4_pDS353", "*"]),
            "pert_target": pd.Categorical(["OST4", None]),
            "assay": pd.Categorical(["10x 3' v1", "10x 3' v1"]),
            "organism": pd.Categorical(["human", "human"]),
            "cell_line": pd.Categorical(["K-562", "K-562"]),
            "cell_type": pd.Categorical(["lymphoblast", "lymphoblast"]),
            "disease": pd.Categorical(
                ["chronic myelogenous leukemia, BCR-ABL1 positive"] * 2
            ),
            "tissue_type": pd.Categorical(["cell culture", "cell culture"]),
            "sex": pd.Categorical(["female", "female"]),
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


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("organism", "Mus musculus"),
        ("cell_line", "WRONG"),
        ("disease", "WRONG"),
        ("guide_sequence", "NNNN"),
    ],
)
def test_obs_semantic_verifier_rejects_wrong_but_non_null_values(field, wrong) -> None:
    expected = pd.DataFrame(
        {
            "organism": ["human"],
            "cell_line": ["K-562"],
            "disease": ["chronic myelogenous leukemia, BCR-ABL1 positive"],
            "guide_sequence": ["GGCTTGTTCGCTGGTGGCGT"],
        },
        index=pd.Index(["CELL_A"], name="cell_barcode"),
    )
    actual = expected.copy(deep=True)
    actual.loc["CELL_A", field] = wrong

    with pytest.raises(AssertionError, match=f"OBS source semantic mismatch: {field}"):
        curation.verify_obs_semantics(actual, expected)


def test_frozen_inputs_are_available_and_match_card_hashes() -> None:
    manifest = curation.load_frozen_input_bindings()

    assert {entry["uncompressed_sha256"] for entry in manifest["inputs"]} == {
        "65388d3d575d99961e2f8fb62d35dd38366d50268068ff144445af6530b54a9b",
        "60530cc3a14fe28e1dbf06c9f62b3e993649750069287083e4351aea3f8318df",
    }


def test_local_verifier_receipt_is_deterministic_and_claims_no_remote_replay() -> None:
    committed = json.loads(LOCAL_RECEIPT.read_text())

    assert committed == curation.build_local_verifier_receipt()
    assert committed["status"] == "LOCAL_PASS_REMOTE_REVIEW_PENDING"
    assert committed["remote_live_verification"] == {
        "reason": "continuation is local-only; no VM, Lamin, GCS, Collection, or payload access",
        "replay_claimed": False,
        "status": "PENDING_INDEPENDENT_REVIEWER",
    }
    assert set(committed["continuation_writes"].values()) == {0}


def test_integrated_handoff_binds_corrected_local_receipt_and_prior_production() -> (
    None
):
    handoff = json.loads(HANDOFF.read_text())

    assert handoff["status"] == "LOCAL_PASS_REMOTE_REVIEW_PENDING"
    assert handoff["source_exhaustive"]["status"] is True
    assert handoff["source_exhaustive"]["source_join_mismatch_count"] == 0
    assert handoff["denominators"]["physical_members"] == 3
    assert handoff["denominators"]["observations"] == 86_111
    assert handoff["production_writes"]["obs_revisions"] == 3
    assert handoff["production_writes"]["var_revisions"] == 3
    assert (
        handoff["remote_live_verification"]["status"] == "PENDING_INDEPENDENT_REVIEWER"
    )
    assert handoff["remote_live_verification"]["replay_claimed"] is False
    assert handoff["continuation_writes"] == {
        "collections": 0,
        "gcs": 0,
        "lamin": 0,
        "obs": 0,
        "var": 0,
        "x": 0,
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
