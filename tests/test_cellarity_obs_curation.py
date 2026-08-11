from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import nbclient
import nbformat
import pandas as pd
import pytest

SCRIPT = (
    Path(__file__).parents[1] / "artifacts/schema_audit/real_dataset_curation_20260723/"
    "cellarity_public_collection/t_9c09e453/curate_obs.py"
)
SPEC = importlib.util.spec_from_file_location("cellarity_obs_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
curation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curation)
EVIDENCE_ROOT = SCRIPT.parent
PROCESSING_DECISIONS_NOTEBOOK = (
    EVIDENCE_ROOT / "Cellarity_public_collection_processing_decisions.ipynb"
)
LIVE_RECEIPT_INDEX = EVIDENCE_ROOT / "live_receipt_index.json"
AUTHORITATIVE_MUTATION_RECEIPT = EVIDENCE_ROOT / "authoritative_mutation_receipt.json"
ZERO_WRITE_VERIFY_RECEIPT = EVIDENCE_ROOT / "zero_write_verify_receipt.json"
OBS_CONTRACT = Path(__file__).parents[1] / "config/obs_completed_contract_v1.json"


def member(kind: str, rows: int) -> dict:
    spec = next(item for item in curation.MEMBERS if item["kind"] == kind)
    return {**spec, "n_obs": rows}


def base_obs(index: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {"obs_uuid": [f"uuid-{value}" for value in index]},
        index=pd.Index(index),
    )


def test_pre_demultiplexing_raw_rows_do_not_claim_treatment() -> None:
    index = ["c1", "c2"]
    source = pd.DataFrame(
        {
            "LIBRARY_ID": ["L1", "L2"],
            "n_counts": [100.0, 200.0],
            "percent_mito": [0.1, 0.2],
            "sample_name": ["Day 1_Lib 1", "Day 2_Lib 1"],
        },
        index=index,
    )

    curated = curation.curate_obs(
        base_obs(index), source, member("gse305979_raw", len(index))
    )

    assert curated["perturbation"].isna().all()
    assert curated["perturbation_state"].eq("unknown").all()
    assert curated["is_control"].isna().all()
    assert curated["timepoint"].tolist() == [1440.0, 2880.0]
    assert curated["is_baseline"].eq(False).all()


def test_pseudobulk_distinguishes_vehicle_from_compound_dose() -> None:
    index = ["s1", "s2"]
    source = pd.DataFrame(
        {
            "bio_sample_id": ["sample-1", "sample-2"],
            "cell_id": ["A375", "A549"],
            "compound_name": ["DMSO", "Drug A"],
            "dose_uM": [pd.NA, 1.0],
            "library_id": ["L1", "L2"],
            "replicate": [1, 1],
            "timepoint_hr": [24.0, 24.0],
        },
        index=index,
    )

    curated = curation.curate_obs(
        base_obs(index), source, member("gse306429_pseudobulk", len(index))
    )

    assert curated["is_pseudobulk"].eq(True).all()
    assert curated["is_control"].tolist() == [True, False]
    assert curated["dose_state"].tolist() == ["not_applicable", "known"]
    assert pd.isna(curated.loc["s1", "dose"])
    assert curated.loc["s2", "dose"] == 1.0
    assert curated["cell_line"].tolist() == ["A375", "A549"]


@pytest.mark.parametrize(
    ("kind", "source_column"),
    [
        ("gse305370_citeseq", "time"),
        ("gse305370_rna", "day"),
    ],
)
def test_gse305370_time_axis_materializes_baseline(
    kind: str, source_column: str
) -> None:
    index = ["day-zero", "day-two"]
    source = pd.DataFrame(
        {
            source_column: [0, 2],
            "donor": ["donor-1", "donor-1"],
            "library": ["library-1", "library-1"],
        },
        index=index,
    )

    curated = curation.curate_obs(base_obs(index), source, member(kind, len(index)))

    assert curated["timepoint"].tolist() == [0.0, 2880.0]
    assert curated["is_baseline"].tolist() == [True, False]
    assert curated["is_baseline_state"].eq("known").all()


def test_gse305370_missing_row_time_keeps_baseline_applicable_unknown() -> None:
    index = ["cell-1"]
    source = pd.DataFrame({"library": ["library-1"]}, index=index)

    curated = curation.curate_obs(
        base_obs(index), source, member("gse305370_multiome", len(index))
    )

    assert curated["is_baseline"].isna().all()
    assert curated["is_baseline_state"].eq("unknown").all()


def test_mutation_readback_requires_writes_and_exact_current_obs_identity() -> None:
    written = {
        "uid": "obs-new",
        "key": "cellarity/family/obs.parquet",
        "hash": "hash-new",
        "version": "2",
    }
    member_readback = {
        "identity": {"prefix": "cellarity/family"},
        "obs": written,
        "already_curated": True,
    }

    with pytest.raises(AssertionError, match="zero writes"):
        curation.validate_mutation_readback([], [member_readback])
    with pytest.raises(AssertionError, match="post-write readback"):
        curation.validate_mutation_readback(
            [written], [{**member_readback, "obs": {**written, "uid": "other"}}]
        )

    assert curation.validate_mutation_readback([written], [member_readback]) == [
        written
    ]


def test_recover_mutation_writes_uses_only_canonical_unreceipted_members() -> None:
    fresh = {"uid": "fresh", "key": "data/cleaned/fresh/obs.parquet"}
    recovered = {"uid": "recovered", "key": "data/cleaned/recovered/obs.parquet"}
    members = [
        {"obs": fresh, "already_curated": True},
        {"obs": recovered, "already_curated": True},
    ]

    assert curation.recover_mutation_writes([fresh], members, expected=2) == [recovered]
    with pytest.raises(AssertionError, match="recovery denominator"):
        curation.recover_mutation_writes([fresh], members, expected=3)


def test_remote_attestation_binds_receipt_digest_and_exact_object_generation() -> None:
    receipt = {"canonical_sha256": "a" * 64, "mode": "mutate"}
    remote = {
        "uri": "gs://bucket/path/mutate-receipt.json",
        "generation": 123,
        "size": 42,
        "sha256": "b" * 64,
        "crc32c": "AAAAAA==",
        "etag": "etag-value",
    }

    attestation = curation.remote_attestation(receipt, remote)

    assert attestation["receipt_canonical_sha256"] == "a" * 64
    assert attestation["remote_identity"] == remote
    assert len(attestation["canonical_sha256"]) == 64


def test_canonical_prefix_is_a_flat_data_cleaned_triplet_prefix() -> None:
    spec = member("gse305370_rna", 1)

    assert curation.canonical_prefix(spec) == (
        "data/cleaned/GSE305370_rna_combined_with_velocity_and_refined_annotations"
    )


def test_replace_collection_members_preserves_unrelated_members_exactly_once() -> None:
    old_a = SimpleNamespace(uid="old-a", key="cellarity/a/obs.parquet")
    old_b = SimpleNamespace(uid="old-b", key="cellarity/b/obs.parquet")
    unrelated = SimpleNamespace(uid="other", key="other/obs.parquet")
    new_a = SimpleNamespace(uid="new-a", key="data/cleaned/a/obs.parquet")
    new_b = SimpleNamespace(uid="new-b", key="data/cleaned/b/obs.parquet")

    replaced = curation.replace_collection_members(
        [old_a, unrelated, old_b], {"old-a": new_a, "old-b": new_b}
    )

    assert [item.uid for item in replaced] == ["new-a", "other", "new-b"]
    with pytest.raises(AssertionError, match="predecessor membership drift"):
        curation.replace_collection_members(
            [old_a, unrelated], {"old-a": new_a, "old-b": new_b}
        )


def test_staging_decommission_gate_is_fail_closed() -> None:
    empty = curation.staging_decommission_gate([])
    assert empty["GCS_DECOMMISSION_READY"] is True
    assert empty["objects_remaining"] == 0

    with pytest.raises(AssertionError, match="staging objects remain"):
        curation.staging_decommission_gate(
            [{"name": "receipt.json", "generation": 123, "size": 42}]
        )


def test_var_gate_requires_exact_human_ensembl_ids_for_every_gene_feature() -> None:
    var = pd.DataFrame(
        {
            "feature_class": ["gene", "gene", "chromatin_accessibility_peak"],
            "stable_feature_id": ["ENSG00000121410", "ENSG00000175899", pd.NA],
            "stable_feature_id_mapping_status": [
                "exact_stable_id",
                "exact_stable_id",
                "not_applicable_non_gene_atac_peak",
            ],
        },
        index=pd.Index(["A1BG", "A2M", "chr1:1-10"]),
    )
    source = {
        "source_var_rows": 3,
        "source_var_index_sha256": curation.ordered_sha256(var.index),
    }

    receipt = curation.verify_var(var, source)

    assert receipt["VAR_ENSEMBL_SPECIES_COMPLETED"] is True
    assert receipt["biological_features_total"] == 2
    assert receipt["stable_ensembl_id_features"] == 2
    assert receipt["non_biological_features_not_applicable"] == 1
    transformed = var.copy()
    transformed["pert_gym_original_var_index"] = var.index
    transformed.index = pd.Index(["A1BG", "A2M", "peak_0"])
    transformed_receipt = curation.verify_var(transformed, source)
    assert transformed_receipt["axis_identity_source"] == "pert_gym_original_var_index"
    broken = var.copy()
    broken.loc["A2M", "stable_feature_id"] = pd.NA
    with pytest.raises(AssertionError, match="VAR Ensembl/species gate failed"):
        curation.verify_var(broken, source)


def test_var_gate_uses_exact_source_axis_column_when_display_index_differs() -> None:
    var = pd.DataFrame(
        {
            "gene_ids": ["ENSG00000121410", "ENSG00000175899"],
            "stable_feature_id": ["ENSG00000121410", "ENSG00000175899"],
            "stable_feature_id_mapping_status": ["exact_stable_id", "exact_stable_id"],
        },
        index=pd.Index(["A1BG", "A2M"]),
    )
    source_axis = pd.Index(var["gene_ids"])

    receipt = curation.verify_var(
        var,
        {
            "source_var_rows": len(var),
            "source_var_index_sha256": curation.ordered_sha256(source_axis),
        },
    )

    assert receipt["axis_identity_source"] == "gene_ids"
    assert receipt["matching_axis_identity_sources"] == ["gene_ids"]
    assert receipt["ordered_var_axis_sha256"] == curation.ordered_sha256(source_axis)


def test_var_gate_accepts_only_frozen_one_to_one_case_normalization() -> None:
    var = pd.DataFrame(
        {
            "pert_gym_original_var_index": ["C1orf159", "A2M"],
            "stable_feature_id": ["ENSG00000121410", "ENSG00000175899"],
            "stable_feature_id_mapping_status": ["exact_stable_id", "exact_stable_id"],
        },
        index=pd.Index(["C1orf159", "A2M"]),
    )
    accepted = pd.Index(var["pert_gym_original_var_index"])
    source = pd.Index(["C1ORF159", "A2M"])
    source_evidence = {
        "source_var_rows": len(var),
        "source_var_index_sha256": curation.ordered_sha256(source),
        "accepted_var_index_sha256": curation.ordered_sha256(accepted),
        "source_var_axis_casefold_sha256": curation.ordered_sha256(
            pd.Index(source.str.casefold())
        ),
        "source_var_axis_case_normalization_mismatches": 1,
    }

    receipt = curation.verify_var(var, source_evidence)

    assert receipt["axis_match_mode"] == "exact_ordered_case_normalization_bijection"
    assert receipt["source_axis_byte_exact"] is False
    assert receipt["axis_identity_source"] == "var.index"
    assert receipt["source_axis_case_normalization_mismatches"] == 1

    ambiguous = pd.concat([var, var.iloc[[0]]], ignore_index=True)
    ambiguous_source = {**source_evidence, "source_var_rows": len(ambiguous)}
    with pytest.raises(AssertionError, match="VAR Ensembl/species gate failed"):
        curation.verify_var(ambiguous, ambiguous_source)


def test_receipt_member_preserves_var_axis_verification() -> None:
    verification = {
        "status": "PASS",
        "VAR_ENSEMBL_SPECIES_COMPLETED": True,
        "ordered_var_axis_sha256": "a" * 64,
        "axis_identity_source": "pert_gym_original_var_index",
    }
    artifact = SimpleNamespace(
        uid="artifact-uid",
        key="data/cleaned/cellarity/family/var.parquet",
        hash="artifact-hash",
        version="0001",
        size=42,
        path="s3://bucket/artifact.parquet",
        description="test artifact",
        created_at="2026-08-11T00:00:00Z",
        run=SimpleNamespace(uid="run-uid"),
    )
    result = {
        "obs_artifact": artifact,
        "x_artifact": artifact,
        "var_artifact": artifact,
        "var_verification": verification,
        "history_count": 1,
        "already_curated": True,
        "source_join": {"exact_index_order_match": True},
        "field_dispositions": {},
    }

    member = curation.strip_runtime(result)

    assert member["var_verification"] == verification


def _canonical_receipt_sha256(receipt: dict[str, object]) -> str:
    unsigned = {
        key: value for key, value in receipt.items() if key != "canonical_sha256"
    }
    payload = json.dumps(
        unsigned, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def test_committed_live_receipts_reconcile_authoritative_transition() -> None:
    mutation = json.loads(AUTHORITATIVE_MUTATION_RECEIPT.read_text())
    verify = json.loads(ZERO_WRITE_VERIFY_RECEIPT.read_text())
    index = json.loads(LIVE_RECEIPT_INDEX.read_text())["receipts"]

    for name, receipt in (
        ("authoritative_mutation", mutation),
        ("zero_write_verify", verify),
    ):
        assert receipt["status"] == "PASS"
        assert receipt["canonical_sha256"] == _canonical_receipt_sha256(receipt)
        assert index[name]["canonical_sha256"] == receipt["canonical_sha256"]
        assert index[name]["remote"]["uri"].startswith(
            "gs://scperturb/data/cleaned/cellarity_public_collection/_receipts/"
        )

    assert mutation["mode"] == "mutate"
    assert mutation["registry_counts"] == {
        "before": {"artifacts": 28590, "collections": 52},
        "after": {"artifacts": 28600, "collections": 55},
    }
    assert mutation["writes"]["obs_revisions"] == 10
    assert mutation["writes"]["collection_writes"] == 3
    assert len(mutation["writes"]["artifacts"]) == 10
    assert len(mutation["post_write_readback"]) == 10
    assert mutation["posthoc_var_verification_binding"] == {
        "original_mutation_receipt_canonical_sha256": (
            "12466181392258c8007f2adbce4034d0b2b62643b0ffce9a7103fda8e3a2d539"
        ),
        "source_receipt_canonical_sha256": verify["canonical_sha256"],
        "source_receipt_mode": "verify",
        "scope": "exact member VAR identities preserved by the mutation transition",
        "temporal_semantics": (
            "VAR proofs were observed by the later zero-write replay and bind to the "
            "mutation members by exact prefix and VAR artifact identity"
        ),
    }
    assert (
        index["authoritative_mutation"]["remote_canonical_sha256"]
        == (
            mutation["posthoc_var_verification_binding"][
                "original_mutation_receipt_canonical_sha256"
            ]
        )
    )
    assert index["authoritative_mutation"]["local_derivation"] == (
        "posthoc VAR proof binding; immutable remote is the original mutation receipt"
    )

    assert verify["mode"] == "verify"
    assert verify["replay_noop"] is True
    assert verify["registry_counts"]["before"] == verify["registry_counts"]["after"]
    assert verify["writes"]["obs_revisions"] == 0
    assert verify["writes"]["collection_writes"] == 0
    assert len(verify["members"]) == 10
    for receipt in (mutation, verify):
        proofs = [member["var_verification"] for member in receipt["members"]]
        assert len(proofs) == 10
        assert all(proof["VAR_ENSEMBL_SPECIES_COMPLETED"] for proof in proofs)
        assert {proof["axis_match_mode"] for proof in proofs} == {
            "byte_exact",
            "exact_ordered_case_normalization_bijection",
        }
        assert sum(not proof["source_axis_byte_exact"] for proof in proofs) == 3
    assert verify["gcs_decommission"]["GCS_DECOMMISSION_READY"] is True
    assert verify["gcs_decommission"]["objects_remaining"] == 0


def test_processing_decisions_notebook_executes_offline() -> None:
    notebook = nbformat.read(PROCESSING_DECISIONS_NOTEBOOK, as_version=4)
    nbclient.NotebookClient(notebook, timeout=120, kernel_name="python3").execute(
        cwd=Path(__file__).parents[1]
    )


def test_every_canonical_field_has_value_state_and_source_columns() -> None:
    index = ["c1"]
    source = pd.DataFrame(
        {
            "CELL_ID": ["CCL-171"],
            "CONCENTRATION_UM": [0.0],
            "LIBRARY_ID": ["L1"],
            "TIMEPOINT_HOURS": [0.0],
        },
        index=index,
    )
    curated = curation.curate_obs(
        base_obs(index), source, member("gse305979_day0_raw", 1)
    )

    for field in curation.CANONICAL_OBS_FIELDS:
        assert field in curated
        assert f"{field}_state" in curated
        assert f"{field}_source" in curated
    assert curated.loc["c1", "perturbation"] == "No treatment"
    assert bool(curated.loc["c1", "is_control"])
    assert bool(curated.loc["c1", "is_baseline"])


def test_canonical_fields_follow_binding_obs_completed_contract() -> None:
    contract = json.loads(OBS_CONTRACT.read_text())

    assert list(curation.CANONICAL_OBS_FIELDS) == contract["canonical_obs_columns"]
    assert len(curation.CANONICAL_OBS_FIELDS) == contract["canonical_obs_column_count"]
    assert curation.OBS_CONTRACT_SHA256 == curation.sha256_file(OBS_CONTRACT)


def test_supplemental_source_backed_fields_are_not_miscounted_as_canonical() -> None:
    index = ["c1"]
    source = pd.DataFrame(
        {
            "CELL_ID": ["CCL-171"],
            "CONCENTRATION_UM": [1.0],
            "LIBRARY_ID": ["L1"],
            "TIMEPOINT_HOURS": [24.0],
        },
        index=index,
    )

    curated = curation.curate_obs(
        base_obs(index), source, member("gse305979_day0_raw", 1)
    )

    assert "source_accession" in curated
    assert "source_accession" not in curation.CANONICAL_OBS_FIELDS
    assert "molecule_sequence" in curated
    assert set(curation.field_dispositions(curated)) == set(
        curation.CANONICAL_OBS_FIELDS
    )


def test_task_owned_revision_reuses_frozen_source_join_and_only_repairs_contract() -> (
    None
):
    existing = curation.curate_obs(
        base_obs(["day-zero", "day-two"]),
        pd.DataFrame(
            {
                "day": [0, 2],
                "donor": ["donor-1", "donor-1"],
                "library": ["library-1", "library-1"],
            },
            index=["day-zero", "day-two"],
        ),
        member("gse305370_rna", 2),
    )
    existing = existing.drop(
        columns=[
            "molecule_sequence",
            "molecule_sequence_state",
            "molecule_sequence_source",
        ]
    )
    existing["is_baseline"] = pd.Series(
        [pd.NA, pd.NA], index=existing.index, dtype="boolean"
    )
    existing["is_baseline_state"] = "not_applicable"

    revised = curation.revise_task_owned_obs(existing, member("gse305370_rna", 2))

    assert revised["is_baseline"].tolist() == [True, False]
    assert revised["is_baseline_state"].eq("known").all()
    assert revised["molecule_sequence"].isna().all()
    assert revised["molecule_sequence_state"].eq("unknown").all()
    assert existing.index.equals(revised.index)
    assert len(existing) == len(revised)


def test_predecessor_receipt_is_accepted_for_source_join_not_mutation_credit() -> None:
    evidence = curation.load_predecessor_source_evidence()

    assert len(evidence["members"]) == 10
    assert evidence["adjudication"]["mutation_credit"] is False
    assert evidence["adjudication"]["source_join_evidence_reusable"] is True
    assert evidence["receipt_canonical_sha256"] == (
        "0f1429d634e8a8ac74ef50ccc8826a867e6652f9fc82a06d645fc64ad244c41c"
    )


def test_metadata_preflight_keeps_exact_vm_and_bounded_capacity_gates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = (
        "pert-gym-worker-eu",
        "jkobject-1549353370965",
        "europe-west1-b",
        "pert-gym-worker-eu",
    )
    monkeypatch.setattr(curation, "require_heavy_vm", lambda: identity)
    monkeypatch.setattr(curation, "_available_memory_bytes", lambda: 32 * 1024**3)
    monkeypatch.setattr(
        curation.shutil,
        "disk_usage",
        lambda _: SimpleNamespace(free=20 * 1024**3),
    )

    capacity = curation.metadata_preflight()

    assert capacity.hostname == "pert-gym-worker-eu"
    assert capacity.free_disk_bytes == 20 * 1024**3
    monkeypatch.setattr(
        curation.shutil,
        "disk_usage",
        lambda _: SimpleNamespace(free=9 * 1024**3),
    )
    with pytest.raises(RuntimeError, match="insufficient metadata-only disk"):
        curation.metadata_preflight()


def test_source_join_requires_exact_unique_index_order() -> None:
    obs = base_obs(["a", "b"])
    source = pd.DataFrame({"LIBRARY_ID": ["L1", "L2"]}, index=["b", "a"])

    with pytest.raises(AssertionError, match="exact index join failed"):
        curation.verify_source_join(obs, source, member("gse305979_day0_raw", len(obs)))


def test_source_join_normalizes_equivalent_string_index_dtypes() -> None:
    obs = base_obs(["a", "b"])
    obs.index = pd.Index(pd.Series(["a", "b"], dtype="string"))
    source = pd.DataFrame({"LIBRARY_ID": ["L1", "L2"]}, index=["a", "b"])

    result = curation.verify_source_join(
        obs, source, member("gse305979_day0_raw", len(obs))
    )

    assert result["rows"] == 2


def test_source_join_uses_unique_index_when_curated_columns_changed() -> None:
    obs = base_obs(["a", "b"])
    obs["cell_line"] = ["normalized-1", "normalized-2"]
    source = pd.DataFrame({"cell_id": ["raw-1", "raw-2"]}, index=obs.index)

    result = curation.verify_source_join(
        obs, source, member("gse306429_demuxed", len(obs))
    )

    assert result["index_unique"] is True
    assert result["column_equalities"] == {"cell_id->cell_line": False}


def test_source_join_proves_non_unique_pseudobulk_rows() -> None:
    index = ["0", "0"]
    source = pd.DataFrame(
        {
            "bio_sample_id": ["sample-1", "sample-2"],
            "cell_id": ["A375", "A549"],
            "compound_name": ["DMSO", "Drug A"],
            "dose_uM": [pd.NA, 1.0],
            "library_id": ["L1", "L2"],
            "replicate": [1, 1],
            "timepoint_hr": [24.0, 24.0],
        },
        index=index,
    )
    obs = source.copy()
    obs["cell_id"] = ["legacy-1", "legacy-2"]
    obs["cell_line"] = ["normalized-1", "normalized-2"]
    obs["original_obs_index"] = index
    obs["obs_uuid"] = ["uuid-1", "uuid-2"]

    result = curation.verify_source_join(
        obs, source, member("gse306429_pseudobulk", len(obs))
    )

    assert result["index_unique"] is False
    assert len(result["column_equalities"]) == len(source.columns)
    assert result["column_equalities"]["cell_id->cell_id"] is False
    assert "bio_sample_id" in result["row_identity_columns"]


def test_source_join_rejects_non_unique_rows_without_stable_identity() -> None:
    obs = base_obs(["0", "0"])
    source = pd.DataFrame({"LIBRARY_ID": ["L1", "L2"]}, index=["0", "0"])

    with pytest.raises(AssertionError, match="lacks exact row identity proof"):
        curation.verify_source_join(obs, source, member("gse305979_day0_raw", len(obs)))


def test_source_join_compares_author_alias_before_normalized_column() -> None:
    obs = base_obs(["a", "b"])
    obs["cell_type"] = ["ontology A", "ontology B"]
    obs["cell_type_from_author"] = ["author A", "author B"]
    source = pd.DataFrame({"cell_type": ["author A", "author B"]}, index=obs.index)

    result = curation.verify_source_join(
        obs, source, member("gse305370_citeseq", len(obs))
    )

    assert result["column_equalities"] == {"cell_type->cell_type_from_author": True}


def test_source_join_compares_cell_id_to_cell_line_alias() -> None:
    obs = base_obs(["a", "b"])
    obs["cell_line"] = ["A375", "A549"]
    source = pd.DataFrame({"cell_id": ["A375", "A549"]}, index=obs.index)

    result = curation.verify_source_join(
        obs, source, member("gse306429_vscores", len(obs))
    )

    assert result["column_equalities"] == {"cell_id->cell_line": True}


def test_frozen_bindings_and_source_manifest_cover_exact_live_identities() -> None:
    manifest = json.loads((EVIDENCE_ROOT / "source_manifest.json").read_text())
    inspection = json.loads((EVIDENCE_ROOT / "source_inspection.json").read_text())
    manifest_by_name = {
        item["filename"]: item for item in manifest["target_source_objects"]
    }

    assert len(manifest_by_name) == len(curation.MEMBERS) == 10
    assert sum(item["n_obs"] for item in manifest_by_name.values()) == 2_212_441
    for spec, inspected in zip(curation.MEMBERS, inspection["members"], strict=True):
        assert (
            manifest_by_name[spec["filename"]]["sha256"]
            == inspected["source"]["sha256"]
        )
        assert spec["x_hash"] == inspected["accepted_artifacts"]["x"]["hash"]
        assert spec["var_uid"] == inspected["accepted_artifacts"]["var"]["uid"]
    assert len(curation.load_frozen_inputs()["inputs"]) == 2


def test_gse305370_rna_uses_latest_obs_identity() -> None:
    spec = next(item for item in curation.MEMBERS if item["kind"] == "gse305370_rna")

    assert spec["before_obs_uid"] == "RJbcZEfscysBCeMj0001"
