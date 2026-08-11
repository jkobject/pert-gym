from __future__ import annotations

import copy
import csv
import hashlib
import json
import subprocess
from pathlib import Path

import pytest

import tools.build_dataset_review_inventory_csv as inventory_builder
from tools.build_dataset_review_inventory_csv import build_rows, summary

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data/pert_gym_dataset_review_inventory.csv"
BASELINE = ROOT / "data/pert_gym_dataset_review_inventory_baseline_20260729.csv"
ACCEPTED_WAVE = (
    ROOT / "artifacts/dataset_completion/accepted_10_dataset_review_snapshot.json"
)
INTEGRATION_MANIFEST = (
    ROOT / "artifacts/dataset_completion/accepted_10_of_10_integration_manifest.json"
)
ACCEPTED_EVIDENCE_DIGESTS = ROOT / "data/accepted_10_evidence_digests.json"

ACCEPTED_WAVE_DATASET_IDS = {
    "temporal/an_alternative_cell_cycle_coordinates_multiciliated_cell_differentiation",
    "temporal/c_elegans_embryogenesis",
    "temporal/drosophila_embryo_dorsal_ventral_patterning_scrna_seq",
    "temporal/organoiddb_odd001099_gse138002",
    "temporal/organoiddb_odd001111_gse130238",
    "temporal/organoiddb_odd001154_gse194214",
    "temporal/organoiddb_odd001155_gse196799",
    "temporal/perturbase_gse107185",
    "temporal/scrnaseq_unravels_the_transcriptional_network_underlying_zebrafish_retina_regene",
    "temporal/stable_chambered_cardioids_from_human_pluripotent_stem_cells_scrna_seq",
}


def _truth(value: str) -> bool:
    return value.lower() == "true"


def test_tracked_baseline_is_the_exact_pre_reconciliation_inventory() -> None:
    assert hashlib.sha256(BASELINE.read_bytes()).hexdigest() == (
        "6f79e32f7d829904debcacfe700ce3cd7b42a71428ba5044fe4be0ee1405842d"
    )
    with BASELINE.open(newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == inventory_builder.LEGACY_COLUMNS
        rows = list(reader)
    assert len(rows) == 92
    assert len({row["dataset_id"] for row in rows}) == 92


def test_dataset_review_inventory_has_unique_dataset_units_and_strict_counts() -> None:
    rows = build_rows()
    counts = summary(rows)

    assert len(rows) == 92
    assert len({row["dataset_id"] for row in rows}) == 92
    assert counts == {
        "unique_datasets": 92,
        "main_baseline_datasets": 26,
        "scoped_scientific_validation_accepted": 18,
        "accepted_wave_scoped_validation": 10,
        "entirely_validated": 0,
        "entirely_validated_main_existing": 0,
        "entirely_validated_jkobject_additions": 0,
        "new_families_registered_and_in_collection": 10,
        "new_families_entirely_validated": 0,
    }

    scoped = {
        row["dataset_id"]
        for row in rows
        if row["scoped_scientific_validation_accepted"]
    }
    assert (
        scoped
        == {
            "SchiebingerLander2019",
            "depmap_ccle/26q1",
            "drug-seq/GSE120222",
            "geo/GSE132080",
            "geo/GSE197452",
            "scperturb/adamson16",
            "scperturb/chang22",
            "scperturb/datlinger17",
        }
        | ACCEPTED_WAVE_DATASET_IDS
    )
    assert not {row["dataset_id"] for row in rows if row["entirely_validated"]}


def test_accepted_wave_binds_aliases_heads_reviewers_and_scientific_evidence() -> None:
    snapshot = json.loads(ACCEPTED_WAVE.read_text())
    integration = json.loads(INTEGRATION_MANIFEST.read_text())
    records = snapshot["datasets"]

    assert snapshot["dataset_count"] == 10
    assert {record["canonical_dataset_id"] for record in records} == (
        ACCEPTED_WAVE_DATASET_IDS
    )
    integration_by_dataset = {item["dataset"]: item for item in integration["datasets"]}
    assert {record["integration_dataset"] for record in records} == set(
        integration_by_dataset
    )
    for record in records:
        assert (
            record["accepted_head"]
            == integration_by_dataset[record["integration_dataset"]]["accepted_sha"]
        )
    assert snapshot["denominator_reconciliation"] == {
        "inventory_rows": 92,
        "strict_ledger_rows": 70,
        "genuinely_new_family_rows": 22,
        "accepted_wave_rows": 10,
        "accepted_wave_overlap_with_prior_scoped_complete": 0,
        "accepted_wave_overlap_with_strict_70_exact_ids": 0,
        "strict_70_alias_reconciliation": "unresolved_no_counter_credit",
        "scoped_validation_before": 8,
        "scoped_validation_after": 18,
        "full_dod_before": 0,
        "full_dod_after": 0,
    }
    for record in records:
        assert record["aliases"]
        assert record["producer_task_id"].startswith("t_")
        assert record["reviewer_task_id"].startswith("t_")
        assert record["scientific_modality"]
        assert record["experimental_axes"]
        assert record["outcomes_endpoints"]
        assert record["annotation_level"]
        assert record["source_evidence"]
        assert record["payload_evidence_path"]
        assert record["scientific_evidence_paths"]
        assert len(record["payload_prefixes"]) == record["physical_member_count"]
        assert all(
            prefix.startswith(("data/cleaned/", "pert-gym/logical/"))
            for prefix in record["payload_prefixes"]
        )
        if record["canonical_data_cleaned_payload"]:
            assert all(
                prefix.startswith("data/cleaned/")
                for prefix in record["payload_prefixes"]
            )


def test_accepted_wave_rejects_cross_dataset_or_unbound_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = json.loads(ACCEPTED_WAVE.read_text())

    def swap_heads(snapshot: dict) -> None:
        (
            snapshot["datasets"][0]["accepted_head"],
            snapshot["datasets"][1]["accepted_head"],
        ) = (
            snapshot["datasets"][1]["accepted_head"],
            snapshot["datasets"][0]["accepted_head"],
        )

    def fabricate_payload(snapshot: dict) -> None:
        snapshot["datasets"][0]["payload_prefixes"][0] = (
            "data/cleaned/fabricated-but-plausible"
        )

    def duplicate_payload_prefix(snapshot: dict) -> None:
        snapshot["datasets"][0]["payload_prefixes"][0] = snapshot["datasets"][0][
            "payload_prefixes"
        ][1]

    def cross_bind_scientific_evidence(snapshot: dict) -> None:
        snapshot["datasets"][0]["scientific_evidence_paths"] = snapshot["datasets"][1][
            "scientific_evidence_paths"
        ]

    def alter_observation_denominator(snapshot: dict) -> None:
        snapshot["datasets"][0]["observations"] += 1

    def swap_reviewers(snapshot: dict) -> None:
        snapshot["datasets"][0]["reviewer_task_id"] = snapshot["datasets"][1][
            "reviewer_task_id"
        ]

    for name, mutate in [
        ("swapped-heads", swap_heads),
        ("fabricated-payload", fabricate_payload),
        ("duplicate-payload-prefix", duplicate_payload_prefix),
        ("cross-bound-scientific-evidence", cross_bind_scientific_evidence),
        ("altered-observation-denominator", alter_observation_denominator),
        ("swapped-reviewer", swap_reviewers),
    ]:
        snapshot = copy.deepcopy(baseline)
        mutate(snapshot)
        candidate = tmp_path / f"{name}.json"
        candidate.write_text(json.dumps(snapshot))
        monkeypatch.setattr(inventory_builder, "ACCEPTED_WAVE", candidate)
        with pytest.raises(RuntimeError):
            inventory_builder.build_rows()


def test_accepted_wave_overlap_is_measured_against_unfiltered_baseline(
    tmp_path: Path,
) -> None:
    with BASELINE.open(newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames is not None
        fieldnames = reader.fieldnames
        rows = list(reader)
    row = next(
        item
        for item in rows
        if item["dataset_id"]
        == "temporal/an_alternative_cell_cycle_coordinates_multiciliated_cell_differentiation"
    )
    for field in [
        "strict_obs_validated",
        "strict_var_validated",
        "chunks_or_structure_validated",
        "cleaning_validated",
        "lamin_registered",
        "in_versioned_collection",
    ]:
        row[field] = "True"
    candidate = tmp_path / BASELINE.name
    with candidate.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    with pytest.raises(RuntimeError, match="overlap_with_prior_scoped_complete"):
        inventory_builder.build_rows(base=candidate)


def test_accepted_receipt_predicates_are_structured_and_fail_closed() -> None:
    snapshot = json.loads(ACCEPTED_WAVE.read_text())
    record = next(
        item
        for item in snapshot["datasets"]
        if item["integration_dataset"] == "GSE138002"
    )
    receipt = json.loads((ROOT / record["payload_evidence_path"]).read_text())
    assert inventory_builder._receipt_contract_valid("GSE138002", receipt)

    corrupted = copy.deepcopy(receipt)
    corrupted["gates"]["collection"] = "FAIL"
    with pytest.raises(RuntimeError, match="accepted receipt assertion failed"):
        inventory_builder._receipt_contract_valid("GSE138002", corrupted)


def test_integration_manifest_is_bound_to_accepted_integration_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / INTEGRATION_MANIFEST.name
    integration = json.loads(INTEGRATION_MANIFEST.read_text())
    integration["datasets"][0]["accepted_sha"] = integration["datasets"][1][
        "accepted_sha"
    ]
    candidate.write_text(json.dumps(integration))
    monkeypatch.setattr(inventory_builder, "INTEGRATION_MANIFEST", candidate)
    with pytest.raises(RuntimeError):
        inventory_builder.build_rows()


def test_accepted_evidence_digest_index_is_complete_and_hash_pinned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    index = json.loads(ACCEPTED_EVIDENCE_DIGESTS.read_text())
    records = {item["dataset"]: item for item in index["datasets"]}
    snapshot = json.loads(ACCEPTED_WAVE.read_text())
    for record in snapshot["datasets"]:
        indexed = records[record["integration_dataset"]]
        assert indexed["accepted_head"] == record["accepted_head"]
        paths = {item["path"] for item in indexed["files"]}
        expected = {
            *record["scientific_evidence_paths"],
            record["payload_evidence_path"],
        }
        if record["processing_decision_notebook_path"]:
            expected.add(record["processing_decision_notebook_path"])
        assert paths == expected

    index["datasets"][0]["files"][0]["sha256"] = "0" * 64
    candidate = tmp_path / ACCEPTED_EVIDENCE_DIGESTS.name
    candidate.write_text(json.dumps(index))
    monkeypatch.setattr(inventory_builder, "ACCEPTED_EVIDENCE_DIGESTS", candidate)
    with pytest.raises(RuntimeError, match="digest index has drifted"):
        inventory_builder.build_rows()


def test_payload_uid_bindings_reject_empty_or_disagreeing_roles() -> None:
    direct_record = {
        "canonical_dataset_id": "synthetic",
        "integration_dataset": "GSE269572",
        "payload_prefixes": ["data/cleaned/synthetic"],
    }
    empty_uid_document = {
        "links": {
            "obs": {"key": "data/cleaned/synthetic/obs.parquet", "uid": ""},
            "X": {"key": "data/cleaned/synthetic/X.h5ad", "uid": ""},
            "var": {"key": "data/cleaned/synthetic/var.parquet", "uid": ""},
        }
    }
    assert inventory_builder._structured_payload_complete(
        direct_record, empty_uid_document, [empty_uid_document]
    ) == (False, False)

    bare_uid_document: dict[str, dict[str, dict[str, object]]] = {
        "links": {
            "obs": {
                "key": "data/cleaned/synthetic/obs.parquet",
                "uid": "obs-uid",
            },
            "X": {
                "key": "data/cleaned/synthetic/X.h5ad",
                "uid": "x-uid",
            },
            "var": {
                "key": "data/cleaned/synthetic/var.parquet",
                "uid": "var-uid",
            },
        }
    }
    assert inventory_builder._structured_payload_complete(
        direct_record, bare_uid_document, [bare_uid_document]
    ) == (False, False)

    current_artifact_document = copy.deepcopy(bare_uid_document)
    for artifact in current_artifact_document["links"].values():
        artifact.update({"hash": f"hash-{artifact['uid']}", "is_latest": True})
    assert inventory_builder._structured_payload_complete(
        direct_record, current_artifact_document, [current_artifact_document]
    ) == (True, True)

    swapped_roles = copy.deepcopy(current_artifact_document)
    swapped_roles["links"]["obs"], swapped_roles["links"]["X"] = (
        swapped_roles["links"]["X"],
        swapped_roles["links"]["obs"],
    )
    with pytest.raises(RuntimeError, match="wrong key role"):
        inventory_builder._structured_payload_complete(
            direct_record, swapped_roles, [swapped_roles]
        )

    current_artifact_document["links"]["obs"]["is_latest"] = False
    assert inventory_builder._structured_payload_complete(
        direct_record, current_artifact_document, [current_artifact_document]
    ) == (False, False)

    snapshot = json.loads(ACCEPTED_WAVE.read_text())
    shared_record = next(
        item
        for item in snapshot["datasets"]
        if item["integration_dataset"] == "GSE196799 / ODD001155"
    )
    documents = [
        json.loads((ROOT / path).read_text())
        for path in {
            shared_record["payload_evidence_path"],
            *shared_record["scientific_evidence_paths"],
        }
    ]
    receipt = next(document for document in documents if "links" in document)
    assert inventory_builder._structured_payload_complete(
        shared_record, receipt, documents
    ) == (True, False)

    sample = shared_record["payload_prefixes"][0].rsplit("/", 1)[-1]
    link = next(row for row in receipt["links"]["rows"] if row["sample"] == sample)
    for obs_uid in ["fabricated-obs-uid", link["obs_uid"]]:
        with_obs_key = copy.deepcopy(documents)
        with_obs_key[0]["adversarial_obs_identity"] = {
            "key": f"{shared_record['payload_prefixes'][0]}/obs.parquet",
            "uid": obs_uid,
        }
        assert inventory_builder._structured_payload_complete(
            shared_record,
            next(document for document in with_obs_key if "links" in document),
            with_obs_key,
        ) == (True, False)

    corrupted = copy.deepcopy(documents)
    corrupted_receipt = next(document for document in corrupted if "links" in document)
    corrupted_receipt["links"]["rows"][0]["x_uid"] = "fabricated-x-uid"
    assert inventory_builder._structured_payload_complete(
        shared_record, corrupted_receipt, corrupted
    ) == (False, False)


def test_accepted_wave_is_scoped_but_full_dod_remains_fail_closed() -> None:
    rows = {
        row["dataset_id"]: row
        for row in build_rows()
        if row["dataset_id"] in ACCEPTED_WAVE_DATASET_IDS
    }
    assert len(rows) == 10
    assert all(row["accepted_wave_scoped_validation"] for row in rows.values())
    assert not any(row["strict_obs_validated"] for row in rows.values())
    assert not any(row["strict_var_validated"] for row in rows.values())
    assert not any(row["chunks_or_structure_validated"] for row in rows.values())
    assert not any(row["cleaning_validated"] for row in rows.values())
    assert all(row["scientific_contract_documented"] for row in rows.values())
    assert not any(row["scientific_contract_bound"] for row in rows.values())
    assert {
        dataset_id
        for dataset_id, row in rows.items()
        if not row["canonical_data_cleaned_payload"]
    } == {
        "temporal/drosophila_embryo_dorsal_ventral_patterning_scrna_seq",
        "temporal/organoiddb_odd001155_gse196799",
        "temporal/perturbase_gse107185",
        "temporal/scrnaseq_unravels_the_transcriptional_network_underlying_zebrafish_retina_regene",
    }
    assert sum(row["structured_payload_evidence"] for row in rows.values()) == 9
    assert all(row["accepted_head_integrated"] for row in rows.values())
    assert (
        sum(row["processing_decision_notebook_present"] for row in rows.values()) == 4
    )
    assert not any(row["processing_decision_notebook"] for row in rows.values())
    assert not any(row["staging_decommissioned_with_receipt"] for row in rows.values())
    assert not any(
        row["inventory_docs_same_snapshot_accepted"] for row in rows.values()
    )
    assert not any(row["exact_head_inventory_pr_merged"] for row in rows.values())
    assert not any(row["entirely_validated"] for row in rows.values())
    for row in rows.values():
        assert "scientific_contract_evidence" in row["missing_requirements"]
        assert "executable_processing_decision_notebook" in row["missing_requirements"]
        assert "staging_decommission_receipt" in row["missing_requirements"]
        assert "accepted_inventory_docs_snapshot" in row["missing_requirements"]
        assert "merged_exact_head_inventory_pr" in row["missing_requirements"]
    for row in rows.values():
        if row["canonical_data_cleaned_payload"]:
            assert "canonical_data_cleaned_payload" not in row["missing_requirements"]
        else:
            assert "canonical_data_cleaned_payload" in row["missing_requirements"]


def test_full_dod_requires_the_accepted_head_integration_gate() -> None:
    row = next(row for row in build_rows() if row["accepted_wave"])
    candidate = dict(row)
    candidate.update(
        {
            "scientific_contract_bound": True,
            "processing_decision_notebook": True,
            "canonical_data_cleaned_payload": True,
            "accepted_head_integrated": False,
            "staging_decommissioned_with_receipt": True,
            "inventory_docs_same_snapshot_accepted": True,
            "exact_head_inventory_pr_merged": True,
        }
    )
    inventory_builder._finalize_row(candidate)
    assert not candidate["entirely_validated"]
    assert "accepted_head_integration" in candidate["missing_requirements"]


def test_every_incomplete_dataset_names_missing_requirements() -> None:
    rows = build_rows()
    for row in rows:
        missing = (
            row["missing_requirements"].split(";")
            if row["missing_requirements"]
            else []
        )
        if row["entirely_validated"]:
            assert missing == []
            assert row["next_review_focus"] == "complete"
        else:
            assert missing
            assert row["next_review_focus"] in missing

    registered_new = [
        row
        for row in rows
        if row["review_scope"] == "genuinely_new_family_22" and row["lamin_registered"]
    ]
    assert len(registered_new) == 10
    assert all(row["accepted_wave"] for row in registered_new)
    assert all(not row["strict_obs_validated"] for row in registered_new)
    assert all(not row["strict_var_validated"] for row in registered_new)
    assert all(not row["entirely_validated"] for row in registered_new)


def test_committed_csv_is_deterministic(tmp_path: Path) -> None:
    rebuilt = tmp_path / OUTPUT.name
    subprocess.run(
        [
            "uv",
            "run",
            "--no-sync",
            "python",
            "tools/build_dataset_review_inventory_csv.py",
            "--output",
            str(rebuilt),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    first = rebuilt.read_bytes()
    assert first == OUTPUT.read_bytes()
    subprocess.run(
        [
            "uv",
            "run",
            "--no-sync",
            "python",
            "tools/build_dataset_review_inventory_csv.py",
            "--output",
            str(rebuilt),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert rebuilt.read_bytes() == first
    assert b"\r\n" not in first

    with OUTPUT.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 92
    assert (
        sum(_truth(row["entirely_validated_main_existing_dataset"]) for row in rows)
        == 0
    )
    assert sum(_truth(row["entirely_validated_jkobject_addition"]) for row in rows) == 0


def test_notebook_exposes_dataset_level_summary_and_missing_requirements() -> None:
    notebook = json.loads(
        (ROOT / "notebooks/explore_dataset_storage.ipynb").read_text()
    )
    cells = {
        cell.get("id"): "".join(cell.get("source", [])) for cell in notebook["cells"]
    }
    assert (
        "one row per reviewed dataset identity" in cells["dataset-level-review-title"]
    )
    source = cells["dataset-level-review-load"]
    assert "pert_gym_dataset_review_inventory.csv" in source
    assert "entirely_validated_main_existing" in source
    assert "entirely_validated_jkobject_additions" in source
    assert "scoped_scientific_validation_accepted" in source
    assert "accepted_wave_scoped_validation" in source
    assert "full_dod_complete" in source
    assert "missing_requirements" in source
