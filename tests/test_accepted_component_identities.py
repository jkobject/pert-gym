from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from tools.seal_accepted_component_identities import (
    LedgerValidationError,
    build_ledger,
    validate_output,
    write_outputs,
)


def canonical_digest(value: object) -> str:
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def make_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    records = [
        {
            "classification": "executable",
            "record_id": f"record_{index}",
            "catalogue_row_ids": [index],
            "component": f"component {index}",
            "target_logical_key": f"pert-gym/logical/component_{index}",
        }
        for index in range(5)
    ]
    catalogue = {
        "records": records,
        "records_sha256": canonical_digest(records),
    }
    catalogue_path = tmp_path / "catalogue.json"
    write_json(catalogue_path, catalogue)

    # The accepted set is intentionally not the first three catalogue records.
    accepted_indices = [1, 3, 4]
    events = []
    db_path = tmp_path / "board.db"
    con = sqlite3.connect(db_path)
    con.executescript(
        """
        create table tasks(id text primary key, assignee text, title text);
        create table task_runs(
            id integer primary key, task_id text, outcome text, metadata text
        );
        create table task_links(parent_id text, child_id text);
        """
    )
    for sequence, record_index in enumerate(accepted_indices):
        task_id = f"t_{sequence + 1:08x}"
        reviewer_id = f"t_{sequence + 101:08x}"
        run_id = sequence + 1
        uri = f"gs://bucket/component_{record_index}/manifest.json#{1000 + sequence}"
        delta = {
            "before": sequence,
            "after": sequence + 1,
            "denominator": 5,
            "unit": "components",
            "mismatch": 0,
            "live_readback": uri,
        }
        metadata = {
            "record_id": f"record_{record_index}",
            "worker_session_id": f"operation-{sequence}",
            "review_task_id": reviewer_id,
            "manifest_sha256": f"{sequence + 1:064x}",
            "product_delta": {"metrics": {"accepted_components": delta}},
        }
        reviewer_metadata = {
            "approved": True,
            "record_id": f"record_{record_index}",
            "verdict": "PASS",
        }
        con.execute("insert into tasks values(?,?,?)", (task_id, "dev", "event"))
        con.execute(
            "insert into task_runs values(?,?,?,?)",
            (run_id, task_id, "completed", json.dumps(metadata)),
        )
        con.execute(
            "insert into tasks values(?,?,?)", (reviewer_id, "reviewer", "review")
        )
        con.execute(
            "insert into task_runs values(?,?,?,?)",
            (run_id + 100, reviewer_id, "completed", json.dumps(reviewer_metadata)),
        )
        con.execute("insert into task_links values(?,?)", (reviewer_id, task_id))
        events.append(
            {
                **delta,
                "schema_version": 1,
                "project": "pert-gym",
                "board": "pert-gym",
                "metric": "accepted_components",
                "task_id": task_id,
                "run_id": run_id,
                "ended_at": 100 + sequence,
            }
        )
    con.commit()
    con.close()
    progress_path = tmp_path / "progress.json"
    write_json(progress_path, {"generated_at": 1234, "ledger_events": events})
    return catalogue_path, progress_path, db_path


def build_fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    catalogue, progress, board = make_inputs(tmp_path)
    output = tmp_path / "accepted_component_identities_v1.json"
    ledger = build_ledger(
        catalogue,
        progress,
        board,
        expected_accepted=3,
        expected_denominator=5,
    )
    write_outputs(output, ledger)
    return catalogue, progress, board, output


def validate_fixture(
    catalogue: Path, progress: Path, board: Path, output: Path
) -> None:
    validate_output(
        output,
        catalogue,
        progress,
        board,
        expected_accepted=3,
        expected_denominator=5,
    )


def test_two_builds_are_byte_identical(tmp_path: Path) -> None:
    catalogue, progress, board = make_inputs(tmp_path)
    first = tmp_path / "first" / "accepted_component_identities_v1.json"
    second = tmp_path / "second" / "accepted_component_identities_v1.json"
    ledger = build_ledger(
        catalogue, progress, board, expected_accepted=3, expected_denominator=5
    )
    write_outputs(first, ledger)
    write_outputs(second, ledger)
    assert first.read_bytes() == second.read_bytes()
    assert (
        first.with_suffix(".tsv").read_bytes()
        == second.with_suffix(".tsv").read_bytes()
    )


def test_validator_rejects_first_n_catalogue_fabrication(tmp_path: Path) -> None:
    catalogue, progress, board, output = build_fixture(tmp_path)
    ledger = json.loads(output.read_text())
    fabricated = copy.deepcopy(ledger)
    first_three = [f"record_{index}" for index in range(3)]
    for component, record_id in zip(
        fabricated["accepted_components"], first_three, strict=True
    ):
        component["record_id"] = record_id
    fabricated["accepted_components"].sort(key=lambda item: item["record_id"])
    fabricated["identity_set_sha256"] = canonical_digest(first_three)
    write_outputs(output, fabricated)
    with pytest.raises(LedgerValidationError, match="authoritative inputs"):
        validate_fixture(catalogue, progress, board, output)


def test_builder_rejects_duplicate_accepted_record_events(tmp_path: Path) -> None:
    catalogue, progress, board = make_inputs(tmp_path)
    payload = json.loads(progress.read_text())
    duplicate_uri = "gs://bucket/component_1/manifest.json#1001"
    payload["ledger_events"][1]["live_readback"] = duplicate_uri
    write_json(progress, payload)
    con = sqlite3.connect(board)
    metadata = json.loads(
        con.execute("select metadata from task_runs where id=2").fetchone()[0]
    )
    metadata["record_id"] = "record_1"
    metadata["product_delta"]["metrics"]["accepted_components"]["live_readback"] = (
        duplicate_uri
    )
    con.execute("update task_runs set metadata=? where id=2", (json.dumps(metadata),))
    reviewer_metadata = json.loads(
        con.execute("select metadata from task_runs where id=102").fetchone()[0]
    )
    reviewer_metadata["record_id"] = "record_1"
    con.execute(
        "update task_runs set metadata=? where id=102",
        (json.dumps(reviewer_metadata),),
    )
    con.commit()
    con.close()
    with pytest.raises(
        LedgerValidationError, match="duplicate accepted record identity"
    ):
        build_ledger(
            catalogue, progress, board, expected_accepted=3, expected_denominator=5
        )


def test_builder_rejects_ambiguous_event_mapping(tmp_path: Path) -> None:
    catalogue, progress, board = make_inputs(tmp_path)
    payload = json.loads(progress.read_text())
    payload["ledger_events"][0]["live_readback"] = (
        "gs://bucket/wrong/manifest.json#1000"
    )
    write_json(progress, payload)
    con = sqlite3.connect(board)
    metadata = json.loads(
        con.execute("select metadata from task_runs where id=1").fetchone()[0]
    )
    metadata["product_delta"]["metrics"]["accepted_components"]["live_readback"] = (
        payload["ledger_events"][0]["live_readback"]
    )
    con.execute("update task_runs set metadata=? where id=1", (json.dumps(metadata),))
    con.commit()
    con.close()
    with pytest.raises(LedgerValidationError, match="ambiguous live-readback mapping"):
        build_ledger(
            catalogue, progress, board, expected_accepted=3, expected_denominator=5
        )


def test_builder_rejects_denominator_mismatch(tmp_path: Path) -> None:
    catalogue, progress, board = make_inputs(tmp_path)
    payload = json.loads(catalogue.read_text())
    payload["records"].pop()
    write_json(catalogue, payload)
    with pytest.raises(LedgerValidationError, match="denominator mismatch"):
        build_ledger(
            catalogue, progress, board, expected_accepted=3, expected_denominator=5
        )


def test_acceptance_requires_accepted_object_identity(tmp_path: Path) -> None:
    catalogue, progress, board = make_inputs(tmp_path)
    con = sqlite3.connect(board)
    code_only_pass = {
        "production_run": False,
        "verdict": "PASS",
        "approved": True,
        "tests": {"focused": "pass"},
    }
    con.execute(
        "update task_runs set metadata=? where id=101",
        (json.dumps(code_only_pass),),
    )
    con.commit()
    con.close()

    with pytest.raises(
        LedgerValidationError, match="lacks an independent acceptance binding"
    ):
        build_ledger(
            catalogue, progress, board, expected_accepted=3, expected_denominator=5
        )

    # The canonical HCT116 reviewer has this shape: it proves the immutable
    # candidate by exact manifest hash and generation rather than record_id.
    manifest_bound_review = {
        "approved": True,
        "candidate": {
            "manifest_sha256": f"{1:064x}",
            "manifest_generation_after": "1000",
            "manifest_generation_before": "1000",
        },
        "verdict": "done",
    }
    con = sqlite3.connect(board)
    con.execute(
        "update task_runs set metadata=? where id=101",
        (json.dumps(manifest_bound_review),),
    )
    con.commit()
    con.close()

    ledger = build_ledger(
        catalogue, progress, board, expected_accepted=3, expected_denominator=5
    )
    first_acceptance = next(
        component["acceptance"]
        for component in ledger["accepted_components"]
        if component["record_id"] == "record_1"
    )
    assert first_acceptance["binding_evidence"] == {
        "kind": "exact_manifest_sha256_generation",
        "manifest_generation": "1000",
        "manifest_sha256": f"{1:064x}",
    }


def test_validator_rejects_source_digest_drift(tmp_path: Path) -> None:
    catalogue, progress, board, output = build_fixture(tmp_path)
    payload = json.loads(catalogue.read_text())
    payload["sealed_note"] = "drift"
    write_json(catalogue, payload)
    with pytest.raises(LedgerValidationError, match="authoritative inputs"):
        validate_fixture(catalogue, progress, board, output)
