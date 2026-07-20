from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from tools import remaining_publication_queue as queue

ROOT = Path(__file__).parents[1]
FROZEN_MANIFEST = (
    ROOT
    / "artifacts/evidence/scp1846-rgc-survival-temporal-v4-132-t_03c886aa/input"
    / "downloadable_logical_publication_manifest_20260713.json"
)
FROZEN_MANIFEST_SHA256 = (
    "ebaaa118c8a4d171432cfa7ce65926718372f2b42947164c6aa21b49261b6ca4"
)


def _record(index: int) -> dict[str, object]:
    return {
        "record_id": f"record_{index:03d}",
        "downloadable": "yes",
        "source_integrity_identity": [f"source:{index}"],
        "source_uri": f"https://example.test/{index}",
        "source_object_identity": f"object-{index}",
        "target_logical_key": f"pert-gym/logical/record_{index:03d}",
    }


def _write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return path


def _json_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def _manifest(
    tmp_path: Path, count: int = 3
) -> tuple[Path, str, list[dict[str, object]]]:
    records = [_record(index) for index in range(count)]
    path = _write_json(tmp_path / "manifest.json", {"records": records})
    return path, queue.sha256_file(path), records


def _accepted_entry(component: queue.Component) -> dict[str, object]:
    return {
        "component_identity": component.identity,
        "record_id": component.record_id,
        "manifest": {
            "uri": f"gs://scperturb/pert-gym/staging/{component.record_id}/revisions/r1/manifest.json",
            "generation": "123",
            "sha256": "a" * 64,
        },
        "independent_review": {
            "reviewer_task_id": "t_1234abcd",
            "mismatch": 0,
            "readback_sha256": "a" * 64,
        },
    }


def _ledger(
    path: Path, entries: list[dict[str, object]], *, denominator: int = 153
) -> Path:
    return _write_json(
        path,
        {
            "format": queue.ACCEPTED_LEDGER_FORMAT,
            "denominator": denominator,
            "entries": entries,
        },
    )


def _queue(tmp_path: Path, count: int = 3, accepted: int = 0) -> queue.PublicationQueue:
    manifest, digest, _records = _manifest(tmp_path, count)
    components = queue.load_components(manifest, expected_sha256=digest)
    ledger = _ledger(
        tmp_path / "accepted.json",
        [_accepted_entry(component) for component in components[:accepted]],
        denominator=count,
    )
    return queue.PublicationQueue.create(
        batch_dir=tmp_path / "batch",
        catalogue_manifest=manifest,
        catalogue_sha256=digest,
        accepted_ledger=ledger,
        accepted_ledger_sha256=queue.sha256_file(ledger),
        denominator=count,
        task_id="t_0d8b711a",
    )


def _gates(
    tmp_path: Path, *, cleanup: bool = True, disk_bytes: int = 100 * 1024**3
) -> queue.ExecutionGates:
    cleanup_path = tmp_path / "cleanup.json"
    if cleanup:
        receipt = {
            "format": queue.CLEANUP_RECEIPT_FORMAT,
            "batch_identity": json.loads(
                (tmp_path / "batch/batch-manifest.json").read_text()
            )["batch_identity"],
            "task_id": "t_0d8b711a",
            "owner": queue.EXPECTED_OWNER,
            "project": queue.EXPECTED_PROJECT,
            "purpose": queue.EXPECTED_PURPOSE,
            "recorded_at": time.time(),
            "previous_payload_terminal": True,
            "vm_stopped": True,
            "lease_released": True,
        }
        _write_json(
            cleanup_path,
            {**receipt, "receipt_sha256": _json_sha256(receipt)},
        )
    return queue.ExecutionGates(
        lease_state="available",
        host_global_capacity_available=True,
        cleanup_receipt=cleanup_path,
        free_disk_bytes=disk_bytes,
    )


def _successful_result(claim: queue.Claim) -> dict[str, object]:
    return {
        "format": queue.EXECUTION_RESULT_FORMAT,
        "component_identity": claim.component_identity,
        "revision": claim.revision,
        "source_parity": {
            "expected_sha256": "1" * 64,
            "observed_sha256": "1" * 64,
            "mismatch": 0,
        },
        "generation_parity": {
            "expected_sha256": "2" * 64,
            "observed_sha256": "2" * 64,
            "mismatch": 0,
        },
        "readback_parity": {
            "expected_sha256": "3" * 64,
            "observed_sha256": "3" * 64,
            "mismatch": 0,
        },
        "publication": {"stages": ["payload", "manifest"], "manifest_last": True},
        "heartbeat": {
            "format": queue.PRODUCT_HEARTBEAT_FORMAT,
            "component_identity": claim.component_identity,
            "revision": claim.revision,
            "status": "terminal",
        },
        "candidate_manifest": {
            "uri": f"gs://scperturb/pert-gym/staging/{claim.component_identity}/revisions/{claim.revision}/manifest.json",
            "generation": "456",
            "sha256": "d" * 64,
        },
    }


def _review_receipt(claim: queue.Claim, result: dict[str, object]) -> dict[str, object]:
    return {
        "format": queue.REVIEW_RECEIPT_FORMAT,
        "component_identity": claim.component_identity,
        "revision": claim.revision,
        "candidate_manifest": result["candidate_manifest"],
        "reviewer_task_id": "t_deadbeef",
        "mismatch": 0,
        "readback_sha256": "d" * 64,
    }


def test_frozen_dry_run_derives_exact_29_accepted_and_124_remaining(
    tmp_path: Path,
) -> None:
    components = queue.load_components(
        FROZEN_MANIFEST, expected_sha256=FROZEN_MANIFEST_SHA256
    )
    assert len(components) == 153
    ledger = _ledger(
        tmp_path / "accepted.json",
        [_accepted_entry(component) for component in components[:29]],
    )

    summary = queue.dry_run(
        catalogue_manifest=FROZEN_MANIFEST,
        catalogue_sha256=FROZEN_MANIFEST_SHA256,
        accepted_ledger=ledger,
        accepted_ledger_sha256=queue.sha256_file(ledger),
        denominator=153,
    )

    assert summary["accepted"] == 29
    assert summary["remaining"] == 124
    assert summary["denominator"] == 153
    assert summary["counts_derived_from_immutable_identities"] is True


def test_exact_accepted_identity_is_skipped(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, accepted=1)

    status = publication_queue.status()
    claim = publication_queue.claim_next()

    assert status["accepted"] == 1
    assert status["remaining"] == 2
    assert claim is not None
    assert claim.record_id == "record_001"


def test_duplicate_component_identity_fails_closed(tmp_path: Path) -> None:
    manifest, _digest, records = _manifest(tmp_path)
    records.append(dict(records[0]))
    _write_json(manifest, {"records": records})

    with pytest.raises(RuntimeError, match="duplicate component identity"):
        queue.load_components(manifest, expected_sha256=queue.sha256_file(manifest))


def test_accepted_ledger_digest_is_pinned_before_parse(tmp_path: Path) -> None:
    manifest, digest, _records = _manifest(tmp_path, count=1)
    ledger = _ledger(tmp_path / "accepted.json", [], denominator=1)
    pinned = queue.sha256_file(ledger)
    _write_json(
        ledger,
        {
            "format": queue.ACCEPTED_LEDGER_FORMAT,
            "denominator": 1,
            "entries": [],
            "tampered": True,
        },
    )

    with pytest.raises(RuntimeError, match="accepted identity ledger SHA-256 mismatch"):
        queue.dry_run(
            catalogue_manifest=manifest,
            catalogue_sha256=digest,
            accepted_ledger=ledger,
            accepted_ledger_sha256=pinned,
            denominator=1,
        )


def test_concurrent_controllers_preserve_one_claim_and_valid_journal(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=2)
    code = """
import sys
from pathlib import Path
from tools.remaining_publication_queue import PublicationQueue
claim = PublicationQueue.open(Path(sys.argv[1])).claim_next()
print(claim.component_identity if claim else 'none')
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(publication_queue.batch_dir)],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    outputs = [process.communicate(timeout=30) for process in processes]

    assert [process.returncode for process in processes] == [0, 0]
    assert outputs[0][0].strip() == outputs[1][0].strip()
    reopened = queue.PublicationQueue.open(publication_queue.batch_dir)
    claimed_events = [
        event for event in reopened._events() if event["event"] == "claimed"
    ]
    assert len(claimed_events) == 1


def test_crash_after_claim_resumes_same_component_and_revision(tmp_path: Path) -> None:
    first = _queue(tmp_path)
    claim = first.claim_next()
    assert claim is not None

    resumed = queue.PublicationQueue.open(first.batch_dir)
    resumed_claim = resumed.claim_next()

    assert resumed_claim == claim
    assert resumed.status()["claimed"] == 1


def test_crash_mid_component_adopts_exact_result_without_relaunch(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    component = publication_queue.components[0]
    result_path = tmp_path / "result.json"

    def crash_after_result(claim: queue.Claim) -> object:
        _write_json(result_path, _successful_result(claim))
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        publication_queue.execute_next(_gates(tmp_path), crash_after_result)

    plan = _write_json(
        tmp_path / "plan.json",
        {
            "format": queue.LAUNCH_PLAN_FORMAT,
            "task_id": "t_0d8b711a",
            "entries": [
                {
                    "component_identity": component.identity,
                    "command": ["uv", "run", "python", "worker.py"],
                    "result_path": str(result_path),
                }
            ],
        },
    )
    resumed = queue.PublicationQueue.open(publication_queue.batch_dir)

    outcome = resumed.run_next(
        _gates(tmp_path),
        plan=plan,
        eta_hours=1.0,
        run=lambda _: pytest.fail("resume must not relaunch an exact completed result"),
    )

    assert outcome["status"] == "awaiting_review"
    assert outcome["revision"] == 1


def test_foreign_lease_waits_without_launching(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path)
    calls: list[queue.Claim] = []
    gates = _gates(tmp_path)
    gates = queue.ExecutionGates(
        lease_state="foreign_live",
        host_global_capacity_available=gates.host_global_capacity_available,
        cleanup_receipt=gates.cleanup_receipt,
        free_disk_bytes=gates.free_disk_bytes,
    )

    outcome = publication_queue.execute_next(gates, lambda claim: calls.append(claim))

    assert outcome["status"] == "waiting"
    assert outcome["reason"] == "bounded-lifecycle-lease"
    assert calls == []


def test_missing_cleanup_receipt_fails_before_launch(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path)
    calls: list[queue.Claim] = []

    with pytest.raises(RuntimeError, match="cleanup receipt"):
        publication_queue.execute_next(
            _gates(tmp_path, cleanup=False), lambda claim: calls.append(claim)
        )

    assert calls == []


def test_disk_threshold_fails_before_launch(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path)
    calls: list[queue.Claim] = []

    with pytest.raises(RuntimeError, match="insufficient disk"):
        publication_queue.execute_next(
            _gates(tmp_path, disk_bytes=queue.MIN_FREE_DISK_BYTES - 1),
            lambda claim: calls.append(claim),
        )

    assert calls == []


def test_cleanup_receipt_content_hash_must_match_before_launch(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path)
    gates = _gates(tmp_path)
    receipt = json.loads(gates.cleanup_receipt.read_text())
    receipt["unexpected_tamper"] = True
    _write_json(gates.cleanup_receipt, receipt)

    with pytest.raises(RuntimeError, match="cleanup receipt"):
        publication_queue.execute_next(gates, lambda _: None)


def test_torn_temporary_checkpoint_is_ignored_on_resume(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path)
    journal = publication_queue.batch_dir / "journal"
    (journal / ".000002-torn.json.tmp").write_text("{")

    reopened = queue.PublicationQueue.open(publication_queue.batch_dir)

    assert reopened.status()["journal_events"] == 1


def test_manifest_last_failure_rejects_revision_without_credit(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path)
    claim = publication_queue.claim_next()
    assert claim is not None
    result = _successful_result(claim)
    result["publication"] = {"stages": ["manifest", "payload"], "manifest_last": False}

    outcome = publication_queue.execute_next(_gates(tmp_path), lambda _: result)

    assert outcome["status"] == "rejected"
    assert publication_queue.status()["accepted"] == 0
    assert publication_queue.status()["rejected_revisions"] == 1


def test_manifest_only_stage_cannot_claim_manifest_last_publication(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path)
    claim = publication_queue.claim_next()
    assert claim is not None
    result = _successful_result(claim)
    result["publication"] = {"stages": ["manifest"], "manifest_last": True}

    outcome = publication_queue.execute_next(_gates(tmp_path), lambda _: result)

    assert outcome["status"] == "rejected"


def test_independent_mismatch_zero_replay_credits_once_and_exact_replay_is_noop(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path)
    claim = publication_queue.claim_next()
    assert claim is not None
    result = _successful_result(claim)
    assert (
        publication_queue.execute_next(_gates(tmp_path), lambda _: result)["status"]
        == "awaiting_review"
    )
    receipt = _review_receipt(claim, result)

    first = publication_queue.replay_review(receipt)
    event_count = publication_queue.status()["journal_events"]
    second = publication_queue.replay_review(receipt)

    assert first["status"] == "accepted"
    assert second["status"] == "accepted"
    assert second["replay"] == "exact-noop"
    assert publication_queue.status()["journal_events"] == event_count
    assert publication_queue.status()["accepted"] == 1


def test_review_receipt_with_mismatch_cannot_credit(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path)
    claim = publication_queue.claim_next()
    assert claim is not None
    result = _successful_result(claim)
    publication_queue.execute_next(_gates(tmp_path), lambda _: result)
    receipt = _review_receipt(claim, result)
    receipt["mismatch"] = 1

    outcome = publication_queue.replay_review(receipt)

    assert outcome["status"] == "rejected"
    assert publication_queue.status()["accepted"] == 0
    assert publication_queue.status()["rejected_revisions"] == 1


def test_third_rejected_immutable_revision_freezes_component_and_continues(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=2)

    for expected_revision in (1, 2, 3):
        claim = publication_queue.claim_next()
        assert claim is not None
        assert claim.record_id == "record_000"
        assert claim.revision == expected_revision
        result = _successful_result(claim)
        assert (
            publication_queue.execute_next(_gates(tmp_path), lambda _: result)["status"]
            == "awaiting_review"
        )
        receipt = _review_receipt(claim, result)
        receipt["mismatch"] = expected_revision
        outcome = publication_queue.replay_review(receipt)

    assert outcome["status"] == "frozen"
    next_claim = publication_queue.claim_next()
    assert next_claim is not None
    assert next_claim.record_id == "record_001"
    assert publication_queue.status()["frozen"] == 1


def test_batch_manifest_and_component_checkpoints_are_immutable_hash_chained(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path)
    claim = publication_queue.claim_next()
    assert claim is not None

    batch_manifest = json.loads(
        (publication_queue.batch_dir / "batch-manifest.json").read_text()
    )
    checkpoints = sorted((publication_queue.batch_dir / "journal").glob("*.json"))
    payloads = [json.loads(path.read_text()) for path in checkpoints]

    assert batch_manifest["format"] == queue.BATCH_MANIFEST_FORMAT
    assert batch_manifest["catalogue_sha256"] == queue.sha256_file(
        publication_queue.catalogue_manifest
    )
    assert payloads[-1]["event"] == "claimed"
    assert payloads[-1]["previous_event_sha256"] == payloads[-2]["event_sha256"]
    assert all(
        path.name.endswith(f"-{payload['event_sha256']}.json")
        for path, payload in zip(checkpoints, payloads, strict=True)
    )


def test_execution_requires_product_heartbeat_and_all_parity_contracts(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path)
    claim = publication_queue.claim_next()
    assert claim is not None
    result = _successful_result(claim)
    result.pop("heartbeat")

    outcome = publication_queue.execute_next(_gates(tmp_path), lambda _: result)

    assert outcome["status"] == "rejected"
    assert "product_execution heartbeat" in str(outcome["reason"])


def test_run_next_dispatches_only_through_approved_lifecycle_launcher(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    component = publication_queue.components[0]
    result_path = tmp_path / "component-result.json"
    plan = _write_json(
        tmp_path / "launch-plan.json",
        {
            "format": queue.LAUNCH_PLAN_FORMAT,
            "task_id": "t_0d8b711a",
            "entries": [
                {
                    "component_identity": component.identity,
                    "command": ["uv", "run", "python", "approved_writer.py"],
                    "result_path": str(result_path),
                }
            ],
        },
    )
    calls: list[list[str]] = []

    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        claim = publication_queue.claim_next()
        assert claim is not None
        _write_json(result_path, _successful_result(claim))
        return subprocess.CompletedProcess(command, 0, "", "")

    outcome = publication_queue.run_next(
        _gates(tmp_path), plan=plan, eta_hours=6.0, run=run
    )

    assert outcome["status"] == "awaiting_review"
    assert calls == [
        [
            sys.executable,
            str(ROOT / "tools/launch_pert_gym_heavy.py"),
            "--task",
            "t_0d8b711a",
            "--eta-hours",
            "6.0",
            "--command",
            "uv",
            "run",
            "python",
            "approved_writer.py",
        ]
    ]


def test_new_claim_cannot_adopt_preexisting_result_without_launcher(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    component = publication_queue.components[0]
    result_path = tmp_path / "result.json"
    _write_json(
        result_path,
        _successful_result(
            queue.Claim(
                component.identity,
                component.record_id,
                component.target_logical_key,
                1,
            )
        ),
    )
    plan = _write_json(
        tmp_path / "plan.json",
        {
            "format": queue.LAUNCH_PLAN_FORMAT,
            "task_id": "t_0d8b711a",
            "entries": [
                {
                    "component_identity": component.identity,
                    "command": ["uv", "run", "python", "worker.py"],
                    "result_path": str(result_path),
                }
            ],
        },
    )
    calls: list[list[str]] = []

    outcome = publication_queue.run_next(
        _gates(tmp_path),
        plan=plan,
        eta_hours=1.0,
        run=lambda command: (
            calls.append(command) or subprocess.CompletedProcess(command, 0)
        ),
    )

    assert outcome["status"] == "blocked"
    assert outcome["reason"] == "preexisting execution result before launcher dispatch"
    assert calls == []
    assert publication_queue.status()["rejected_revisions"] == 0


def test_lifecycle_launcher_failure_does_not_consume_immutable_revision(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    component = publication_queue.components[0]
    plan = _write_json(
        tmp_path / "plan.json",
        {
            "format": queue.LAUNCH_PLAN_FORMAT,
            "task_id": "t_0d8b711a",
            "entries": [
                {
                    "component_identity": component.identity,
                    "command": ["uv", "run", "python", "worker.py"],
                    "result_path": str(tmp_path / "result.json"),
                }
            ],
        },
    )

    outcome = publication_queue.run_next(
        _gates(tmp_path),
        plan=plan,
        eta_hours=1.0,
        run=lambda command: subprocess.CompletedProcess(command, 17),
    )

    assert outcome["status"] == "blocked"
    assert publication_queue.status()["rejected_revisions"] == 0
    claim = publication_queue.claim_next()
    assert claim is not None
    assert claim.revision == 1


def test_run_next_rejects_duplicate_or_foreign_launch_plan_identity(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    entry: dict[str, Any] = {
        "component_identity": publication_queue.components[0].identity,
        "command": ["false"],
        "result_path": str(tmp_path / "result.json"),
    }
    plan = _write_json(
        tmp_path / "launch-plan.json",
        {
            "format": queue.LAUNCH_PLAN_FORMAT,
            "task_id": "t_0d8b711a",
            "entries": [entry, dict(entry)],
        },
    )

    with pytest.raises(RuntimeError, match="duplicate launch-plan identity"):
        publication_queue.run_next(
            _gates(tmp_path), plan=plan, eta_hours=1.0, run=lambda _: None
        )
