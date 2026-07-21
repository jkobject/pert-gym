from __future__ import annotations

import base64
import hashlib
import json
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

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
CANONICAL_LEDGER = Path(
    "/Users/jkobject/Documents/pert-gym/artifacts/orchestration/"
    "publication_queue/accepted_component_identities_v1.json"
)
CANONICAL_LEDGER_SHA256 = (
    "21ef9ff60495469b8ef96bc4afe4eb2c13758ef971765fd701b79057d25bf63c"
)
CANONICAL_IDENTITY_SET_SHA256 = (
    "680202fd51bfec1e1d21635d5cf0eee5003d0f6202c0246c25aaddb7131eb2f0"
)
GATE_AUTHORITY_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
REVIEW_AUTHORITY_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(
    bytes(range(32, 64))
)


def _write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    return path


def _json_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _record(index: int) -> dict[str, object]:
    return {
        "record_id": f"record_{index:03d}",
        "downloadable": "yes",
        "source_integrity_identity": [f"source:{index}"],
        "source_uri": f"https://example.test/{index}",
        "source_object_identity": f"object-{index}",
        "target_logical_key": f"pert-gym/logical/record_{index:03d}",
    }


def _manifest(
    tmp_path: Path, count: int = 3
) -> tuple[Path, str, list[dict[str, object]]]:
    records = [_record(index) for index in range(count)]
    path = _write_json(tmp_path / "manifest.json", {"records": records})
    return path, queue.sha256_file(path), records


def _accepted_component(component: queue.Component, index: int) -> dict[str, object]:
    digest = f"{index + 1:064x}"
    generation = str(1000 + index)
    return {
        "record_id": component.record_id,
        "target_logical_key": component.target_logical_key,
        "live_readback": {
            "uri": (
                f"gs://scperturb/pert-gym/staging/{component.record_id}/"
                f"revisions/r1/manifest.json#{generation}"
            ),
            "generation": generation,
            "sha256": digest,
        },
        "event": {"task_id": f"t_{index + 1:08x}"},
        "acceptance": {
            "task_id": f"t_{index + 101:08x}",
            "run_id": index + 1001,
            "profile": "reviewer",
            "verdict": "PASS",
            "metadata_sha256": f"{index + 101:064x}",
        },
    }


def _ledger(
    path: Path,
    components: list[queue.Component],
    accepted_indices: list[int],
    catalogue_sha256: str,
) -> Path:
    accepted = [components[index] for index in accepted_indices]
    record_ids = sorted(component.record_id for component in accepted)
    return _write_json(
        path,
        {
            "schema_id": queue.ACCEPTED_LEDGER_FORMAT,
            "accepted": len(accepted),
            "denominator": len(components),
            "remaining": len(components) - len(accepted),
            "source_files": {"catalogue": {"sha256": catalogue_sha256}},
            "identity_set_sha256": _json_sha256(record_ids),
            "accepted_components": [
                _accepted_component(component, index)
                for index, component in zip(accepted_indices, accepted, strict=True)
            ],
        },
    )


def _public_key_bytes(private_key: Ed25519PrivateKey) -> bytes:
    return private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def _authority_files(tmp_path: Path) -> tuple[Path, Path]:
    gate_key = tmp_path / "gate-authority.pub"
    gate_key.write_bytes(_public_key_bytes(GATE_AUTHORITY_PRIVATE_KEY))
    reviewer_key = tmp_path / "reviewer-authority.pub"
    reviewer_key.write_bytes(_public_key_bytes(REVIEW_AUTHORITY_PRIVATE_KEY))
    return gate_key, reviewer_key


def _queue(
    tmp_path: Path,
    count: int = 3,
    accepted_indices: list[int] | None = None,
) -> queue.PublicationQueue:
    manifest, digest, _ = _manifest(tmp_path, count)
    components = queue.load_components(manifest, expected_sha256=digest)
    ledger = _ledger(
        tmp_path / "accepted.json", components, accepted_indices or [], digest
    )
    gate_key, reviewer_key = _authority_files(tmp_path)
    return queue.PublicationQueue.create(
        batch_dir=tmp_path / "batch",
        catalogue_manifest=manifest,
        catalogue_sha256=digest,
        accepted_ledger=ledger,
        accepted_ledger_sha256=queue.sha256_file(ledger),
        gate_authority_public_key=gate_key,
        gate_authority_public_key_sha256=queue.sha256_file(gate_key),
        reviewer_authority_public_key=reviewer_key,
        reviewer_authority_public_key_sha256=queue.sha256_file(reviewer_key),
        denominator=count,
        task_id="t_52e652fe",
    )


def _claim(publication_queue: queue.PublicationQueue) -> queue.Claim:
    claim = publication_queue.claim_next()
    assert claim is not None
    return claim


def _receipt(
    value: dict[str, object], private_key: Ed25519PrivateKey
) -> dict[str, object]:
    unsigned = {**value, "receipt_sha256": _json_sha256(value)}
    return {
        **unsigned,
        "signature": base64.b64encode(
            private_key.sign(queue._canonical_json(unsigned))
        ).decode("ascii"),
    }


def _resign(
    receipt: dict[str, object], private_key: Ed25519PrivateKey
) -> dict[str, object]:
    payload = {
        key: value
        for key, value in receipt.items()
        if key not in {"receipt_sha256", "signature"}
    }
    return _receipt(payload, private_key)


def _gates(
    tmp_path: Path,
    publication_queue: queue.PublicationQueue,
    claim: queue.Claim,
    *,
    lease_state: object = "available",
    capacity_available: object = True,
    recorded_at: float | None = None,
    component_identity: str | None = None,
) -> queue.ExecutionGates:
    now = time.time() if recorded_at is None else recorded_at
    common = {
        "receipt_id": uuid.uuid4().hex,
        "batch_identity": publication_queue._manifest["batch_identity"],
        "task_id": "t_52e652fe",
        "component_identity": component_identity or claim.component_identity,
        "revision": claim.revision,
        "owner": queue.EXPECTED_OWNER,
        "project": queue.EXPECTED_PROJECT,
        "purpose": queue.EXPECTED_PURPOSE,
        "host": socket.gethostname(),
        "observer": queue.EXPECTED_GATE_OBSERVER,
        "instance": "pert-gym-worker-eu",
        "lease_generation": "lease-generation-1",
        "recorded_at": now,
        "valid_until": now + 60,
    }
    lease = _receipt(
        {
            **common,
            "format": queue.LEASE_RECEIPT_FORMAT,
            "state": lease_state,
            "lease_until": now + 3600,
        },
        GATE_AUTHORITY_PRIVATE_KEY,
    )
    capacity = _receipt(
        {
            **common,
            "format": queue.CAPACITY_RECEIPT_FORMAT,
            "available": capacity_available,
        },
        GATE_AUTHORITY_PRIVATE_KEY,
    )
    cleanup = _receipt(
        {
            **common,
            "format": queue.CLEANUP_RECEIPT_FORMAT,
            "previous_payload_terminal": True,
            "vm_stopped": True,
            "lease_released": True,
            "cleanup_subject_generation": "previous-payload-generation",
        },
        GATE_AUTHORITY_PRIVATE_KEY,
    )
    return queue.ExecutionGates(
        lease_receipt=_write_json(tmp_path / "lease.json", lease),
        capacity_receipt=_write_json(tmp_path / "capacity.json", capacity),
        cleanup_receipt=_write_json(tmp_path / "cleanup.json", cleanup),
    )


def _successful_result(
    publication_queue: queue.PublicationQueue,
    claim: queue.Claim,
    *,
    dispatch: object | None = None,
) -> dict[str, object]:
    component = next(
        item
        for item in publication_queue.components
        if item.identity == claim.component_identity
    )
    result: dict[str, object] = {
        "format": queue.EXECUTION_RESULT_FORMAT,
        "component_identity": claim.component_identity,
        "revision": claim.revision,
        "source_parity": {
            "expected_source_identity_sha256": component.identity,
            "observed_source_identity_sha256": component.identity,
            "mismatch": 0,
        },
        "generation_parity": {
            "expected_generation": "456",
            "observed_generation": "456",
            "mismatch": 0,
        },
        "readback_parity": {
            "expected_manifest_sha256": "d" * 64,
            "observed_manifest_sha256": "d" * 64,
            "mismatch": 0,
        },
        "publication": {
            "stages": list(queue.REQUIRED_PUBLICATION_STAGES),
            "manifest_last": True,
        },
        "heartbeat": {
            "format": queue.PRODUCT_HEARTBEAT_FORMAT,
            "component_identity": claim.component_identity,
            "revision": claim.revision,
            "status": "terminal",
        },
        "candidate_manifest": {
            "uri": (
                f"gs://scperturb/pert-gym/staging/{claim.component_identity}/"
                f"revisions/{claim.revision}/manifest.json"
            ),
            "generation": "456",
            "sha256": "d" * 64,
        },
    }
    if dispatch is not None:
        result["dispatch"] = dispatch
    return result


def _review_receipt(
    publication_queue: queue.PublicationQueue,
    claim: queue.Claim,
    result: dict[str, object],
) -> dict[str, object]:
    state = publication_queue._state()[claim.component_identity]
    return _receipt(
        {
            "format": queue.REVIEW_RECEIPT_FORMAT,
            "batch_identity": publication_queue._manifest["batch_identity"],
            "component_identity": claim.component_identity,
            "revision": claim.revision,
            "candidate_manifest": result["candidate_manifest"],
            "reviewer_task_id": "t_deadbeef",
            "reviewer_profile": "reviewer",
            "mismatch": 0,
            "readback_sha256": "d" * 64,
            "reviewed_manifest_bytes_sha256": "d" * 64,
            "reviewed_manifest_generation": "456",
            "execution_result_sha256": state["execution_result_sha256"],
            "parity_evidence_sha256": state["parity_evidence_sha256"],
        },
        REVIEW_AUTHORITY_PRIVATE_KEY,
    )


def _execute_success(
    tmp_path: Path, publication_queue: queue.PublicationQueue, claim: queue.Claim
) -> dict[str, object]:
    result = _successful_result(publication_queue, claim)
    outcome = publication_queue.execute_next(
        _gates(tmp_path, publication_queue, claim), lambda _: result
    )
    assert outcome["status"] == "awaiting_review"
    return result


def _plan(
    tmp_path: Path, publication_queue: queue.PublicationQueue
) -> tuple[Path, Path]:
    result_path = tmp_path / "component-result.json"
    return (
        _write_json(
            tmp_path / "launch-plan.json",
            {
                "format": queue.LAUNCH_PLAN_FORMAT,
                "task_id": "t_52e652fe",
                "entries": [
                    {
                        "component_identity": publication_queue.components[0].identity,
                        "command": ["uv", "run", "python", "approved_writer.py"],
                        "result_path": str(result_path),
                    }
                ],
            },
        ),
        result_path,
    )


def test_canonical_frozen_dry_run_proves_exact_real_29_and_124() -> None:
    if not CANONICAL_LEDGER.is_file():
        pytest.skip("canonical ignored local evidence artifact is unavailable")
    assert queue.sha256_file(CANONICAL_LEDGER) == CANONICAL_LEDGER_SHA256
    summary = queue.dry_run(
        catalogue_manifest=FROZEN_MANIFEST,
        catalogue_sha256=FROZEN_MANIFEST_SHA256,
        accepted_ledger=CANONICAL_LEDGER,
        accepted_ledger_sha256=CANONICAL_LEDGER_SHA256,
        denominator=153,
    )
    assert summary["accepted"] == 29
    assert summary["remaining"] == 124
    assert summary["accepted_ledger_sha256"] == CANONICAL_LEDGER_SHA256
    ledger = json.loads(CANONICAL_LEDGER.read_bytes())
    assert ledger["identity_set_sha256"] == CANONICAL_IDENTITY_SET_SHA256


def test_nonpositional_accepted_identity_is_skipped(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=3, accepted_indices=[1])
    claim = _claim(publication_queue)
    assert claim.record_id == "record_000"
    assert publication_queue.status()["accepted"] == 1


def test_duplicate_component_identity_fails_closed(tmp_path: Path) -> None:
    manifest, _, records = _manifest(tmp_path)
    records.append(dict(records[0]))
    _write_json(manifest, {"records": records})
    with pytest.raises(RuntimeError, match="duplicate component identity"):
        queue.load_components(manifest, expected_sha256=queue.sha256_file(manifest))


def test_pinned_inputs_are_read_once_and_digest_checked(tmp_path: Path) -> None:
    manifest, digest, _ = _manifest(tmp_path, count=1)
    components = queue.load_components(manifest, expected_sha256=digest)
    ledger = _ledger(tmp_path / "accepted.json", components, [], digest)
    pinned = queue.sha256_file(ledger)
    _write_json(ledger, {"tampered": True})
    with pytest.raises(RuntimeError, match="accepted identity ledger SHA-256 mismatch"):
        queue.dry_run(
            catalogue_manifest=manifest,
            catalogue_sha256=digest,
            accepted_ledger=ledger,
            accepted_ledger_sha256=pinned,
            denominator=1,
        )


def test_two_controllers_deterministically_make_one_claim(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=2)
    ready = tmp_path / "ready"
    go = tmp_path / "go"
    code = """
import sys, time
from pathlib import Path
from tools.remaining_publication_queue import PublicationQueue
batch, ready, go, token = map(Path, sys.argv[1:])
(ready / token.name).touch()
while not go.exists(): time.sleep(0.002)
claim = PublicationQueue.open(batch).claim_next()
print(claim.component_identity if claim else 'none')
"""
    ready.mkdir()
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                code,
                str(publication_queue.batch_dir),
                str(ready),
                str(go),
                str(tmp_path / str(index)),
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for index in range(2)
    ]
    deadline = time.time() + 10
    while len(list(ready.iterdir())) != 2 and time.time() < deadline:
        time.sleep(0.01)
    assert len(list(ready.iterdir())) == 2
    go.touch()
    outputs = [process.communicate(timeout=30) for process in processes]
    assert [process.returncode for process in processes] == [0, 0]
    assert outputs[0][0].strip() == outputs[1][0].strip()
    reopened = queue.PublicationQueue.open(publication_queue.batch_dir)
    claimed = [event for event in reopened._events() if event["event"] == "claimed"]
    assert len(claimed) == 1
    assert [event["sequence"] for event in reopened._events()] == list(
        range(1, len(reopened._events()) + 1)
    )


def test_incomplete_batch_creation_never_publishes_final_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, digest, _ = _manifest(tmp_path, count=1)
    components = queue.load_components(manifest, expected_sha256=digest)
    ledger = _ledger(tmp_path / "accepted.json", components, [], digest)
    gate_key, reviewer_key = _authority_files(tmp_path)
    original = queue.PublicationQueue._exclusive_json

    def crash(path: Path, value: object) -> None:
        if path.name == "batch-manifest.json":
            raise KeyboardInterrupt
        original(path, value)

    monkeypatch.setattr(queue.PublicationQueue, "_exclusive_json", staticmethod(crash))
    with pytest.raises(KeyboardInterrupt):
        queue.PublicationQueue.create(
            batch_dir=tmp_path / "batch",
            catalogue_manifest=manifest,
            catalogue_sha256=digest,
            accepted_ledger=ledger,
            accepted_ledger_sha256=queue.sha256_file(ledger),
            gate_authority_public_key=gate_key,
            gate_authority_public_key_sha256=queue.sha256_file(gate_key),
            reviewer_authority_public_key=reviewer_key,
            reviewer_authority_public_key_sha256=queue.sha256_file(reviewer_key),
            denominator=1,
            task_id="t_52e652fe",
        )
    assert not (tmp_path / "batch").exists()


def test_torn_temp_is_ignored_but_torn_final_fails_closed(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path)
    journal = publication_queue.batch_dir / "journal"
    (journal / ".000002-torn.json.tmp").write_text("{")
    assert (
        queue.PublicationQueue.open(publication_queue.batch_dir).status()[
            "journal_events"
        ]
        == 1
    )
    (journal / ("000002-" + "0" * 64 + ".json")).write_text("{")
    with pytest.raises(RuntimeError, match="unavailable or malformed"):
        queue.PublicationQueue.open(publication_queue.batch_dir)


def test_awaiting_review_does_not_stop_independent_components(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=2)
    first = _claim(publication_queue)
    _execute_success(tmp_path, publication_queue, first)
    second = _claim(publication_queue)
    assert second.record_id == "record_001"


def test_crash_after_claim_resumes_exact_component_and_revision(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=2)
    claim = _claim(publication_queue)
    resumed = queue.PublicationQueue.open(publication_queue.batch_dir)
    assert resumed.claim_next() == claim
    assert resumed.status()["claimed"] == 1


def test_three_independent_rejections_freeze_only_rejected_component(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=2)
    for expected_revision in (1, 2, 3):
        claim = _claim(publication_queue)
        assert claim.record_id == "record_000"
        assert claim.revision == expected_revision
        result = _execute_success(tmp_path, publication_queue, claim)
        receipt = _review_receipt(publication_queue, claim, result)
        receipt["mismatch"] = expected_revision
        receipt = _resign(receipt, REVIEW_AUTHORITY_PRIVATE_KEY)
        outcome = publication_queue.replay_review(receipt)
        replay = publication_queue.replay_review(receipt)
        assert replay["replay"] == "exact-noop"
    assert outcome["status"] == "frozen"
    assert _claim(publication_queue).record_id == "record_001"


def test_execution_validation_failure_never_consumes_rejection_budget(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    for _ in range(3):
        claim = _claim(publication_queue)
        result = _successful_result(publication_queue, claim)
        result.pop("heartbeat")
        outcome = publication_queue.execute_next(
            _gates(tmp_path, publication_queue, claim), lambda _: result
        )
        assert outcome["status"] == "blocked"
    assert publication_queue.status()["rejected_revisions"] == 0
    assert publication_queue.status()["frozen"] == 0
    assert _claim(publication_queue).revision == 1


@pytest.mark.parametrize("bad_value", ["false", 0, 1, None, [], {}])
def test_capacity_receipt_rejects_non_boolean_json_values(
    tmp_path: Path, bad_value: object
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    with pytest.raises(RuntimeError, match="availability must be boolean"):
        publication_queue.execute_next(
            _gates(
                tmp_path,
                publication_queue,
                claim,
                capacity_available=bad_value,
            ),
            lambda _: pytest.fail("executor must not run"),
        )


def test_gate_receipts_are_fresh_and_component_bound(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    with pytest.raises(RuntimeError, match="stale"):
        publication_queue.execute_next(
            _gates(
                tmp_path,
                publication_queue,
                claim,
                recorded_at=time.time() - queue.GATE_MAX_AGE_SECONDS - 1,
            ),
            lambda _: None,
        )
    claim = _claim(publication_queue)
    with pytest.raises(RuntimeError, match="bound to this claim"):
        publication_queue.execute_next(
            _gates(
                tmp_path,
                publication_queue,
                claim,
                component_identity="f" * 64,
            ),
            lambda _: None,
        )


def test_producer_cannot_forge_gate_receipt_by_rehashing_public_fields(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    gates = _gates(tmp_path, publication_queue, claim)
    forged = json.loads(gates.capacity_receipt.read_text())
    forged["available"] = False
    forged["receipt_sha256"] = _json_sha256(
        {
            key: value
            for key, value in forged.items()
            if key not in {"receipt_sha256", "signature"}
        }
    )
    _write_json(gates.capacity_receipt, forged)
    with pytest.raises(RuntimeError, match="signature is invalid"):
        publication_queue.execute_next(gates, lambda _: pytest.fail("must not run"))


def test_foreign_lease_and_unavailable_capacity_wait_without_executor(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    calls: list[queue.Claim] = []
    outcome = publication_queue.execute_next(
        _gates(
            tmp_path,
            publication_queue,
            claim,
            lease_state="foreign_live",
        ),
        calls.append,
    )
    assert outcome == {"status": "waiting", "reason": "bounded-lifecycle-lease"}
    assert calls == []
    claim = _claim(publication_queue)
    outcome = publication_queue.execute_next(
        _gates(
            tmp_path,
            publication_queue,
            claim,
            capacity_available=False,
        ),
        calls.append,
    )
    assert outcome == {"status": "waiting", "reason": "host-global-heavy-capacity"}
    assert calls == []


def test_missing_cleanup_receipt_fails_before_executor(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    gates = _gates(tmp_path, publication_queue, claim)
    gates.cleanup_receipt.unlink()
    with pytest.raises(RuntimeError, match="cleanup receipt"):
        publication_queue.execute_next(gates, lambda _: pytest.fail("must not run"))


def test_cleanup_receipt_is_single_use_even_when_launch_fails(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    gates = _gates(tmp_path, publication_queue, claim)
    outcome = publication_queue.execute_next(
        gates,
        lambda _: (_ for _ in ()).throw(RuntimeError("launcher failed")),
    )
    assert outcome["status"] == "blocked"
    retry_claim = _claim(publication_queue)
    with pytest.raises(RuntimeError, match="already consumed"):
        publication_queue.execute_next(
            gates,
            lambda _: pytest.fail("replayed cleanup must not reach executor"),
        )
    assert retry_claim == claim


def test_actual_disk_observation_fails_before_executor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    monkeypatch.setattr(
        queue.shutil,
        "disk_usage",
        lambda _: queue.shutil._ntuple_diskusage(100, 99, 1),
    )
    with pytest.raises(RuntimeError, match="insufficient disk"):
        publication_queue.execute_next(
            _gates(tmp_path, publication_queue, claim), lambda _: None
        )


def test_parity_and_mandatory_pre_manifest_stages_are_exact(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    result = _successful_result(publication_queue, claim)
    result["source_parity"] = {"mismatch": 0}
    outcome = publication_queue.execute_next(
        _gates(tmp_path, publication_queue, claim), lambda _: result
    )
    assert outcome["status"] == "blocked"
    claim = _claim(publication_queue)
    result = _successful_result(publication_queue, claim)
    result["publication"] = {"stages": ["manifest"], "manifest_last": True}
    outcome = publication_queue.execute_next(
        _gates(tmp_path, publication_queue, claim), lambda _: result
    )
    assert outcome["status"] == "blocked"
    assert publication_queue.status()["rejected_revisions"] == 0


def test_review_is_independent_and_bound_to_manifest_result_and_parity(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    result = _execute_success(tmp_path, publication_queue, claim)
    receipt = _review_receipt(publication_queue, claim, result)
    receipt["reviewer_task_id"] = "t_52e652fe"
    receipt = _resign(receipt, REVIEW_AUTHORITY_PRIVATE_KEY)
    with pytest.raises(RuntimeError, match="independent reviewer"):
        publication_queue.replay_review(receipt)
    receipt = _review_receipt(publication_queue, claim, result)
    receipt["reviewed_manifest_generation"] = "999"
    receipt = _resign(receipt, REVIEW_AUTHORITY_PRIVATE_KEY)
    with pytest.raises(RuntimeError, match="generation/bytes"):
        publication_queue.replay_review(receipt)
    receipt = _review_receipt(publication_queue, claim, result)
    receipt["parity_evidence_sha256"] = "a" * 64
    receipt = _resign(receipt, REVIEW_AUTHORITY_PRIVATE_KEY)
    with pytest.raises(RuntimeError, match="does not match"):
        publication_queue.replay_review(receipt)


def test_producer_cannot_forge_independent_review_by_copying_public_state(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    result = _execute_success(tmp_path, publication_queue, claim)
    forged = _review_receipt(publication_queue, claim, result)
    forged["reviewer_task_id"] = "t_cafebabe"
    forged["receipt_sha256"] = _json_sha256(
        {
            key: value
            for key, value in forged.items()
            if key not in {"receipt_sha256", "signature"}
        }
    )
    with pytest.raises(RuntimeError, match="signature is invalid"):
        publication_queue.replay_review(forged)


def test_signed_review_receipt_cannot_replay_across_distinct_batches(
    tmp_path: Path,
) -> None:
    queue_one = _queue(tmp_path / "one", count=1)
    queue_two = _queue(tmp_path / "two", count=1)
    assert (
        queue_one._manifest["batch_identity"] != queue_two._manifest["batch_identity"]
    )
    claim_one = _claim(queue_one)
    claim_two = _claim(queue_two)
    result_one = _execute_success(tmp_path / "one", queue_one, claim_one)
    _execute_success(tmp_path / "two", queue_two, claim_two)
    receipt = _review_receipt(queue_one, claim_one, result_one)
    with pytest.raises(RuntimeError, match="valid independent reviewer receipt"):
        queue_two.replay_review(receipt)


def test_acceptance_replay_is_exact_noop(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=1)
    claim = _claim(publication_queue)
    result = _execute_success(tmp_path, publication_queue, claim)
    receipt = _review_receipt(publication_queue, claim, result)
    first = publication_queue.replay_review(receipt)
    event_count = publication_queue.status()["journal_events"]
    second = publication_queue.replay_review(receipt)
    assert first["accepted_product_credit"] == 1
    assert second["replay"] == "exact-noop"
    assert publication_queue.status()["journal_events"] == event_count


def test_preexisting_result_is_rejected_without_launcher(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=1)
    plan, result_path = _plan(tmp_path, publication_queue)
    claim = _claim(publication_queue)
    _write_json(result_path, _successful_result(publication_queue, claim))
    calls: list[list[str]] = []
    outcome = publication_queue.run_next(
        _gates(tmp_path, publication_queue, claim),
        plan=plan,
        eta_hours=1,
        run=lambda command: (
            calls.append(command) or subprocess.CompletedProcess(command, 0)
        ),
    )
    assert outcome["status"] == "blocked"
    assert "preexisting execution result" in str(outcome["reason"])
    assert calls == []
    assert publication_queue.status()["rejected_revisions"] == 0


def test_lifecycle_launcher_failure_does_not_consume_revision(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=1)
    plan, result_path = _plan(tmp_path, publication_queue)
    claim = _claim(publication_queue)
    outcome = publication_queue.run_next(
        _gates(tmp_path, publication_queue, claim),
        plan=plan,
        eta_hours=1,
        run=lambda command: subprocess.CompletedProcess(command, 17),
    )
    assert outcome["status"] == "blocked"
    assert publication_queue.status()["rejected_revisions"] == 0
    retry_claim = _claim(publication_queue)
    assert retry_claim.revision == 1

    def successful_retry(command: list[str]) -> subprocess.CompletedProcess[str]:
        dispatch = publication_queue._state()[retry_claim.component_identity][
            "dispatch"
        ]
        _write_json(
            result_path,
            _successful_result(publication_queue, retry_claim, dispatch=dispatch),
        )
        return subprocess.CompletedProcess(command, 0)

    retry = publication_queue.run_next(
        _gates(tmp_path, publication_queue, retry_claim),
        plan=plan,
        eta_hours=1,
        run=successful_retry,
    )
    assert retry["status"] == "awaiting_review"


def test_bound_result_resumes_after_crash_without_relaunch(tmp_path: Path) -> None:
    publication_queue = _queue(tmp_path, count=1)
    plan, result_path = _plan(tmp_path, publication_queue)
    claim = _claim(publication_queue)

    def crash_after_result(_: list[str]) -> subprocess.CompletedProcess[str]:
        dispatch = publication_queue._state()[claim.component_identity]["dispatch"]
        _write_json(
            result_path,
            _successful_result(publication_queue, claim, dispatch=dispatch),
        )
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        publication_queue.run_next(
            _gates(tmp_path, publication_queue, claim),
            plan=plan,
            eta_hours=1,
            run=crash_after_result,
        )
    resumed = queue.PublicationQueue.open(publication_queue.batch_dir)
    outcome = resumed.run_next(
        _gates(tmp_path, resumed, claim),
        plan=plan,
        eta_hours=1,
        run=lambda _: pytest.fail("bound crash replay must not relaunch"),
    )
    assert outcome["status"] == "awaiting_review"


def test_bad_resume_gate_cannot_erase_durable_dispatch_or_cause_relaunch(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    plan, result_path = _plan(tmp_path, publication_queue)
    claim = _claim(publication_queue)
    with pytest.raises(KeyboardInterrupt):
        publication_queue.run_next(
            _gates(tmp_path, publication_queue, claim),
            plan=plan,
            eta_hours=1,
            run=lambda _: (_ for _ in ()).throw(KeyboardInterrupt()),
        )
    dispatch = publication_queue._state()[claim.component_identity]["dispatch"]
    assert isinstance(dispatch, dict)

    bad_gates = _gates(tmp_path, publication_queue, claim)
    bad_gates.cleanup_receipt.unlink()
    with pytest.raises(RuntimeError, match="cleanup receipt is unavailable"):
        publication_queue.run_next(
            bad_gates,
            plan=plan,
            eta_hours=1,
            run=lambda _: pytest.fail("bad resume must not relaunch"),
        )
    retry_claim = _claim(publication_queue)
    assert publication_queue._state()[claim.component_identity]["dispatch"] == dispatch
    waiting = publication_queue.run_next(
        _gates(tmp_path, publication_queue, retry_claim),
        plan=plan,
        eta_hours=1,
        run=lambda _: pytest.fail("pending dispatch must not relaunch"),
    )
    assert waiting["status"] == "waiting"
    _write_json(
        result_path,
        _successful_result(publication_queue, retry_claim, dispatch=dispatch),
    )
    resumed = publication_queue.run_next(
        _gates(tmp_path, publication_queue, retry_claim),
        plan=plan,
        eta_hours=1,
        run=lambda _: pytest.fail("bound result replay must not relaunch"),
    )
    assert resumed["status"] == "awaiting_review"


def test_stale_result_after_predispatch_crash_cannot_be_adopted(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    plan, result_path = _plan(tmp_path, publication_queue)
    claim = _claim(publication_queue)
    with pytest.raises(KeyboardInterrupt):
        publication_queue.run_next(
            _gates(tmp_path, publication_queue, claim),
            plan=plan,
            eta_hours=1,
            run=lambda _: (_ for _ in ()).throw(KeyboardInterrupt()),
        )
    _write_json(result_path, _successful_result(publication_queue, claim))
    calls: list[list[str]] = []
    outcome = publication_queue.run_next(
        _gates(tmp_path, publication_queue, claim),
        plan=plan,
        eta_hours=1,
        run=lambda command: (
            calls.append(command) or subprocess.CompletedProcess(command, 0)
        ),
    )
    assert outcome["status"] == "blocked"
    assert "dispatch binding" in str(outcome["reason"])
    assert calls == []


def test_run_next_only_calls_pinned_launcher_with_dispatch_environment(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    plan, result_path = _plan(tmp_path, publication_queue)
    claim = _claim(publication_queue)
    calls: list[list[str]] = []

    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        dispatch = publication_queue._state()[claim.component_identity]["dispatch"]
        _write_json(
            result_path,
            _successful_result(publication_queue, claim, dispatch=dispatch),
        )
        return subprocess.CompletedProcess(command, 0)

    outcome = publication_queue.run_next(
        _gates(tmp_path, publication_queue, claim), plan=plan, eta_hours=6, run=run
    )
    assert outcome["status"] == "awaiting_review"
    assert calls[0][:2] == [sys.executable, str(queue.APPROVED_LIFECYCLE_LAUNCHER)]
    assert calls[0][2:8] == [
        "--task",
        "t_52e652fe",
        "--eta-hours",
        "6",
        "--command",
        "env",
    ]
    assert calls[0][8].startswith("PERT_GYM_PUBLICATION_DISPATCH_ID=")
    assert calls[0][9].startswith("PERT_GYM_PUBLICATION_LAUNCH_ENTRY_SHA256=")
    encoded_dispatch = calls[0][10].removeprefix("PERT_GYM_PUBLICATION_DISPATCH_B64=")
    assert (
        json.loads(base64.b64decode(encoded_dispatch))
        == publication_queue._state()[claim.component_identity]["dispatch"]
    )


def test_duplicate_or_foreign_launch_plan_identity_fails_closed(
    tmp_path: Path,
) -> None:
    publication_queue = _queue(tmp_path, count=1)
    entry: dict[str, Any] = {
        "component_identity": publication_queue.components[0].identity,
        "command": ["false"],
        "result_path": str(tmp_path / "result.json"),
    }
    plan = _write_json(
        tmp_path / "plan.json",
        {
            "format": queue.LAUNCH_PLAN_FORMAT,
            "task_id": "t_52e652fe",
            "entries": [entry, dict(entry)],
        },
    )
    claim = _claim(publication_queue)
    with pytest.raises(RuntimeError, match="duplicate launch-plan identity"):
        publication_queue.run_next(
            _gates(tmp_path, publication_queue, claim),
            plan=plan,
            eta_hours=1,
            run=lambda _: pytest.fail("must not launch"),
        )
