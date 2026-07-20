#!/usr/bin/env python3
"""Checkpointed, identity-derived queue for remaining publication components.

The queue is a control-plane state machine.  It never self-accepts a candidate:
only an exact independent mismatch-0 review receipt can add product credit.  All
state transitions are immutable hash-chained JSON journal entries so an exact
claim or review can be replayed safely after a crash.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence, cast

ACCEPTED_LEDGER_FORMAT = "pert-gym.accepted-component-identities/v1"
BATCH_MANIFEST_FORMAT = "pert-gym.remaining-publication-batch/v1"
JOURNAL_FORMAT = "pert-gym.remaining-publication-journal-event/v1"
CLEANUP_RECEIPT_FORMAT = "pert-gym.heavy-cleanup-receipt/v1"
EXECUTION_RESULT_FORMAT = "pert-gym.component-execution-result/v1"
PRODUCT_HEARTBEAT_FORMAT = "pert-gym.product-execution-heartbeat/v1"
REVIEW_RECEIPT_FORMAT = "pert-gym.independent-component-readback/v1"
LAUNCH_PLAN_FORMAT = "pert-gym.component-launch-plan/v1"
MIN_FREE_DISK_BYTES = 40 * 1024**3
MAX_REJECTED_REVISIONS = 3
HOST_GLOBAL_QUEUE_LOCK = Path("/tmp/pert-gym-host-global-heavy-capacity.lock")
APPROVED_LIFECYCLE_LAUNCHER = Path(__file__).with_name("launch_pert_gym_heavy.py")
CLEANUP_RECEIPT_MAX_AGE_SECONDS = 24 * 60 * 60
EXPECTED_OWNER = "jkobject"
EXPECTED_PROJECT = "pert-gym"
EXPECTED_PURPOSE = "pert-gym-longrun"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TASK_ID = re.compile(r"^t_[0-9a-f]{8}$")
_IMMUTABLE_MANIFEST_URI = re.compile(
    r"^gs://scperturb/pert-gym/staging/.+/revisions/[^/]+/manifest\.json$"
)
_IDENTITY_FIELDS = (
    "record_id",
    "source_integrity_identity",
    "source_uri",
    "source_object_identity",
    "target_logical_key",
)


class ExecutionInfrastructureError(RuntimeError):
    """A launch/control-plane failure that must not consume a data revision."""


class QueueBusy(RuntimeError):
    """Another controller currently owns the queue execution lane."""


@dataclass(frozen=True)
class Component:
    identity: str
    record_id: str
    target_logical_key: str
    source_identity: dict[str, object]


@dataclass(frozen=True)
class Claim:
    component_identity: str
    record_id: str
    target_logical_key: str
    revision: int


@dataclass(frozen=True)
class ExecutionGates:
    """Fresh scheduler observations required before invoking the lifecycle launcher."""

    lease_state: str
    host_global_capacity_available: bool
    cleanup_receipt: Path
    free_disk_bytes: int


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_object(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"{label} is unavailable or malformed: {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be a JSON object")
    return cast(dict[str, object], value)


def _load_pinned_object(
    path: Path, *, expected_sha256: str, label: str
) -> dict[str, object]:
    expected = _require_sha256(expected_sha256, f"{label} identity")
    try:
        payload = path.read_bytes()
        observed = hashlib.sha256(payload).hexdigest()
        value = json.loads(payload)
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"{label} is unavailable or malformed: {path}") from exc
    if observed != expected:
        raise RuntimeError(
            f"{label} SHA-256 mismatch: expected {expected}, observed {observed}"
        )
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be a JSON object")
    return cast(dict[str, object], value)


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RuntimeError(f"{label} must be a lowercase SHA-256")
    return value


def _require_int(value: object, label: str, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise RuntimeError(f"{label} must be an integer >= {minimum}")
    return value


def _component_from_record(record: object) -> Component:
    if not isinstance(record, dict):
        raise RuntimeError("catalogue component must be an object")
    record = cast(dict[str, object], record)
    missing = [field for field in _IDENTITY_FIELDS if field not in record]
    if missing:
        raise RuntimeError(f"catalogue component identity is incomplete: {missing}")
    record_id = record["record_id"]
    logical_key = record["target_logical_key"]
    source_integrity = record["source_integrity_identity"]
    if (
        not isinstance(record_id, str)
        or not record_id
        or not isinstance(logical_key, str)
        or not logical_key.startswith("pert-gym/logical/")
        or not isinstance(source_integrity, list)
        or not source_integrity
        or any(not isinstance(item, str) or not item for item in source_integrity)
    ):
        raise RuntimeError("catalogue component immutable identity is malformed")
    source_identity = {field: record[field] for field in _IDENTITY_FIELDS}
    return Component(
        identity=_sha256_json(source_identity),
        record_id=record_id,
        target_logical_key=logical_key,
        source_identity=source_identity,
    )


def load_components(path: Path, *, expected_sha256: str) -> list[Component]:
    """Load only the frozen downloadable denominator and derive exact identities."""
    manifest = _load_pinned_object(
        path, expected_sha256=expected_sha256, label="catalogue manifest"
    )
    records = manifest.get("records")
    if not isinstance(records, list):
        raise RuntimeError("catalogue manifest records are malformed")
    components = []
    for record in records:
        if isinstance(record, dict):
            record_object = cast(dict[str, object], record)
            if record_object.get("downloadable") == "yes":
                components.append(_component_from_record(record_object))
    identities = [component.identity for component in components]
    if len(identities) != len(set(identities)):
        raise RuntimeError("duplicate component identity in frozen catalogue")
    record_ids = [component.record_id for component in components]
    if len(record_ids) != len(set(record_ids)):
        raise RuntimeError("duplicate record_id in frozen catalogue")
    return components


def _validate_manifest_identity(value: object, label: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"uri", "generation", "sha256"}:
        raise RuntimeError(f"{label} immutable manifest identity is malformed")
    value = cast(dict[str, object], value)
    uri = value["uri"]
    generation = value["generation"]
    digest = value["sha256"]
    if (
        not isinstance(uri, str)
        or _IMMUTABLE_MANIFEST_URI.fullmatch(uri) is None
        or not isinstance(generation, str)
        or not generation.isdigit()
    ):
        raise RuntimeError(f"{label} immutable manifest identity is incoherent")
    return {
        "uri": uri,
        "generation": generation,
        "sha256": _require_sha256(digest, f"{label} manifest"),
    }


def _load_accepted(
    path: Path,
    *,
    expected_sha256: str,
    components: Sequence[Component],
    denominator: int,
) -> tuple[set[str], list[dict[str, object]]]:
    ledger = _load_pinned_object(
        path, expected_sha256=expected_sha256, label="accepted identity ledger"
    )
    if ledger.get("format") != ACCEPTED_LEDGER_FORMAT:
        raise RuntimeError("accepted identity ledger format is unsupported")
    if ledger.get("denominator") != denominator:
        raise RuntimeError("accepted identity ledger denominator mismatch")
    entries = ledger.get("entries")
    if not isinstance(entries, list):
        raise RuntimeError("accepted identity ledger entries are malformed")
    by_identity = {component.identity: component for component in components}
    accepted: set[str] = set()
    normalized: list[dict[str, object]] = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {
            "component_identity",
            "record_id",
            "manifest",
            "independent_review",
        }:
            raise RuntimeError("accepted identity ledger entry is malformed")
        entry = cast(dict[str, object], entry)
        identity = _require_sha256(
            entry["component_identity"], "accepted component identity"
        )
        component = by_identity.get(identity)
        if component is None or entry["record_id"] != component.record_id:
            raise RuntimeError(
                "accepted component identity is absent from frozen catalogue"
            )
        if identity in accepted:
            raise RuntimeError("duplicate accepted component identity")
        manifest = _validate_manifest_identity(entry["manifest"], "accepted")
        review = entry["independent_review"]
        if not isinstance(review, dict) or set(review) != {
            "reviewer_task_id",
            "mismatch",
            "readback_sha256",
        }:
            raise RuntimeError("accepted independent review is malformed")
        review = cast(dict[str, object], review)
        if (
            not isinstance(review["reviewer_task_id"], str)
            or _TASK_ID.fullmatch(review["reviewer_task_id"]) is None
            or review["mismatch"] != 0
        ):
            raise RuntimeError(
                "accepted identity lacks independent reviewer mismatch-0"
            )
        readback = _require_sha256(
            review["readback_sha256"], "accepted independent readback"
        )
        if readback != manifest["sha256"]:
            raise RuntimeError("accepted readback is not bound to manifest bytes")
        accepted.add(identity)
        normalized.append(
            {
                "component_identity": identity,
                "record_id": component.record_id,
                "manifest": manifest,
                "independent_review": {**review, "readback_sha256": readback},
            }
        )
    return accepted, normalized


def dry_run(
    *,
    catalogue_manifest: Path,
    catalogue_sha256: str,
    accepted_ledger: Path,
    accepted_ledger_sha256: str,
    denominator: int,
) -> dict[str, object]:
    components = load_components(catalogue_manifest, expected_sha256=catalogue_sha256)
    if len(components) != denominator:
        raise RuntimeError("frozen catalogue denominator mismatch")
    accepted, _entries = _load_accepted(
        accepted_ledger,
        expected_sha256=accepted_ledger_sha256,
        components=components,
        denominator=denominator,
    )
    return {
        "format": "pert-gym.remaining-publication-dry-run/v1",
        "denominator": denominator,
        "accepted": len(accepted),
        "remaining": denominator - len(accepted),
        "catalogue_sha256": catalogue_sha256,
        "accepted_ledger_sha256": accepted_ledger_sha256,
        "counts_derived_from_immutable_identities": True,
    }


@contextmanager
def _file_lock(path: Path, *, blocking: bool = True):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        operation = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(handle.fileno(), operation)
        except BlockingIOError as exc:
            raise QueueBusy(f"queue lock is held: {path}") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class PublicationQueue:
    def __init__(self, batch_dir: Path, batch_manifest: dict[str, object]):
        self.batch_dir = batch_dir
        self._manifest = batch_manifest
        self.catalogue_manifest = Path(str(batch_manifest["catalogue_manifest"]))
        raw_components = batch_manifest["components"]
        if not isinstance(raw_components, list):
            raise RuntimeError("batch component list is malformed")
        self.components = []
        for item in raw_components:
            if isinstance(item, dict):
                item_object = cast(dict[str, object], item)
                self.components.append(
                    Component(
                        identity=str(item_object["identity"]),
                        record_id=str(item_object["record_id"]),
                        target_logical_key=str(item_object["target_logical_key"]),
                        source_identity=cast(
                            dict[str, object], item_object["source_identity"]
                        ),
                    )
                )
        accepted_entries = batch_manifest["accepted_entries"]
        if not isinstance(accepted_entries, list):
            raise RuntimeError("batch accepted entry list is malformed")
        self._baseline_accepted = {
            str(cast(dict[str, object], entry)["component_identity"])
            for entry in accepted_entries
            if isinstance(entry, dict)
        }

    @classmethod
    def create(
        cls,
        *,
        batch_dir: Path,
        catalogue_manifest: Path,
        catalogue_sha256: str,
        accepted_ledger: Path,
        accepted_ledger_sha256: str,
        denominator: int,
        task_id: str,
    ) -> "PublicationQueue":
        if _TASK_ID.fullmatch(task_id) is None:
            raise RuntimeError("batch task id is malformed")
        if batch_dir.exists():
            raise RuntimeError(f"batch directory already exists: {batch_dir}")
        components = load_components(
            catalogue_manifest, expected_sha256=catalogue_sha256
        )
        if len(components) != denominator:
            raise RuntimeError("frozen catalogue denominator mismatch")
        _accepted, accepted_entries = _load_accepted(
            accepted_ledger,
            expected_sha256=accepted_ledger_sha256,
            components=components,
            denominator=denominator,
        )
        batch_dir.mkdir(parents=True)
        manifest: dict[str, object] = {
            "format": BATCH_MANIFEST_FORMAT,
            "task_id": task_id,
            "created_at": time.time(),
            "catalogue_manifest": str(catalogue_manifest.resolve()),
            "catalogue_sha256": catalogue_sha256,
            "accepted_ledger": str(accepted_ledger.resolve()),
            "accepted_ledger_sha256": accepted_ledger_sha256,
            "lifecycle_launcher_sha256": sha256_file(APPROVED_LIFECYCLE_LAUNCHER),
            "denominator": denominator,
            "components": [asdict(component) for component in components],
            "accepted_entries": accepted_entries,
        }
        manifest["batch_identity"] = _sha256_json(manifest)
        cls._exclusive_json(batch_dir / "batch-manifest.json", manifest)
        publication_queue = cls(batch_dir, manifest)
        publication_queue._append_event("initialized", None, 0, {})
        return publication_queue

    @classmethod
    def open(cls, batch_dir: Path) -> "PublicationQueue":
        manifest = _load_object(batch_dir / "batch-manifest.json", "batch manifest")
        if manifest.get("format") != BATCH_MANIFEST_FORMAT:
            raise RuntimeError("batch manifest format is unsupported")
        identity = manifest.get("batch_identity")
        unsigned = {
            key: value for key, value in manifest.items() if key != "batch_identity"
        }
        if identity != _sha256_json(unsigned):
            raise RuntimeError("batch manifest identity mismatch")
        publication_queue = cls(batch_dir, manifest)
        publication_queue._events()
        return publication_queue

    @staticmethod
    def _exclusive_json(path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(value, indent=2, sort_keys=True) + "\n"
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("x", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except FileExistsError as exc:
            raise RuntimeError(
                f"refusing overwrite of immutable checkpoint: {path}"
            ) from exc
        finally:
            temporary.unlink(missing_ok=True)

    def _events(self) -> list[dict[str, object]]:
        paths = sorted((self.batch_dir / "journal").glob("*.json"))
        events: list[dict[str, object]] = []
        previous: str | None = None
        for sequence, path in enumerate(paths, 1):
            event = _load_object(path, "queue journal event")
            recorded_hash = event.get("event_sha256")
            unsigned = {
                key: value for key, value in event.items() if key != "event_sha256"
            }
            expected_hash = _sha256_json(unsigned)
            if (
                event.get("format") != JOURNAL_FORMAT
                or event.get("sequence") != sequence
                or event.get("previous_event_sha256") != previous
                or recorded_hash != expected_hash
                or path.name != f"{sequence:06d}-{expected_hash}.json"
            ):
                raise RuntimeError("queue journal hash chain is invalid")
            events.append(event)
            previous = expected_hash
        if not events:
            raise RuntimeError("queue journal is empty")
        return events

    def _append_event(
        self,
        event: str,
        component_identity: str | None,
        revision: int,
        details: Mapping[str, object],
    ) -> dict[str, object]:
        with _file_lock(self.batch_dir / "control.lock"):
            return self._append_event_unlocked(
                event, component_identity, revision, details
            )

    def _append_event_unlocked(
        self,
        event: str,
        component_identity: str | None,
        revision: int,
        details: Mapping[str, object],
    ) -> dict[str, object]:
        events = self._events() if (self.batch_dir / "journal").exists() else []
        payload: dict[str, object] = {
            "format": JOURNAL_FORMAT,
            "sequence": len(events) + 1,
            "recorded_at": time.time(),
            "event": event,
            "component_identity": component_identity,
            "revision": revision,
            "details": dict(details),
            "previous_event_sha256": (
                str(events[-1]["event_sha256"]) if events else None
            ),
        }
        payload["event_sha256"] = _sha256_json(payload)
        path = (
            self.batch_dir
            / "journal"
            / (f"{payload['sequence']:06d}-{payload['event_sha256']}.json")
        )
        self._exclusive_json(path, payload)
        return payload

    def _state(self) -> dict[str, dict[str, object]]:
        state: dict[str, dict[str, object]] = {
            component.identity: {
                "status": (
                    "accepted"
                    if component.identity in self._baseline_accepted
                    else "pending"
                ),
                "revision": 1,
                "rejections": 0,
                "candidate_manifest": None,
                "review_receipt_sha256": None,
            }
            for component in self.components
        }
        for event in self._events()[1:]:
            identity = event["component_identity"]
            if not isinstance(identity, str) or identity not in state:
                raise RuntimeError(
                    "queue event references a foreign component identity"
                )
            item = state[identity]
            kind = event["event"]
            revision = event["revision"]
            details = event["details"]
            if not isinstance(revision, int) or not isinstance(details, dict):
                raise RuntimeError("queue event payload is malformed")
            details = cast(dict[str, object], details)
            if kind in {"claimed", "running", "awaiting_review"}:
                item["status"] = kind
                item["revision"] = revision
            elif kind == "launch_blocked":
                item["status"] = "pending"
                item["revision"] = revision
            elif kind == "rejected":
                item["rejections"] = (
                    _require_int(item["rejections"], "rejection count") + 1
                )
                item["revision"] = revision + 1
                item["status"] = (
                    "frozen"
                    if _require_int(item["rejections"], "rejection count")
                    >= MAX_REJECTED_REVISIONS
                    else "pending"
                )
            elif kind == "accepted":
                item["status"] = "accepted"
                item["revision"] = revision
                item["review_receipt_sha256"] = details.get("receipt_sha256")
            else:
                raise RuntimeError(f"unknown queue journal event: {kind!r}")
            if kind == "awaiting_review":
                item["candidate_manifest"] = details.get("candidate_manifest")
        return state

    def status(self) -> dict[str, object]:
        state = self._state()
        counts: dict[str, int] = {}
        for item in state.values():
            status = str(item["status"])
            counts[status] = counts.get(status, 0) + 1
        events = self._events()
        return {
            "format": "pert-gym.remaining-publication-status/v1",
            "batch_identity": self._manifest["batch_identity"],
            "denominator": len(self.components),
            "accepted": counts.get("accepted", 0),
            "remaining": len(self.components) - counts.get("accepted", 0),
            "pending": counts.get("pending", 0),
            "claimed": counts.get("claimed", 0) + counts.get("running", 0),
            "awaiting_review": counts.get("awaiting_review", 0),
            "frozen": counts.get("frozen", 0),
            "rejected_revisions": sum(
                1 for event in events if event["event"] == "rejected"
            ),
            "journal_events": len(events),
        }

    def list_components(self) -> list[dict[str, object]]:
        state = self._state()
        return [
            {
                **asdict(component),
                **state[component.identity],
            }
            for component in self.components
        ]

    def claim_next(self) -> Claim | None:
        with _file_lock(self.batch_dir / "control.lock"):
            return self._claim_next_unlocked()

    def _claim_next_unlocked(self) -> Claim | None:
        state = self._state()
        active = [
            component
            for component in self.components
            if state[component.identity]["status"] in {"claimed", "running"}
        ]
        if len(active) > 1:
            raise RuntimeError(
                "multiple components are active; refusing queue corruption"
            )
        if active:
            component = active[0]
            return Claim(
                component.identity,
                component.record_id,
                component.target_logical_key,
                _require_int(
                    state[component.identity]["revision"],
                    "component revision",
                    minimum=1,
                ),
            )
        if any(item["status"] == "awaiting_review" for item in state.values()):
            return None
        component = next(
            (
                item
                for item in self.components
                if state[item.identity]["status"] == "pending"
            ),
            None,
        )
        if component is None:
            return None
        revision = _require_int(
            state[component.identity]["revision"], "component revision", minimum=1
        )
        self._append_event_unlocked("claimed", component.identity, revision, {})
        return Claim(
            component.identity,
            component.record_id,
            component.target_logical_key,
            revision,
        )

    def _validate_gates(
        self, gates: ExecutionGates
    ) -> tuple[bool, str | None, str | None]:
        if (
            type(gates.host_global_capacity_available) is not bool
            or not isinstance(gates.free_disk_bytes, int)
            or isinstance(gates.free_disk_bytes, bool)
            or gates.free_disk_bytes < 0
        ):
            raise RuntimeError("execution gate values are malformed")
        if gates.lease_state != "available":
            if gates.lease_state == "foreign_live":
                return False, "bounded-lifecycle-lease", None
            raise RuntimeError("bounded lifecycle lease state is malformed")
        if not gates.host_global_capacity_available:
            return False, "host-global-heavy-capacity", None
        receipt = _load_object(gates.cleanup_receipt, "cleanup receipt")
        expected_receipt_keys = {
            "format",
            "batch_identity",
            "task_id",
            "owner",
            "project",
            "purpose",
            "recorded_at",
            "previous_payload_terminal",
            "vm_stopped",
            "lease_released",
            "receipt_sha256",
        }
        recorded_receipt_sha256 = receipt.get("receipt_sha256")
        unsigned_receipt = {
            key: value for key, value in receipt.items() if key != "receipt_sha256"
        }
        if (
            set(receipt) != expected_receipt_keys
            or receipt.get("format") != CLEANUP_RECEIPT_FORMAT
            or receipt.get("batch_identity") != self._manifest["batch_identity"]
            or receipt.get("task_id") != self._manifest["task_id"]
            or receipt.get("owner") != EXPECTED_OWNER
            or receipt.get("project") != EXPECTED_PROJECT
            or receipt.get("purpose") != EXPECTED_PURPOSE
            or receipt.get("previous_payload_terminal") is not True
            or receipt.get("vm_stopped") is not True
            or receipt.get("lease_released") is not True
            or recorded_receipt_sha256 != _sha256_json(unsigned_receipt)
        ):
            raise RuntimeError("cleanup receipt does not prove terminal cleanup")
        cleanup_sha256 = _require_sha256(recorded_receipt_sha256, "cleanup receipt")
        recorded_at = receipt.get("recorded_at")
        now = time.time()
        if (
            not isinstance(recorded_at, (int, float))
            or isinstance(recorded_at, bool)
            or recorded_at > now + 300
            or now - recorded_at > CLEANUP_RECEIPT_MAX_AGE_SECONDS
        ):
            raise RuntimeError("cleanup receipt is stale or future-dated")
        actual_free_disk = shutil.disk_usage(self.batch_dir).free
        if min(gates.free_disk_bytes, actual_free_disk) < MIN_FREE_DISK_BYTES:
            raise RuntimeError("insufficient disk for publication component")
        return True, None, cleanup_sha256

    @staticmethod
    def _validate_execution_result(result: object, claim: Claim) -> dict[str, object]:
        if not isinstance(result, dict):
            raise RuntimeError("execution result is not a JSON object")
        result = cast(dict[str, object], result)
        if (
            result.get("format") != EXECUTION_RESULT_FORMAT
            or result.get("component_identity") != claim.component_identity
            or result.get("revision") != claim.revision
        ):
            raise RuntimeError("execution result identity mismatch")
        heartbeat = result.get("heartbeat")
        if not isinstance(heartbeat, dict):
            raise RuntimeError("product_execution heartbeat contract is missing")
        heartbeat = cast(dict[str, object], heartbeat)
        if (
            heartbeat.get("format") != PRODUCT_HEARTBEAT_FORMAT
            or heartbeat.get("component_identity") != claim.component_identity
            or heartbeat.get("revision") != claim.revision
            or heartbeat.get("status") != "terminal"
        ):
            raise RuntimeError("product_execution heartbeat contract is missing")
        for label in ("source_parity", "generation_parity", "readback_parity"):
            parity = result.get(label)
            if not isinstance(parity, dict):
                raise RuntimeError(f"{label} mismatch")
            parity = cast(dict[str, object], parity)
            if (
                set(parity) != {"expected_sha256", "observed_sha256", "mismatch"}
                or parity.get("mismatch") != 0
            ):
                raise RuntimeError(f"{label} mismatch")
            expected = _require_sha256(
                parity.get("expected_sha256"), f"{label} expected"
            )
            observed = _require_sha256(
                parity.get("observed_sha256"), f"{label} observed"
            )
            if expected != observed:
                raise RuntimeError(f"{label} mismatch")
        publication = result.get("publication")
        if not isinstance(publication, dict):
            raise RuntimeError("manifest-last publication receipt is missing")
        publication = cast(dict[str, object], publication)
        stages = publication.get("stages")
        if not isinstance(stages, list) or any(
            not isinstance(stage, str) for stage in stages
        ):
            raise RuntimeError("manifest-last publication contract failed")
        stages = cast(list[str], stages)
        if (
            publication.get("manifest_last") is not True
            or len(stages) < 2
            or "payload" not in stages[:-1]
            or stages.count("manifest") != 1
            or stages[-1] != "manifest"
        ):
            raise RuntimeError("manifest-last publication contract failed")
        manifest = _validate_manifest_identity(
            result.get("candidate_manifest"), "candidate"
        )
        return {**result, "candidate_manifest": manifest}

    def execute_next(
        self,
        gates: ExecutionGates,
        executor: Callable[[Claim], object],
    ) -> dict[str, object]:
        try:
            with _file_lock(HOST_GLOBAL_QUEUE_LOCK, blocking=False):
                return self._execute_next_locked(gates, executor)
        except QueueBusy:
            return {"status": "waiting", "reason": "host-global-heavy-capacity"}

    def _execute_next_locked(
        self,
        gates: ExecutionGates,
        executor: Callable[[Claim], object],
    ) -> dict[str, object]:
        ready, reason, cleanup_sha256 = self._validate_gates(gates)
        if not ready:
            return {"status": "waiting", "reason": reason}
        claim = self.claim_next()
        if claim is None:
            return {"status": "idle", "reason": "no-claimable-component"}
        state = self._state()[claim.component_identity]
        if state["status"] == "claimed":
            used_cleanup_receipts = set()
            for event in self._events():
                details = event["details"]
                if event["event"] == "running" and isinstance(details, dict):
                    used_cleanup_receipts.add(
                        cast(dict[str, object], details).get("cleanup_receipt_sha256")
                    )
            if cleanup_sha256 in used_cleanup_receipts:
                raise RuntimeError("cleanup receipt was already consumed by a launch")
            self._append_event(
                "running",
                claim.component_identity,
                claim.revision,
                {"cleanup_receipt_sha256": cleanup_sha256},
            )
        try:
            execution_result = executor(claim)
        except Exception as exc:
            self._append_event(
                "launch_blocked",
                claim.component_identity,
                claim.revision,
                {"reason": str(exc)},
            )
            return {
                "status": "blocked",
                "reason": str(exc),
                "component_identity": claim.component_identity,
                "revision": claim.revision,
            }
        try:
            result = self._validate_execution_result(execution_result, claim)
        except Exception as exc:
            self._append_event(
                "rejected",
                claim.component_identity,
                claim.revision,
                {"reason": str(exc)},
            )
            frozen = self._state()[claim.component_identity]["status"] == "frozen"
            return {
                "status": "frozen" if frozen else "rejected",
                "reason": str(exc),
                "component_identity": claim.component_identity,
                "revision": claim.revision,
            }
        self._append_event(
            "awaiting_review",
            claim.component_identity,
            claim.revision,
            {
                "execution_result_sha256": _sha256_json(result),
                "candidate_manifest": cast(
                    dict[str, object], result["candidate_manifest"]
                ),
                "producer_credit": 0,
            },
        )
        return {
            "status": "awaiting_review",
            "component_identity": claim.component_identity,
            "revision": claim.revision,
            "producer_credit": 0,
        }

    def _load_launch_plan(self, path: Path) -> dict[str, dict[str, object]]:
        plan = _load_object(path, "component launch plan")
        if (
            plan.get("format") != LAUNCH_PLAN_FORMAT
            or plan.get("task_id") != self._manifest["task_id"]
        ):
            raise RuntimeError("component launch plan identity mismatch")
        entries = plan.get("entries")
        if not isinstance(entries, list):
            raise RuntimeError("component launch plan entries are malformed")
        result: dict[str, dict[str, object]] = {}
        catalogue_identities = {component.identity for component in self.components}
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {
                "component_identity",
                "command",
                "result_path",
            }:
                raise RuntimeError("component launch-plan entry is malformed")
            entry = cast(dict[str, object], entry)
            identity = _require_sha256(
                entry["component_identity"], "launch-plan component identity"
            )
            if identity in result:
                raise RuntimeError("duplicate launch-plan identity")
            if identity not in catalogue_identities:
                raise RuntimeError("foreign launch-plan identity")
            command = entry["command"]
            result_path = entry["result_path"]
            if (
                not isinstance(command, list)
                or not command
                or any(
                    not isinstance(argument, str) or not argument
                    for argument in command
                )
                or not isinstance(result_path, str)
                or not Path(result_path).is_absolute()
            ):
                raise RuntimeError("component launch-plan command/result is malformed")
            result[identity] = entry.copy()
        return result

    def run_next(
        self,
        gates: ExecutionGates,
        *,
        plan: Path,
        eta_hours: float,
        run: Callable[[list[str]], subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> dict[str, object]:
        """Dispatch one claim only through the bounded project lifecycle launcher.

        The approved remote writer must materialize the identity-bound execution
        result named by the reviewed launch plan.  If that immutable result is
        already present after a controller crash, it is replayed without launching
        the VM again.
        """
        if eta_hours <= 0:
            raise RuntimeError("component ETA must be positive")
        if sha256_file(APPROVED_LIFECYCLE_LAUNCHER) != self._manifest.get(
            "lifecycle_launcher_sha256"
        ):
            raise RuntimeError("approved lifecycle launcher identity drift")
        launch_entries = self._load_launch_plan(plan)
        resume_running_identities = {
            identity
            for identity, item in self._state().items()
            if item["status"] == "running"
        }

        def launch(claim: Claim) -> object:
            entry = launch_entries.get(claim.component_identity)
            if entry is None:
                raise ExecutionInfrastructureError(
                    "claimed component is absent from launch plan"
                )
            result_path = Path(str(entry["result_path"]))
            if result_path.exists():
                if claim.component_identity in resume_running_identities:
                    return _load_object(result_path, "component execution result")
                raise ExecutionInfrastructureError(
                    "preexisting execution result before launcher dispatch"
                )
            command = cast(list[str], entry["command"])
            launcher_command = [
                sys.executable,
                str(APPROVED_LIFECYCLE_LAUNCHER),
                "--task",
                str(self._manifest["task_id"]),
                "--eta-hours",
                str(eta_hours),
                "--command",
                *command,
            ]
            completed = run(launcher_command)
            if completed.returncode:
                raise ExecutionInfrastructureError(
                    f"bounded lifecycle launcher exited {completed.returncode}"
                )
            if not result_path.is_file():
                raise ExecutionInfrastructureError(
                    "bounded lifecycle launcher produced no execution result"
                )
            return _load_object(result_path, "component execution result")

        return self.execute_next(gates, launch)

    def replay_review(self, receipt: Mapping[str, object]) -> dict[str, object]:
        with _file_lock(self.batch_dir / "control.lock"):
            return self._replay_review_unlocked(receipt)

    def _replay_review_unlocked(
        self, receipt: Mapping[str, object]
    ) -> dict[str, object]:
        if receipt.get("format") != REVIEW_RECEIPT_FORMAT:
            raise RuntimeError("independent review receipt format is unsupported")
        identity = _require_sha256(
            receipt.get("component_identity"), "review component identity"
        )
        revision = receipt.get("revision")
        reviewer = receipt.get("reviewer_task_id")
        mismatch = receipt.get("mismatch")
        if (
            not isinstance(revision, int)
            or isinstance(revision, bool)
            or revision <= 0
            or not isinstance(reviewer, str)
            or _TASK_ID.fullmatch(reviewer) is None
            or reviewer == self._manifest["task_id"]
            or not isinstance(mismatch, int)
            or isinstance(mismatch, bool)
            or mismatch < 0
        ):
            raise RuntimeError("valid independent reviewer receipt is required")
        candidate_manifest = _validate_manifest_identity(
            receipt.get("candidate_manifest"), "reviewed candidate"
        )
        readback_sha256 = _require_sha256(
            receipt.get("readback_sha256"), "independent readback"
        )
        if readback_sha256 != candidate_manifest["sha256"]:
            raise RuntimeError("independent readback is not bound to candidate bytes")
        state = self._state()
        if identity not in state:
            raise RuntimeError("review receipt references a foreign component")
        receipt_sha256 = _sha256_json(dict(receipt))
        item = state[identity]
        if item["status"] == "accepted":
            if item["review_receipt_sha256"] != receipt_sha256:
                raise RuntimeError("conflicting review replay for accepted component")
            return {
                "status": "accepted",
                "component_identity": identity,
                "replay": "exact-noop",
            }
        if (
            item["status"] != "awaiting_review"
            or item["revision"] != revision
            or item["candidate_manifest"] != candidate_manifest
        ):
            raise RuntimeError(
                "review receipt does not match awaiting immutable revision"
            )
        event = "accepted" if mismatch == 0 else "rejected"
        self._append_event_unlocked(
            event,
            identity,
            revision,
            {
                "receipt_sha256": receipt_sha256,
                "reviewer_task_id": reviewer,
                "mismatch": mismatch,
                "candidate_manifest": candidate_manifest,
            },
        )
        if mismatch:
            frozen = self._state()[identity]["status"] == "frozen"
            return {
                "status": "frozen" if frozen else "rejected",
                "component_identity": identity,
                "revision": revision,
                "accepted_product_credit": 0,
                "independent_mismatch": mismatch,
            }
        return {
            "status": "accepted",
            "component_identity": identity,
            "revision": revision,
            "accepted_product_credit": 1,
            "independent_mismatch": 0,
        }


def _print_json(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)

    def source_arguments(command: argparse.ArgumentParser) -> None:
        command.add_argument("--catalogue-manifest", type=Path, required=True)
        command.add_argument("--catalogue-sha256", required=True)
        command.add_argument("--accepted-ledger", type=Path, required=True)
        command.add_argument("--accepted-ledger-sha256", required=True)
        command.add_argument("--denominator", type=int, default=153)

    dry = subparsers.add_parser("dry-run")
    source_arguments(dry)
    create = subparsers.add_parser("create")
    source_arguments(create)
    create.add_argument("--batch-dir", type=Path, required=True)
    create.add_argument("--task", required=True)
    for action in ("list", "status"):
        command = subparsers.add_parser(action)
        command.add_argument("--batch-dir", type=Path, required=True)
    replay = subparsers.add_parser("replay")
    replay.add_argument("--batch-dir", type=Path, required=True)
    replay.add_argument("--receipt", type=Path, required=True)
    run_next = subparsers.add_parser("run-next")
    run_next.add_argument("--batch-dir", type=Path, required=True)
    run_next.add_argument("--gates", type=Path, required=True)
    run_next.add_argument("--plan", type=Path, required=True)
    run_next.add_argument("--eta-hours", type=float, required=True)
    args = parser.parse_args(argv)

    if args.action == "dry-run":
        _print_json(
            dry_run(
                catalogue_manifest=args.catalogue_manifest,
                catalogue_sha256=args.catalogue_sha256,
                accepted_ledger=args.accepted_ledger,
                accepted_ledger_sha256=args.accepted_ledger_sha256,
                denominator=args.denominator,
            )
        )
    elif args.action == "create":
        publication_queue = PublicationQueue.create(
            batch_dir=args.batch_dir,
            catalogue_manifest=args.catalogue_manifest,
            catalogue_sha256=args.catalogue_sha256,
            accepted_ledger=args.accepted_ledger,
            accepted_ledger_sha256=args.accepted_ledger_sha256,
            denominator=args.denominator,
            task_id=args.task,
        )
        _print_json(publication_queue.status())
    elif args.action == "list":
        _print_json(PublicationQueue.open(args.batch_dir).list_components())
    elif args.action == "status":
        _print_json(PublicationQueue.open(args.batch_dir).status())
    elif args.action == "replay":
        receipt = _load_object(args.receipt, "independent review receipt")
        _print_json(PublicationQueue.open(args.batch_dir).replay_review(receipt))
    else:
        gate_payload = _load_object(args.gates, "execution gates")
        lease_state = gate_payload.get("lease_state")
        capacity = gate_payload.get("host_global_capacity_available")
        cleanup = gate_payload.get("cleanup_receipt")
        free_disk = gate_payload.get("free_disk_bytes")
        if (
            not isinstance(lease_state, str)
            or type(capacity) is not bool
            or not isinstance(cleanup, str)
            or not Path(cleanup).is_absolute()
            or not isinstance(free_disk, int)
            or isinstance(free_disk, bool)
            or free_disk < 0
        ):
            raise RuntimeError("execution gates are malformed")
        gates = ExecutionGates(
            lease_state=lease_state,
            host_global_capacity_available=capacity,
            cleanup_receipt=Path(cleanup),
            free_disk_bytes=free_disk,
        )
        _print_json(
            PublicationQueue.open(args.batch_dir).run_next(
                gates, plan=args.plan, eta_hours=args.eta_hours
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
