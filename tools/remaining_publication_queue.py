#!/usr/bin/env python3
"""Checkpointed, identity-derived queue for remaining publication components.

The queue is a control-plane state machine.  It never self-accepts a candidate:
only an exact independent mismatch-0 review receipt can add product credit.  All
state transitions are immutable hash-chained JSON journal entries so an exact
claim or review can be replayed safely after a crash.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence, cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

ACCEPTED_LEDGER_FORMAT = "pert-gym.accepted-component-identities/v1"
BATCH_MANIFEST_FORMAT = "pert-gym.remaining-publication-batch/v1"
JOURNAL_FORMAT = "pert-gym.remaining-publication-journal-event/v1"
CLEANUP_RECEIPT_FORMAT = "pert-gym.heavy-cleanup-receipt/v1"
LEASE_RECEIPT_FORMAT = "pert-gym.bounded-lifecycle-lease-observation/v1"
CAPACITY_RECEIPT_FORMAT = "pert-gym.host-heavy-capacity-observation/v1"
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
EXPECTED_GATE_OBSERVER = "pert-gym.lifecycle-observer/v1"
AUTHORITY_KEY_FORMAT = "pert-gym.ed25519-authority-key/v1"
GATE_MAX_AGE_SECONDS = 5 * 60
REQUIRED_PUBLICATION_STAGES = (
    "source",
    "payload",
    "generation_readback",
    "parity",
    "manifest",
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TASK_ID = re.compile(r"^t_[0-9a-f]{8}$")
_IMMUTABLE_MANIFEST_URI = re.compile(
    r"^gs://scperturb/pert-gym/staging/.+/(?:[^/]*revisions|builds)/[^/]+/manifest\.json$"
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


class DispatchPending(RuntimeError):
    """A durable dispatch exists; wait for its exact bound result without relaunch."""


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

    lease_receipt: Path
    capacity_receipt: Path
    cleanup_receipt: Path


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


def _load_authority_key(
    path: Path, *, expected_sha256: str, label: str
) -> dict[str, str]:
    expected_sha256 = _require_sha256(expected_sha256, f"expected {label} SHA-256")
    try:
        key_bytes = path.read_bytes()
    except OSError as exc:
        raise RuntimeError(f"unable to read {label}: {path}") from exc
    if hashlib.sha256(key_bytes).hexdigest() != expected_sha256:
        raise RuntimeError(f"{label} SHA-256 mismatch")
    if len(key_bytes) != 32:
        raise RuntimeError(f"{label} must be a raw 32-byte Ed25519 public key")
    return {
        "format": AUTHORITY_KEY_FORMAT,
        "sha256": expected_sha256,
        "public_key_base64": base64.b64encode(key_bytes).decode("ascii"),
    }


def _verify_signed_receipt(
    receipt: Mapping[str, object], authority: Mapping[str, object], label: str
) -> str:
    if authority.get("format") != AUTHORITY_KEY_FORMAT:
        raise RuntimeError(f"{label} authority format is unsupported")
    encoded_key = authority.get("public_key_base64")
    authority_sha256 = authority.get("sha256")
    signature = receipt.get("signature")
    if (
        not isinstance(encoded_key, str)
        or not isinstance(authority_sha256, str)
        or not isinstance(signature, str)
    ):
        raise RuntimeError(f"{label} signature is missing")
    unsigned = dict(receipt)
    unsigned.pop("signature", None)
    claimed_sha256 = unsigned.pop("receipt_sha256", None)
    payload = _canonical_json(unsigned)
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if claimed_sha256 != actual_sha256:
        raise RuntimeError(f"{label} digest is invalid")
    try:
        public_key_bytes = base64.b64decode(encoded_key, validate=True)
        signature_bytes = base64.b64decode(signature, validate=True)
        if (
            len(public_key_bytes) != 32
            or hashlib.sha256(public_key_bytes).hexdigest() != authority_sha256
        ):
            raise ValueError("authority key digest mismatch")
        Ed25519PublicKey.from_public_bytes(public_key_bytes).verify(
            signature_bytes,
            _canonical_json({**unsigned, "receipt_sha256": actual_sha256}),
        )
    except (ValueError, InvalidSignature) as exc:
        raise RuntimeError(f"{label} signature is invalid") from exc
    return actual_sha256


def _load_object(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"{label} is unavailable or malformed: {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be a JSON object")
    return cast(dict[str, object], value)


def _load_object_with_digest(path: Path, label: str) -> tuple[dict[str, object], str]:
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"{label} is unavailable or malformed: {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be a JSON object")
    return cast(dict[str, object], value), hashlib.sha256(payload).hexdigest()


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
    catalogue_sha256: str,
    components: Sequence[Component],
    denominator: int,
) -> tuple[set[str], list[dict[str, object]]]:
    ledger = _load_pinned_object(
        path, expected_sha256=expected_sha256, label="accepted identity ledger"
    )
    entries = ledger.get("accepted_components")
    accepted_count = ledger.get("accepted")
    if ledger.get("schema_id") != ACCEPTED_LEDGER_FORMAT:
        raise RuntimeError("accepted identity ledger format is unsupported")
    source_files = ledger.get("source_files")
    if not isinstance(source_files, dict):
        raise RuntimeError("accepted ledger source binding is malformed")
    source_files = cast(dict[str, object], source_files)
    source_catalogue = source_files.get("catalogue")
    if not isinstance(source_catalogue, dict):
        raise RuntimeError("accepted ledger is not bound to the frozen catalogue")
    source_catalogue = cast(dict[str, object], source_catalogue)
    if source_catalogue.get("sha256") != catalogue_sha256:
        raise RuntimeError("accepted ledger is not bound to the frozen catalogue")
    if (
        not isinstance(entries, list)
        or not isinstance(accepted_count, int)
        or isinstance(accepted_count, bool)
        or accepted_count != len(entries)
        or ledger.get("denominator") != denominator
        or ledger.get("remaining") != denominator - accepted_count
    ):
        raise RuntimeError("accepted identity ledger denominator mismatch")
    by_record_id = {component.record_id: component for component in components}
    accepted: set[str] = set()
    normalized: list[dict[str, object]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise RuntimeError("accepted identity ledger entry is malformed")
        entry = cast(dict[str, object], entry)
        record_id = entry.get("record_id")
        component = by_record_id.get(str(record_id))
        if (
            component is None
            or entry.get("target_logical_key") != component.target_logical_key
        ):
            raise RuntimeError(
                "accepted component identity is absent from frozen catalogue"
            )
        identity = component.identity
        if identity in accepted:
            raise RuntimeError("duplicate accepted component identity")
        live_readback = entry.get("live_readback")
        if not isinstance(live_readback, dict):
            raise RuntimeError("accepted immutable readback is malformed")
        live_readback = cast(dict[str, object], live_readback)
        uri = live_readback.get("uri")
        generation = live_readback.get("generation")
        if not isinstance(uri, str):
            raise RuntimeError("accepted immutable readback is malformed")
        uri_parts = uri.split("#", 1)
        base_uri = uri_parts[0]
        if len(uri_parts) == 2 and uri_parts[1] != str(generation):
            raise RuntimeError("accepted readback generation is incoherent")
        manifest = _validate_manifest_identity(
            {
                "uri": base_uri,
                "generation": generation,
                "sha256": live_readback.get("sha256"),
            },
            "accepted",
        )
        review = entry.get("acceptance")
        event = entry.get("event")
        if not isinstance(review, dict) or not isinstance(event, dict):
            raise RuntimeError("accepted independent review is malformed")
        review = cast(dict[str, object], review)
        event = cast(dict[str, object], event)
        reviewer_task_id = review.get("task_id")
        if (
            not isinstance(reviewer_task_id, str)
            or _TASK_ID.fullmatch(reviewer_task_id) is None
            or reviewer_task_id == event.get("task_id")
            or review.get("profile") not in {"reviewer", "tester", "default"}
            or review.get("verdict") != "PASS"
            or not isinstance(review.get("run_id"), int)
            or isinstance(review.get("run_id"), bool)
        ):
            raise RuntimeError(
                "accepted identity lacks independent reviewer mismatch-0"
            )
        metadata_sha256 = _require_sha256(
            review.get("metadata_sha256"), "accepted review metadata"
        )
        accepted.add(identity)
        normalized.append(
            {
                "component_identity": identity,
                "record_id": component.record_id,
                "manifest": manifest,
                "independent_review": {
                    "reviewer_task_id": reviewer_task_id,
                    "reviewer_profile": review["profile"],
                    "review_run_id": review["run_id"],
                    "review_metadata_sha256": metadata_sha256,
                    "mismatch": 0,
                    "readback_sha256": manifest["sha256"],
                },
            }
        )
    record_ids = sorted(
        component.record_id
        for component in components
        if component.identity in accepted
    )
    if (
        ledger.get("identity_set_sha256")
        != hashlib.sha256(_canonical_json(record_ids)).hexdigest()
    ):
        raise RuntimeError("accepted identity-set digest mismatch")
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
        catalogue_sha256=catalogue_sha256,
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
        authority_keys = batch_manifest.get("authority_keys")
        if not isinstance(authority_keys, dict):
            raise RuntimeError("batch authority keys are malformed")
        authority_keys = cast(dict[str, object], authority_keys)
        gate_authority = authority_keys.get("gate_observer")
        review_authority = authority_keys.get("reviewer")
        if not isinstance(gate_authority, dict) or not isinstance(
            review_authority, dict
        ):
            raise RuntimeError("batch authority keys are malformed")
        self._gate_authority = cast(dict[str, object], gate_authority)
        self._review_authority = cast(dict[str, object], review_authority)

    @classmethod
    def create(
        cls,
        *,
        batch_dir: Path,
        catalogue_manifest: Path,
        catalogue_sha256: str,
        accepted_ledger: Path,
        accepted_ledger_sha256: str,
        gate_authority_public_key: Path,
        gate_authority_public_key_sha256: str,
        reviewer_authority_public_key: Path,
        reviewer_authority_public_key_sha256: str,
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
            catalogue_sha256=catalogue_sha256,
            components=components,
            denominator=denominator,
        )
        authority_keys = {
            "gate_observer": _load_authority_key(
                gate_authority_public_key,
                expected_sha256=gate_authority_public_key_sha256,
                label="gate authority public key",
            ),
            "reviewer": _load_authority_key(
                reviewer_authority_public_key,
                expected_sha256=reviewer_authority_public_key_sha256,
                label="reviewer authority public key",
            ),
        }
        batch_dir.parent.mkdir(parents=True, exist_ok=True)
        staging_dir = batch_dir.with_name(f".{batch_dir.name}.{uuid.uuid4().hex}.tmp")
        staging_dir.mkdir()
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
            "authority_keys": authority_keys,
        }
        manifest["batch_identity"] = _sha256_json(manifest)
        cls._exclusive_json(staging_dir / "batch-manifest.json", manifest)
        publication_queue = cls(staging_dir, manifest)
        publication_queue._append_event("initialized", None, 0, {})
        try:
            os.rename(staging_dir, batch_dir)
            cls._fsync_directory(batch_dir.parent)
        except FileExistsError as exc:
            raise RuntimeError(f"batch directory already exists: {batch_dir}") from exc
        return cls(batch_dir, manifest)

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
            if path.exists():
                raise FileExistsError(path)
            os.replace(temporary, path)
            PublicationQueue._fsync_directory(path.parent)
        except FileExistsError as exc:
            raise RuntimeError(
                f"refusing overwrite of immutable checkpoint: {path}"
            ) from exc
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        directory_fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

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
                "execution_result_sha256": None,
                "parity_evidence_sha256": None,
                "dispatch": None,
                "review_receipt_sha256": None,
                "review_receipts": {},
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
            if kind in {"claimed", "running", "dispatch_prepared", "awaiting_review"}:
                item["status"] = kind
                item["revision"] = revision
                if kind == "claimed":
                    item["candidate_manifest"] = None
                    item["execution_result_sha256"] = None
                    item["parity_evidence_sha256"] = None
            elif kind == "launch_blocked":
                item["status"] = "pending"
                item["revision"] = revision
                if details.get("clear_dispatch") is True:
                    item["dispatch"] = None
            elif kind == "rejected":
                receipts = cast(dict[int, object], item["review_receipts"])
                receipts[revision] = details.get("receipt_sha256")
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
                item["dispatch"] = None
                item["candidate_manifest"] = None
                item["execution_result_sha256"] = None
                item["parity_evidence_sha256"] = None
            elif kind == "accepted":
                item["status"] = "accepted"
                item["revision"] = revision
                item["review_receipt_sha256"] = details.get("receipt_sha256")
            else:
                raise RuntimeError(f"unknown queue journal event: {kind!r}")
            if kind == "awaiting_review":
                item["candidate_manifest"] = details.get("candidate_manifest")
                item["execution_result_sha256"] = details.get("execution_result_sha256")
                item["parity_evidence_sha256"] = details.get("parity_evidence_sha256")
            elif kind == "dispatch_prepared":
                item["dispatch"] = details
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
            "claimed": (
                counts.get("claimed", 0)
                + counts.get("running", 0)
                + counts.get("dispatch_prepared", 0)
            ),
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
            if state[component.identity]["status"]
            in {"claimed", "running", "dispatch_prepared"}
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
        self, claim: Claim, gates: ExecutionGates
    ) -> tuple[bool, str | None, dict[str, str]]:
        now = time.time()
        host = socket.gethostname()

        def load_receipt(path: Path, label: str) -> tuple[dict[str, object], str]:
            receipt = _load_object(path, label)
            digest = _verify_signed_receipt(receipt, self._gate_authority, label)
            receipt_id = receipt.get("receipt_id")
            if (
                not isinstance(receipt_id, str)
                or re.fullmatch(r"[0-9a-f]{32}", receipt_id) is None
            ):
                raise RuntimeError(f"{label} identity is malformed")
            recorded_at = receipt.get("recorded_at")
            valid_until = receipt.get("valid_until")
            if (
                not isinstance(recorded_at, (int, float))
                or isinstance(recorded_at, bool)
                or not isinstance(valid_until, (int, float))
                or isinstance(valid_until, bool)
                or recorded_at > now + 5
                or now - recorded_at > GATE_MAX_AGE_SECONDS
                or valid_until < now
                or valid_until <= recorded_at
            ):
                raise RuntimeError(f"{label} is stale, expired, or future-dated")
            return receipt, digest

        common = {
            "batch_identity": self._manifest["batch_identity"],
            "task_id": self._manifest["task_id"],
            "component_identity": claim.component_identity,
            "revision": claim.revision,
            "owner": EXPECTED_OWNER,
            "project": EXPECTED_PROJECT,
            "purpose": EXPECTED_PURPOSE,
            "host": host,
            "observer": EXPECTED_GATE_OBSERVER,
        }

        def require_keys(
            receipt: Mapping[str, object], label: str, specific: set[str]
        ) -> None:
            expected = set(common) | {
                "receipt_id",
                "recorded_at",
                "valid_until",
                "instance",
                "lease_generation",
                "receipt_sha256",
                "signature",
                *specific,
            }
            if set(receipt) != expected:
                raise RuntimeError(f"{label} fields are malformed")

        lease, lease_sha256 = load_receipt(gates.lease_receipt, "lease receipt")
        require_keys(lease, "lease receipt", {"format", "state", "lease_until"})
        lease_state = lease.get("state")
        if lease.get("format") != LEASE_RECEIPT_FORMAT or any(
            lease.get(key) != value for key, value in common.items()
        ):
            raise RuntimeError("lease receipt is malformed or not bound to this claim")
        if lease_state == "foreign_live":
            return False, "bounded-lifecycle-lease", {}
        if lease_state != "available":
            raise RuntimeError("lease receipt state is malformed")
        lease_until = lease.get("lease_until")
        lease_generation = lease.get("lease_generation")
        instance = lease.get("instance")
        if (
            not isinstance(lease_until, (int, float))
            or isinstance(lease_until, bool)
            or lease_until <= now
            or not isinstance(lease_generation, str)
            or not lease_generation
            or not isinstance(instance, str)
            or not instance
        ):
            raise RuntimeError("lease receipt lacks a live generation/instance binding")

        capacity, capacity_sha256 = load_receipt(
            gates.capacity_receipt, "capacity receipt"
        )
        require_keys(capacity, "capacity receipt", {"format", "available"})
        if any(capacity.get(key) != value for key, value in common.items()) or any(
            capacity.get(key) != value
            for key, value in {
                "format": CAPACITY_RECEIPT_FORMAT,
                "lease_generation": lease_generation,
                "instance": instance,
            }.items()
        ):
            raise RuntimeError(
                "capacity receipt is malformed or not bound to this claim"
            )
        if type(capacity.get("available")) is not bool:
            raise RuntimeError("capacity receipt availability must be boolean")
        if capacity["available"] is not True:
            return False, "host-global-heavy-capacity", {}

        cleanup, cleanup_sha256 = load_receipt(gates.cleanup_receipt, "cleanup receipt")
        require_keys(
            cleanup,
            "cleanup receipt",
            {
                "format",
                "previous_payload_terminal",
                "vm_stopped",
                "lease_released",
                "cleanup_subject_generation",
            },
        )
        if any(cleanup.get(key) != value for key, value in common.items()) or any(
            cleanup.get(key) != value
            for key, value in {
                "format": CLEANUP_RECEIPT_FORMAT,
                "lease_generation": lease_generation,
                "instance": instance,
                "previous_payload_terminal": True,
                "vm_stopped": True,
                "lease_released": True,
            }.items()
        ):
            raise RuntimeError("cleanup receipt does not prove claim-bound cleanup")
        cleanup_subject = cleanup.get("cleanup_subject_generation")
        if not isinstance(cleanup_subject, str) or not cleanup_subject:
            raise RuntimeError("cleanup receipt lacks cleanup subject generation")
        if shutil.disk_usage(self.batch_dir).free < MIN_FREE_DISK_BYTES:
            raise RuntimeError("insufficient disk for publication component")
        return (
            True,
            None,
            {
                "lease_receipt_sha256": lease_sha256,
                "capacity_receipt_sha256": capacity_sha256,
                "cleanup_receipt_sha256": cleanup_sha256,
                "lease_generation": lease_generation,
                "instance": instance,
            },
        )

    @staticmethod
    def _validate_execution_result(
        result: object,
        claim: Claim,
        component: Component,
        expected_dispatch: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        if not isinstance(result, dict):
            raise RuntimeError("execution result is not a JSON object")
        result = cast(dict[str, object], result)
        if (
            result.get("format") != EXECUTION_RESULT_FORMAT
            or result.get("component_identity") != claim.component_identity
            or result.get("revision") != claim.revision
        ):
            raise RuntimeError("execution result identity mismatch")
        if expected_dispatch is not None and result.get("dispatch") != dict(
            expected_dispatch
        ):
            raise RuntimeError("execution result lacks the durable dispatch binding")
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
        manifest = _validate_manifest_identity(
            result.get("candidate_manifest"), "candidate"
        )
        source_parity = result.get("source_parity")
        generation_parity = result.get("generation_parity")
        readback_parity = result.get("readback_parity")
        if not isinstance(source_parity, dict):
            raise RuntimeError("source_parity mismatch")
        if not isinstance(generation_parity, dict):
            raise RuntimeError("generation_parity mismatch")
        if not isinstance(readback_parity, dict):
            raise RuntimeError("readback_parity mismatch")
        source_parity = cast(dict[str, object], source_parity)
        generation_parity = cast(dict[str, object], generation_parity)
        readback_parity = cast(dict[str, object], readback_parity)
        if (
            set(source_parity)
            != {
                "expected_source_identity_sha256",
                "observed_source_identity_sha256",
                "mismatch",
            }
            or source_parity.get("mismatch") != 0
            or source_parity.get("expected_source_identity_sha256")
            != component.identity
            or source_parity.get("observed_source_identity_sha256")
            != component.identity
        ):
            raise RuntimeError("source_parity mismatch")
        if (
            set(generation_parity)
            != {"expected_generation", "observed_generation", "mismatch"}
            or generation_parity.get("mismatch") != 0
            or generation_parity.get("expected_generation") != manifest["generation"]
            or generation_parity.get("observed_generation") != manifest["generation"]
        ):
            raise RuntimeError("generation_parity mismatch")
        if (
            set(readback_parity)
            != {"expected_manifest_sha256", "observed_manifest_sha256", "mismatch"}
            or readback_parity.get("mismatch") != 0
            or readback_parity.get("expected_manifest_sha256") != manifest["sha256"]
            or readback_parity.get("observed_manifest_sha256") != manifest["sha256"]
        ):
            raise RuntimeError("readback_parity mismatch")
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
            or tuple(stages) != REQUIRED_PUBLICATION_STAGES
        ):
            raise RuntimeError("manifest-last publication contract failed")
        parity_evidence = {
            "source_parity": source_parity,
            "generation_parity": generation_parity,
            "readback_parity": readback_parity,
        }
        return {
            **result,
            "candidate_manifest": manifest,
            "parity_evidence_sha256": _sha256_json(parity_evidence),
        }

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
        claim = self.claim_next()
        if claim is None:
            return {"status": "idle", "reason": "no-claimable-component"}
        component = next(
            item
            for item in self.components
            if item.identity == claim.component_identity
        )
        try:
            ready, reason, gate_bindings = self._validate_gates(claim, gates)
        except Exception as exc:
            self._append_event(
                "launch_blocked",
                claim.component_identity,
                claim.revision,
                {"reason": str(exc), "clear_dispatch": False},
            )
            raise
        if not ready:
            self._append_event(
                "launch_blocked",
                claim.component_identity,
                claim.revision,
                {
                    "reason": reason or "execution gate unavailable",
                    "clear_dispatch": False,
                },
            )
            return {"status": "waiting", "reason": reason}
        state = self._state()[claim.component_identity]
        if state["status"] == "claimed":
            used_cleanup_receipts = set()
            for event in self._events():
                details = event["details"]
                if event["event"] == "running" and isinstance(details, dict):
                    used_cleanup_receipts.add(
                        cast(dict[str, object], details).get("cleanup_receipt_sha256")
                    )
            cleanup_sha256 = gate_bindings["cleanup_receipt_sha256"]
            if cleanup_sha256 in used_cleanup_receipts:
                raise RuntimeError("cleanup receipt was already consumed by a launch")
            self._append_event(
                "running",
                claim.component_identity,
                claim.revision,
                gate_bindings,
            )
        try:
            execution_result = executor(claim)
        except Exception as exc:
            if isinstance(exc, DispatchPending):
                return {
                    "status": "waiting",
                    "reason": str(exc),
                    "component_identity": claim.component_identity,
                    "revision": claim.revision,
                }
            self._append_event(
                "launch_blocked",
                claim.component_identity,
                claim.revision,
                {"reason": str(exc), "clear_dispatch": True},
            )
            return {
                "status": "blocked",
                "reason": str(exc),
                "component_identity": claim.component_identity,
                "revision": claim.revision,
            }
        try:
            result = self._validate_execution_result(execution_result, claim, component)
        except Exception as exc:
            self._append_event(
                "launch_blocked",
                claim.component_identity,
                claim.revision,
                {"reason": str(exc), "clear_dispatch": True},
            )
            return {
                "status": "blocked",
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
                "parity_evidence_sha256": result["parity_evidence_sha256"],
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

    def _load_launch_plan(self, path: Path) -> tuple[dict[str, dict[str, object]], str]:
        plan, plan_sha256 = _load_object_with_digest(path, "component launch plan")
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
        return result, plan_sha256

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
        launch_entries, launch_plan_sha256 = self._load_launch_plan(plan)

        def launch(claim: Claim) -> object:
            entry = launch_entries.get(claim.component_identity)
            if entry is None:
                raise ExecutionInfrastructureError(
                    "claimed component is absent from launch plan"
                )
            result_path = Path(str(entry["result_path"]))
            launch_entry_sha256 = _sha256_json(entry)
            state = self._state()[claim.component_identity]
            dispatch = state.get("dispatch")
            component = next(
                item
                for item in self.components
                if item.identity == claim.component_identity
            )
            if dispatch is not None:
                if not isinstance(dispatch, dict):
                    raise ExecutionInfrastructureError(
                        "durable dispatch checkpoint is malformed"
                    )
                dispatch = cast(dict[str, object], dispatch)
                if any(
                    dispatch.get(key) != value
                    for key, value in {
                        "batch_identity": self._manifest["batch_identity"],
                        "component_identity": claim.component_identity,
                        "revision": claim.revision,
                        "launch_plan_sha256": launch_plan_sha256,
                        "launch_entry_sha256": launch_entry_sha256,
                        "result_path": str(result_path),
                    }.items()
                ):
                    raise ExecutionInfrastructureError(
                        "durable dispatch does not match the immutable launch plan"
                    )
                if not result_path.is_file():
                    raise DispatchPending(
                        "durable launcher dispatch is pending its bound result"
                    )
                replayed = _load_object(result_path, "component execution result")
                self._validate_execution_result(replayed, claim, component, dispatch)
                return replayed
            if result_path.exists():
                raise ExecutionInfrastructureError(
                    "preexisting execution result before launcher dispatch"
                )
            running_event = next(
                event
                for event in reversed(self._events())
                if event["event"] == "running"
                and event["component_identity"] == claim.component_identity
                and event["revision"] == claim.revision
            )
            running_details = cast(dict[str, object], running_event["details"])
            dispatch = {
                "batch_identity": self._manifest["batch_identity"],
                "component_identity": claim.component_identity,
                "revision": claim.revision,
                "dispatch_id": uuid.uuid4().hex,
                "launch_plan_sha256": launch_plan_sha256,
                "launch_entry_sha256": launch_entry_sha256,
                "result_path": str(result_path),
                "lease_receipt_sha256": running_details["lease_receipt_sha256"],
                "capacity_receipt_sha256": running_details["capacity_receipt_sha256"],
                "cleanup_receipt_sha256": running_details["cleanup_receipt_sha256"],
                "lease_generation": running_details["lease_generation"],
            }
            self._append_event(
                "dispatch_prepared",
                claim.component_identity,
                claim.revision,
                dispatch,
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
                "env",
                f"PERT_GYM_PUBLICATION_DISPATCH_ID={dispatch['dispatch_id']}",
                f"PERT_GYM_PUBLICATION_LAUNCH_ENTRY_SHA256={launch_entry_sha256}",
                "PERT_GYM_PUBLICATION_DISPATCH_B64="
                + base64.b64encode(_canonical_json(dispatch)).decode("ascii"),
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
            produced = _load_object(result_path, "component execution result")
            self._validate_execution_result(produced, claim, component, dispatch)
            return produced

        return self.execute_next(gates, launch)

    def replay_review(self, receipt: Mapping[str, object]) -> dict[str, object]:
        with _file_lock(self.batch_dir / "control.lock"):
            return self._replay_review_unlocked(receipt)

    def _replay_review_unlocked(
        self, receipt: Mapping[str, object]
    ) -> dict[str, object]:
        if receipt.get("format") != REVIEW_RECEIPT_FORMAT:
            raise RuntimeError("independent review receipt format is unsupported")
        if set(receipt) != {
            "format",
            "batch_identity",
            "component_identity",
            "revision",
            "candidate_manifest",
            "reviewer_task_id",
            "reviewer_profile",
            "mismatch",
            "readback_sha256",
            "reviewed_manifest_bytes_sha256",
            "reviewed_manifest_generation",
            "execution_result_sha256",
            "parity_evidence_sha256",
            "receipt_sha256",
            "signature",
        }:
            raise RuntimeError("independent review receipt fields are malformed")
        receipt_sha256 = _verify_signed_receipt(
            receipt, self._review_authority, "independent review receipt"
        )
        identity = _require_sha256(
            receipt.get("component_identity"), "review component identity"
        )
        revision = receipt.get("revision")
        reviewer = receipt.get("reviewer_task_id")
        reviewer_profile = receipt.get("reviewer_profile")
        mismatch = receipt.get("mismatch")
        if (
            receipt.get("batch_identity") != self._manifest["batch_identity"]
            or not isinstance(revision, int)
            or isinstance(revision, bool)
            or revision <= 0
            or not isinstance(reviewer, str)
            or _TASK_ID.fullmatch(reviewer) is None
            or reviewer == self._manifest["task_id"]
            or reviewer_profile != "reviewer"
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
        reviewed_manifest_bytes_sha256 = _require_sha256(
            receipt.get("reviewed_manifest_bytes_sha256"),
            "reviewed immutable manifest bytes",
        )
        reviewed_generation = receipt.get("reviewed_manifest_generation")
        execution_result_sha256 = _require_sha256(
            receipt.get("execution_result_sha256"), "reviewed execution result"
        )
        parity_evidence_sha256 = _require_sha256(
            receipt.get("parity_evidence_sha256"), "reviewed parity evidence"
        )
        if (
            reviewed_manifest_bytes_sha256 != candidate_manifest["sha256"]
            or reviewed_generation != candidate_manifest["generation"]
        ):
            raise RuntimeError(
                "review is not bound to immutable manifest generation/bytes"
            )
        state = self._state()
        if identity not in state:
            raise RuntimeError("review receipt references a foreign component")
        item = state[identity]
        if item["status"] == "accepted":
            if item["review_receipt_sha256"] != receipt_sha256:
                raise RuntimeError("conflicting review replay for accepted component")
            return {
                "status": "accepted",
                "component_identity": identity,
                "replay": "exact-noop",
            }
        prior_receipts = cast(dict[int, object], item["review_receipts"])
        if revision in prior_receipts:
            if prior_receipts[revision] != receipt_sha256:
                raise RuntimeError("conflicting review replay for rejected revision")
            return {
                "status": "frozen" if item["status"] == "frozen" else "rejected",
                "component_identity": identity,
                "revision": revision,
                "replay": "exact-noop",
                "accepted_product_credit": 0,
            }
        if (
            item["status"] != "awaiting_review"
            or item["revision"] != revision
            or item["candidate_manifest"] != candidate_manifest
            or item["execution_result_sha256"] != execution_result_sha256
            or item["parity_evidence_sha256"] != parity_evidence_sha256
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
                "reviewer_profile": reviewer_profile,
                "mismatch": mismatch,
                "candidate_manifest": candidate_manifest,
                "reviewed_manifest_bytes_sha256": reviewed_manifest_bytes_sha256,
                "reviewed_manifest_generation": reviewed_generation,
                "execution_result_sha256": execution_result_sha256,
                "parity_evidence_sha256": parity_evidence_sha256,
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
    create.add_argument("--gate-authority-public-key", type=Path, required=True)
    create.add_argument("--gate-authority-public-key-sha256", required=True)
    create.add_argument("--reviewer-authority-public-key", type=Path, required=True)
    create.add_argument("--reviewer-authority-public-key-sha256", required=True)
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
            gate_authority_public_key=args.gate_authority_public_key,
            gate_authority_public_key_sha256=args.gate_authority_public_key_sha256,
            reviewer_authority_public_key=args.reviewer_authority_public_key,
            reviewer_authority_public_key_sha256=(
                args.reviewer_authority_public_key_sha256
            ),
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
        lease = gate_payload.get("lease_receipt")
        capacity = gate_payload.get("capacity_receipt")
        cleanup = gate_payload.get("cleanup_receipt")
        if (
            not isinstance(lease, str)
            or not Path(lease).is_absolute()
            or not isinstance(capacity, str)
            or not Path(capacity).is_absolute()
            or not isinstance(cleanup, str)
            or not Path(cleanup).is_absolute()
        ):
            raise RuntimeError("execution gates are malformed")
        gates = ExecutionGates(
            lease_receipt=Path(lease),
            capacity_receipt=Path(capacity),
            cleanup_receipt=Path(cleanup),
        )
        _print_json(
            PublicationQueue.open(args.batch_dir).run_next(
                gates, plan=args.plan, eta_hours=args.eta_hours
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
