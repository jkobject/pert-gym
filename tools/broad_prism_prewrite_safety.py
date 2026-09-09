"""Local fail-closed evidence and recovery primitives for Broad PRISM writes.

The functions in this module are deliberately registry-agnostic.  The curation
entry point supplies fresh, independently captured local snapshots and the one
candidate-save callback.  No network or Lamin operation is performed here.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import uuid
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

PLAN_FORMAT = "pert-gym.broad-prism-prewrite-plan.v2"
AUTHORIZATION_FORMAT = "pert-gym.broad-prism-prewrite-authorization.v2"
PREWRITE_FORMAT = "pert-gym.broad-prism-prewrite-journal.v2"
TERMINAL_FORMAT = "pert-gym.broad-prism-terminal-journal.v2"
RECEIPT_FORMAT = "pert-gym.broad-prism-write-receipt.v2"


class EvidenceError(RuntimeError):
    """Raised for missing, malformed, inconsistent, or resealed evidence."""


class DriftError(EvidenceError):
    """Raised when an independently collected state differs from authorization."""


class IdentityError(DriftError):
    """Raised for invalid or non-identical OBS identity streams."""


class RecoveryError(EvidenceError):
    """Raised when local recovery cannot prove a unique safe result."""


class ReviewRequired(ValueError):
    """Raised for source cases that require a separate scientific decision."""


class CrashAfterCandidateSave(RuntimeError):
    """Test-only crash injection immediately after the sole candidate save."""


def canonical_json_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    ).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def seal_document(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a canonical self-digesting evidence document.

    A digest is over all document fields other than itself; callers therefore
    cannot alter a field without either invalidating it or explicitly resealing
    it (which still breaks the chain references in its parent documents).
    """

    document = dict(payload)
    document.pop("document_sha256", None)
    document["document_sha256"] = canonical_json_sha256(document)
    return document


def require_sealed_document(
    document: Mapping[str, Any], *, expected_format: str | None = None
) -> dict[str, Any]:
    if not isinstance(document, Mapping):
        raise EvidenceError("evidence document is not an object")
    actual = document.get("document_sha256")
    if not isinstance(actual, str) or len(actual) != 64:
        raise EvidenceError("evidence document lacks a SHA-256 seal")
    unsealed = dict(document)
    unsealed.pop("document_sha256", None)
    if canonical_json_sha256(unsealed) != actual:
        raise EvidenceError("evidence document digest mismatch")
    if expected_format is not None and document.get("format") != expected_format:
        raise EvidenceError("evidence document format mismatch")
    return dict(document)


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    if temporary.exists():
        raise RecoveryError(f"partial evidence exists: {temporary}")
    try:
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        # Preserve a partial file as an explicit recovery signal rather than
        # silently making the next attempt overwrite ambiguous bytes.
        raise


def write_sealed_document(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    if path.exists():
        raise RecoveryError(f"sealed evidence already exists: {path}")
    document = seal_document(payload)
    _atomic_write_bytes(
        path, json.dumps(document, sort_keys=True, indent=2).encode("utf-8") + b"\n"
    )
    return document


def write_or_match_sealed_document(
    path: Path, payload: Mapping[str, Any], *, expected_format: str
) -> dict[str, Any]:
    """Persist evidence once, or prove an existing document is byte-equivalent."""
    if path.exists():
        existing = read_sealed_document(path, expected_format=expected_format)
        if existing != seal_document(payload):
            raise EvidenceError(f"existing sealed evidence does not match: {path}")
        return existing
    return write_sealed_document(path, payload)


def read_sealed_document(path: Path, *, expected_format: str) -> dict[str, Any]:
    partial = path.with_suffix(path.suffix + ".part")
    if partial.exists():
        raise EvidenceError(f"incomplete evidence file exists: {partial}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise EvidenceError(f"required evidence missing: {path}") from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"required evidence is incomplete: {path}") from exc
    return require_sealed_document(raw, expected_format=expected_format)


def evidence_paths(run_root: Path) -> dict[str, Path]:
    return {
        "plan": run_root / "sealed_plan.json",
        "authorization": run_root / "sealed_authorization.json",
        "prewrite": run_root / "prewrite_journal.json",
        "terminal": run_root / "terminal_journal.json",
        "receipt": run_root / "write_receipt.json",
    }


def _required_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise EvidenceError(f"missing {name}")
    return value


def validate_plan_pins(plan: Mapping[str, Any]) -> None:
    require_sealed_document(plan, expected_format=PLAN_FORMAT)
    _required_string(plan.get("task_id"), "plan task_id")
    _required_string(plan.get("dataset_id"), "plan dataset_id")
    code = plan.get("code")
    if not isinstance(code, Mapping):
        raise EvidenceError("plan lacks code provenance")
    commit = _required_string(code.get("commit"), "code commit")
    script_sha = _required_string(code.get("script_sha256"), "script SHA-256")
    if len(commit) != 40 or any(
        character not in "0123456789abcdef" for character in commit
    ):
        raise EvidenceError("code commit is not pinned to a full lowercase SHA-1")
    if len(script_sha) != 64:
        raise EvidenceError("script SHA-256 is not pinned")
    sources = plan.get("sources")
    if not isinstance(sources, Mapping) or not _required_string(
        sources.get("release"), "source release"
    ):
        raise EvidenceError("plan lacks source release provenance")
    for name, source in sources.items():
        if name == "release":
            continue
        if not isinstance(source, Mapping):
            raise EvidenceError(f"source {name} is not a tuple")
        _required_string(source.get("uri"), f"source {name} URI")
        _required_string(source.get("generation"), f"source {name} generation")
        if not isinstance(source.get("bytes"), int) or source["bytes"] < 0:
            raise EvidenceError(f"source {name} bytes are not pinned")
        sha = _required_string(source.get("sha256"), f"source {name} SHA-256")
        if len(sha) != 64:
            raise EvidenceError(f"source {name} SHA-256 is malformed")


def _identity(value: Mapping[str, Any]) -> dict[str, Any]:
    required = ("uid", "key", "sha256", "bytes", "path")
    result: dict[str, Any] = {}
    for key in required:
        if key not in value or value[key] in (None, ""):
            raise DriftError(f"artifact identity lacks {key}")
        result[key] = value[key]
    for dimension in ("rows", "n_obs", "n_vars"):
        if dimension in value:
            if not isinstance(value[dimension], int) or value[dimension] < 0:
                raise DriftError(f"artifact {dimension} is invalid")
            result[dimension] = value[dimension]
    return result


def _same_identity(
    actual: Mapping[str, Any], expected: Mapping[str, Any], label: str
) -> None:
    if _identity(actual) != _identity(expected):
        raise DriftError(f"{label} identity drift")


def _state_triplet(state: Mapping[str, Any]) -> Mapping[str, Any]:
    triplet = state.get("triplet")
    if not isinstance(triplet, Mapping):
        raise DriftError("snapshot lacks triplet")
    for key in ("obs", "X", "var", "links"):
        if not isinstance(triplet.get(key), Mapping):
            raise DriftError(f"snapshot lacks triplet {key}")
    return triplet


def verify_authorized_transition(
    before: Mapping[str, Any], after: Mapping[str, Any], candidate: Mapping[str, Any]
) -> None:
    """Independently prove only the authorized OBS candidate transition occurred."""

    before_triplet = _state_triplet(before)
    after_triplet = _state_triplet(after)
    _same_identity(after_triplet["obs"], candidate, "candidate OBS")
    _same_identity(before_triplet["X"], after_triplet["X"], "X")
    _same_identity(before_triplet["var"], after_triplet["var"], "VAR")
    if after_triplet["links"] != before_triplet["links"]:
        raise DriftError("OBS→X→VAR feature links drifted")
    if after_triplet["links"].get("obs_to_x") != after_triplet["X"].get("uid"):
        raise DriftError("OBS→X link does not target X")
    if after_triplet["links"].get("x_to_var") != after_triplet["var"].get("uid"):
        raise DriftError("X→VAR link does not target VAR")
    if after_triplet["obs"].get("rows") != after_triplet["X"].get("n_obs"):
        raise DriftError("OBS rows do not equal X observations")
    if after_triplet["X"].get("n_vars") != after_triplet["var"].get("rows"):
        raise DriftError("X variables do not equal VAR rows")
    if before.get("collections") != after.get("collections"):
        raise DriftError("Collection snapshot drifted")
    if before.get("unrelated") != after.get("unrelated"):
        raise DriftError("unrelated registry or Collection drifted")
    before_artifacts = {
        _identity(item)["uid"]: _identity(item)
        for item in before.get("artifacts", [])
        if isinstance(item, Mapping)
    }
    after_artifacts = {
        _identity(item)["uid"]: _identity(item)
        for item in after.get("artifacts", [])
        if isinstance(item, Mapping)
    }
    candidate_identity = _identity(candidate)
    if after_artifacts.get(candidate_identity["uid"]) != candidate_identity:
        raise DriftError("candidate registry identity drifted")
    for uid, identity in before_artifacts.items():
        if after_artifacts.get(uid) != identity:
            raise DriftError("predecessor registry artifact drifted")
    unexpected = (
        set(after_artifacts) - set(before_artifacts) - {candidate_identity["uid"]}
    )
    if unexpected:
        raise DriftError("unauthorized registry artifact transition")


def _verify_live_candidate(
    state: Mapping[str, Any], candidate: Mapping[str, Any]
) -> None:
    triplet = _state_triplet(state)
    _same_identity(triplet["obs"], candidate, "fresh candidate OBS")
    if triplet["links"].get("obs_to_x") != triplet["X"].get("uid"):
        raise DriftError("fresh OBS→X link mismatch")
    if triplet["links"].get("x_to_var") != triplet["var"].get("uid"):
        raise DriftError("fresh X→VAR link mismatch")
    if triplet["obs"].get("rows") != triplet["X"].get("n_obs"):
        raise DriftError("fresh OBS/X dimension mismatch")
    if triplet["X"].get("n_vars") != triplet["var"].get("rows"):
        raise DriftError("fresh X/VAR dimension mismatch")


def _validate_authorization(
    plan: Mapping[str, Any], authorization: Mapping[str, Any]
) -> None:
    validate_plan_pins(plan)
    require_sealed_document(authorization, expected_format=AUTHORIZATION_FORMAT)
    expected = {
        "task_id": plan.get("task_id"),
        "dataset_id": plan.get("dataset_id"),
        "plan_sha256": plan.get("document_sha256"),
        "approved": True,
    }
    if any(authorization.get(key) != value for key, value in expected.items()):
        raise EvidenceError("authorization does not bind the exact sealed plan")


def execute_one_write_transition(
    *,
    run_root: Path,
    plan: Mapping[str, Any],
    authorization: Mapping[str, Any],
    candidates: Callable[[], Iterable[Mapping[str, Any]]],
    save_candidate: Callable[[], Mapping[str, Any]],
    before_state: Mapping[str, Any],
    fresh_state: Callable[[], Mapping[str, Any]],
    crash_after_candidate_save: bool = False,
) -> dict[str, Any]:
    """Persist admission before save and recover a unique candidate without re-save."""

    _validate_authorization(plan, authorization)
    paths = evidence_paths(run_root)
    write_or_match_sealed_document(paths["plan"], plan, expected_format=PLAN_FORMAT)
    if paths["receipt"].exists():
        receipt = verify_zero_write_evidence(run_root, fresh_state=fresh_state)
        result = dict(receipt)
        result["replay"] = {"writes": 0, "status": "verified_no_op"}
        return result
    if paths["authorization"].exists():
        persisted_authorization = read_sealed_document(
            paths["authorization"], expected_format=AUTHORIZATION_FORMAT
        )
        if persisted_authorization != dict(authorization):
            raise EvidenceError(
                "persisted authorization differs from supplied authorization"
            )
    else:
        write_or_match_sealed_document(
            paths["authorization"],
            authorization,
            expected_format=AUTHORIZATION_FORMAT,
        )
    if paths["prewrite"].exists():
        prewrite = read_sealed_document(
            paths["prewrite"], expected_format=PREWRITE_FORMAT
        )
    else:
        _same_identity(
            _state_triplet(before_state)["obs"],
            plan["predecessor"],
            "pre-write predecessor OBS",
        )
        prewrite = write_sealed_document(
            paths["prewrite"],
            {
                "format": PREWRITE_FORMAT,
                "task_id": plan["task_id"],
                "dataset_id": plan["dataset_id"],
                "authorization_sha256": authorization["document_sha256"],
                "plan_sha256": plan["document_sha256"],
                "predecessor": plan["predecessor"],
                "candidate": plan["candidate"],
                "code": plan["code"],
                "sources": plan["sources"],
                "independent_before_state": before_state,
                "independent_before_state_sha256": canonical_json_sha256(before_state),
                "state": "admitted_before_candidate_save",
            },
        )
    expected_candidate = plan["candidate"]
    compatible = [
        item
        for item in candidates()
        if _identity(item) == _identity(expected_candidate)
    ]
    all_candidates = list(candidates())
    if len(all_candidates) != len(compatible):
        raise RecoveryError("candidate discovery found an incompatible live candidate")
    writes = 0
    if not compatible:
        saved = save_candidate()
        _same_identity(saved, expected_candidate, "saved candidate")
        writes = 1
        if crash_after_candidate_save:
            raise CrashAfterCandidateSave("injected crash after candidate save")
    elif len(compatible) != 1:
        raise RecoveryError("candidate discovery is not uniquely recoverable")
    state = fresh_state()
    _verify_live_candidate(state, expected_candidate)
    recorded_before = prewrite.get("independent_before_state")
    if not isinstance(recorded_before, Mapping) or (
        prewrite.get("independent_before_state_sha256")
        != canonical_json_sha256(recorded_before)
    ):
        raise EvidenceError(
            "pre-write journal lacks a digest-valid independent baseline"
        )
    verify_authorized_transition(recorded_before, state, expected_candidate)
    terminal = write_or_match_sealed_document(
        paths["terminal"],
        {
            "format": TERMINAL_FORMAT,
            "task_id": plan["task_id"],
            "dataset_id": plan["dataset_id"],
            "authorization_sha256": authorization["document_sha256"],
            "plan_sha256": plan["document_sha256"],
            "prewrite_sha256": prewrite["document_sha256"],
            "candidate": expected_candidate,
            "terminal_state": "sealed",
            "fresh_state_sha256": canonical_json_sha256(state),
        },
        expected_format=TERMINAL_FORMAT,
    )
    receipt = write_or_match_sealed_document(
        paths["receipt"],
        {
            "format": RECEIPT_FORMAT,
            "task_id": plan["task_id"],
            "dataset_id": plan["dataset_id"],
            "authorization_sha256": authorization["document_sha256"],
            "plan_sha256": plan["document_sha256"],
            "prewrite_sha256": prewrite["document_sha256"],
            "terminal_sha256": terminal["document_sha256"],
            "candidate": expected_candidate,
            "terminal_state": "sealed",
            "replay": {
                "writes": writes,
                "status": "initial_write" if writes else "recovered",
            },
        },
        expected_format=RECEIPT_FORMAT,
    )
    return receipt


def verify_zero_write_evidence(
    run_root: Path, *, fresh_state: Callable[[], Mapping[str, Any]]
) -> dict[str, Any]:
    paths = evidence_paths(run_root)
    plan = read_sealed_document(paths["plan"], expected_format=PLAN_FORMAT)
    authorization = read_sealed_document(
        paths["authorization"], expected_format=AUTHORIZATION_FORMAT
    )
    _validate_authorization(plan, authorization)
    prewrite = read_sealed_document(paths["prewrite"], expected_format=PREWRITE_FORMAT)
    terminal = read_sealed_document(paths["terminal"], expected_format=TERMINAL_FORMAT)
    receipt = read_sealed_document(paths["receipt"], expected_format=RECEIPT_FORMAT)
    shared = (
        "task_id",
        "dataset_id",
        "authorization_sha256",
        "plan_sha256",
        "prewrite_sha256",
    )
    if any(terminal.get(key) != receipt.get(key) for key in shared):
        raise EvidenceError("terminal journal and receipt chain mismatch")
    if receipt.get("terminal_sha256") != terminal.get("document_sha256"):
        raise EvidenceError("receipt does not bind terminal journal")
    if prewrite.get("authorization_sha256") != authorization.get("document_sha256"):
        raise EvidenceError("pre-write journal does not bind authorization")
    if terminal.get("prewrite_sha256") != prewrite.get("document_sha256"):
        raise EvidenceError("terminal journal does not bind pre-write journal")
    if (
        terminal.get("terminal_state") != "sealed"
        or receipt.get("terminal_state") != "sealed"
    ):
        raise EvidenceError("terminal evidence is not sealed")
    _verify_live_candidate(fresh_state(), receipt.get("candidate", {}))
    return receipt


def ordered_obs_identity(rows: Iterable[tuple[int, str]]) -> dict[str, Any]:
    digest = hashlib.sha256()
    count = 0
    first: tuple[int, str] | None = None
    last: tuple[int, str] | None = None
    # SQLite's on-disk unique indexes make duplicate detection exact without a
    # process-resident set that grows with the 22M-row OBS denominator.
    with tempfile.TemporaryDirectory(prefix="broad-prism-identity-") as temporary:
        database = sqlite3.connect(Path(temporary) / "seen.sqlite")
        try:
            database.execute("CREATE TABLE seen (idx INTEGER UNIQUE, uid TEXT UNIQUE)")
            for original_index, raw_uuid in rows:
                if not isinstance(original_index, int) or original_index < 0:
                    raise IdentityError(
                        "original_obs_index must be a non-negative integer"
                    )
                try:
                    parsed = uuid.UUID(raw_uuid)
                except (ValueError, AttributeError) as exc:
                    raise IdentityError("obs_uuid is not a UUID") from exc
                if parsed.version != 1:
                    raise IdentityError("obs_uuid must be UUID v1 material")
                normalized = str(parsed)
                try:
                    database.execute(
                        "INSERT INTO seen VALUES (?, ?)", (original_index, normalized)
                    )
                except sqlite3.IntegrityError as exc:
                    raise IdentityError(
                        "ordered OBS identity contains a duplicate"
                    ) from exc
                item = (original_index, normalized)
                if first is None:
                    first = item
                last = item
                digest.update(f"{original_index}\x1f{normalized}\n".encode("ascii"))
                count += 1
        finally:
            database.close()
    return {"count": count, "sha256": digest.hexdigest(), "first": first, "last": last}


def verify_ordered_obs_identity(
    expected: Mapping[str, Any], rows: Iterable[tuple[int, str]]
) -> None:
    actual = ordered_obs_identity(rows)
    if actual != dict(expected):
        raise IdentityError("ordered OBS UUID/original-index identity drift")


def stream_batches(
    batches: Iterable[range], *, batch_limit: int, emit: Callable[[range], None]
) -> dict[str, int]:
    if batch_limit <= 0:
        raise ValueError("batch_limit must be positive")
    total = peak_rows = peak_batches = 0
    for batch in batches:
        if not isinstance(batch, range):
            raise TypeError(
                "stream input must yield ranges, not materialized row lists"
            )
        rows = len(batch)
        if rows > batch_limit:
            raise MemoryError("row batch exceeds the configured bounded envelope")
        peak_rows = max(peak_rows, rows)
        peak_batches = max(peak_batches, 1)
        emit(batch)
        total += rows
    return {"rows": total, "peak_rows": peak_rows, "peak_batches": peak_batches}


def parse_pass(raw: str) -> bool:
    token = raw.strip().lower()
    if token == "true":
        return True
    if token == "false":
        return False
    raise ValueError(f"unrecognized PASS token: {raw!r}")


def parse_treatment_type(raw: str) -> str:
    token = raw.strip().lower()
    if token == "trt_cp":
        return token
    if token == "trt_poscon":
        raise ReviewRequired("trt_poscon requires an explicit review decision")
    raise ValueError(f"unrecognized treatment type: {raw!r}")


def source_coordinates(ordinal: int, chunk_size: int) -> dict[str, int]:
    if ordinal < 0 or chunk_size <= 0:
        raise ValueError("source ordinal and chunk size must be positive")
    return {
        "source_file_row_number": ordinal + 2,
        "source_row_chunk_index": ordinal // chunk_size,
        "source_row_offset_in_chunk": ordinal % chunk_size,
    }


def recover_file(
    destination: Path, expected_sha256: str, rebuild: Callable[[Path], None]
) -> str:
    partial = destination.with_suffix(destination.suffix + ".part")
    if destination.exists() and partial.exists():
        raise RecoveryError("completed and partial files coexist ambiguously")
    if partial.exists():
        partial.replace(partial.with_suffix(partial.suffix + ".quarantined"))
    if destination.exists() and sha256_file(destination) == expected_sha256:
        return "reused"
    if destination.exists():
        destination.replace(
            destination.with_suffix(destination.suffix + ".quarantined")
        )
    rebuild(destination)
    if not destination.exists() or sha256_file(destination) != expected_sha256:
        raise RecoveryError("rebuild did not produce the expected exact file")
    return "rebuilt"
