#!/usr/bin/env python3
"""Seal and validate the strict accepted publication-component identity ledger."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sqlite3
from pathlib import Path
from typing import Any

SCHEMA_ID = "pert-gym.accepted-component-identities/v1"
EXPECTED_DENOMINATOR = 153
EXPECTED_ACCEPTED = 29

# The first four accepted ledger runs predate record_id on accepted-component
# events. These are explicit identity bindings, never positional catalogue picks.
LEGACY_RECORD_BINDINGS = {
    "t_591607e3": "xatlas_hct116",
    "t_d08ef390": "xatlas_hek293t",
    "t_536f36cd": "temporal_v4_099_cell_culture_differentiation_and_proliferation_conditions_influence_the_in_vitro",
    "t_664e9dc0": "temporal_v4_013_cerebellar_organoid_using_microfluidics_and_combinatorial_barcoding_based_techno",
}
LEGACY_ACCEPTANCE_BINDINGS = {
    # Canonical post-production review of the exact immutable HCT116 candidate.
    "t_591607e3": "t_d2535006",
    # The original linked reviewer failed on mutable QA provenance. This later
    # exact evidence review is the accepted binding for the retained event.
    "t_f196b29f": "t_96c5aeda",
}
LEGACY_ACCEPTANCE_RUN_BINDINGS = {
    "t_591607e3": 2828,
    "t_f196b29f": 3691,
}
SUPPORTING_ACCEPTANCE_BINDINGS = {
    # Independent readback tester feeding the canonical HCT116 reviewer above.
    "t_591607e3": (("t_09626817", 2827),),
}
LEGACY_MANIFEST_GENERATIONS = {"t_591607e3": "1784029269136670"}


class LedgerValidationError(ValueError):
    """The accepted identity ledger is incomplete, ambiguous, or inconsistent."""


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode()


def _compact_bytes(value: Any) -> bytes:
    return json.dumps(
        value, separators=(",", ":"), sort_keys=True, ensure_ascii=False
    ).encode()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _metadata_digest(metadata: dict[str, Any]) -> str:
    return _sha256_bytes(_compact_bytes(metadata))


def _get_path(value: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = value
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _manifest_hash(metadata: dict[str, Any]) -> str | None:
    paths = (
        ("manifest", "sha256"),
        ("candidate", "manifest_sha256"),
        ("immutable_manifest", "sha256"),
        ("immutable_evidence", "manifest_sha256"),
        ("immutable_evidence", "materialization_manifest_sha256"),
        ("hashes", "generation_pinned_manifest"),
        ("evidence", "dataset_manifest_sha256"),
        ("evidence", "immutable_manifest_sha256"),
        ("readback", "manifest_sha256"),
        ("evidence_hashes", "manifest_sha256"),
        ("manifest_sha256",),
    )
    for path in paths:
        value = _get_path(metadata, path)
        if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
            return value
    return None


def _manifest_generation(uri: str, metadata: dict[str, Any]) -> str | None:
    if "#" in uri:
        fragment = uri.rsplit("#", 1)[1]
        if fragment.isdigit():
            return fragment
    for path in (("manifest", "generation"), ("manifest_generation",)):
        value = _get_path(metadata, path)
        if value is not None and str(value).isdigit():
            return str(value)
    return None


def _task_run(
    con: sqlite3.Connection, task_id: str, run_id: int | None = None
) -> sqlite3.Row:
    if run_id is None:
        row = con.execute(
            "select tr.*,t.assignee,t.title from task_runs tr join tasks t on t.id=tr.task_id "
            "where tr.task_id=? and tr.outcome='completed' order by tr.id desc limit 1",
            (task_id,),
        ).fetchone()
    else:
        row = con.execute(
            "select tr.*,t.assignee,t.title from task_runs tr join tasks t on t.id=tr.task_id "
            "where tr.id=? and tr.task_id=?",
            (run_id, task_id),
        ).fetchone()
    if row is None:
        raise LedgerValidationError(
            f"missing completed task run: task={task_id} run={run_id}"
        )
    if row["outcome"] != "completed":
        raise LedgerValidationError(
            f"non-completed task run: task={task_id} run={row['id']}"
        )
    return row


def _task_ids(value: Any, key_path: str = "") -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{key_path}.{key}" if key_path else key
            found.extend(_task_ids(child, path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_task_ids(child, f"{key_path}[{index}]"))
    elif isinstance(value, str) and re.fullmatch(r"t_[0-9a-f]{8}", value):
        found.append((key_path, value))
    return found


def _approved(metadata: dict[str, Any]) -> bool:
    if metadata.get("approved") is True or metadata.get("accepted") is True:
        return True
    verdicts = {
        str(metadata.get(key, "")).upper()
        for key in ("verdict", "canonical_outcome", "canonical_terminal_outcome")
    }
    return bool(verdicts & {"PASS", "APPROVE", "DONE"})


def _scalar_values(value: Any, key_path: str = "") -> list[tuple[str, Any]]:
    found: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{key_path}.{key}" if key_path else key
            found.extend(_scalar_values(child, path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_scalar_values(child, f"{key_path}[{index}]"))
    elif isinstance(value, (str, int)) and not isinstance(value, bool):
        found.append((key_path, value))
    return found


def _accepted_object_evidence(
    metadata: dict[str, Any],
    *,
    record_id: str,
    target_logical_key: str,
    manifest_uri: str,
    manifest_generation: str | None,
    manifest_hash: str | None,
) -> dict[str, Any] | None:
    scalars = _scalar_values(metadata)
    scalar_strings = {str(value) for _, value in scalars}
    if record_id in scalar_strings:
        return {"kind": "exact_record_id", "record_id": record_id}
    if target_logical_key and target_logical_key in scalar_strings:
        return {
            "kind": "exact_target_logical_key",
            "target_logical_key": target_logical_key,
        }

    if not manifest_hash or not manifest_generation:
        return None
    scalar_text = "\n".join(str(value) for _, value in scalars)
    hash_pattern = rf"(?<![0-9a-f]){re.escape(manifest_hash)}(?![0-9a-f])"
    generation_pattern = rf"(?<!\d){re.escape(manifest_generation)}(?!\d)"
    if not re.search(hash_pattern, scalar_text) or not re.search(
        generation_pattern, scalar_text
    ):
        return None

    manifest_base_uri = manifest_uri.split("#", 1)[0]
    candidate_manifest_uris = set()
    for path, value in scalars:
        if not isinstance(value, str) or (
            "manifest" not in path.lower() and "gs://" not in value
        ):
            continue
        for candidate_uri in re.findall(
            r"gs://[^\s`\"']+/manifest\.json(?:#\d+)?", value
        ):
            candidate_manifest_uris.add(candidate_uri.split("#", 1)[0])
    if candidate_manifest_uris and manifest_base_uri not in candidate_manifest_uris:
        return None
    evidence = {
        "kind": "exact_manifest_sha256_generation",
        "manifest_generation": manifest_generation,
        "manifest_sha256": manifest_hash,
    }
    if manifest_base_uri in candidate_manifest_uris:
        evidence = {
            "kind": "exact_manifest_uri_sha256_generation",
            "manifest_uri": manifest_base_uri,
            "manifest_generation": manifest_generation,
            "manifest_sha256": manifest_hash,
        }
    return evidence


def _acceptance_binding(
    con: sqlite3.Connection,
    event_task_id: str,
    event_metadata: dict[str, Any],
    *,
    record_id: str,
    target_logical_key: str,
    manifest_uri: str,
    manifest_generation: str | None,
    manifest_hash: str | None,
) -> dict[str, Any]:
    candidates: list[tuple[int, str, str, int | None]] = []
    explicit = LEGACY_ACCEPTANCE_BINDINGS.get(event_task_id)
    if explicit:
        candidates.append(
            (
                100,
                "legacy_explicit_independent_gate",
                explicit,
                LEGACY_ACCEPTANCE_RUN_BINDINGS[event_task_id],
            )
        )

    for path, task_id in _task_ids(event_metadata):
        lower = path.lower()
        if any(
            bad in lower for bad in ("prior_review", "historical_failed", "invalidated")
        ):
            continue
        if "review" in lower or "final_gate" in lower:
            candidates.append((90, path, task_id, None))
        elif "accepted_outcome" in lower or "parent_outcome" in lower:
            candidates.append((80, path, task_id, None))

    parent_ids = [
        str(row[0])
        for row in con.execute(
            "select parent_id from task_links where child_id=? order by parent_id",
            (event_task_id,),
        )
    ]
    for task_id in parent_ids:
        row = con.execute(
            "select assignee from tasks where id=?", (task_id,)
        ).fetchone()
        if row and row[0] == "reviewer":
            candidates.append((70, "linked_reviewer_parent", task_id, None))

    errors: list[str] = []
    for _, source, task_id, candidate_run_id in sorted(
        set(candidates), reverse=True, key=lambda candidate: candidate[:3]
    ):
        try:
            row = _task_run(con, task_id, candidate_run_id)
            metadata = json.loads(row["metadata"] or "{}")
        except (LedgerValidationError, json.JSONDecodeError) as exc:
            errors.append(f"{task_id}: {exc}")
            continue
        if row["assignee"] not in {"reviewer", "tester", "default"}:
            continue
        if not _approved(metadata) and not (
            task_id == explicit and str(metadata.get("verdict", "")).lower() == "done"
        ):
            continue
        binding_evidence = _accepted_object_evidence(
            metadata,
            record_id=record_id,
            target_logical_key=target_logical_key,
            manifest_uri=manifest_uri,
            manifest_generation=manifest_generation,
            manifest_hash=manifest_hash,
        )
        if binding_evidence is None:
            errors.append(f"{task_id}: approval lacks accepted-object identity")
            continue
        acceptance = {
            "task_id": task_id,
            "run_id": int(row["id"]),
            "profile": str(row["assignee"]),
            "source": source,
            "verdict": "PASS",
            "metadata_sha256": _metadata_digest(metadata),
            "binding_evidence": binding_evidence,
        }
        supporting_evidence = []
        for supporting_task_id, supporting_run_id in SUPPORTING_ACCEPTANCE_BINDINGS.get(
            event_task_id, ()
        ):
            supporting_row = _task_run(con, supporting_task_id, supporting_run_id)
            supporting_metadata = json.loads(supporting_row["metadata"] or "{}")
            supporting_binding = _accepted_object_evidence(
                supporting_metadata,
                record_id=record_id,
                target_logical_key=target_logical_key,
                manifest_uri=manifest_uri,
                manifest_generation=manifest_generation,
                manifest_hash=manifest_hash,
            )
            if not _approved(supporting_metadata) or supporting_binding is None:
                raise LedgerValidationError(
                    f"supporting acceptance {supporting_task_id} lacks exact object evidence"
                )
            supporting_evidence.append(
                {
                    "task_id": supporting_task_id,
                    "run_id": int(supporting_row["id"]),
                    "profile": str(supporting_row["assignee"]),
                    "source": "supporting_exact_candidate_test",
                    "verdict": "PASS",
                    "metadata_sha256": _metadata_digest(supporting_metadata),
                    "binding_evidence": supporting_binding,
                }
            )
        if supporting_evidence:
            acceptance["supporting_evidence"] = supporting_evidence
        return acceptance
    detail = "; ".join(errors)
    raise LedgerValidationError(
        f"accepted event {event_task_id} lacks an independent acceptance binding"
        + (f": {detail}" if detail else "")
    )


def _event_record_id(task_id: str, metadata: dict[str, Any]) -> tuple[str, str]:
    record_id = metadata.get("record_id")
    if isinstance(record_id, str) and record_id:
        return record_id, "event_run_metadata.record_id"
    legacy = LEGACY_RECORD_BINDINGS.get(task_id)
    if legacy:
        return legacy, "explicit_legacy_task_binding"
    raise LedgerValidationError(
        f"accepted event {task_id} has no immutable record identity"
    )


def _records_digest(records: list[dict[str, Any]]) -> str:
    return _sha256_bytes(_compact_bytes(records))


def build_ledger(
    catalogue_path: Path,
    progress_path: Path,
    board_db_path: Path,
    *,
    expected_accepted: int = EXPECTED_ACCEPTED,
    expected_denominator: int = EXPECTED_DENOMINATOR,
) -> dict[str, Any]:
    catalogue = json.loads(catalogue_path.read_text())
    progress = json.loads(progress_path.read_text())
    executable = sorted(
        (
            record
            for record in catalogue.get("records", [])
            if record.get("classification") == "executable"
        ),
        key=lambda record: record["record_id"],
    )
    if len(executable) != expected_denominator:
        raise LedgerValidationError(
            f"denominator mismatch: executable={len(executable)} expected={expected_denominator}"
        )
    by_id = {record["record_id"]: record for record in executable}
    if len(by_id) != len(executable):
        raise LedgerValidationError("duplicate record_id in executable catalogue")

    events = [
        event
        for event in progress.get("ledger_events", [])
        if event.get("metric") == "accepted_components"
    ]
    if len(events) != expected_accepted:
        raise LedgerValidationError(
            f"accepted event count mismatch: events={len(events)} expected={expected_accepted}"
        )

    con = sqlite3.connect(f"file:{board_db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    components: list[dict[str, Any]] = []
    try:
        for index, event in enumerate(events):
            if event.get("before") != index or event.get("after") != index + 1:
                raise LedgerValidationError(
                    f"accepted ledger continuity mismatch at event {index}: "
                    f"{event.get('before')}->{event.get('after')}"
                )
            if event.get("denominator") != expected_denominator:
                raise LedgerValidationError("accepted event denominator mismatch")
            if event.get("unit") != "components" or event.get("mismatch") != 0:
                raise LedgerValidationError(
                    "accepted event is not a strict zero-mismatch component delta"
                )
            task_id = str(event.get("task_id") or "")
            run_id = event.get("run_id")
            if not re.fullmatch(r"t_[0-9a-f]{8}", task_id) or not isinstance(
                run_id, int
            ):
                raise LedgerValidationError(
                    "accepted event lacks stable task/run identity"
                )
            row = _task_run(con, task_id, run_id)
            metadata = json.loads(row["metadata"] or "{}")
            product_event = _get_path(
                metadata, ("product_delta", "metrics", "accepted_components")
            )
            expected_product_event = {
                key: event[key]
                for key in (
                    "before",
                    "after",
                    "denominator",
                    "unit",
                    "mismatch",
                    "live_readback",
                )
            }
            if product_event is not None and product_event != expected_product_event:
                raise LedgerValidationError(
                    f"progress/run product delta mismatch for task={task_id} run={run_id}"
                )
            if product_event is None and not (
                isinstance(metadata.get("record_id"), str) and _approved(metadata)
            ):
                raise LedgerValidationError(
                    f"accepted event {task_id} lacks an exact delta or approved identity handoff"
                )
            operation_id = metadata.get("worker_session_id")
            if not isinstance(operation_id, str) or not operation_id:
                raise LedgerValidationError(
                    f"accepted event {task_id} lacks stable operation identity"
                )
            record_id, mapping_source = _event_record_id(task_id, metadata)
            record = by_id.get(record_id)
            if record is None:
                raise LedgerValidationError(
                    f"accepted event maps outside executable universe: {record_id}"
                )
            live_readback = str(event.get("live_readback") or "")
            target_key = str(record.get("target_logical_key") or "")
            target_suffix = target_key.removeprefix("pert-gym/logical/")
            if record_id == "xatlas_hct116":
                identity_token = "hct116_filtered_dual_guide_cells"
            elif record_id == "xatlas_hek293t":
                identity_token = "hek293t_filtered_dual_guide_cells"
            else:
                identity_token = target_suffix
            if not identity_token or identity_token not in live_readback:
                raise LedgerValidationError(
                    f"ambiguous live-readback mapping for {task_id}: {record_id}"
                )
            generation = _manifest_generation(
                live_readback, metadata
            ) or LEGACY_MANIFEST_GENERATIONS.get(task_id)
            manifest_hash = _manifest_hash(metadata)
            components.append(
                {
                    "record_id": record_id,
                    "catalogue_row_ids": record.get("catalogue_row_ids", []),
                    "component": record.get("component"),
                    "target_logical_key": target_key,
                    "mapping_source": mapping_source,
                    "event": {
                        "operation_id": operation_id,
                        "run_id": run_id,
                        "task_id": task_id,
                        "before": event["before"],
                        "after": event["after"],
                        "ended_at": event.get("ended_at"),
                    },
                    "live_readback": {
                        "uri": live_readback,
                        "generation": generation,
                        "sha256": manifest_hash,
                    },
                    "acceptance": _acceptance_binding(
                        con,
                        task_id,
                        metadata,
                        record_id=record_id,
                        target_logical_key=target_key,
                        manifest_uri=live_readback,
                        manifest_generation=generation,
                        manifest_hash=manifest_hash,
                    ),
                    "event_run_metadata_sha256": _metadata_digest(metadata),
                }
            )
    finally:
        con.close()

    components.sort(key=lambda component: component["record_id"])
    record_ids = [component["record_id"] for component in components]
    if len(set(record_ids)) != expected_accepted:
        raise LedgerValidationError("duplicate accepted record identity")

    return {
        "schema_id": SCHEMA_ID,
        "accepted": expected_accepted,
        "denominator": expected_denominator,
        "remaining": expected_denominator - expected_accepted,
        "sort_order": "record_id ascending (Unicode code-point order)",
        "source_files": {
            "catalogue": {
                "path": str(catalogue_path),
                "sha256": _sha256_file(catalogue_path),
                "records_sha256": catalogue.get("records_sha256"),
            },
            "accepted_progress_snapshot": {
                "path": str(progress_path),
                "sha256": _sha256_file(progress_path),
                "generated_at": progress.get("generated_at"),
            },
        },
        "catalogue": {
            "executable_count": len(executable),
            "executable_records_sha256": _records_digest(executable),
        },
        "identity_set_sha256": _sha256_bytes(_compact_bytes(record_ids)),
        "accepted_components": components,
    }


def render_tsv(ledger: dict[str, Any]) -> bytes:
    output = io.StringIO(newline="")
    fields = [
        "record_id",
        "catalogue_row_ids",
        "target_logical_key",
        "event_task_id",
        "event_run_id",
        "operation_id",
        "acceptance_task_id",
        "acceptance_run_id",
        "live_readback_uri",
        "live_readback_generation",
        "live_readback_sha256",
    ]
    writer = csv.DictWriter(
        output, fieldnames=fields, dialect="excel-tab", lineterminator="\n"
    )
    writer.writeheader()
    for component in ledger["accepted_components"]:
        writer.writerow(
            {
                "record_id": component["record_id"],
                "catalogue_row_ids": ",".join(map(str, component["catalogue_row_ids"])),
                "target_logical_key": component["target_logical_key"],
                "event_task_id": component["event"]["task_id"],
                "event_run_id": component["event"]["run_id"],
                "operation_id": component["event"]["operation_id"],
                "acceptance_task_id": component["acceptance"]["task_id"],
                "acceptance_run_id": component["acceptance"]["run_id"],
                "live_readback_uri": component["live_readback"]["uri"],
                "live_readback_generation": component["live_readback"]["generation"]
                or "",
                "live_readback_sha256": component["live_readback"]["sha256"] or "",
            }
        )
    return output.getvalue().encode()


def write_outputs(output_path: Path, ledger: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    json_bytes = _canonical_bytes(ledger)
    tsv_path = output_path.with_suffix(".tsv")
    tsv_bytes = render_tsv(ledger)
    output_path.write_bytes(json_bytes)
    tsv_path.write_bytes(tsv_bytes)
    checksums = (
        f"{_sha256_bytes(json_bytes)}  {output_path.name}\n"
        f"{_sha256_bytes(tsv_bytes)}  {tsv_path.name}\n"
    )
    output_path.with_suffix(".sha256").write_text(checksums)


def validate_output(
    output_path: Path,
    catalogue_path: Path,
    progress_path: Path,
    board_db_path: Path,
    *,
    expected_accepted: int = EXPECTED_ACCEPTED,
    expected_denominator: int = EXPECTED_DENOMINATOR,
) -> dict[str, Any]:
    observed = json.loads(output_path.read_text())
    expected = build_ledger(
        catalogue_path,
        progress_path,
        board_db_path,
        expected_accepted=expected_accepted,
        expected_denominator=expected_denominator,
    )
    if observed != expected:
        raise LedgerValidationError(
            "sealed ledger differs from authoritative inputs (digest or mapping drift)"
        )
    if output_path.read_bytes() != _canonical_bytes(observed):
        raise LedgerValidationError("sealed ledger is not canonical byte-stable JSON")
    tsv_path = output_path.with_suffix(".tsv")
    if tsv_path.read_bytes() != render_tsv(observed):
        raise LedgerValidationError("mapping TSV differs from sealed ledger")
    checksum_path = output_path.with_suffix(".sha256")
    expected_checksums = (
        f"{_sha256_file(output_path)}  {output_path.name}\n"
        f"{_sha256_file(tsv_path)}  {tsv_path.name}\n"
    )
    if checksum_path.read_text() != expected_checksums:
        raise LedgerValidationError("checksum sidecar drift")
    return observed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("build", "validate"))
    parser.add_argument("--catalogue", required=True, type=Path)
    parser.add_argument("--progress", required=True, type=Path)
    parser.add_argument("--board-db", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.mode == "build":
        ledger = build_ledger(args.catalogue, args.progress, args.board_db)
        write_outputs(args.output, ledger)
    else:
        ledger = validate_output(
            args.output, args.catalogue, args.progress, args.board_db
        )
    print(
        f"PASS {ledger['accepted']}/{ledger['denominator']} unique accepted identities "
        f"identity_set_sha256={ledger['identity_set_sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
