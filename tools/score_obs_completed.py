#!/usr/bin/env python3
"""Score pert-gym OBS_COMPLETED evidence without Lamin, GCS, obs, X, or var reads."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

FIELD_STATES = {"present", "alias_only", "manifest_only", "missing", "not_applicable"}


def load_contract(path: str | Path) -> dict[str, Any]:
    contract = json.loads(Path(path).read_text(encoding="utf-8"))
    if len(contract.get("canonical_obs_columns", [])) != 44:
        raise ValueError(
            "OBS_COMPLETED contract must contain exactly 44 canonical obs columns"
        )
    return contract


def load_manifest(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def load_evidence(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("datasets"), list):
        return payload["datasets"]
    raise ValueError("evidence must be a JSON list or an object with a datasets list")


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _check_bool(
    name: str,
    value: Any,
    failed: list[str],
    blocked: list[str],
    checks: dict[str, str],
) -> None:
    if value is True:
        checks[name] = "pass"
    elif value is False:
        checks[name] = "fail"
        failed.append(name)
    else:
        checks[name] = "blocked"
        blocked.append(f"{name}.evidence_missing")


def _score_dataset(
    logical_dataset: str,
    members: list[dict[str, Any]],
    evidence: dict[str, Any] | None,
    contract: dict[str, Any],
) -> dict[str, Any]:
    failed: list[str] = []
    blocked: list[str] = []
    checks: dict[str, str] = {}
    fields_total = len(contract["canonical_obs_columns"]) * len(members)
    fields_applicable = fields_total
    fields_covered = 0
    identity_passed = 0
    evidence_members = (evidence or {}).get("members", {})

    for member in members:
        member_key = str(member.get("artifact_key") or member.get("prefix") or "")
        member_evidence = evidence_members.get(member_key)
        if not isinstance(member_evidence, dict):
            blocked.append("member_evidence.missing")
            checks[f"member:{member_key}"] = "blocked"
            continue

        fields = member_evidence.get("fields", {})
        for field in contract["canonical_obs_columns"]:
            item = fields.get(field)
            prefix = f"fields.{field}"
            if not isinstance(item, dict):
                blocked.append(f"{prefix}.evidence_missing")
                continue
            state = item.get("state")
            if state not in FIELD_STATES:
                failed.append(f"{prefix}.invalid_state:{state}")
                continue
            if state == "not_applicable":
                fields_applicable -= 1
                continue
            if state == "missing":
                failed.append(f"{prefix}.missing")
                continue
            if state == "manifest_only":
                blocked.append(f"{prefix}.manifest_only")
                continue
            if not str(item.get("source", "")).strip():
                failed.append(f"{prefix}.missing_source")
                continue
            non_null = item.get("non_null_rows")
            total = item.get("total_rows")
            if (
                not isinstance(non_null, int)
                or not isinstance(total, int)
                or total <= 0
            ):
                blocked.append(f"{prefix}.coverage_evidence_missing")
                continue
            if non_null != total:
                failed.append(f"{prefix}.incomplete_coverage:{non_null}/{total}")
                continue
            fields_covered += 1

        identity = member_evidence.get("identity", {})
        before = len(failed) + len(blocked)
        for name in contract["required_identity_checks"]:
            _check_bool(f"identity.{name}", identity.get(name), failed, blocked, checks)
        if len(failed) + len(blocked) == before:
            identity_passed += 1

    dataset_checks = (evidence or {}).get("dataset_checks", {})
    for name in contract["required_dataset_checks"]:
        _check_bool(name, dataset_checks.get(name), failed, blocked, checks)

    duplicate = dataset_checks.get("duplicate_status")
    if duplicate in contract["duplicate_status_pass"]:
        checks["duplicate_status"] = "pass"
    elif duplicate in {"duplicate", "subduplicate", "excluded"}:
        checks["duplicate_status"] = "fail"
        failed.append(f"duplicate_status.{duplicate}")
    else:
        checks["duplicate_status"] = "blocked"
        blocked.append("duplicate_status.evidence_missing_or_unchecked")

    for name in ("loader_projectable", "model_ready"):
        _check_bool(name, dataset_checks.get(name), failed, blocked, checks)

    quality = dataset_checks.get("quality_flag")
    if quality in contract["quality_flag_pass"]:
        checks["quality_flag"] = "pass"
    elif quality == "low_quality":
        checks["quality_flag"] = "fail"
        failed.append("quality_flag.low_quality")
    else:
        checks["quality_flag"] = "blocked"
        blocked.append("quality_flag.evidence_missing_or_unknown")

    for name in ("citations", "provenance"):
        value = dataset_checks.get(name)
        if isinstance(value, list) and value:
            checks[name] = "pass"
        elif isinstance(value, list):
            checks[name] = "fail"
            failed.append(f"{name}.empty")
        else:
            checks[name] = "blocked"
            blocked.append(f"{name}.evidence_missing")

    fabricated = dataset_checks.get("fabricated_values")
    if fabricated is False:
        checks["absence_of_fabrication"] = "pass"
    elif fabricated is True:
        checks["absence_of_fabrication"] = "fail"
        failed.append("fabricated_values.present")
    else:
        checks["absence_of_fabrication"] = "blocked"
        blocked.append("fabricated_values.evidence_missing")

    failed = sorted(set(failed))
    blocked = sorted(set(blocked))
    status = "false" if failed else "blocked" if blocked else "true"
    return {
        "logical_dataset": logical_dataset,
        "OBS_COMPLETED": status,
        "failed_checks": failed,
        "blocked_checks": blocked,
        "denominators": {
            "members_total": len(members),
            "members_with_evidence": sum(
                1
                for member in members
                if str(member.get("artifact_key") or member.get("prefix") or "")
                in evidence_members
            ),
            "rows_manifest": sum(_int(member.get("n_obs")) for member in members),
            "canonical_fields_total": fields_total,
            "canonical_fields_applicable": fields_applicable,
            "canonical_fields_covered": fields_covered,
            "identity_members_passed": identity_passed,
        },
        "checks": checks,
    }


def score_manifest(
    manifest_rows: Iterable[dict[str, Any]],
    evidence_rows: Iterable[dict[str, Any]],
    contract: dict[str, Any],
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in manifest_rows:
        logical_dataset = str(
            row.get("logical_dataset") or row.get("dataset_id") or ""
        ).strip()
        if not logical_dataset:
            raise ValueError("every manifest row needs logical_dataset or dataset_id")
        grouped[logical_dataset].append(row)

    evidence_by_dataset = {
        str(item.get("logical_dataset")): item
        for item in evidence_rows
        if isinstance(item, dict) and item.get("logical_dataset")
    }
    datasets = [
        _score_dataset(name, grouped[name], evidence_by_dataset.get(name), contract)
        for name in sorted(grouped)
    ]
    counts = Counter(item["OBS_COMPLETED"] for item in datasets)
    return {
        "contract_id": contract["contract_id"],
        "read_only": True,
        "loads_X": False,
        "datasets": datasets,
        "summary": {
            "datasets_total": len(datasets),
            "true": counts["true"],
            "false": counts["false"],
            "blocked": counts["blocked"],
        },
    }


def write_result(result: dict[str, Any], output: str | Path) -> None:
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", required=True, help="Canonical member manifest TSV"
    )
    parser.add_argument("--evidence", help="Optional OBS evidence JSON")
    parser.add_argument(
        "--contract",
        default="config/obs_completed_contract_v1.json",
        help="Machine-readable contract JSON",
    )
    parser.add_argument("--output", help="Write JSON here; stdout when omitted")
    args = parser.parse_args()

    result = score_manifest(
        load_manifest(args.manifest),
        load_evidence(args.evidence),
        load_contract(args.contract),
    )
    if args.output:
        write_result(result, args.output)
    else:
        print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
