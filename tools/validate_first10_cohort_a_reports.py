#!/usr/bin/env python3
"""Validate cohort-A audit reports and fail closed on evidence gaps."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIR = ROOT / "artifacts" / "first10_audit" / "cohort_a"
EXPECTED = {"E-MTAB-9304", "GSE130238", "GSE138002"}
ALLOWED_ACTION_PREFIXES = {
    "derive",
    "drop",
    "keep",
    "map",
    "move",
    "normalize",
    "preserve",
    "rename",
    "replace",
    "state",
    "type",
}
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _builder_functions() -> tuple[Any, Any]:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from tools.build_first10_cohort_a_reports import build_report, render_markdown

    return build_report, render_markdown


def _parse_description(artifact: dict[str, Any]) -> dict[str, Any]:
    description = artifact.get("description")
    if not description:
        return {}
    parsed = json.loads(description)
    if not isinstance(parsed, dict):
        raise AssertionError("artifact description must decode to an object")
    return parsed


def validate_report(report: dict[str, Any], report_path: Path) -> list[str]:
    errors: list[str] = []
    dataset = report.get("dataset", report_path.stem)

    def check(condition: bool, message: str) -> None:
        if not condition:
            errors.append(f"{dataset}: {message}")

    check(
        report.get("format") == "pert-gym.first10-cohort-a-correction-plan/v1",
        "bad format",
    )
    check(report.get("task_id") == "t_2122b5f4", "wrong task id")
    check(report.get("read_only") is True, "audit must declare read_only=true")
    check(report.get("branch") == "jkobject", "wrong Lamin branch")
    check(report.get("instance") == "laminlabs/pertdata", "wrong Lamin instance")

    expected_live_path = report_path.parent / f"{dataset}.audit.json"
    try:
        expected_live_reference = str(expected_live_path.resolve().relative_to(ROOT))
    except ValueError:
        errors.append(f"{dataset}: report directory is outside the repository root")
        expected_live_reference = ""
    check(
        report.get("live_evidence_file") == expected_live_reference,
        "live_evidence_file is not the exact same-directory frozen audit",
    )
    try:
        live = json.loads(expected_live_path.read_text(encoding="utf-8"))
        build_report, _ = _builder_functions()
        expected_report = build_report(dataset, live, expected_live_path)
        check(
            report == expected_report,
            "report diverges from the frozen audit and declarative correction plan",
        )
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        errors.append(f"{dataset}: cannot rebuild report from frozen audit: {exc}")

    inventory = report.get("current_obs_inventory", {}).get("columns", {})
    decisions = report.get("obs_column_decisions", {})
    check(
        set(inventory) == set(decisions),
        "OBS decisions do not cover every and only current column",
    )
    for column, item in decisions.items():
        action = str(item.get("action", ""))
        check(
            any(
                action == prefix or action.startswith(f"{prefix}_")
                for prefix in ALLOWED_ACTION_PREFIXES
            ),
            f"unsupported action for {column}: {action}",
        )
        check(bool(item.get("reason")), f"missing reason for {column}")

    schema = report.get("proposed_post_fix_obs_schema", [])
    check(len(schema) == len(set(schema)), "proposed OBS schema has duplicate columns")
    for required in (
        "obs_uuid",
        "original_obs_index",
        "perturbation",
        "perturbation_type",
        "is_control",
        "is_baseline",
    ):
        check(required in schema, f"proposed OBS schema misses {required}")
    for column, item in decisions.items():
        action = str(item.get("action", ""))
        target = item.get("target")
        remains_row_level = not action.startswith(("drop", "move", "type_auxiliary"))
        if target and remains_row_level:
            check(
                target in schema,
                f"row-level target {target} from {column} is absent from proposed schema",
            )

    for source in report.get("source_evidence", []):
        check(
            str(source.get("url", "")).startswith("https://"),
            "source URL must be HTTPS",
        )
        if "sha256" in source:
            check(
                bool(SHA256.fullmatch(str(source["sha256"]))), "invalid source SHA-256"
            )

    identity = report.get("current_identity", {})
    canonical = identity.get("canonical_artifacts", {})
    related = report.get("raw_provenance", {}).get("related_lamin_artifacts", [])
    check(isinstance(related, list), "related Lamin artifact inventory is malformed")
    check(
        set(canonical) == {"obs", "X", "var"}, "canonical triplet roles are incomplete"
    )
    for role, artifact in canonical.items():
        candidates = artifact.get("latest_exact_key_candidates", [])
        inventory_matches = [
            item
            for item in related
            if item.get("is_latest") is True and item.get("key") == artifact.get("key")
        ]
        check(
            len(candidates) == 1
            and candidates[0].get("key") == artifact.get("key")
            and candidates[0].get("uid") == artifact.get("uid"),
            f"{role} is not the unique current artifact for its exact key",
        )
        check(
            len(inventory_matches) == 1
            and inventory_matches[0].get("uid") == artifact.get("uid")
            and inventory_matches == candidates,
            f"{role} exact-key candidates disagree with complete related inventory",
        )
        payload = artifact.get("payload_evidence", {})
        description = _parse_description(artifact)
        downloaded = payload.get("downloaded_sha256")
        described = description.get("object_sha256") or description.get(
            "payload_sha256"
        )
        if payload.get("path_exists"):
            check(
                bool(SHA256.fullmatch(str(downloaded))),
                f"{role} missing downloaded SHA-256",
            )
            check(
                downloaded == described,
                f"{role} downloaded SHA differs from artifact description",
            )
        else:
            check(
                bool(payload.get("error")),
                f"{role} missing payload has no readback error",
            )

    triplet = report.get("triplet_validation", {})
    inspected = identity.get("payload_inspection", {}).get("artifacts", {})
    check(
        set(inspected) == {"obs", "X", "var"}, "inspected triplet roles are incomplete"
    )
    for role, artifact in inspected.items():
        candidates = artifact.get("latest_exact_key_candidates", [])
        inventory_matches = [
            item
            for item in related
            if item.get("is_latest") is True and item.get("key") == artifact.get("key")
        ]
        check(
            len(candidates) == 1
            and candidates[0].get("key") == artifact.get("key")
            and candidates[0].get("uid") == artifact.get("uid"),
            f"inspected {role} is not the unique current artifact for its exact key",
        )
        check(
            len(inventory_matches) == 1
            and inventory_matches[0].get("uid") == artifact.get("uid")
            and inventory_matches == candidates,
            f"inspected {role} exact-key candidates disagree with complete related inventory",
        )
    for invariant in (
        "obs_rows_equal_X_rows",
        "var_rows_equal_X_columns",
        "obs_index_equal_X_obs_index",
        "var_index_equal_X_var_index",
        "obs_X_link_is_exact",
        "X_var_link_is_exact",
        "same_prefix_var",
    ):
        check(
            triplet.get(invariant) is True,
            f"failed inspected-payload invariant {invariant}",
        )
    if set(inspected) == {"obs", "X", "var"}:
        obs_link = triplet.get("obs_X_link", {})
        var_link = triplet.get("X_var_link", {})
        check(
            obs_link.get("resolved") is True
            and obs_link.get("latest_exact_key_candidate_count") == 1
            and obs_link.get("key") == inspected["X"].get("key")
            and obs_link.get("uid") == inspected["X"].get("uid"),
            "OBS->X link is not bound to the exact unique current X artifact",
        )
        check(
            var_link.get("resolved") is True
            and var_link.get("latest_exact_key_candidate_count") == 1
            and var_link.get("key") == inspected["var"].get("key")
            and var_link.get("uid") == inspected["var"].get("uid"),
            "X->VAR link is not bound to the exact unique current VAR artifact",
        )
        same_prefix_candidates = triplet.get("same_prefix_var_latest_candidates", [])
        check(
            len(same_prefix_candidates) == 1
            and same_prefix_candidates[0].get("key") == inspected["var"].get("key")
            and same_prefix_candidates[0].get("uid") == inspected["var"].get("uid"),
            "same-prefix VAR is not the one exact unique current linked artifact",
        )

    identifier = report.get("current_var_inventory", {}).get("identifier_audit", {})
    check(identifier.get("unique") is True, "VAR identifiers are not unique")
    check(identifier.get("duplicate_count") == 0, "VAR duplicate count is nonzero")
    obs_inventory = report.get("current_obs_inventory", {})
    var_inventory = report.get("current_var_inventory", {})
    x_inventory = report.get("current_X", {})
    obs_shape = obs_inventory.get("shape", [])
    var_shape = var_inventory.get("shape", [])
    x_shape = x_inventory.get("shape", [])
    check(
        len(obs_shape) == 2 and len(var_shape) == 2 and len(x_shape) == 2,
        "OBS/VAR/X shapes are malformed",
    )
    if len(obs_shape) == 2 and len(var_shape) == 2 and len(x_shape) == 2:
        check(obs_shape[0] == x_shape[0], "OBS/X row counts disagree")
        check(var_shape[0] == x_shape[1], "VAR/X feature counts disagree")
        check(
            identifier.get("rows") == var_shape[0],
            "VAR identifier audit row count disagrees",
        )
        check(
            obs_inventory.get("index_summary", {}).get("rows") == obs_shape[0],
            "OBS index summary row count disagrees",
        )
        check(
            var_inventory.get("index_summary", {}).get("rows") == var_shape[0],
            "VAR index summary row count disagrees",
        )
    for label, inventory in (("OBS", obs_inventory), ("VAR", var_inventory)):
        digest = inventory.get("index_sha256_ordered")
        check(
            bool(SHA256.fullmatch(str(digest))),
            f"{label} ordered index SHA-256 is invalid",
        )
    rows = identifier.get("rows")
    for pattern_name, pattern in identifier.get("patterns", {}).items():
        count = pattern.get("count")
        check(
            isinstance(count, int) and isinstance(rows, int) and 0 <= count <= rows,
            f"VAR pattern count is inconsistent for {pattern_name}",
        )

    temporal = report.get("temporal_verdict", {})
    check(bool(temporal.get("verdict")), "missing temporal verdict")
    check(bool(temporal.get("evidence")), "missing temporal evidence")
    check(bool(report.get("chunk_verdict")), "missing chunk verdict")
    check(
        bool(report.get("var_plan", {}).get("steps")),
        "missing VAR remediation/retention steps",
    )
    check(bool(report.get("remediation_steps")), "missing executable remediation steps")
    check(bool(report.get("validators")), "missing validators")
    check(bool(report.get("residual_risks")), "missing residual risks")

    if dataset == "E-MTAB-9304":
        check(
            all(
                not artifact["payload_evidence"].get("path_exists")
                for artifact in canonical.values()
            ),
            "known canonical missing-payload defect is no longer represented",
        )
        check(
            identity.get("payload_inspection", {}).get("relation_to_canonical")
            == "fallback_noncanonical_partial_payload",
            "legacy fallback scope is not explicit",
        )
        patterns = identifier.get("patterns", {})
        check(
            patterns.get("actual_tab", {}).get("count") == 16936,
            "FlyBase TAB defect count changed",
        )
        check(
            patterns.get("literal_backslash_t", {}).get("count") == 0,
            "misclassified TAB as backslash-t",
        )
        check(
            patterns.get("literal_slash_t", {}).get("count") == 0,
            "misclassified TAB as /t",
        )
        check(
            report.get("temporal_verdict", {}).get("verdict")
            == "non_temporal_single_stage",
            "wrong temporal verdict",
        )
    else:
        check(
            all(
                artifact["payload_evidence"].get("path_exists")
                for artifact in canonical.values()
            ),
            "canonical payload unavailable",
        )
        patterns = identifier.get("patterns", {})
        for bad in (
            "actual_tab",
            "literal_backslash_t",
            "literal_slash_t",
            "carriage_return_or_newline",
            "leading_or_trailing_whitespace",
        ):
            check(
                patterns.get(bad, {}).get("count") == 0, f"unexpected VAR defect {bad}"
            )
        check(
            identifier.get("patterns", {}).get("ensembl_gene", {}).get("count")
            == 33694,
            "not all VAR IDs are ENSG",
        )

    return errors


def validate_packet(report_path: Path) -> list[str]:
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"{report_path.name}: cannot load report JSON: {exc}"]
    errors = validate_report(report, report_path)
    markdown_path = report_path.with_suffix("").with_suffix(".report.md")
    try:
        actual_markdown = markdown_path.read_text(encoding="utf-8")
        _, render_markdown = _builder_functions()
        expected_markdown = render_markdown(report)
        if actual_markdown != expected_markdown:
            errors.append(
                f"{report.get('dataset', report_path.stem)}: Markdown diverges from report JSON"
            )
    except OSError as exc:
        errors.append(f"{report_path.name}: cannot load Markdown partner: {exc}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_DIR)
    args = parser.parse_args()
    paths = sorted(args.report_dir.glob("*.report.json"))
    datasets = {path.name.removesuffix(".report.json") for path in paths}
    errors = []
    if datasets != EXPECTED:
        errors.append(
            f"report set mismatch: expected={sorted(EXPECTED)} actual={sorted(datasets)}"
        )
    for path in paths:
        errors.extend(validate_packet(path))
    if errors:
        raise SystemExit("\n".join(errors))
    print(f"PASS: validated {len(paths)} cohort-A report packets")


if __name__ == "__main__":
    main()
