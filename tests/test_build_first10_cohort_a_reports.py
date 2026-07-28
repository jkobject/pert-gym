from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from tools.build_first10_cohort_a_reports import PLANS, build_report, render_markdown
from tools.validate_first10_cohort_a_reports import validate_report

REPORT_DIR = Path(__file__).parents[1] / "artifacts" / "first10_audit" / "cohort_a"


@pytest.mark.parametrize("dataset", sorted(PLANS))
def test_generated_report_covers_live_columns_and_validates(dataset: str) -> None:
    live_path = REPORT_DIR / f"{dataset}.audit.json"
    live = json.loads(live_path.read_text(encoding="utf-8"))
    report = build_report(dataset, live, live_path)

    assert set(report["current_obs_inventory"]["columns"]) == set(
        report["obs_column_decisions"]
    )
    assert validate_report(report, REPORT_DIR / f"{dataset}.report.json") == []
    assert "No LaminDB, GCS, or Collection mutation" in render_markdown(report)


def test_report_builder_fails_when_a_live_column_has_no_decision() -> None:
    dataset = "GSE130238"
    live_path = REPORT_DIR / f"{dataset}.audit.json"
    live = json.loads(live_path.read_text(encoding="utf-8"))
    live["obs"]["columns"]["unexpected_new_column"] = {}

    with pytest.raises(ValueError, match="OBS decisions mismatch"):
        build_report(dataset, live, live_path)


def test_emtab_report_distinguishes_actual_tab_from_text_lookalikes() -> None:
    report = json.loads(
        (REPORT_DIR / "E-MTAB-9304.report.json").read_text(encoding="utf-8")
    )
    patterns = report["current_var_inventory"]["identifier_audit"]["patterns"]

    assert patterns["actual_tab"]["count"] == 16936
    assert patterns["literal_backslash_t"]["count"] == 0
    assert patterns["literal_slash_t"]["count"] == 0
    assert report["temporal_verdict"]["verdict"] == "non_temporal_single_stage"


def test_validator_rejects_report_divergence_from_frozen_audit() -> None:
    path = REPORT_DIR / "GSE130238.report.json"
    report = json.loads(path.read_text(encoding="utf-8"))
    tampered = deepcopy(report)
    tampered["current_obs_inventory"]["columns"]["fabricated"] = {}
    tampered["obs_column_decisions"]["fabricated"] = {
        "action": "drop",
        "target": None,
        "reason": "fabricated matching decision",
    }

    errors = validate_report(tampered, path)

    assert any("diverges from the frozen audit" in error for error in errors)


def test_validator_rejects_contradictory_var_inventory() -> None:
    path = REPORT_DIR / "GSE138002.report.json"
    report = json.loads(path.read_text(encoding="utf-8"))
    tampered = deepcopy(report)
    tampered["current_var_inventory"]["identifier_audit"]["rows"] = 1

    errors = validate_report(tampered, path)

    assert any("VAR identifier audit row count disagrees" in error for error in errors)


@pytest.mark.parametrize("field", ["key", "uid"])
def test_validator_rejects_stale_resolved_link_identity(field: str) -> None:
    dataset = "GSE130238"
    live = json.loads(
        (REPORT_DIR / f"{dataset}.audit.json").read_text(encoding="utf-8")
    )
    live["triplet_validation"]["obs_X_link"][field] = f"stale-{field}"
    with TemporaryDirectory(dir=REPORT_DIR) as directory:
        live_path = Path(directory) / f"{dataset}.audit.json"
        live_path.write_text(json.dumps(live), encoding="utf-8")
        report = build_report(dataset, live, live_path)
        report_path = Path(directory) / f"{dataset}.report.json"

        errors = validate_report(report, report_path)

    assert any("OBS->X link is not bound" in error for error in errors)


def test_validator_rejects_duplicate_current_same_prefix_var() -> None:
    dataset = "GSE138002"
    live = json.loads(
        (REPORT_DIR / f"{dataset}.audit.json").read_text(encoding="utf-8")
    )
    candidates = live["triplet_validation"]["same_prefix_var_latest_candidates"]
    candidates.append({**candidates[0], "uid": "duplicate-var-uid"})
    with TemporaryDirectory(dir=REPORT_DIR) as directory:
        live_path = Path(directory) / f"{dataset}.audit.json"
        live_path.write_text(json.dumps(live), encoding="utf-8")
        report = build_report(dataset, live, live_path)
        report_path = Path(directory) / f"{dataset}.report.json"

        errors = validate_report(report, report_path)

    assert any("same-prefix VAR is not" in error for error in errors)


def test_validator_reconciles_candidates_with_complete_related_inventory() -> None:
    dataset = "GSE138002"
    live = json.loads(
        (REPORT_DIR / f"{dataset}.audit.json").read_text(encoding="utf-8")
    )
    duplicate = {
        **live["triplet_validation"]["same_prefix_var_latest_candidates"][0],
        "uid": "coordinated-duplicate-var-uid",
    }
    live["related_lamin_artifacts"].append(duplicate)
    with TemporaryDirectory(dir=REPORT_DIR) as directory:
        live_path = Path(directory) / f"{dataset}.audit.json"
        live_path.write_text(json.dumps(live), encoding="utf-8")
        report = build_report(dataset, live, live_path)
        report_path = Path(directory) / f"{dataset}.report.json"

        errors = validate_report(report, report_path)

    assert any("disagree with complete related inventory" in error for error in errors)
