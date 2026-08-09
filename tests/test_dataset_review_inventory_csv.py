from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path

from tools.build_dataset_review_inventory_csv import build_rows, summary

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data/pert_gym_dataset_review_inventory.csv"


def _truth(value: str) -> bool:
    return value.lower() == "true"


def test_dataset_review_inventory_has_unique_dataset_units_and_strict_counts() -> None:
    rows = build_rows()
    counts = summary(rows)

    assert len(rows) == 92
    assert len({row["dataset_id"] for row in rows}) == 92
    assert counts == {
        "unique_datasets": 92,
        "main_baseline_datasets": 26,
        "entirely_validated": 8,
        "entirely_validated_main_existing": 5,
        "entirely_validated_jkobject_additions": 3,
        "new_families_registered_and_in_collection": 10,
        "new_families_entirely_validated": 0,
    }

    full = {row["dataset_id"] for row in rows if row["entirely_validated"]}
    assert full == {
        "SchiebingerLander2019",
        "depmap_ccle/26q1",
        "drug-seq/GSE120222",
        "geo/GSE132080",
        "geo/GSE197452",
        "scperturb/adamson16",
        "scperturb/chang22",
        "scperturb/datlinger17",
    }

    additions = {
        row["dataset_id"] for row in rows if row["entirely_validated_jkobject_addition"]
    }
    assert additions == {"depmap_ccle/26q1", "geo/GSE132080", "geo/GSE197452"}


def test_every_incomplete_dataset_names_missing_requirements() -> None:
    rows = build_rows()
    for row in rows:
        missing = (
            row["missing_requirements"].split(";")
            if row["missing_requirements"]
            else []
        )
        if row["entirely_validated"]:
            assert missing == []
            assert row["next_review_focus"] == "complete"
        else:
            assert missing
            assert row["next_review_focus"] in missing

    registered_new = [
        row
        for row in rows
        if row["review_scope"] == "genuinely_new_family_22" and row["lamin_registered"]
    ]
    assert len(registered_new) == 10
    assert all(not row["strict_obs_validated"] for row in registered_new)
    assert all(not row["strict_var_validated"] for row in registered_new)
    assert all(not row["entirely_validated"] for row in registered_new)


def test_committed_csv_is_deterministic(tmp_path: Path) -> None:
    rebuilt = tmp_path / OUTPUT.name
    subprocess.run(
        [
            "uv",
            "run",
            "--no-sync",
            "python",
            "tools/build_dataset_review_inventory_csv.py",
            "--output",
            str(rebuilt),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    first = rebuilt.read_bytes()
    subprocess.run(
        [
            "uv",
            "run",
            "--no-sync",
            "python",
            "tools/build_dataset_review_inventory_csv.py",
            "--output",
            str(rebuilt),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert rebuilt.read_bytes() == first
    assert b"\r\n" not in first

    with OUTPUT.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 92
    assert (
        sum(_truth(row["entirely_validated_main_existing_dataset"]) for row in rows)
        == 5
    )
    assert sum(_truth(row["entirely_validated_jkobject_addition"]) for row in rows) == 3


def test_notebook_exposes_dataset_level_summary_and_missing_requirements() -> None:
    notebook = json.loads(
        (ROOT / "notebooks/explore_dataset_storage.ipynb").read_text()
    )
    cells = {
        cell.get("id"): "".join(cell.get("source", [])) for cell in notebook["cells"]
    }
    assert (
        "one row per reviewed dataset identity" in cells["dataset-level-review-title"]
    )
    source = cells["dataset-level-review-load"]
    assert "pert_gym_dataset_review_inventory.csv" in source
    assert "entirely_validated_main_existing" in source
    assert "entirely_validated_jkobject_additions" in source
    assert "missing_requirements" in source
