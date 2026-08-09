from __future__ import annotations

import csv
import importlib.util
import re
import subprocess
from pathlib import Path
from typing import Any

import nbformat
import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools/build_dataset_storage_inventory_csv.py"
CSV = ROOT / "data/pert_gym_dataset_storage_inventory.csv"
NOTEBOOK = ROOT / "notebooks/explore_dataset_storage.ipynb"


def load_module():
    spec = importlib.util.spec_from_file_location("dataset_storage_inventory", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_rows() -> list[dict[str, str]]:
    with CSV.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_inventory_includes_lamin_only_and_fail_closed_completion() -> None:
    module = load_module()
    rows = read_rows()
    rebuilt = module.build_rows(rows)

    assert len(rebuilt) == 404
    lamin_only = [
        row
        for row in rebuilt
        if module._truth(row["in_lamindb"])
        and not module._truth(row["in_raw"])
        and not module._truth(row["in_cleaned"])
    ]
    assert len(lamin_only) == 172
    assert "DRUG-seq" in {row["dataset_name"] for row in lamin_only}
    assert "excluded/vars_helpers" not in {row["dataset_name"] for row in rebuilt}
    assert all(row["lamin_inventory_evidence"] for row in lamin_only)
    assert {row["lamin_branch_scope"] for row in lamin_only} == {
        "jkobject_only",
        "main_and_jkobject_with_jkobject_revision",
    }
    assert all(
        row["lamin_catalog_status"]
        == "working_or_historical_not_in_canonical_cleaned_layout"
        for row in lamin_only
    )
    assert all(not module._truth(row["in_canonical_lamindb"]) for row in lamin_only)
    assert sum(row["lamin_branch_scope"] == "jkobject_only" for row in lamin_only) == 62
    assert (
        sum(
            row["lamin_branch_scope"] == "main_and_jkobject_with_jkobject_revision"
            for row in lamin_only
        )
        == 110
    )

    complete = [row for row in rebuilt if module._truth(row["completely_done"])]
    assert len(complete) == 10
    assert {row["lamin_dataset_id"] for row in complete} == {
        "DRUG-seq",
        "SchiebingerLander2019",
        "depmap_ccle26q1",
        "prism_collection/GSE132080",
        "prism_collection/GSE197452_Perturb-seq",
        "scperturb/adamson16_GSM2406675_10X001",
        "scperturb/adamson16_GSM2406677_10X005",
        "scperturb/adamson16_GSM2406681_10X010",
        "scperturb/chang22",
        "scperturb/datlinger17",
    }
    assert all(module._truth(row["obs_done"]) for row in complete)
    assert all(module._truth(row["var_done"]) for row in complete)
    assert all(module._truth(row["structurally_done"]) for row in complete)


def test_user_notebook_exposes_complete_and_branch_scoped_catalog_views() -> None:
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    cells = {cell.get("id"): cell for cell in notebook.cells}
    assert "review-inventory-status-explain" in cells
    source = cells["review-inventory-status-views"].source
    assert "complete_datasets" in source
    assert "catalog_without_canonical_cleaned" in source
    assert "jkobject_only_catalog" in source
    assert "both_branches_jkobject_revision" in source
    assert "lamin_only_datasets" not in source
    assert "completely_done == True" in source

    for cell_id in ("94a5cecf", "b1ac96df"):
        assert cells[cell_id].cell_type == "markdown"


def test_notebook_cleaned_loader_paths_are_bounded_without_network() -> None:
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    cells = {cell.get("id"): cell for cell in notebook.cells}
    namespace: dict[str, Any] = {"re": re}
    exec(cells["cleaned-loader"].source, namespace)

    assert namespace["cleaned_dataset_paths"]("SCP1467") == {
        "h5ad": "gs://scperturb/data/cleaned/SCP1467/X.h5ad",
        "obs": "gs://scperturb/data/cleaned/SCP1467/obs.parquet",
        "var": "gs://scperturb/data/cleaned/SCP1467/var.parquet",
    }
    with pytest.raises(ValueError, match="one safe canonical path segment"):
        namespace["cleaned_dataset_paths"]("../SCP1467")


def test_committed_storage_csv_is_deterministic(tmp_path: Path) -> None:
    rebuilt = tmp_path / CSV.name
    command = [
        "uv",
        "run",
        "--no-sync",
        "python",
        str(SCRIPT),
        "--base",
        str(CSV),
        "--output",
        str(rebuilt),
    ]
    subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    first = rebuilt.read_bytes()
    subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    assert rebuilt.read_bytes() == first
    assert b"\r\n" not in first
