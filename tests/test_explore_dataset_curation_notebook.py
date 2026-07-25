from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

import nbformat
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "notebooks/explore_dataset_curation_progress.ipynb"
SNAPSHOT = ROOT / "data/dataset_curation_progress.csv"
EXPORTER = ROOT / "tools/export_dataset_curation_progress.py"


def load_exporter():
    spec = importlib.util.spec_from_file_location("dataset_progress_exporter", EXPORTER)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_notebook_is_substantial_deterministic_and_output_free():
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    assert len(notebook.cells) >= 30
    assert sum(cell.cell_type == "markdown" for cell in notebook.cells) >= 20
    assert sum(cell.cell_type == "code" for cell in notebook.cells) >= 10
    ids = [cell.id for cell in notebook.cells]
    assert len(ids) == len(set(ids))
    assert all(
        cell.get("execution_count") is None
        for cell in notebook.cells
        if cell.cell_type == "code"
    )
    assert all(
        not cell.get("outputs") for cell in notebook.cells if cell.cell_type == "code"
    )
    headings = "\n".join(
        cell.source for cell in notebook.cells if cell.cell_type == "markdown"
    )
    assert "# 1." in headings and "# 10." in headings
    assert "ingestion_record_present" in headings
    assert "strict acceptance" in headings.lower()


def test_notebook_contains_no_credentials_absolute_paths_or_writes():
    text = NOTEBOOK.read_text()
    forbidden = [
        "/Users/",
        "jkobject-1549353370965",
        "ln.save(",
        ".save()",
        ".delete(",
        "gsutil rm",
        "gcloud storage rm",
    ]
    assert not any(token in text for token in forbidden)
    assert "RUN_LAMIN_LIVE = False" in text
    assert "[:25]" in text


def test_snapshot_has_exactly_70_unique_owner_rows():
    frame = pd.read_csv(SNAPSHOT)
    assert len(frame) == 70
    assert frame["real_dataset_id"].nunique() == 70
    assert frame["owner_task_id"].nunique() == 70
    assert frame["position"].tolist() == list(range(1, 71))


def test_exporter_reads_board_and_emits_complete_inventory(tmp_path):
    board = tmp_path / "kanban.db"
    connection = sqlite3.connect(board)
    connection.execute(
        "CREATE TABLE tasks (id TEXT, title TEXT, status TEXT, assignee TEXT, completed_at INTEGER)"
    )
    for position in range(1, 71):
        connection.execute(
            "INSERT INTO tasks VALUES (?, ?, ?, ?, ?)",
            (
                f"t_{position:08d}",
                f"curate OBS+VAR [{position:02d}/70]: dataset/{position:02d}",
                "done" if position == 1 else "blocked",
                "dev",
                123 if position == 1 else None,
            ),
        )
    connection.commit()
    connection.close()

    repo = tmp_path / "repo"
    (repo / "artifacts").mkdir(parents=True)
    output = tmp_path / "snapshot.csv"
    exporter = load_exporter()
    frame = exporter.export_snapshot(board, repo, output)

    assert output.exists()
    assert len(frame) == 70
    assert frame["real_dataset_id"].nunique() == 70
    assert bool(frame.iloc[0]["owner_card_done"])
    assert not bool(frame.iloc[1]["owner_card_done"])


def test_generator_matches_committed_notebook(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "dataset_progress_notebook_builder",
        ROOT / "tools/build_explore_dataset_curation_notebook.py",
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    generated = module.build_notebook()
    committed = nbformat.read(NOTEBOOK, as_version=4)
    assert json.loads(nbformat.writes(generated)) == json.loads(
        nbformat.writes(committed)
    )
