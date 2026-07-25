#!/usr/bin/env python3
"""Export a small, read-only snapshot of the 70 dataset owner cards.

This script never writes to LaminDB or the Kanban database. It produces a CSV
that lets the tutorial notebook work without Hermes or cloud credentials.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path

import pandas as pd

TITLE_RE = re.compile(r"^curate OBS\+VAR \[(\d{2})/70\]: (.+)$")


def _normalise(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _phase3_index(repo: Path) -> list[dict]:
    path = repo / "artifacts/phase3_ingestion_progress.json"
    if not path.exists():
        return []
    return json.loads(path.read_text()).get("ingested", [])


def _phase3_matches(dataset_id: str, rows: list[dict]) -> list[str]:
    """Return conservative exact-token matches, never fuzzy guesses."""
    candidates = {_normalise(dataset_id), _normalise(dataset_id.split("/")[-1])}
    matches: list[str] = []
    for row in rows:
        values = {str(row.get("dataset", "")), str(row.get("prefix", ""))}
        tokens = {_normalise(v) for v in values if v}
        if candidates & tokens:
            matches.append(str(row.get("prefix") or row.get("dataset")))
    return sorted(set(matches))


def _evidence_counts(repo: Path, dataset_id: str) -> tuple[int, int]:
    token = _normalise(dataset_id.split("/")[-1])
    manifests = 0
    notebooks = 0
    for path in (repo / "artifacts/schema_audit").glob(
        "real_dataset_curation_*/**/source_manifest.json"
    ):
        if token and token in _normalise(str(path)):
            manifests += 1
    for path in (repo / "notebooks/datasets").glob("*.ipynb"):
        if token and token in _normalise(path.name):
            notebooks += 1
    return manifests, notebooks


def export_snapshot(board_db: Path, repo: Path, output: Path) -> pd.DataFrame:
    connection = sqlite3.connect(f"file:{board_db}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    cards = connection.execute(
        """
        SELECT id, title, status, assignee, completed_at
        FROM tasks
        WHERE title LIKE 'curate OBS+VAR [%/70]: %'
        ORDER BY title
        """
    ).fetchall()
    phase3 = _phase3_index(repo)
    records: list[dict] = []
    for card in cards:
        match = TITLE_RE.match(card["title"])
        if not match:
            continue
        position, dataset_id = int(match.group(1)), match.group(2)
        manifests, notebooks = _evidence_counts(repo, dataset_id)
        phase3_matches = _phase3_matches(dataset_id, phase3)
        records.append(
            {
                "position": position,
                "real_dataset_id": dataset_id,
                "owner_task_id": card["id"],
                "workflow_status": card["status"],
                "assignee": card["assignee"],
                "owner_card_done": card["status"] == "done",
                "ingestion_record_present": bool(phase3_matches),
                "ingestion_prefixes": "; ".join(phase3_matches),
                "source_manifest_count": manifests,
                "processing_notebook_count": notebooks,
                "completed_at_unix": card["completed_at"],
            }
        )
    frame = pd.DataFrame(records).sort_values("position").reset_index(drop=True)
    if len(frame) != 70 or frame["real_dataset_id"].nunique() != 70:
        raise ValueError(
            f"Expected 70 unique owner cards, got {len(frame)} rows and "
            f"{frame['real_dataset_id'].nunique()} unique IDs"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return frame


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--board-db", type=Path, required=True)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/dataset_curation_progress.csv"),
    )
    args = parser.parse_args()
    frame = export_snapshot(args.board_db, args.repo, args.output)
    print(f"wrote {len(frame)} datasets to {args.output}")


if __name__ == "__main__":
    main()
