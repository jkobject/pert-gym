"""Lightweight Lamin read-back for PRISM GSE269596 cell_line metadata.

Loads obs.parquet artifacts only and checks obs -> X links without loading X.h5ad.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT_STR = str(REPO_ROOT)
if sys.path[0] != REPO_ROOT_STR:
    sys.path.insert(0, REPO_ROOT_STR)

from tools.lamin_context import connect_pertdata  # noqa: E402

EXPECTED_CHUNKS = 75
EXPECTED_ROWS = 74312
EXPECTED_CELL_LINES = ["HEK293T", "K562"]


def main() -> None:
    ln = connect_pertdata()
    chunks_checked = 0
    rows_checked = 0
    cell_line_non_unknown = 0
    cell_line_unique: set[str] = set()
    x_links_checked = 0
    loaded_x = False

    for i in range(EXPECTED_CHUNKS):
        key = f"prism_collection/GSE269596/chunk_{i:04d}/obs.parquet"
        obs_artifact = ln.Artifact.get(key=key)
        if obs_artifact is None:
            raise SystemExit(f"missing obs artifact: {key}")
        links = obs_artifact.features.get_values()
        if "X" not in links:
            raise SystemExit(f"missing X feature link for {key}")
        obs = obs_artifact.load()
        if "cell_line" not in obs.columns:
            raise SystemExit(f"missing cell_line for {key}")
        non_unknown = obs["cell_line"].astype(str).ne("unknown") & obs[
            "cell_line"
        ].notna()
        chunks_checked += 1
        rows_checked += len(obs)
        cell_line_non_unknown += int(non_unknown.sum())
        cell_line_unique.update(
            map(str, obs.loc[non_unknown, "cell_line"].dropna().unique())
        )
        x_links_checked += 1

    summary = {
        "dataset": "GSE269596",
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "chunks_checked": chunks_checked,
        "rows_checked": rows_checked,
        "cell_line_non_unknown": cell_line_non_unknown,
        "cell_line_unique": sorted(cell_line_unique),
        "x_links_checked": x_links_checked,
        "loaded_x": loaded_x,
    }
    summary["ok"] = (
        chunks_checked == EXPECTED_CHUNKS
        and rows_checked == EXPECTED_ROWS
        and cell_line_non_unknown == EXPECTED_ROWS
        and sorted(cell_line_unique) == EXPECTED_CELL_LINES
        and not loaded_x
    )
    print(json.dumps(summary, indent=2))
    if not summary["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
