#!/usr/bin/env python3
"""Build the explanatory dataset-curation progress notebook deterministically."""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

OUT = Path("notebooks/explore_dataset_curation_progress.ipynb")


def md(cell_id: str, text: str):
    cell = nbf.v4.new_markdown_cell(text.strip() + "\n")
    cell["id"] = cell_id
    return cell


def code(cell_id: str, text: str):
    cell = nbf.v4.new_code_cell(text.strip() + "\n")
    cell["id"] = cell_id
    return cell


def build_notebook():
    cells = [
        md(
            "title",
            """
# Explore pert-gym dataset curation progress

This notebook is a **read-only, explainable dashboard** for the 70 biological datasets in the pert-gym curation programme. It answers four practical questions:

1. Which datasets have evidence that they were ingested or added to the Lamin workflow?
2. Which owner cards are complete, active, queued, or blocked?
3. Which datasets already have local source manifests or reconstruction notebooks?
4. Where should I look next for a particular dataset?

It deliberately does **not** turn weak evidence into a false “accepted” label. Strict acceptance requires an independent reviewer and immutable live readback.
""",
        ),
        md(
            "audience",
            """
## Audience, prerequisites, and safety

This is for project contributors who want to understand the data work without learning the internal ingestion machinery first.

- The default path is completely offline and reads a small versioned CSV.
- No cell writes to LaminDB, GCS, or the Kanban board.
- The optional Lamin section is disabled by default and performs bounded metadata reads only.
- Run the notebook from the repository root with the project environment (`uv run jupyter lab`).
""",
        ),
        md(
            "goals",
            """
## Learning goals

By the end you should be able to:

- distinguish ingestion evidence, owner-card completion, and strict acceptance;
- filter the 70-dataset inventory without editing complex code;
- inspect one dataset and locate its evidence;
- understand what remains incomplete;
- refresh the local snapshot safely when Hermes is available.
""",
        ),
        md(
            "roadmap",
            """
## Roadmap

1. Load the offline snapshot.
2. Learn the status vocabulary.
3. Summarise and filter the 70 datasets.
4. Inspect a single dataset and its local evidence.
5. Optionally refresh from Kanban or inspect LaminDB read-only.
6. Export a small shortlist for follow-up.
""",
        ),
        md("chapter1", "# 1. Setup: small choices you can edit"),
        md(
            "root-explain",
            """
### Find the repository safely

The following cell supports running from either the repository root or the `notebooks/` directory. It does not contain a machine-specific absolute path.
""",
        ),
        code(
            "imports",
            """
from pathlib import Path
import json

import pandas as pd
from IPython.display import Markdown, display

ROOT = Path.cwd()
if not (ROOT / "artifacts").is_dir():
    ROOT = ROOT.parent
assert (ROOT / "artifacts").is_dir(), "Run from the pert-gym repo or notebooks/"
print("Repository:", ROOT)
""",
        ),
        md(
            "options-explain",
            """
### Editable options

Change only these values for most explorations. `DATASET_QUERY` accepts a substring such as `GSE132080`, `scperturb`, or `depmap`. `SHOW_ROWS` controls table size.
""",
        ),
        code(
            "options",
            """
DATASET_QUERY = ""       # Example: "GSE132080"
SHOW_ROWS = 12            # Number of rows shown in previews
RUN_LAMIN_LIVE = False    # Keep False unless you explicitly want a bounded live read
""",
        ),
        md("chapter2", "# 2. What each status means"),
        md(
            "vocabulary",
            """
## Three notions that must stay separate

| Notion | What it proves | What it does **not** prove |
|---|---|---|
| `ingestion_record_present` | A conservative exact match exists in the versioned phase-3 ingestion journal | Final Collection membership or reviewer acceptance |
| `owner_card_done` | The dataset's owner card reached `done` | That every historical project counter advanced |
| strict accepted counters | Independently accepted aggregate OBS/VAR outcomes recorded in `current-status.md` | A row-level identity list unless the ledger supplies one |

This separation is intentional. A dataset can be present in Lamin yet still need OBS completion, Ensembl VAR remediation, Collection checks, or independent review.
""",
        ),
        md(
            "triplet",
            """
## The core Lamin data model

Expression datasets are linked as:

```text
OBS table  →  expression matrix X  →  VAR table
```

A complete curation outcome checks row identity and metadata in OBS, feature identity and order in VAR, exact matrix dimensions/parity in X, and the Collection links that expose the triplet. Chunked datasets may share one dataset-level VAR when their axes are identical.
""",
        ),
        md("chapter3", "# 3. Load the offline 70-dataset snapshot"),
        md(
            "snapshot-explain",
            """
The committed CSV is a small snapshot exported read-only from the pert-gym Kanban board. It contains identifiers and evidence pointers, not matrices or private credentials.
""",
        ),
        code(
            "load-snapshot",
            """
SNAPSHOT_PATH = ROOT / "data/dataset_curation_progress.csv"
progress = pd.read_csv(SNAPSHOT_PATH)
assert len(progress) == 70
assert progress["real_dataset_id"].nunique() == 70
progress.head(3)
""",
        ),
        md(
            "columns-explain",
            """
### Column guide

- `real_dataset_id`: stable biological reporting identity.
- `owner_task_id`: sole durable owner card for that identity.
- `workflow_status`: live board status at snapshot time.
- `owner_card_done`: workflow completion marker, not a substitute for reviewer evidence.
- `ingestion_record_present`: exact match in the older versioned ingestion journal.
- `source_manifest_count`: local source/provenance manifests found.
- `processing_notebook_count`: local per-dataset reconstruction notebooks found.
""",
        ),
        code(
            "show-columns",
            """
pd.DataFrame({
    "column": progress.columns,
    "non_null": [int(progress[c].notna().sum()) for c in progress.columns],
    "example": [str(progress[c].dropna().iloc[0]) if progress[c].notna().any() else "—" for c in progress.columns],
})
""",
        ),
        md(
            "strict-counters",
            """
## Strict aggregate counters

The canonical accepted counters live in `docs/project/current-status.md`. They are kept separate from the row table because the status document does not always provide a complete machine-readable identity ledger.
""",
        ),
        code(
            "read-counters",
            """
status_text = (ROOT / "docs/project/current-status.md").read_text()
for line in status_text.splitlines():
    if "accepted OBS recovery" in line or "VAR dataset remediations" in line:
        print(line)
""",
        ),
        md("chapter4", "# 4. Overview: what is done, active, and incomplete?"),
        md(
            "workflow-summary-explain",
            """
The workflow status summary shows queue state, not scientific truth. It is nevertheless useful for finding work that is active or has not reached the owner-card terminal state.
""",
        ),
        code(
            "workflow-summary",
            """
workflow_summary = (
    progress.groupby("workflow_status", dropna=False)
    .size()
    .rename("datasets")
    .sort_values(ascending=False)
    .to_frame()
)
workflow_summary
""",
        ),
        md(
            "evidence-summary-explain",
            """
The next table combines only explicit booleans and file counts. Notice that these columns answer different questions and should not be added into a single score.
""",
        ),
        code(
            "evidence-summary",
            """
pd.DataFrame({
    "measure": [
        "owner card done",
        "ingestion record present",
        "at least one source manifest",
        "at least one processing notebook",
    ],
    "datasets": [
        int(progress["owner_card_done"].sum()),
        int(progress["ingestion_record_present"].sum()),
        int((progress["source_manifest_count"] > 0).sum()),
        int((progress["processing_notebook_count"] > 0).sum()),
    ],
})
""",
        ),
        md(
            "incomplete-explain",
            """
## Datasets not yet fully closed by their owner card

This is the safest broad “still incomplete” view available offline. A blocked card may simply be held for just-in-time release; inspect its context before interpreting the reason.
""",
        ),
        code(
            "incomplete-table",
            """
incomplete = progress.loc[~progress["owner_card_done"]].copy()
incomplete[[
    "position", "real_dataset_id", "workflow_status", "owner_task_id",
    "ingestion_record_present", "source_manifest_count", "processing_notebook_count",
]].head(SHOW_ROWS)
""",
        ),
        md(
            "added-explain",
            """
## Datasets with a conservative ingestion-journal match

These rows have evidence of an earlier ingestion event. They may still need OBS/VAR curation or final Collection/reviewer verification, so the table says “ingestion record present,” not “fully accepted.”
""",
        ),
        code(
            "ingested-table",
            """
ingested = progress.loc[progress["ingestion_record_present"]].copy()
ingested[[
    "position", "real_dataset_id", "workflow_status", "owner_card_done",
    "ingestion_prefixes",
]].head(SHOW_ROWS)
""",
        ),
        md(
            "gap-explain",
            """
## Useful gap view

This view finds datasets with an ingestion record but no completed owner card. It is often the most relevant answer to “present in Lamin, but not fully curated yet.”
""",
        ),
        code(
            "gap-table",
            """
present_but_open = progress.loc[
    progress["ingestion_record_present"] & ~progress["owner_card_done"]
].copy()
present_but_open[[
    "position", "real_dataset_id", "workflow_status", "owner_task_id",
    "source_manifest_count", "processing_notebook_count",
]].head(SHOW_ROWS)
""",
        ),
        md("chapter5", "# 5. Search and inspect one dataset"),
        md(
            "search-explain",
            """
Set `DATASET_QUERY` near the top, then rerun from this cell. An empty query shows the first rows; matching is case-insensitive and literal.
""",
        ),
        code(
            "search",
            """
query = DATASET_QUERY.strip()
selection = progress if not query else progress.loc[
    progress["real_dataset_id"].str.contains(query, case=False, regex=False)
]
selection.head(SHOW_ROWS)
""",
        ),
        md(
            "detail-explain",
            """
### A readable one-dataset summary

If multiple rows match, this uses the first. The code is intentionally explicit so you can easily add fields.
""",
        ),
        code(
            "detail",
            '''
if selection.empty:
    display(Markdown("**No dataset matched.** Change `DATASET_QUERY` and rerun."))
else:
    row = selection.iloc[0]
    summary = f"""
### `{row.real_dataset_id}`

- **Owner card:** `{row.owner_task_id}`
- **Workflow status:** `{row.workflow_status}`
- **Owner card done:** `{bool(row.owner_card_done)}`
- **Ingestion journal match:** `{bool(row.ingestion_record_present)}`
- **Matched prefix(es):** `{row.ingestion_prefixes if pd.notna(row.ingestion_prefixes) else 'none'}`
- **Source manifests:** `{int(row.source_manifest_count)}`
- **Processing notebooks:** `{int(row.processing_notebook_count)}`
"""
    display(Markdown(summary))
''',
        ),
        md(
            "evidence-paths-explain",
            """
## Locate local evidence without opening large payloads

This bounded search lists only manifest and notebook paths whose filename/path contains the selected dataset token. It never reads matrices.
""",
        ),
        code(
            "evidence-paths",
            """
if not selection.empty:
    token = selection.iloc[0].real_dataset_id.split("/")[-1].lower().replace("-", "_")
    candidate_paths = [
        *ROOT.glob("artifacts/schema_audit/real_dataset_curation_*/**/source_manifest.json"),
        *ROOT.glob("notebooks/datasets/*.ipynb"),
    ]
    matches = [p.relative_to(ROOT) for p in candidate_paths if token.replace("_", "") in str(p).lower().replace("_", "").replace("-", "")]
    display(pd.DataFrame({"local_evidence_path": [str(p) for p in matches]}))
""",
        ),
        md(
            "manifest-explain",
            """
### Preview a source manifest

When a matching manifest exists, show only its top-level identity and source fields. This keeps the exploration readable.
""",
        ),
        code(
            "manifest-preview",
            """
manifest_paths = [ROOT / p for p in matches if str(p).endswith("source_manifest.json")] if not selection.empty else []
if manifest_paths:
    manifest = json.loads(manifest_paths[0].read_text())
    preview_keys = ["real_dataset_id", "dataset_id", "task_id", "source_authority", "series_accession", "series_url", "publication", "writes"]
    display({key: manifest.get(key) for key in preview_keys if key in manifest})
else:
    print("No matching source manifest in this checkout.")
""",
        ),
        md("chapter6", "# 6. Optional refresh from the local Kanban board"),
        md(
            "refresh-explain",
            """
The committed snapshot is reproducible but becomes stale. On a Hermes machine, refresh it with the repository script:

```bash
uv run python tools/export_dataset_curation_progress.py \
  --board-db ~/.hermes/kanban/boards/pert-gym/kanban.db \
  --repo . \
  --output data/dataset_curation_progress.csv
```

The script opens SQLite with `mode=ro`; it cannot mutate the board. Review the CSV diff before committing it.
""",
        ),
        code(
            "snapshot-age",
            """
print("Snapshot file modified:", pd.Timestamp(SNAPSHOT_PATH.stat().st_mtime, unit="s", tz="UTC"))
print("Rows:", len(progress), "| unique identities:", progress.real_dataset_id.nunique())
""",
        ),
        md("chapter7", "# 7. Optional bounded LaminDB inspection"),
        md(
            "lamin-safety",
            """
## Before enabling live mode

Live Lamin access involves three separate things:

1. **Authentication:** your local Lamin/SSO credentials identify you.
2. **Authorization:** the instance and branch decide what you may read.
3. **Cloud billing:** only direct requester-pays GCS reads need a caller-owned billing project; this notebook performs metadata queries and does not download matrices.

Use the project helper, which pins `laminlabs/pertdata` on branch `jkobject`. Never paste credentials into this notebook. Keep `RUN_LAMIN_LIVE=False` for ordinary offline exploration.
""",
        ),
        md(
            "lamin-query-explain",
            """
The live cell below is intentionally bounded to one selected dataset token and at most 25 artifact rows. It performs no save, update, delete, Collection mutation, or bulk payload read.
""",
        ),
        code(
            "lamin-query",
            """
if RUN_LAMIN_LIVE:
    from tools.lamin_context import connect_pertdata

    if selection.empty:
        raise ValueError("Choose a DATASET_QUERY before enabling live mode")
    ln = connect_pertdata()
    token = selection.iloc[0].real_dataset_id.split("/")[-1]
    live_rows = list(
        ln.Artifact.filter(key__icontains=token)
        .values("uid", "key", "description")[:25]
    )
    display(pd.DataFrame(live_rows))
else:
    print("Live Lamin query disabled. Set RUN_LAMIN_LIVE=True to run one bounded read.")
""",
        ),
        md(
            "live-interpretation",
            """
### How to interpret a live match

An Artifact match proves that an object with a matching key exists on the pinned branch. It does not by itself prove:

- correct OBS→X→VAR links;
- exact source parity;
- canonical Collection membership;
- replay idempotence;
- independent reviewer acceptance.

Those claims require the corresponding immutable receipts and reviewer handoff.
""",
        ),
        md("chapter8", "# 8. Build a follow-up shortlist"),
        md(
            "shortlist-explain",
            """
This simple shortlist prioritises datasets that have an ingestion record but whose owner card is not done, then datasets with local source evidence. It is a convenience view, not an execution scheduler.
""",
        ),
        code(
            "shortlist",
            """
shortlist = progress.loc[~progress["owner_card_done"]].copy()
shortlist["has_local_source_evidence"] = shortlist["source_manifest_count"] > 0
shortlist = shortlist.sort_values(
    ["ingestion_record_present", "has_local_source_evidence", "position"],
    ascending=[False, False, True],
)
shortlist[[
    "position", "real_dataset_id", "workflow_status", "owner_task_id",
    "ingestion_record_present", "has_local_source_evidence",
]].head(SHOW_ROWS)
""",
        ),
        md(
            "export-explain",
            """
### Optional local export

Uncomment the final line to save the currently displayed shortlist. The export is local and contains no matrix data or credentials.
""",
        ),
        code(
            "export-shortlist",
            """
EXPORT_PATH = ROOT / "artifacts/exploration/my_dataset_shortlist.csv"
# shortlist.head(SHOW_ROWS).to_csv(EXPORT_PATH, index=False)
print("Uncomment the export line to write:", EXPORT_PATH.relative_to(ROOT))
""",
        ),
        md(
            "limitations",
            """
# 9. Limitations and troubleshooting

- **Snapshot is stale:** rerun the read-only export command in Chapter 6.
- **No ingestion match:** exact matching is conservative; absence means “not found in this journal,” not “absent from Lamin.”
- **Card is blocked:** inspect its Kanban context; a block can be a deliberate JIT hold.
- **Live query fails:** verify local Lamin authentication and branch access. Do not add tokens to the notebook.
- **Requester-pays error:** this notebook does not need raw GCS. For a separate bounded raw-data read, use a caller-owned billing project with billing enabled, required APIs, ADC, IAM access, and an explicit cost cap.
- **Strict acceptance identity is unclear:** use the immutable reviewer receipt rather than inferring acceptance from a PR, task status, or Artifact presence.
""",
        ),
        md(
            "recap",
            """
# 10. Recap

You now have three complementary views:

1. the 70-row owner-card inventory;
2. conservative ingestion and local-evidence indicators;
3. optional bounded live Lamin metadata for one selected dataset.

The notebook intentionally refuses to collapse these into a misleading completion score. For operational truth, pair this exploration with `TODO.md`, `docs/project/current-status.md`, and the selected dataset's reviewer receipt.
""",
        ),
    ]
    nb = nbf.v4.new_notebook(
        cells=cells,
        metadata={
            "kernelspec": {
                "display_name": "pert-gym (project uv)",
                "language": "python",
                "name": "pert-gym",
            },
            "language_info": {"name": "python", "version": "3.11"},
        },
    )
    return nb


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(build_notebook(), OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
