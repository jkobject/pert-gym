# Notebooks

Store exploratory and reproducible analysis notebooks here.

## Dataset-curation progress

- `explore_dataset_curation_progress.ipynb` is the explainable, offline-first
  dashboard for the 70 biological dataset owner cards. It separates ingestion
  evidence, workflow completion, strict accepted counters, source manifests, and
  optional bounded Lamin metadata instead of collapsing them into one score.
- Refresh its versioned snapshot with
  `tools/export_dataset_curation_progress.py`; the exporter opens the Kanban
  database read-only. Regenerate the notebook deterministically with
  `tools/build_explore_dataset_curation_notebook.py`.

## Current canonical Lamin exploration

- `explore_unified_pertdata_collection.ipynb` is the read-only, safe exploration notebook for `laminlabs/pertdata` branch `jkobject` and `pert-gym/canonical/20260621`. It keeps large matrices metadata-only and loads `X` only for the tiny reviewed `model-ready` member.
- `explore_unified_pertdata_collection.py` is a percent-cell script mirror for text review and lightweight execution.
