# Notebooks

Store exploratory and reproducible analysis notebooks here.

## Dataset storage explorer

- `explore_dataset_storage.ipynb` explores the **actual data objects** across
  local downloads/working directories, raw GCS staging, processed/logical GCS
  staging, and live LaminDB Collections/Artifacts. It shows real paths, URIs,
  UIDs, sizes, observation counts, feature links, and bounded payload previews.
- The notebook is read-only and navigates GCS one level at a time; it never
  recursively walks sparse-Zarr object trees or materializes a large remote
  matrix on the Mac.
- Regenerate it deterministically with
  `tools/build_explore_dataset_storage_notebook.py`.

## Current canonical Lamin exploration

- `explore_unified_pertdata_collection.ipynb` is the read-only, safe exploration notebook for `laminlabs/pertdata` branch `jkobject` and `pert-gym/canonical/20260621`. It keeps large matrices metadata-only and loads `X` only for the tiny reviewed `model-ready` member.
- `explore_unified_pertdata_collection.py` is a percent-cell script mirror for text review and lightweight execution.
