# Processing-decisions notebooks

`_template_processing_decisions.ipynb` is the metadata-first, executable
contract for one logical dataset. Copy it before starting a dataset notebook and
replace every `TODO` with evidence or an explicit pending decision. This template
is not proof of per-dataset coverage, nor does it certify a dataset or Collection
as model-ready by itself.

## Purpose: durable reconstruction after GCS staging removal

Project-owned GCS staging is temporary cache, not the authoritative
reconstruction layer. After migration, the dataset notebook must allow a future
operator to understand and reproduce processing from immutable upstream sources
or a retained Lamin raw artifact, without relying on a disappearing GCS object.
The contract records every temporary GCS input/output alongside its durable
replacement and the prerequisites for `safe_to_remove_gcs`.

A notebook must not claim reproducibility if its only remaining source is an
unretained GCS object. The helper validation rejects that state. A valid claim
requires at least one immutable upstream source or a retained Lamin raw artifact
with a key or UID.

## How to use the template

1. Copy the template to a dataset-specific filename; preserve code cells and
   replace only the `contract` metadata.
2. Record facts (observed sources, artifact keys/UIDs, validation readback) as
   facts, choices as decisions, and missing evidence as pending work. Do not
   paste stale output and present it as a live Lamin result.
3. Keep the three delta count vocabularies separate: `artifact_count` is the
   physical artifact delta, `logical_dataset_count` is the logical dataset
   delta, and `collection_count` is the Collection delta. List corresponding
   keys/UIDs/revisions rather than inferring one count from another.
4. Inventory all temporary GCS dependencies, including staging inputs and
   generated sidecars. For each, state its purpose, durable replacement, and
   prerequisites before it can be deleted.
5. Fill the reconstruction section with immutable source URLs/accessions and
   checksums, or a retained Lamin raw artifact. State the exact script, commit,
   card ID, lineage map, and branch policy needed to rerun processing.
6. Record validation/readback evidence with its exact denominator; record
   Collection/model-ready membership plus a query example. A planned query is
   not evidence of membership.

## Execution safety

The default template executes only local metadata validation; it performs no
GCS or Lamin I/O and never loads an `X` payload. Optional live Lamin inspection
is disabled by default. A dataset notebook may enable it only after a hard host
guard permits exactly `pert-gym-worker-eu`; on a Mac it must remain disabled.

Use the project environment for validation:

```bash
uv run --no-sync python - <<'PY'
from pathlib import Path
import nbformat
from nbclient import NotebookClient

path = Path("notebooks/datasets/_template_processing_decisions.ipynb")
notebook = nbformat.read(path, as_version=4)
nbformat.validate(notebook)
NotebookClient(notebook, timeout=60, kernel_name="python3").execute(cwd=path.parents[2])
PY
uv run --no-sync pytest -q tests/test_processing_decisions.py
```

Do not save executed output back into a template. The committed notebook has no
outputs so a reader cannot mistake old local state for current Lamin state.
