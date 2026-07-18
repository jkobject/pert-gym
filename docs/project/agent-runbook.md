
# pert-gym agent runbook

`AGENTS.md` is the single boot file. This page holds task routing and detailed operating rules for workers who need more than the short boot gist.

## Source hierarchy

If documents disagree, prefer:

1. `TODO.md` for current Kanban/project gist.
2. `docs/project/current-status.md` for latest counts/status snapshot.
3. `docs/pert_gym_schema.md` for binding schema and unified Collection contract.
4. This file for agent execution rules.
5. Files under `docs/archive/` only as historical evidence.

## Lamin safety

- Work on Lamin instance `laminlabs/pertdata`, branch `jkobject` only.
- Never write to Lamin `main` from agent work.
- Do not use the global `lamin` CLI; connect through `tools.lamin_context.connect_pertdata()`.
- Always call `ln.track()` before notebook/script artifact writes.
- Do local planning, duplicate checks, and dry-runs before Lamin writes.
- Never full-load huge matrices; use metadata-only queries, backed reads, chunking, GCS staging, or high-memory workers.
- Durable curated data belongs in LaminDB; pre-ingestion raw files should live in GCS staging, not Git.

Connection pattern:

```python
from tools.lamin_context import connect_pertdata
ln = connect_pertdata()
ln.track()
assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
assert ln.setup.settings.branch.name == "jkobject"
```

## GCS / cache / VM placement rules

Verified Mac mount: `/Users/jkobject/mnt/gcs/scperturb`, but this mount is for tiny diagnostics only. `gs://scperturb` is in `EUROPE-WEST1` and Requester Pays is enabled.

### Hard rule: no big data jobs on the Mac mini

Do **not** run large GCS/Lamin jobs on the Mac mini. This includes:

- full-collection validators;
- staging lanes;
- repeated bucket scans;
- jobs likely to move >500MB;
- jobs touching >100 GCS objects;
- repeated reads of GCS/Lamin-backed obs/X/var artifacts.

Those jobs must run on `pert-gym-worker-eu` in `europe-west1-b`, or the worker must be provisioned before dispatch. Do not silently fall back to Mac-local Lamin/GCS reads.

### EU worker contract

`pert-gym-worker-eu` should remain the default large-job worker for `scperturb`:

- zone: `europe-west1-b`, same region as `gs://scperturb`;
- repo checkout: `~/work/pert-gym`;
- Lamin credentials verified for `tools.lamin_context.connect_pertdata()` on `laminlabs/pertdata`, branch `jkobject`;
- GCS access via VM service account;
- Requester Pays billing project: `jkobject-1549353370965`.

Preferred command pattern:

```bash
gcloud compute ssh pert-gym-worker-eu --zone europe-west1-b --command \
  'cd ~/work/pert-gym && gcloud storage ls --billing-project=jkobject-1549353370965 gs://scperturb/pert-gym/staging/...'
```

Use `tools/sync_to_vm_cache.sh` for remote cache copies; it defaults to `pert-gym-worker-eu`, passes the billing project, and fails if the worker is missing rather than pushing work back to the Mac.

### Tiny Mac exception

For genuinely tiny bounded reads on the Mac, materialize explicit objects first:

```bash
./tools/gcs_cache.py gs://scperturb/path/to/object.ext
```

Then work only on `data/gcs_cache/`. This exception is for small diagnostics, not validators or staging lanes.

Avoid wide scans such as `find`, `ls -R`, `du`, or `rsync` on `/Users/jkobject/mnt/gcs/scperturb`; use targeted `gcloud storage ls --billing-project=jkobject-1549353370965 gs://...` instead.

## Triplet contract

Canonical expression datasets use same-prefix triplets:

```text
<dataset_prefix>/obs.parquet
<dataset_prefix>/X.h5ad
<dataset_prefix>/var.parquet
```

`obs -> X -> var` links are stored through Lamin feature links. Every canonical triplet must have same-prefix `var.parquet`. RNA/expression belongs in `X.h5ad`; extra matrices use `X_<name>.h5ad`/`var_<name>.parquet`; embeddings/obsm payloads use `obsm_<name>.parquet` or h5ad/zarr.

## Role/task routing

Every durable Kanban card must carry a bounded context packet: the biological
dataset/source, allowed writes, exact Collection and payload target, duplicate
checks, validation commands, relevant files/docs, expected evidence, and an
explicit list of repository areas the worker should not read. Missing context is
a blocker to name precisely, not a reason to browse the whole repository.

Implementation work uses isolated worktrees under
`/Users/jkobject/.openclaw/worktrees/pert-gym/<task-id>`. Treat the shared checkout
at `/Users/jkobject/.openclaw/workspace/work/pert-gym` as read-only except for
cache/materialized-data operations and explicit emergency maintenance.

### CTO / orchestrator

Read:

1. `AGENTS.md`
2. `TODO.md`
3. `docs/project/current-status.md` only when current counts/status matter
4. `docs/pert_gym_schema.md` only for schema/contract tasks

### Ingestion / Lamin worker

Read:

1. `AGENTS.md`
2. The card context packet
3. `docs/project/agent-runbook.md` safety sections
4. `data/README.md` only for the named source/dataset family
5. `docs/pert_gym_schema.md` when writing/querying triplets or Collections

### Model / benchmark worker

Read:

1. `AGENTS.md`
2. The card context packet
3. `docs/model_environments.md` for optional model envs
4. The specific baseline doc named in the card (`cpa`, `lpm`, `trvae`, etc.)

### Docs-only worker

Read:

1. `AGENTS.md`
2. The exact target docs
3. `docs/project/current-status.md` only if status/counts are being changed

Docs-only tasks must not alter code or data.

## Safe validation commands

```bash
uv run --extra dev python -m pytest tests/test_query_unified_collection.py
uv run python artifacts/schema_audit/validate_unified_collection_queries_20260621.py
uv run python artifacts/scripts/smoke_explore_unified_pertdata_collection.py
uv run python tools/audit_lamin_triplet_schema.py
uv run python tools/plan_phase3_ingestion.py
uv run --extra dev pytest tests/test_metrics.py tests/test_smoke.py tests/test_models_and_evaluate.py -q
uv run --extra classical --extra dev pytest tests/test_classical_baselines.py -q
```

Do not add heavy model dependencies to the base environment; use separate extras or model-specific environments.
