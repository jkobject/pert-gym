# Egress-cost-aware agent behavior

Agent-operational rules for GCS/Lamin placement are maintained in:

- [`project/agent-runbook.md`](project/agent-runbook.md), section **GCS / cache / VM placement rules**
- root [`AGENTS.md`](https://github.com/jkobject/pert-gym/blob/main/AGENTS.md), short boot summary

Do not treat this rendered docs page as the source of truth for agent execution policy.

Current short version:

- No big GCS/Lamin jobs on the Mac mini.
- `gs://scperturb` is in `EUROPE-WEST1` and Requester Pays is enabled.
- Large validators/staging/scans must run on `pert-gym-worker-eu` in `europe-west1-b` with billing project `jkobject-1549353370965`.
- `tools/sync_to_vm_cache.sh` is the helper for remote bucket-local copies.
