# Tools

Utility scripts for data prep, automation, and maintenance tasks.

## Heavy EU VM launcher

Do not start `pert-gym-worker-eu` or dispatch a heavy payload with bare
`gcloud compute start/ssh`. Use the bounded control-plane launcher:

```bash
uv run python tools/launch_pert_gym_heavy.py \
  --task t_1234abcd \
  --eta-hours 6 \
  --command bash -lc \
  'cd ~/work/pert-gym && uv run python tools/pert_gym_vm_runner.py --run-id t_1234abcd --allow-lamin-writes --command python tools/heavy_job.py'
```

The lease duration is `max(ETA + 2h, 8h)` and may not exceed the 14-hour
policy ceiling. The launcher atomically adds exact `owner`, `project`,
`purpose`, `task`, and `lease-until` labels, writes the local compute-guard
lease by atomic replacement, and performs two exact GCE readbacks before
launch. A successful terminal payload stops the task-owned VM. A failed
payload leaves the VM and the original bounded lease intact for inspection;
the launcher never shortens a live lease.

Production writers that require a shorter hard lifecycle use both minute
options together and an exact task-specific purpose:

```bash
uv run python tools/launch_pert_gym_heavy.py \
  --task t_79ff033e \
  --eta-hours 2 \
  --purpose gse132080-obs-var-curation \
  --lease-minutes 150 \
  --absolute-max-minutes 180 \
  --command bash -lc \
  'cd ~/work/pert-gym && uv run python tools/pert_gym_vm_runner.py --run-id t_79ff033e --allow-lamin-writes --command python tools/curate_gse132080.py'
```

Minute-bounded writers supervise one exact remote payload PID, renew only while
that PID is live, terminate at the absolute ceiling, and stop the VM and clear
the task lease on every terminal path. Both minute options are required, the
lease cannot exceed the absolute ceiling, and neither value may exceed six
hours. This is a production-writer lifecycle; do not pass `--verify-only` to a
command that can write.
