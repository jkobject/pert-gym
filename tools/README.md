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
