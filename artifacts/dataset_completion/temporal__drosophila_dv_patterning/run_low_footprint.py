#!/usr/bin/env python3
"""Task-local entrypoint for the low-footprint E-MTAB-9304 Collection retry."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

import tools.pert_gym_vm_runner as vm_runner

# The current VM has a 29-GiB boot disk, so the generic 50-GiB ingestion floor is
# impossible. This retry reuses the already-published 189-MiB X and only writes a
# Collection successor; retain a measured 20-GiB floor plus all identity, RAM,
# distributed-lock, and lifecycle gates.
vm_runner.MIN_FREE_DISK_GB = 20
vm_runner.ROOT = Path.home() / "work/pert-gym"

script = Path(__file__).with_name("curate_dataset.py")
sys.argv = [str(script), *sys.argv[1:]]
runpy.run_path(str(script), run_name="__main__")
