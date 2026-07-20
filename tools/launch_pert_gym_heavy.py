#!/usr/bin/env python3
"""Control-plane launcher for bounded heavy work on ``pert-gym-worker-eu``.

The launcher publishes a task-owned, time-bounded lease to both GCE labels and
the local compute guard before it starts the VM or dispatches a payload. It then
proves the exact labels by readback. A clean terminal payload owns the VM stop;
a failed payload leaves the VM and its original lease intact for inspection.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Sequence

INSTANCE = "pert-gym-worker-eu"
ZONE = "europe-west1-b"
GCE_PROJECT = "jkobject-1549353370965"
OWNER = "jkobject"
PROJECT_LABEL = "pert-gym"
PURPOSE = "pert-gym-longrun"
INSTANCE_ID = "3715582979673213789"
MIN_LEASE_HOURS = 8.0
LEASE_MARGIN_HOURS = 2.0
DEFAULT_MAX_LEASE_HOURS = 14.0
DEFAULT_LOCAL_LEASE_PATH = (
    Path.home()
    / ".hermes"
    / "state"
    / "gcp_compute_cost_guard.d"
    / "pert-gym-worker-eu.json"
)
_TASK_RE = re.compile(r"^t_[0-9a-f]{8}$")
_LABEL_TIME_RE = re.compile(r"^\d{8}t\d{6}z$")
Run = Callable[[list[str]], subprocess.CompletedProcess[str]]


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=600,
    )


def _run_payload(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        text=True,
    )


def task_label(task: str) -> str:
    """Return the exact reversible GCE-label representation of a Kanban task."""
    if not _TASK_RE.fullmatch(task):
        raise ValueError("task id must match t_<8 lowercase hexadecimal characters>")
    return task.replace("_", "-", 1)


def lease_deadline(
    now: datetime,
    *,
    eta_hours: float,
    max_hours: float = DEFAULT_MAX_LEASE_HOURS,
) -> datetime:
    """Compute ``max(ETA + 2h, 8h)`` without exceeding the policy ceiling."""
    current = now.astimezone(timezone.utc)
    if eta_hours <= 0:
        raise ValueError("ETA hours must be positive")
    duration = max(eta_hours + LEASE_MARGIN_HOURS, MIN_LEASE_HOURS)
    if duration > max_hours:
        raise ValueError(
            f"ETA plus safety margin ({duration:g}h) exceeds the "
            f"{max_hours:g} hour lease policy"
        )
    return current + timedelta(hours=duration)


def _parse_label_deadline(value: object) -> datetime | None:
    if not isinstance(value, str) or not _LABEL_TIME_RE.fullmatch(value.lower()):
        return None
    try:
        return datetime.strptime(value.lower(), "%Y%m%dt%H%M%Sz").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None


def _gcloud(*arguments: str) -> list[str]:
    return ["gcloud", "--project", GCE_PROJECT, "compute", *arguments]


def _checked(
    run: Run,
    command: list[str],
    *,
    operation: str,
) -> subprocess.CompletedProcess[str]:
    result = run(command)
    if result.returncode:
        output = ((result.stdout or "") + (result.stderr or ""))[-1500:]
        raise RuntimeError(f"{operation} failed with rc={result.returncode}: {output}")
    return result


def _describe(run: Run) -> dict[str, object]:
    result = _checked(
        run,
        _gcloud(
            "instances",
            "describe",
            INSTANCE,
            "--zone",
            ZONE,
            "--format=json(id,name,zone,status,labels)",
        ),
        operation="instance describe",
    )
    try:
        instance = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError("instance describe returned invalid JSON") from exc
    if not isinstance(instance, dict):
        raise RuntimeError("instance describe did not return an object")
    zone = str(instance.get("zone", "")).rsplit("/", maxsplit=1)[-1]
    identity = {
        "id": str(instance.get("id", "")),
        "name": instance.get("name"),
        "zone": zone,
    }
    expected = {"id": INSTANCE_ID, "name": INSTANCE, "zone": ZONE}
    if identity != expected:
        raise RuntimeError(
            f"refusing unexpected pert-gym instance identity: {identity!r}"
        )
    return instance


def _lease_labels(task: str, deadline: datetime) -> dict[str, str]:
    return {
        "owner": OWNER,
        "project": PROJECT_LABEL,
        "purpose": PURPOSE,
        "task": task_label(task),
        "lease-until": deadline.strftime("%Y%m%dt%H%M%Sz").lower(),
    }


def _verify_lease(instance: dict[str, object], expected: dict[str, str]) -> None:
    labels = instance.get("labels")
    if not isinstance(labels, dict):
        raise RuntimeError("lease label readback mismatch: labels are absent")
    mismatches = {
        key: {"expected": value, "actual": labels.get(key)}
        for key, value in expected.items()
        if labels.get(key) != value
    }
    if mismatches:
        raise RuntimeError(f"lease label readback mismatch: {mismatches!r}")


def _atomic_write_local_lease(
    path: Path,
    *,
    task: str,
    deadline: datetime,
) -> None:
    expires_at = (
        deadline.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    payload = {
        "allowed_instances": [INSTANCE],
        "expires_at": expires_at,
        "instance_expires_at": {INSTANCE: expires_at},
        "owner": OWNER,
        "project": PROJECT_LABEL,
        "purpose": PURPOSE,
        "task": task,
        "label_task": task_label(task),
        "reason": "bounded pert-gym heavy launcher lease; defense in depth with GCE label",
    }
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def _effective_deadline(
    instance: dict[str, object],
    *,
    task: str,
    requested: datetime,
    now: datetime,
) -> datetime:
    """Preserve a live same-task lease and reject a live foreign task lease."""
    labels = instance.get("labels")
    if not isinstance(labels, dict):
        return requested
    existing = _parse_label_deadline(labels.get("lease-until"))
    if existing is None or existing <= now:
        return requested
    if existing > now + timedelta(hours=DEFAULT_MAX_LEASE_HOURS):
        raise RuntimeError(
            f"existing live lease exceeds the {DEFAULT_MAX_LEASE_HOURS:g} hour "
            "lease policy; refusing to shorten or inherit it"
        )
    existing_task = labels.get("task")
    expected_task = task_label(task)
    if existing_task not in (None, expected_task):
        raise RuntimeError(
            f"instance has an active lease owned by another task: {existing_task!r}"
        )
    return max(existing, requested)


def launch_heavy_command(
    *,
    task: str,
    eta_hours: float,
    command: Sequence[str],
    local_lease_path: Path = DEFAULT_LOCAL_LEASE_PATH,
    now: datetime | None = None,
    run: Run = _run,
    payload_run: Run | None = None,
) -> int:
    """Lease, start if needed, dispatch one heavy command, and stop on success."""
    if not command:
        raise ValueError("heavy command must not be empty")
    task_label(task)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    requested = lease_deadline(current, eta_hours=eta_hours)

    before = _describe(run)
    if before.get("status") not in {"RUNNING", "TERMINATED"}:
        raise RuntimeError(f"refusing instance status {before.get('status')!r}")
    deadline = _effective_deadline(before, task=task, requested=requested, now=current)
    labels = _lease_labels(task, deadline)
    label_argument = ",".join(f"{key}={value}" for key, value in labels.items())
    _checked(
        run,
        _gcloud(
            "instances",
            "add-labels",
            INSTANCE,
            "--zone",
            ZONE,
            "--labels",
            label_argument,
            "--quiet",
        ),
        operation="GCE lease publication",
    )
    readback = _describe(run)
    _verify_lease(readback, labels)
    _atomic_write_local_lease(local_lease_path, task=task, deadline=deadline)

    # A second fresh read closes the lease-readback -> start/dispatch interval.
    immediately_before_launch = _describe(run)
    _verify_lease(immediately_before_launch, labels)
    status = immediately_before_launch.get("status")
    if status == "TERMINATED":
        _checked(
            run,
            _gcloud("instances", "start", INSTANCE, "--zone", ZONE, "--quiet"),
            operation="instance start",
        )
    elif status != "RUNNING":
        raise RuntimeError(f"refusing pre-launch instance status {status!r}")

    payload_transport = payload_run or (_run_payload if run is _run else run)
    payload = payload_transport(
        _gcloud(
            "ssh",
            INSTANCE,
            "--zone",
            ZONE,
            "--command",
            shlex.join(list(command)),
        )
    )
    if payload.returncode:
        return payload.returncode

    immediately_before_stop = _describe(run)
    _verify_lease(immediately_before_stop, labels)
    _checked(
        run,
        _gcloud("instances", "stop", INSTANCE, "--zone", ZONE, "--quiet"),
        operation="terminal instance stop",
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", required=True)
    parser.add_argument("--eta-hours", type=float, required=True)
    parser.add_argument("--command", nargs=argparse.REMAINDER, required=True)
    args = parser.parse_args(argv)
    return launch_heavy_command(
        task=args.task,
        eta_hours=args.eta_hours,
        command=args.command,
    )


if __name__ == "__main__":
    raise SystemExit(main())
