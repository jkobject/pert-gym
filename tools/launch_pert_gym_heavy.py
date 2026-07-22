#!/usr/bin/env python3
"""Control-plane launcher for bounded heavy work on ``pert-gym-worker-eu``.

The launcher publishes a task-owned, time-bounded lease to both GCE labels and
the local compute guard before it starts the VM or dispatches a payload. It then
proves the exact labels by readback. The legacy long-run mode preserves failed
payload state for inspection. Minute-bounded writer and verify-only modes instead
supervise one exact remote payload PID, enforce an absolute ceiling, and clear
terminal lease state after stopping the VM.
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
from typing import Callable, Protocol, Sequence

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
MAX_BOUNDED_WRITER_MINUTES = 360.0
MAX_PAYLOAD_HEARTBEAT_AGE_SECONDS = 600
DEFAULT_SSH_READINESS_TIMEOUT_SECONDS = 300.0
SSH_READINESS_INTERVAL_SECONDS = 5.0
DEFAULT_LOCAL_LEASE_PATH = (
    Path.home()
    / ".hermes"
    / "state"
    / "gcp_compute_cost_guard.d"
    / "pert-gym-worker-eu.json"
)
_TASK_RE = re.compile(r"^t_[0-9a-f]{8}$")
_LABEL_TIME_RE = re.compile(r"^\d{8}t\d{6}z$")
_LABEL_VALUE_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")
Run = Callable[[list[str]], subprocess.CompletedProcess[str]]


class PayloadProcess(Protocol):
    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...


PayloadStart = Callable[[list[str]], PayloadProcess]


def _run(
    command: list[str], *, timeout_seconds: float = 600
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


def _run_payload(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        text=True,
    )


def _start_payload(command: list[str]) -> PayloadProcess:
    return subprocess.Popen(command, text=True)


def _bounded_control_run(
    run: Run,
    *,
    absolute_deadline: float,
    monotonic: Callable[[], float],
    lifecycle_mode: str = "verify-only",
) -> Run:
    """Bound real control-plane subprocesses by the remaining lifecycle time."""
    if run is not _run:
        return run

    def bounded(command: list[str]) -> subprocess.CompletedProcess[str]:
        remaining = absolute_deadline - monotonic()
        if remaining <= 0:
            raise RuntimeError(f"{lifecycle_mode} absolute ceiling exhausted")
        try:
            return _run(command, timeout_seconds=min(600.0, remaining))
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"{lifecycle_mode} control operation exceeded absolute ceiling"
            ) from exc

    return bounded


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


def _lease_labels(
    task: str, deadline: datetime, *, purpose: str = PURPOSE
) -> dict[str, str]:
    return {
        "owner": OWNER,
        "project": PROJECT_LABEL,
        "purpose": purpose,
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
    purpose: str = PURPOSE,
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
        "purpose": purpose,
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


def _publish_gce_lease(
    run: Run,
    *,
    task: str,
    purpose: str,
    deadline: datetime,
    previous: dict[str, str] | None = None,
) -> dict[str, str]:
    if previous is not None:
        _verify_lease(_describe(run), previous)
    labels = _lease_labels(task, deadline, purpose=purpose)
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
    _verify_lease(_describe(run), labels)
    return labels


def _verify_lease_cleared(instance: dict[str, object]) -> None:
    labels = instance.get("labels")
    if not isinstance(labels, dict):
        labels = {}
    remaining = {
        key: labels[key]
        for key in ("owner", "project", "purpose", "task", "lease-until")
        if key in labels
    }
    if remaining:
        raise RuntimeError(f"terminal lease clear readback mismatch: {remaining!r}")


def _clear_terminal_lease(
    run: Run,
    *,
    labels: dict[str, str],
    local_lease_path: Path,
) -> None:
    before_stop = _describe(run)
    _verify_lease(before_stop, labels)
    if before_stop.get("status") == "RUNNING":
        _checked(
            run,
            _gcloud("instances", "stop", INSTANCE, "--zone", ZONE, "--quiet"),
            operation="terminal instance stop",
        )
    stopped = _describe(run)
    _verify_lease(stopped, labels)
    if stopped.get("status") != "TERMINATED":
        raise RuntimeError("terminal instance stop readback mismatch")
    _checked(
        run,
        _gcloud(
            "instances",
            "remove-labels",
            INSTANCE,
            "--zone",
            ZONE,
            "--labels",
            "owner,project,purpose,task,lease-until",
            "--quiet",
        ),
        operation="terminal GCE lease clear",
    )
    _verify_lease_cleared(_describe(run))
    local_lease_path.unlink(missing_ok=True)
    local_lease_path.with_suffix(local_lease_path.suffix + ".tmp").unlink(
        missing_ok=True
    )


def _cleanup_terminal_failure(
    run: Run,
    *,
    labels: dict[str, str],
    local_lease_path: Path,
    primary: BaseException,
) -> None:
    """Compensate exact owned state while preserving the primary failure."""
    try:
        _clear_terminal_lease(
            run,
            labels=labels,
            local_lease_path=local_lease_path,
        )
    except BaseException as cleanup:
        primary.add_note(f"terminal cleanup also failed: {cleanup}")
        raise primary from cleanup


def _read_remote_payload_pid(run: Run, path: str) -> int | None:
    result = run(
        _gcloud(
            "ssh",
            INSTANCE,
            "--zone",
            ZONE,
            "--command",
            f"cat -- {shlex.quote(path)}",
        )
    )
    value = (result.stdout or "").strip()
    if result.returncode or not value.isdecimal() or int(value) <= 1:
        return None
    return int(value)


def _remote_payload_is_live(run: Run, path: str, pid: int) -> bool:
    path_q = shlex.quote(path)
    command = f'test "$(cat -- {path_q})" = {pid} && kill -0 {pid}'
    return not run(
        _gcloud("ssh", INSTANCE, "--zone", ZONE, "--command", command)
    ).returncode


def _remote_payload_has_fresh_heartbeat(
    run: Run,
    *,
    heartbeat_path: str,
    expected_pid: int,
) -> bool:
    command = (
        "set -eu; "
        f"heartbeat=$(cat -- {shlex.quote(heartbeat_path)}); "
        "now=$(date +%s); "
        'printf "%s %s\\n" "$heartbeat" "$now" '
        "# payload-heartbeat"
    )
    result = run(_gcloud("ssh", INSTANCE, "--zone", ZONE, "--command", command))
    if result.returncode:
        return False
    fields = (result.stdout or "").strip().split()
    if len(fields) != 3:
        return False
    try:
        heartbeat_pid, heartbeat_at, now = (int(field) for field in fields)
    except ValueError:
        return False
    age = now - heartbeat_at
    return (
        heartbeat_pid == expected_pid and 0 <= age <= MAX_PAYLOAD_HEARTBEAT_AGE_SECONDS
    )


def _is_broad_prism_command(command: Sequence[str]) -> bool:
    return any(
        Path(argument).name == "curate_broad_prism_obs.py"
        or argument == "tools.curate_broad_prism_obs"
        for argument in command
    )


def _signal_remote_payload(run: Run, path: str, pid: int) -> None:
    path_q = shlex.quote(path)
    command = f'test "$(cat -- {path_q})" = {pid} && kill -TERM {pid}'
    run(_gcloud("ssh", INSTANCE, "--zone", ZONE, "--command", command))


def _is_retryable_ssh_readiness_failure(
    result: subprocess.CompletedProcess[str],
) -> bool:
    if result.returncode != 255:
        return False
    output = f"{result.stdout or ''}\n{result.stderr or ''}".lower()
    return any(
        marker in output
        for marker in (
            "connection refused",
            "connection timed out",
            "operation timed out",
        )
    )


def _wait_for_ssh_readiness(
    run: Run,
    *,
    labels: dict[str, str],
    started: float,
    absolute_deadline: float,
    timeout_seconds: float,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> None:
    """Acquire SSH readiness without crossing lease ownership or lifecycle bounds."""
    readiness_deadline = min(started + timeout_seconds, absolute_deadline)
    while True:
        tick = monotonic()
        if tick >= absolute_deadline:
            raise RuntimeError("SSH readiness absolute ceiling exhausted")
        if tick >= readiness_deadline:
            raise RuntimeError("SSH readiness timeout exhausted")

        _verify_lease(_describe(run), labels)
        result = run(_gcloud("ssh", INSTANCE, "--zone", ZONE, "--command", "true"))
        if result.returncode == 0:
            # Close the final readiness -> PID/payload interval with fresh ownership proof.
            _verify_lease(_describe(run), labels)
            if monotonic() >= absolute_deadline:
                raise RuntimeError("SSH readiness absolute ceiling exhausted")
            if monotonic() >= readiness_deadline:
                raise RuntimeError("SSH readiness timeout exhausted")
            return
        if not _is_retryable_ssh_readiness_failure(result):
            output = ((result.stdout or "") + (result.stderr or ""))[-1500:]
            raise RuntimeError(
                "non-retryable SSH readiness failure "
                f"with rc={result.returncode}: {output}"
            )

        remaining = min(readiness_deadline, absolute_deadline) - monotonic()
        if remaining > 0:
            sleep(min(SSH_READINESS_INTERVAL_SECONDS, remaining))


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
    purpose: str = PURPOSE,
    verify_only: bool = False,
    lease_minutes: float | None = None,
    absolute_max_minutes: float | None = None,
    payload_start: PayloadStart = _start_payload,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    readiness_timeout_seconds: float = DEFAULT_SSH_READINESS_TIMEOUT_SECONDS,
) -> int:
    """Lease, start if needed, dispatch one heavy command, and stop on success."""
    if not command:
        raise ValueError("heavy command must not be empty")
    task_label(task)
    if not _LABEL_VALUE_RE.fullmatch(purpose):
        raise ValueError("purpose must be an exact GCE label value")
    minute_lifecycle_requested = (
        lease_minutes is not None or absolute_max_minutes is not None
    )
    if _is_broad_prism_command(command) and not minute_lifecycle_requested:
        raise ValueError(
            "Broad PRISM writer requires lease-minutes and absolute-max-minutes"
        )
    if minute_lifecycle_requested and (
        lease_minutes is None or absolute_max_minutes is None
    ):
        raise ValueError(
            "lease-minutes and absolute-max-minutes must be provided together"
        )
    bounded_lifecycle = lease_minutes is not None and absolute_max_minutes is not None
    lifecycle_mode = "verify-only" if verify_only else "bounded writer"
    if verify_only:
        if purpose == PURPOSE:
            raise ValueError("verify-only launch requires an exact verify-only purpose")
        if lease_minutes is None or not 0 < lease_minutes <= 60:
            raise ValueError("verify-only lease must be at most 60 minutes")
        if absolute_max_minutes is None or not 0 < absolute_max_minutes <= 90:
            raise ValueError("verify-only absolute ceiling must be at most 90 minutes")
        if absolute_max_minutes < lease_minutes:
            raise ValueError("verify-only absolute ceiling cannot precede its lease")
        if readiness_timeout_seconds <= 0:
            raise ValueError("SSH readiness timeout must be positive")
    elif lease_minutes is not None and absolute_max_minutes is not None:
        if purpose == PURPOSE:
            raise ValueError("bounded writer launch requires an exact purpose")
        if not 0 < lease_minutes <= MAX_BOUNDED_WRITER_MINUTES:
            raise ValueError("bounded writer lease must be at most 360 minutes")
        if not 0 < absolute_max_minutes <= MAX_BOUNDED_WRITER_MINUTES:
            raise ValueError(
                "bounded writer absolute ceiling must be at most 360 minutes"
            )
        if absolute_max_minutes < lease_minutes:
            raise ValueError("bounded writer absolute ceiling cannot precede its lease")
        if readiness_timeout_seconds <= 0:
            raise ValueError("SSH readiness timeout must be positive")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if lease_minutes is not None and absolute_max_minutes is not None:
        requested = current + timedelta(minutes=lease_minutes)
        lifecycle_started = monotonic()
        absolute_monotonic_deadline = lifecycle_started + absolute_max_minutes * 60
    else:
        requested = lease_deadline(current, eta_hours=eta_hours)
        lifecycle_started = None
        absolute_monotonic_deadline = None
    control_run = (
        _bounded_control_run(
            run,
            absolute_deadline=absolute_monotonic_deadline,
            lifecycle_mode=lifecycle_mode,
            monotonic=monotonic,
        )
        if absolute_monotonic_deadline is not None
        else run
    )

    before = _describe(control_run)
    if before.get("status") not in {"RUNNING", "TERMINATED"}:
        raise RuntimeError(f"refusing instance status {before.get('status')!r}")
    if bounded_lifecycle:
        before_labels = before.get("labels")
        existing = _parse_label_deadline(
            before_labels.get("lease-until")
            if isinstance(before_labels, dict)
            else None
        )
        if existing is not None and existing > requested:
            raise RuntimeError(
                f"existing live lease exceeds the requested {lifecycle_mode} lease; "
                "refusing to shorten it"
            )
    deadline = _effective_deadline(before, task=task, requested=requested, now=current)
    labels = _publish_gce_lease(
        control_run,
        task=task,
        purpose=purpose,
        deadline=deadline,
    )

    try:
        _atomic_write_local_lease(
            local_lease_path,
            task=task,
            deadline=deadline,
            purpose=purpose,
        )
        if (
            absolute_monotonic_deadline is not None
            and monotonic() >= absolute_monotonic_deadline
        ):
            raise RuntimeError(f"{lifecycle_mode} absolute ceiling exhausted")
        # A second fresh read closes the lease-readback -> start/dispatch interval.
        immediately_before_launch = _describe(control_run)
        _verify_lease(immediately_before_launch, labels)
        status = immediately_before_launch.get("status")
        if status == "TERMINATED":
            _checked(
                control_run,
                _gcloud("instances", "start", INSTANCE, "--zone", ZONE, "--quiet"),
                operation="instance start",
            )
        elif status != "RUNNING":
            raise RuntimeError(f"refusing pre-launch instance status {status!r}")
    except BaseException as primary:
        if bounded_lifecycle:
            _cleanup_terminal_failure(
                run,
                labels=labels,
                local_lease_path=local_lease_path,
                primary=primary,
            )
        raise

    if bounded_lifecycle:
        assert lease_minutes is not None and absolute_max_minutes is not None
        assert lifecycle_started is not None
        assert absolute_monotonic_deadline is not None
        pid_path = f"/tmp/pert-gym/{task}/bounded-payload.pid"
        heartbeat_path = f"/tmp/pert-gym/{task}/bounded-payload.heartbeat"
        remote = (
            "set -eu; umask 077; "
            f"mkdir -p {shlex.quote(str(Path(pid_path).parent))}; "
            f"rm -f -- {shlex.quote(heartbeat_path)} "
            f"{shlex.quote(heartbeat_path + '.partial')}; "
            f"printf '%s\\n' $$ > {shlex.quote(pid_path)}; "
            f"export PERT_GYM_PAYLOAD_HEARTBEAT_PATH={shlex.quote(heartbeat_path)}; "
            f"exec {shlex.join(list(command))}"
        )
        started = lifecycle_started
        absolute_deadline = current + timedelta(minutes=absolute_max_minutes)
        renewal_after = started + lease_minutes * 30
        pid: int | None = None
        process: PayloadProcess | None = None
        try:
            _wait_for_ssh_readiness(
                control_run,
                labels=labels,
                started=started,
                absolute_deadline=absolute_monotonic_deadline,
                timeout_seconds=readiness_timeout_seconds,
                monotonic=monotonic,
                sleep=sleep,
            )
            _checked(
                control_run,
                _gcloud(
                    "ssh",
                    INSTANCE,
                    "--zone",
                    ZONE,
                    "--command",
                    f"rm -f -- {shlex.quote(pid_path)} "
                    f"{shlex.quote(heartbeat_path)} "
                    f"{shlex.quote(heartbeat_path + '.partial')}",
                ),
                operation=f"stale {lifecycle_mode} payload PID cleanup",
            )
            if monotonic() >= absolute_monotonic_deadline:
                raise RuntimeError(f"{lifecycle_mode} absolute ceiling exhausted")
            process = payload_start(
                _gcloud("ssh", INSTANCE, "--zone", ZONE, "--command", remote)
            )
            for _ in range(12):
                if monotonic() >= absolute_monotonic_deadline:
                    raise RuntimeError(f"{lifecycle_mode} absolute ceiling exhausted")
                pid = _read_remote_payload_pid(control_run, pid_path)
                if monotonic() >= absolute_monotonic_deadline:
                    raise RuntimeError(f"{lifecycle_mode} absolute ceiling exhausted")
                if pid is not None:
                    break
                if process.poll() is not None:
                    raise RuntimeError(
                        f"{lifecycle_mode} payload exited before publishing its exact PID"
                    )
                remaining = absolute_monotonic_deadline - monotonic()
                sleep(min(5, max(0.0, remaining)))
            if pid is None:
                raise RuntimeError(
                    f"{lifecycle_mode} payload did not publish its exact PID"
                )

            timed_out = False
            while process.poll() is None:
                tick = monotonic()
                elapsed = tick - started
                wall_now = current + timedelta(seconds=elapsed)
                if wall_now >= absolute_deadline:
                    _signal_remote_payload(run, pid_path, pid)
                    timed_out = True
                    break
                if tick >= renewal_after:
                    if not _remote_payload_is_live(control_run, pid_path, pid):
                        raise RuntimeError(
                            "refusing lease renewal without the exact live payload PID"
                        )
                    if not _remote_payload_has_fresh_heartbeat(
                        control_run,
                        heartbeat_path=heartbeat_path,
                        expected_pid=pid,
                    ):
                        raise RuntimeError(
                            "refusing lease renewal without a fresh exact payload heartbeat"
                        )
                    if monotonic() >= absolute_monotonic_deadline:
                        _signal_remote_payload(run, pid_path, pid)
                        timed_out = True
                        break
                    renewed = min(
                        wall_now + timedelta(minutes=lease_minutes), absolute_deadline
                    )
                    if renewed > deadline:
                        labels = _publish_gce_lease(
                            control_run,
                            task=task,
                            purpose=purpose,
                            deadline=renewed,
                            previous=labels,
                        )
                        _atomic_write_local_lease(
                            local_lease_path,
                            task=task,
                            deadline=renewed,
                            purpose=purpose,
                        )
                        deadline = renewed
                        if monotonic() >= absolute_monotonic_deadline:
                            _signal_remote_payload(run, pid_path, pid)
                            timed_out = True
                            break
                    renewal_after = tick + lease_minutes * 30
                remaining = absolute_monotonic_deadline - monotonic()
                sleep(min(5, max(0.0, remaining)))
            returncode = 124 if timed_out else process.wait()
        except BaseException as primary:
            _cleanup_terminal_failure(
                run,
                labels=labels,
                local_lease_path=local_lease_path,
                primary=primary,
            )
            raise
        _clear_terminal_lease(
            run,
            labels=labels,
            local_lease_path=local_lease_path,
        )
        if timed_out and process is not None:
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pass
        return returncode

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
    parser.add_argument("--purpose", default=PURPOSE)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--lease-minutes", type=float)
    parser.add_argument("--absolute-max-minutes", type=float)
    parser.add_argument("--command", nargs=argparse.REMAINDER, required=True)
    args = parser.parse_args(argv)
    if _is_broad_prism_command(args.command) and (
        args.lease_minutes is None or args.absolute_max_minutes is None
    ):
        parser.error(
            "Broad PRISM writer requires --lease-minutes and --absolute-max-minutes"
        )
    return launch_heavy_command(
        task=args.task,
        eta_hours=args.eta_hours,
        command=args.command,
        purpose=args.purpose,
        verify_only=args.verify_only,
        lease_minutes=args.lease_minutes,
        absolute_max_minutes=args.absolute_max_minutes,
    )


if __name__ == "__main__":
    raise SystemExit(main())
