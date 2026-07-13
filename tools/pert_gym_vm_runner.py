#!/usr/bin/env python3
"""Fail-closed, VM-only runner for heavy pert-gym ingestion work.

The runner never permits heavy work from a Darwin host or an unexpected VM.  It
serializes Lamin writers, records run state below ``artifacts/vm_runs``, and
exports the requester-pays billing project to child processes.  The bounded
smoke mode deliberately performs no Lamin or network operation.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import platform
import resource
import selectors
import shutil
import signal
import socket
import subprocess
import sys
import time
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterator, Sequence, TextIO
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
ALLOWED_HEAVY_HOSTS = frozenset({"pert-gym-worker-eu"})
EXPECTED_GCE_PROJECT = "jkobject-1549353370965"
EXPECTED_ZONE = "europe-west1-b"
BILLING_PROJECT = "jkobject-1549353370965"
MIN_FREE_DISK_GB = 50
MIN_AVAILABLE_MEMORY_GB = 16
PRODUCTION_HEARTBEAT_SECONDS = 30.0
METADATA_BASE_URL = "http://metadata.google.internal/computeMetadata/v1"
DEFAULT_LAMIN_WRITER_LOCK_DIR = Path("/tmp/pert-gym")
LAMIN_WRITER_LOCK_DIR_ENV = "PERT_GYM_LAMIN_WRITER_LOCK_DIR"
_LOCK_METADATA_FIELDS = frozenset(
    {"run_id", "pid", "host", "project", "zone", "branch", "started_at"}
)
_LEASE_TOKEN = object()


@dataclass(frozen=True)
class Preflight:
    hostname: str
    project: str
    zone: str
    instance: str
    free_disk_bytes: int
    available_memory_bytes: int
    billing_project: str


@dataclass
class LaminWriterLease:
    """Capability issued only while this process holds every writer lock."""

    run_id: str
    _token: object | None = None


def has_lamin_writer_lease(lease: LaminWriterLease | None) -> bool:
    """Return whether ``lease`` was issued by the active shared lock contract."""
    return isinstance(lease, LaminWriterLease) and lease._token is _LEASE_TOKEN


def _available_memory_bytes() -> int:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        raise RuntimeError("VM preflight requires /proc/meminfo")
    values = {}
    for line in meminfo.read_text(encoding="utf-8").splitlines():
        key, value = line.split(":", maxsplit=1)
        values[key] = int(value.strip().split()[0]) * 1024
    try:
        return values["MemAvailable"]
    except KeyError as exc:
        raise RuntimeError("VM preflight could not read MemAvailable") from exc


def _metadata_value(path: str) -> str:
    """Read a mandatory GCE metadata value or fail closed outside the VM."""
    request = Request(
        f"{METADATA_BASE_URL}/{path}", headers={"Metadata-Flavor": "Google"}
    )
    try:
        with urlopen(request, timeout=2) as response:  # nosec B310: fixed GCE endpoint
            value = response.read().decode("utf-8").strip()
    except OSError as exc:
        raise RuntimeError(
            f"VM identity check could not read GCE metadata {path}"
        ) from exc
    if not value:
        raise RuntimeError(f"VM identity check received empty GCE metadata {path}")
    return value


def require_heavy_vm() -> tuple[str, str, str, str]:
    """Reject local Macs and every host outside the exact heavy-host allowlist."""
    if platform.system() == "Darwin":
        raise RuntimeError("refusing heavy run on Darwin; use an approved heavy VM")
    hostname = socket.gethostname().split(".", maxsplit=1)[0]
    if hostname not in ALLOWED_HEAVY_HOSTS:
        raise RuntimeError(
            f"refusing heavy run on host {hostname!r}; expected one of "
            f"{sorted(ALLOWED_HEAVY_HOSTS)!r}"
        )
    project = _metadata_value("project/project-id")
    zone = _metadata_value("instance/zone").rsplit("/", maxsplit=1)[-1]
    instance = _metadata_value("instance/name")
    expected = {
        "project": EXPECTED_GCE_PROJECT,
        "zone": EXPECTED_ZONE,
        "instance": hostname,
    }
    actual = {"project": project, "zone": zone, "instance": instance}
    if actual != expected:
        raise RuntimeError(f"refusing unpinned GCE identity: {actual!r}")
    return hostname, project, zone, instance


def preflight() -> Preflight:
    """Return measured capacity after applying fail-closed host/resource gates."""
    hostname, project, zone, instance = require_heavy_vm()
    free_disk = shutil.disk_usage(ROOT).free
    available_memory = _available_memory_bytes()
    if free_disk < MIN_FREE_DISK_GB * 1024**3:
        raise RuntimeError(
            f"insufficient disk: {free_disk / 1024**3:.1f} GiB free; "
            f"need {MIN_FREE_DISK_GB:.1f} GiB"
        )
    if available_memory < MIN_AVAILABLE_MEMORY_GB * 1024**3:
        raise RuntimeError(
            f"insufficient RAM: {available_memory / 1024**3:.1f} GiB available; "
            f"need {MIN_AVAILABLE_MEMORY_GB:.1f} GiB"
        )
    return Preflight(
        hostname=hostname,
        project=project,
        zone=zone,
        instance=instance,
        free_disk_bytes=free_disk,
        available_memory_bytes=available_memory,
        billing_project=BILLING_PROJECT,
    )


def vm_global_lamin_writer_lock_path(worktree: Path | None = None) -> Path:
    """Return the host-global writer lock path, never a worktree-relative path."""
    del worktree  # Kept for callers/tests that demonstrate CWD independence.
    lock_dir = Path(
        os.environ.get(LAMIN_WRITER_LOCK_DIR_ENV, str(DEFAULT_LAMIN_WRITER_LOCK_DIR))
    ).expanduser()
    if not lock_dir.is_absolute():
        raise RuntimeError(f"{LAMIN_WRITER_LOCK_DIR_ENV} must be an absolute host path")
    lock_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    return lock_dir / "lamin-writer.lock"


def legacy_lamin_writer_lock_path(worktree: Path | None = None) -> Path:
    """Return the pre-global-lock location for a worktree during migration.

    Existing runners acquired this inode before the host-global lock existed.
    New production commands acquire both locks so that an in-flight legacy
    writer remains mutually exclusive without being stopped or modified.
    """
    root = (ROOT if worktree is None else worktree).resolve()
    return root / "artifacts" / "vm_runs" / "lamin-writer.lock"


def legacy_lamin_writer_lock_paths() -> tuple[Path, ...]:
    """Return every legacy lock inode for this repository's registered worktrees.

    A runner from before host-global locking may still hold the old lock below
    any registered worktree. New writers acquire all of those legacy inodes
    while the migration compatibility path remains active.
    """
    result = subprocess.run(
        ["git", "-C", str(ROOT), "worktree", "list", "--porcelain"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise RuntimeError("could not enumerate worktrees for legacy Lamin locks")
    worktrees = {
        Path(line.removeprefix("worktree ")).resolve()
        for line in result.stdout.splitlines()
        if line.startswith("worktree ")
    }
    worktrees.add(ROOT.resolve())
    if not worktrees:
        raise RuntimeError("could not find a worktree for legacy Lamin locks")
    return tuple(
        legacy_lamin_writer_lock_path(worktree)
        for worktree in sorted(worktrees, key=lambda path: str(path))
    )


def _process_start_ticks(pid: int) -> str | None:
    """Return Linux process start ticks, which distinguish a reused PID."""
    try:
        remainder = (
            Path(f"/proc/{pid}/stat")
            .read_text(encoding="utf-8")
            .rsplit(")", maxsplit=1)[1]
        )
        # Field 3 is process state; field 22 (starttime) is index 19 after it.
        fields = remainder.split()
        if fields[0] == "Z":
            return None
        return fields[19]
    except (FileNotFoundError, IndexError, OSError):
        return None


def _read_lock_metadata(handle: TextIO) -> dict[str, object] | None:
    handle.seek(0)
    raw = handle.read()
    if not raw.strip():
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("refusing unreadable Lamin writer lock metadata") from exc
    if not isinstance(value, dict):
        raise RuntimeError("refusing non-object Lamin writer lock metadata")
    return value


def _write_lock_metadata(handle: TextIO, metadata: dict[str, object]) -> None:
    handle.seek(0)
    handle.truncate()
    handle.write(json.dumps(metadata, sort_keys=True) + "\n")
    handle.flush()
    os.fsync(handle.fileno())


def _is_live_owner(metadata: dict[str, object]) -> bool:
    if metadata.get("status") != "acquired":
        return False
    pid = metadata.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool):
        raise RuntimeError("refusing Lamin writer lock with invalid owner PID")
    current_start_ticks = _process_start_ticks(pid)
    if current_start_ticks is None:
        return False
    recorded_start_ticks = metadata.get("process_start_ticks")
    if not isinstance(recorded_start_ticks, str):
        raise RuntimeError(
            "refusing live legacy Lamin writer lock without process identity"
        )
    return current_start_ticks == recorded_start_ticks


@contextmanager
def lamin_writer_lock(
    lock_path: Path,
    metadata: dict[str, object],
    *,
    check_live_metadata: bool = True,
) -> Iterator[None]:
    """Hold the non-blocking global Lamin writer lock with PID-reuse-safe recovery.

    ``flock`` is the atomic ownership primitive.  Metadata is examined only after
    acquiring it, so stale state can never displace a live kernel-held lock.
    """
    missing = _LOCK_METADATA_FIELDS - metadata.keys()
    if missing:
        raise ValueError(
            f"writer lock metadata missing required fields: {sorted(missing)}"
        )
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another Lamin writer holds {lock_path}") from exc

        acquired = False
        try:
            previous = _read_lock_metadata(handle)
            if (
                check_live_metadata
                and previous is not None
                and _is_live_owner(previous)
            ):
                raise RuntimeError(
                    "refusing to steal Lamin writer lock with live owner metadata"
                )
            acquisition_metadata = {
                **metadata,
                "process_start_ticks": _process_start_ticks(os.getpid()),
                "status": "acquired",
                "acquired_at": time.time(),
            }
            _write_lock_metadata(handle, acquisition_metadata)
            acquired = True
            yield
        finally:
            try:
                if acquired:
                    released = {
                        **metadata,
                        "process_start_ticks": _process_start_ticks(os.getpid()),
                        "status": "released",
                        "released_at": time.time(),
                    }
                    _write_lock_metadata(handle, released)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def lamin_writer_lease(
    *, run_id: str, preflight_result: Preflight | None = None
) -> Iterator[LaminWriterLease]:
    """Acquire the host-global and every legacy Lamin writer lock together."""
    result = preflight() if preflight_result is None else preflight_result
    metadata = {
        "pid": os.getpid(),
        "run_id": run_id,
        "host": result.hostname,
        "project": result.project,
        "zone": result.zone,
        "branch": _git_branch(),
        "started_at": time.time(),
        **asdict(result),
    }
    lease = LaminWriterLease(run_id=run_id)
    with lamin_writer_lock(vm_global_lamin_writer_lock_path(), metadata):
        with ExitStack() as legacy_locks:
            # A legacy lock's metadata predates PID identity. Its kernel flock
            # is authoritative: after acquisition, stale metadata is safe.
            for legacy_lock_path in legacy_lamin_writer_lock_paths():
                legacy_locks.enter_context(
                    lamin_writer_lock(
                        legacy_lock_path, metadata, check_live_metadata=False
                    )
                )
            lease._token = _LEASE_TOKEN
            try:
                yield lease
            finally:
                lease._token = None


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def run_bounded_smoke(
    *, run_dir: Path, cells: int, chunk_size: int = 5_000
) -> dict[str, object]:
    """Measure bounded chunk scheduling without importing Lamin or reading data."""
    if cells not in {10_000, 25_000}:
        raise ValueError("bounded smoke supports exactly 10000 or 25000 cells")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    started = time.monotonic()
    heartbeat_path = run_dir / "heartbeat.json"
    checkpoint_path = run_dir / "checkpoints" / f"smoke_{cells}.json"
    chunk_count = 0
    max_chunk_cells = 0
    for start in range(0, cells, chunk_size):
        end = min(cells, start + chunk_size)
        chunk_count += 1
        max_chunk_cells = max(max_chunk_cells, end - start)
        state = {"cells": cells, "last_completed_end": end, "chunk_count": chunk_count}
        _write_json(checkpoint_path, state)
        _write_json(heartbeat_path, {**state, "kind": "bounded_smoke"})
    measurement = {
        "cells": cells,
        "chunk_size": chunk_size,
        "chunk_count": chunk_count,
        "max_chunk_cells": max_chunk_cells,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "lamin_writes": 0,
    }
    _write_json(run_dir / "smoke" / f"{cells}.json", measurement)
    return measurement


def _child_env() -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "GOOGLE_CLOUD_PROJECT": BILLING_PROJECT,
            "GCLOUD_PROJECT": BILLING_PROJECT,
            "PERT_GYM_GCS_USER_PROJECT": BILLING_PROJECT,
            "XDG_CACHE_HOME": str(ROOT / ".lamin-cache"),
            "LAMIN_SETTINGS_DIR": str(ROOT / ".lamin-pertgym"),
        }
    )
    return env


def _git_branch() -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "branch", "--show-current"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() or "detached-or-unavailable"


def command_identity(command: Sequence[str]) -> str:
    payload = json.dumps(list(command), separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _pid_is_live(value: object) -> bool:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        return False
    try:
        os.kill(value, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def run_command(command: Sequence[str], *, run_dir: Path, run_id: str) -> int:
    """Run a production child with durable periodic liveness and progress state.

    ``checkpoints/production.json`` is intentionally independent of the child:
    a silent or stuck command still emits a current timestamp, PID, and observed
    stdout-line count.  On interruption it remains a durable handoff for an
    operator to inspect before resuming the ingestion command.
    """
    log_path = run_dir / "logs" / "runner.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path = run_dir / "checkpoints" / "production.json"
    started_at = time.time()
    stdout_lines = 0

    def publish(
        status: str,
        *,
        exit_code: int | None = None,
        signal_name: str | None = None,
    ) -> None:
        state: dict[str, object] = {
            "status": status,
            "run_id": run_id,
            "started_at": started_at,
            "updated_at": time.time(),
            "stdout_lines": stdout_lines,
            "command": list(command),
            "command_sha256": command_identity(command),
            "resume_contract": "inspect checkpoint then rerun the approved command",
        }
        if status == "running":
            state["pid"] = process.pid
        else:
            state["ended_at"] = time.time()
        if exit_code is not None:
            state["exit_code"] = exit_code
        if signal_name is not None:
            state["signal"] = signal_name
        _write_json(checkpoint_path, state)
        _write_json(run_dir / "heartbeat.json", state)

    with log_path.open("ab") as log:
        child_env = _child_env()
        child_env["PERT_GYM_VM_RUNNER_LOCK_RUN_ID"] = run_id
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert process.stdout is not None
        received_signal: int | None = None
        old_handlers: dict[int, object] = {}

        def interrupt(signum: int, _frame: object) -> None:
            nonlocal received_signal
            received_signal = signum
            if process.poll() is None:
                try:
                    process.send_signal(signum)
                except ProcessLookupError:
                    pass

        for signum in (signal.SIGINT, signal.SIGTERM):
            old_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, interrupt)
        publish("running")
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        stdout_fd = process.stdout.fileno()
        os.set_blocking(stdout_fd, False)

        def drain_stdout() -> None:
            """Copy only currently available child bytes without delaying liveness."""
            nonlocal stdout_lines
            while True:
                try:
                    output = os.read(stdout_fd, 64 * 1024)
                except BlockingIOError:
                    return
                if not output:
                    return
                stdout_lines += output.count(b"\n")
                log.write(output)
                log.flush()
                stdout_buffer = getattr(sys.stdout, "buffer", None)
                if stdout_buffer is not None:
                    stdout_buffer.write(output)
                    stdout_buffer.flush()
                else:
                    sys.stdout.write(output.decode("utf-8", errors="surrogateescape"))
                    sys.stdout.flush()

        try:
            while True:
                for _, _ in selector.select(timeout=PRODUCTION_HEARTBEAT_SECONDS):
                    drain_stdout()
                if received_signal is not None:
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.terminate()
                        process.wait(timeout=5)
                    drain_stdout()
                    interrupted_exit = 128 + received_signal
                    publish(
                        "interrupted",
                        exit_code=interrupted_exit,
                        signal_name=signal.Signals(received_signal).name,
                    )
                    return interrupted_exit
                exit_code = process.poll()
                if exit_code is not None:
                    drain_stdout()
                    child_signal = (
                        signal.Signals(-exit_code).name if exit_code < 0 else None
                    )
                    publish(
                        (
                            "completed"
                            if exit_code == 0
                            else "interrupted"
                            if exit_code < 0
                            else "failed"
                        ),
                        exit_code=exit_code,
                        signal_name=child_signal,
                    )
                    return exit_code
                publish("running")
        finally:
            selector.close()
            for signum, handler in old_handlers.items():
                signal.signal(signum, handler)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-id", default=time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="resume an existing identity-matched runner directory append-only",
    )
    parser.add_argument("--smoke", type=int, choices=(10_000, 25_000), action="append")
    parser.add_argument("--chunk-size", type=int, default=5_000)
    parser.add_argument("--allow-lamin-writes", action="store_true")
    parser.add_argument("--command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if not args.smoke and not args.command:
        parser.error("choose --smoke 10000/25000 or an explicit --command")
    if args.command and not args.allow_lamin_writes:
        parser.error("--command requires --allow-lamin-writes")

    preflight_result = preflight()
    run_dir = ROOT / "artifacts" / "vm_runs" / args.run_id
    if run_dir.exists():
        if not args.resume:
            raise RuntimeError(f"run directory already exists: {run_dir}")
        checkpoint_path = run_dir / "checkpoints" / "production.json"
        try:
            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError) as error:
            raise RuntimeError(
                "resume requires a readable production checkpoint"
            ) from error
        if checkpoint.get("run_id") != args.run_id or checkpoint.get("status") not in {
            "running",
            "failed",
            "interrupted",
        }:
            raise RuntimeError("resume checkpoint identity or status is not resumable")
        if checkpoint.get("command_sha256") != command_identity(args.command or []):
            raise RuntimeError("resume command identity mismatch")
        if checkpoint.get("command") != list(args.command or []):
            raise RuntimeError("resume command identity mismatch")
        if checkpoint.get("status") == "running":
            if _pid_is_live(checkpoint.get("pid")):
                raise RuntimeError("resume checkpoint PID is still live")
            stale_path = run_dir / f"stale-checkpoint-{time.time_ns()}.json"
            _write_json(
                stale_path,
                {
                    **checkpoint,
                    "stale_detected_at": time.time(),
                    "stale_pid": checkpoint.get("pid"),
                },
            )
            checkpoint = {
                key: value for key, value in checkpoint.items() if key != "pid"
            }
            checkpoint.update(
                {
                    "status": "interrupted",
                    "ended_at": time.time(),
                    "signal": "stale_running_checkpoint",
                }
            )
            _write_json(checkpoint_path, checkpoint)
        _write_json(
            run_dir / f"resume-preflight-{time.time_ns()}.json",
            asdict(preflight_result),
        )
    else:
        if args.resume:
            raise RuntimeError(f"resume run directory does not exist: {run_dir}")
        run_dir.mkdir(parents=True)
        _write_json(run_dir / "preflight.json", asdict(preflight_result))
    _write_json(
        run_dir / "heartbeat.json",
        {"status": "started", "run_id": args.run_id, "resume": args.resume},
    )

    measurements = [
        run_bounded_smoke(run_dir=run_dir, cells=cells, chunk_size=args.chunk_size)
        for cells in args.smoke or []
    ]
    _write_json(run_dir / "smoke-summary.json", measurements)
    if args.command:
        with lamin_writer_lease(run_id=args.run_id, preflight_result=preflight_result):
            exit_code = run_command(args.command, run_dir=run_dir, run_id=args.run_id)
            if exit_code:
                raise RuntimeError(
                    f"command exited {exit_code}; see {run_dir / 'logs' / 'runner.log'}"
                )
    _write_json(
        run_dir / "heartbeat.json", {"status": "completed", "run_id": args.run_id}
    )
    print(json.dumps({"run_dir": str(run_dir), "smoke": measurements}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
