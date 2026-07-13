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
import json
import os
import platform
import resource
import selectors
import shutil
import socket
import subprocess
import sys
import time
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterator, Protocol, Sequence, TextIO
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
# Production execution is intentionally pinned to the sole active worker.
# Capacity VMs may exist for future operations but must remain dormant: reject
# them before the runner constructs candidates, touches GCS/Lamin, or acquires
# a distributed writer lease.
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
GCS_LAMIN_WRITER_LOCK_BUCKET = "scperturb"
GCS_LAMIN_WRITER_LOCK_OBJECT = "pert-gym/locks/lamin-writer-v1.json"
GCS_LAMIN_WRITER_LEASE_SECONDS = 120.0
_LOCK_METADATA_FIELDS = frozenset(
    {"run_id", "pid", "host", "project", "zone", "branch", "started_at"}
)


@dataclass(frozen=True)
class Preflight:
    hostname: str
    project: str
    zone: str
    instance: str
    free_disk_bytes: int
    available_memory_bytes: int
    billing_project: str


@dataclass(frozen=True)
class LeaseObject:
    generation: int
    payload: dict[str, object]


class LeaseGenerationConflict(RuntimeError):
    """A conditional GCS operation lost its object-generation precondition."""


class LeaseBackend(Protocol):
    def read(self) -> LeaseObject | None: ...

    def create(self, payload: dict[str, object]) -> LeaseObject: ...

    def replace(self, generation: int, payload: dict[str, object]) -> LeaseObject: ...

    def delete(self, generation: int) -> None: ...


class GcsLeaseBackend:
    """Generation-match GCS backend pinned to pert-gym's requester-pays bucket."""

    def __init__(self) -> None:
        try:
            from google.api_core.exceptions import NotFound, PreconditionFailed
            from google.cloud import storage
        except ImportError as exc:
            raise RuntimeError(
                "GCS lease backend dependencies are unavailable"
            ) from exc
        self._not_found = NotFound
        self._precondition_failed = PreconditionFailed
        self._client = storage.Client(project=EXPECTED_GCE_PROJECT)
        self._blob = self._client.bucket(
            GCS_LAMIN_WRITER_LOCK_BUCKET, user_project=BILLING_PROJECT
        ).blob(GCS_LAMIN_WRITER_LOCK_OBJECT)

    def _snapshot(self) -> LeaseObject:
        self._blob.reload()
        generation = self._blob.generation
        if generation is None:
            raise RuntimeError("GCS lease object is missing its generation")
        try:
            payload = json.loads(self._blob.download_as_text())
        except json.JSONDecodeError as exc:
            raise RuntimeError("GCS lease object contains invalid JSON") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("GCS lease object must contain a JSON object")
        return LeaseObject(int(generation), payload)

    def read(self) -> LeaseObject | None:
        try:
            return self._snapshot()
        except self._not_found:
            return None

    def create(self, payload: dict[str, object]) -> LeaseObject:
        try:
            self._blob.upload_from_string(
                json.dumps(payload, sort_keys=True),
                content_type="application/json",
                if_generation_match=0,
            )
            return self._snapshot()
        except self._precondition_failed as exc:
            raise LeaseGenerationConflict("GCS lease object already exists") from exc

    def replace(self, generation: int, payload: dict[str, object]) -> LeaseObject:
        try:
            self._blob.upload_from_string(
                json.dumps(payload, sort_keys=True),
                content_type="application/json",
                if_generation_match=generation,
            )
            return self._snapshot()
        except self._precondition_failed as exc:
            raise LeaseGenerationConflict("GCS lease generation changed") from exc

    def delete(self, generation: int) -> None:
        try:
            self._blob.delete(if_generation_match=generation)
        except self._precondition_failed as exc:
            raise LeaseGenerationConflict("GCS lease generation changed") from exc


class DistributedLaminWriterLease:
    """Fail-closed one-writer lease over a generation-pinned GCS object."""

    def __init__(
        self,
        *,
        backend: LeaseBackend,
        metadata: dict[str, object],
        ttl_seconds: float = GCS_LAMIN_WRITER_LEASE_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("distributed Lamin writer lease TTL must be positive")
        missing = _LOCK_METADATA_FIELDS - metadata.keys()
        if missing:
            raise ValueError(
                f"writer lease metadata missing required fields: {sorted(missing)}"
            )
        self._backend = backend
        self._metadata = dict(metadata)
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self.lease_id = uuid.uuid4().hex
        self._generation: int | None = None
        self.held = False

    def _payload(self) -> dict[str, object]:
        now = self._clock()
        return {
            **self._metadata,
            "protocol": "pert-gym-lamin-writer-lease-v1",
            "lease_id": self.lease_id,
            "issued_at": now,
            "expires_at": now + self._ttl_seconds,
        }

    def _verify_ownership(self, object_: LeaseObject) -> None:
        if object_.payload.get("lease_id") != self.lease_id:
            raise RuntimeError(
                "GCS lease operation did not prove this writer owns the lease"
            )
        if self._expiration(object_) <= self._clock():
            raise RuntimeError("GCS lease operation returned an already-expired lease")

    @staticmethod
    def _expiration(object_: LeaseObject) -> float:
        expires_at = object_.payload.get("expires_at")
        if not isinstance(expires_at, (int, float)) or isinstance(expires_at, bool):
            raise RuntimeError("refusing GCS lease with invalid expiration metadata")
        return float(expires_at)

    def acquire(self) -> None:
        if self.held:
            raise RuntimeError("distributed Lamin writer lease is already held")
        payload = self._payload()
        try:
            object_ = self._backend.create(payload)
        except LeaseGenerationConflict:
            try:
                existing = self._backend.read()
                if existing is None:
                    raise RuntimeError("GCS lease changed during acquisition")
                if self._expiration(existing) > self._clock():
                    raise RuntimeError(
                        "another distributed Lamin writer holds the lease"
                    )
                self._backend.delete(existing.generation)
                object_ = self._backend.create(payload)
            except LeaseGenerationConflict as exc:
                raise RuntimeError(
                    "could not prove distributed Lamin writer lease"
                ) from exc
            except RuntimeError:
                raise
            except Exception as exc:
                raise RuntimeError(
                    "could not prove distributed Lamin writer lease"
                ) from exc
        except Exception as exc:
            raise RuntimeError(
                "could not prove distributed Lamin writer lease"
            ) from exc
        try:
            self._verify_ownership(object_)
        except Exception as exc:
            raise RuntimeError(
                "could not prove distributed Lamin writer lease"
            ) from exc
        self._generation = object_.generation
        self.held = True

    def renew(self) -> None:
        if not self.held or self._generation is None:
            raise RuntimeError("distributed Lamin writer lease is not held")
        try:
            object_ = self._backend.replace(self._generation, self._payload())
            self._verify_ownership(object_)
        except Exception as exc:
            self.held = False
            self._generation = None
            raise RuntimeError(
                "distributed Lamin writer lease renewal could not be proven"
            ) from exc
        self._generation = object_.generation

    def release(self) -> None:
        if not self.held or self._generation is None:
            return
        try:
            self._backend.delete(self._generation)
        except Exception as exc:
            self.held = False
            self._generation = None
            raise RuntimeError(
                "distributed Lamin writer lease release ownership could not be proven"
            ) from exc
        self.held = False
        self._generation = None

    def __enter__(self) -> "DistributedLaminWriterLease":
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()


def distributed_lamin_writer_lease(
    metadata: dict[str, object],
) -> DistributedLaminWriterLease:
    """Build the mandatory cross-VM lease; initialization is fail-closed."""
    return DistributedLaminWriterLease(backend=GcsLeaseBackend(), metadata=metadata)


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


def run_command(
    command: Sequence[str],
    *,
    run_dir: Path,
    run_id: str,
    renew_writer_lease: Callable[[], None] | None = None,
) -> int:
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

    def publish(status: str, *, exit_code: int | None = None) -> None:
        if renew_writer_lease is not None:
            renew_writer_lease()
        state: dict[str, object] = {
            "status": status,
            "run_id": run_id,
            "pid": process.pid,
            "started_at": started_at,
            "updated_at": time.time(),
            "stdout_lines": stdout_lines,
            "resume_contract": "inspect checkpoint then rerun the approved command",
        }
        if exit_code is not None:
            state["exit_code"] = exit_code
        _write_json(checkpoint_path, state)
        _write_json(run_dir / "heartbeat.json", state)

    with log_path.open("wb") as log:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=_child_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert process.stdout is not None
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
                exit_code = process.poll()
                if exit_code is not None:
                    drain_stdout()
                    publish(
                        "completed" if exit_code == 0 else "failed", exit_code=exit_code
                    )
                    return exit_code
                publish("running")
        except BaseException:
            # Losing proof of the distributed lease must stop the child rather
            # than leave a potentially writing process running after failure.
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            raise
        finally:
            selector.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-id", default=time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
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
    run_dir.mkdir(parents=True, exist_ok=False)
    _write_json(run_dir / "preflight.json", asdict(preflight_result))
    _write_json(
        run_dir / "heartbeat.json", {"status": "started", "run_id": args.run_id}
    )

    lock_metadata = {
        "pid": os.getpid(),
        "run_id": args.run_id,
        "host": preflight_result.hostname,
        "project": preflight_result.project,
        "zone": preflight_result.zone,
        "branch": _git_branch(),
        "started_at": time.time(),
        **asdict(preflight_result),
    }
    measurements = [
        run_bounded_smoke(run_dir=run_dir, cells=cells, chunk_size=args.chunk_size)
        for cells in args.smoke or []
    ]
    _write_json(run_dir / "smoke-summary.json", measurements)
    if args.command:
        with distributed_lamin_writer_lease(lock_metadata) as distributed_lease:
            with lamin_writer_lock(vm_global_lamin_writer_lock_path(), lock_metadata):
                with ExitStack() as legacy_locks:
                    # A legacy lock's metadata predates PID identity. Its kernel
                    # flock is authoritative: once acquired, no legacy writer is
                    # live, so do not reject a safely recovered legacy inode on
                    # stale metadata.
                    for legacy_lock_path in legacy_lamin_writer_lock_paths():
                        legacy_locks.enter_context(
                            lamin_writer_lock(
                                legacy_lock_path,
                                lock_metadata,
                                check_live_metadata=False,
                            )
                        )
                    exit_code = run_command(
                        args.command,
                        run_dir=run_dir,
                        run_id=args.run_id,
                        renew_writer_lease=distributed_lease.renew,
                    )
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
