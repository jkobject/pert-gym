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
import shutil
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterator, Sequence

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_HEAVY_HOST = "pert-gym-worker-eu"
EXPECTED_ZONE = "europe-west1-b"
BILLING_PROJECT = "jkobject-1549353370965"


@dataclass(frozen=True)
class Preflight:
    hostname: str
    zone: str
    free_disk_bytes: int
    available_memory_bytes: int
    billing_project: str


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


def require_heavy_vm(*, expected_host: str = EXPECTED_HEAVY_HOST) -> str:
    """Reject local Macs and every host except the dedicated heavy worker."""
    if platform.system() == "Darwin":
        raise RuntimeError("refusing heavy run on Darwin; use pert-gym-worker-eu")
    hostname = socket.gethostname().split(".", maxsplit=1)[0]
    if hostname != expected_host:
        raise RuntimeError(
            f"refusing heavy run on host {hostname!r}; expected {expected_host!r}"
        )
    return hostname


def preflight(
    *,
    expected_host: str = EXPECTED_HEAVY_HOST,
    zone: str = EXPECTED_ZONE,
    min_free_disk_gb: float = 50,
    min_available_memory_gb: float = 16,
) -> Preflight:
    """Return measured capacity after applying fail-closed host/resource gates."""
    hostname = require_heavy_vm(expected_host=expected_host)
    free_disk = shutil.disk_usage(ROOT).free
    available_memory = _available_memory_bytes()
    if free_disk < min_free_disk_gb * 1024**3:
        raise RuntimeError(
            f"insufficient disk: {free_disk / 1024**3:.1f} GiB free; "
            f"need {min_free_disk_gb:.1f} GiB"
        )
    if available_memory < min_available_memory_gb * 1024**3:
        raise RuntimeError(
            f"insufficient RAM: {available_memory / 1024**3:.1f} GiB available; "
            f"need {min_available_memory_gb:.1f} GiB"
        )
    return Preflight(
        hostname=hostname,
        zone=zone,
        free_disk_bytes=free_disk,
        available_memory_bytes=available_memory,
        billing_project=BILLING_PROJECT,
    )


@contextmanager
def lamin_writer_lock(lock_path: Path, metadata: dict[str, object]) -> Iterator[None]:
    """Hold one non-blocking, process-visible Lamin writer lock for a run."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another Lamin writer holds {lock_path}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(metadata, sort_keys=True) + "\n")
        handle.flush()
        try:
            yield
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


def run_command(command: Sequence[str], *, log_path: Path) -> int:
    """Run an explicitly approved command, teeing stdout/stderr into a run log."""
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=_child_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert process.stdout is not None
        for line in process.stdout:
            sys.stdout.write(line)
            log.write(line)
        return process.wait()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-host", default=EXPECTED_HEAVY_HOST)
    parser.add_argument("--zone", default=EXPECTED_ZONE)
    parser.add_argument(
        "--run-id", default=time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    )
    parser.add_argument("--min-free-disk-gb", type=float, default=50)
    parser.add_argument("--min-available-memory-gb", type=float, default=16)
    parser.add_argument("--smoke", type=int, choices=(10_000, 25_000), action="append")
    parser.add_argument("--chunk-size", type=int, default=5_000)
    parser.add_argument("--allow-lamin-writes", action="store_true")
    parser.add_argument("--command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if not args.smoke and not args.command:
        parser.error("choose --smoke 10000/25000 or an explicit --command")
    if args.command and not args.allow_lamin_writes:
        parser.error("--command requires --allow-lamin-writes")

    preflight_result = preflight(
        expected_host=args.expected_host,
        zone=args.zone,
        min_free_disk_gb=args.min_free_disk_gb,
        min_available_memory_gb=args.min_available_memory_gb,
    )
    run_dir = ROOT / "artifacts" / "vm_runs" / args.run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    _write_json(run_dir / "preflight.json", asdict(preflight_result))
    _write_json(
        run_dir / "heartbeat.json", {"status": "started", "run_id": args.run_id}
    )

    lock_metadata = {
        "pid": os.getpid(),
        "run_id": args.run_id,
        **asdict(preflight_result),
    }
    with lamin_writer_lock(
        ROOT / "artifacts" / "vm_runs" / "lamin-writer.lock", lock_metadata
    ):
        measurements = [
            run_bounded_smoke(run_dir=run_dir, cells=cells, chunk_size=args.chunk_size)
            for cells in args.smoke or []
        ]
        _write_json(run_dir / "smoke-summary.json", measurements)
        if args.command:
            exit_code = run_command(
                args.command, log_path=run_dir / "logs" / "runner.log"
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
