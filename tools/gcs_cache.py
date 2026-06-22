#!/usr/bin/env python3
"""Resolve GCS-staged payloads to local files for backed readers on macOS.

macOS workers should not assume the VPS-only /mnt/gcs/scperturb mount exists.
Use ``ensure_gcs_object_local("gs://scperturb/...")`` before tools that need a
normal filesystem path, such as ``anndata.read_h5ad(..., backed="r")``.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_ROOT = ROOT / "data/gcs_cache"


def is_gcs_uri(value: str) -> bool:
    return value.startswith("gs://")


def gcs_uri_to_local_path(uri: str, cache_root: Path = DEFAULT_CACHE_ROOT) -> Path:
    if not is_gcs_uri(uri):
        return Path(uri)
    without_scheme = uri[len("gs://") :]
    if not without_scheme or "/" not in without_scheme:
        raise ValueError(f"Expected gs://bucket/object, got {uri!r}")
    return cache_root / without_scheme


def _run_text(command: list[str]) -> str:
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    return result.stdout.strip()


def gcs_object_size(uri: str) -> int:
    """Return remote object size in bytes using the authenticated gcloud CLI."""
    output = _run_text(["gcloud", "storage", "ls", "-l", uri])
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("TOTAL:"):
            continue
        parts = stripped.split()
        if parts and parts[0].isdigit():
            return int(parts[0])
    raise RuntimeError(f"Could not parse object size from gcloud output for {uri!r}: {output!r}")


def ensure_gcs_object_local(uri_or_path: str | Path, cache_root: Path = DEFAULT_CACHE_ROOT) -> Path:
    """Return a local path, copying gs:// objects into the project cache if needed."""
    value = str(uri_or_path)
    if not is_gcs_uri(value):
        path = Path(value)
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    target = gcs_uri_to_local_path(value, cache_root=cache_root)
    expected_size = gcs_object_size(value)
    if target.exists() and target.stat().st_size == expected_size:
        print(f"GCS_CACHE_HIT {value} -> {target} size={expected_size}", flush=True)
        return target
    if target.exists():
        print(
            f"GCS_CACHE_STALE {target} size={target.stat().st_size} expected={expected_size}; recopying",
            flush=True,
        )
        target.unlink()

    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + ".partial")
    if partial.exists():
        partial.unlink()
    print(f"GCS_CACHE_COPY {value} -> {target} expected_size={expected_size}", flush=True)
    subprocess.run(["gcloud", "storage", "cp", value, str(partial)], check=True)
    actual_size = partial.stat().st_size
    if actual_size != expected_size:
        partial.unlink(missing_ok=True)
        raise RuntimeError(f"Size mismatch after copy for {value}: {actual_size} != {expected_size}")
    partial.replace(target)
    print(f"GCS_CACHE_READY {target} size={actual_size}", flush=True)
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("uri_or_path", help="gs:// object or existing local path")
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE_ROOT)
    args = parser.parse_args()
    print(ensure_gcs_object_local(args.uri_or_path, cache_root=args.cache_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
