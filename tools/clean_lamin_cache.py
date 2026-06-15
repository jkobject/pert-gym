#!/usr/bin/env python3
"""Remove local Lamin artifact-cache copies after remote verification.

This intentionally targets only cached artifact payloads (h5ad/parquet/csv/zip
copies) under the project-local cache. It does not touch Lamin settings,
credentials, branch files, or the global shared cache.
"""

from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_SUFFIXES = {
    ".csv",
    ".gz",
    ".h5ad",
    ".parquet",
    ".tsv",
    ".xlsx",
    ".zip",
}


def clean_cache(cache_root: Path, *, dry_run: bool = False) -> tuple[int, int]:
    """Delete cached artifact payloads and return (files, bytes)."""
    if not cache_root.exists():
        return 0, 0

    deleted = 0
    n_bytes = 0
    for path in sorted(cache_root.rglob("*")):
        if not path.is_file() or path.suffix not in DEFAULT_SUFFIXES:
            continue
        size = path.stat().st_size
        print(("WOULD_DELETE" if dry_run else "DELETE"), path, size)
        deleted += 1
        n_bytes += size
        if not dry_run:
            path.unlink()

    if not dry_run:
        for directory in sorted(
            (p for p in cache_root.rglob("*") if p.is_dir()),
            key=lambda p: len(p.parts),
            reverse=True,
        ):
            try:
                directory.rmdir()
            except OSError:
                pass
    return deleted, n_bytes


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=Path(".lamin-cache/lamindb"),
        help="Cache root to clean; defaults to the project-local Lamin cache.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    deleted, n_bytes = clean_cache(args.cache_root, dry_run=args.dry_run)
    print(f"SUMMARY files={deleted} bytes={n_bytes}")


if __name__ == "__main__":
    main()
