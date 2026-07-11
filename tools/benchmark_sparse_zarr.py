#!/usr/bin/env python3
"""Benchmark sparse CSR/CSC Zarr serialization on the designated worker VM.

This script is deliberately local-store only: it never imports Lamin and never
mutates an artifact. Run it on ``pert-gym-worker-eu`` as required by
``logical_sparse_zarr_policy.v1.json``.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import tempfile
import time
from pathlib import Path


def peak_rss_bytes() -> int:
    """Return max RSS in bytes on Linux/macOS."""

    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if os.uname().sysname == "Darwin" else value * 1024


def stored_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def matrix_for(n_obs: int, sparse_format: str):
    import numpy as np
    from scipy import sparse

    n_vars = 2_000
    density = min(0.02, 60 / n_vars)
    matrix = sparse.random(
        n_obs,
        n_vars,
        density=density,
        format=sparse_format,
        random_state=20260711 + n_obs,
        dtype=np.float32,
    )
    matrix.sum_duplicates()
    matrix.sort_indices()
    return matrix


def write_and_readback(root: Path, matrix, sparse_format: str) -> dict[str, object]:
    import numpy as np
    import zarr
    from scipy import sparse

    started = time.perf_counter()
    group = zarr.open_group(str(root), mode="w")
    group.attrs.update(
        {"format": sparse_format, "shape": list(matrix.shape), "nnz": matrix.nnz}
    )
    group.create_dataset("data", data=matrix.data, chunks=(min(matrix.nnz, 65_536),))
    group.create_dataset(
        "indices", data=matrix.indices, chunks=(min(matrix.nnz, 65_536),)
    )
    group.create_dataset(
        "indptr", data=matrix.indptr, chunks=(min(len(matrix.indptr), 65_536),)
    )
    write_seconds = time.perf_counter() - started

    started = time.perf_counter()
    loaded = zarr.open_group(str(root), mode="r")
    constructor = sparse.csr_matrix if sparse_format == "csr" else sparse.csc_matrix
    roundtrip = constructor(
        (
            np.asarray(loaded["data"]),
            np.asarray(loaded["indices"]),
            np.asarray(loaded["indptr"]),
        ),
        shape=tuple(loaded.attrs["shape"]),
    )
    read_seconds = time.perf_counter() - started
    parity = (
        roundtrip.shape == matrix.shape
        and roundtrip.nnz == matrix.nnz
        and np.array_equal(roundtrip.data, matrix.data)
        and np.array_equal(roundtrip.indices, matrix.indices)
        and np.array_equal(roundtrip.indptr, matrix.indptr)
    )
    return {
        "write_seconds": write_seconds,
        "read_seconds": read_seconds,
        "readback_parity": parity,
        "stored_bytes": stored_bytes(root),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, default=None)
    args = parser.parse_args()

    host = os.uname().nodename
    if "pert-gym-worker-eu" not in host:
        raise RuntimeError(
            f"VM-only benchmark refused on host {host!r}; expected pert-gym-worker-eu"
        )

    base = args.workdir or Path(tempfile.mkdtemp(prefix="pert-gym-sparse-zarr-"))
    created_base = args.workdir is None
    results: list[dict[str, object]] = []
    try:
        for n_obs in (5_000, 10_000, 25_000):
            for sparse_format in ("csr", "csc"):
                case_dir = base / f"{sparse_format}-{n_obs}"
                matrix = matrix_for(n_obs, sparse_format)
                before = peak_rss_bytes()
                result = write_and_readback(case_dir, matrix, sparse_format)
                result.update(
                    {
                        "n_obs": n_obs,
                        "n_vars": int(matrix.shape[1]),
                        "nnz": int(matrix.nnz),
                        "format": sparse_format,
                        "peak_rss_bytes": max(before, peak_rss_bytes()),
                    }
                )
                results.append(result)
        if not all(case["readback_parity"] for case in results):
            raise RuntimeError("sparse Zarr benchmark readback parity failed")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps({"host": host, "cases": results}, indent=2) + "\n"
        )
    finally:
        if created_base:
            shutil.rmtree(base, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
