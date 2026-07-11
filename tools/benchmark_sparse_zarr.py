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


def local_rss_bytes() -> int:
    """Return this process's resident memory in bytes on Linux or macOS."""
    if os.uname().sysname == "Darwin":
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    statm = Path("/proc/self/statm").read_text().split()
    return int(statm[1]) * os.sysconf("SC_PAGE_SIZE")


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


def write_and_readback(
    root: Path, matrix, sparse_format: str, *, source_row_start: int
) -> dict[str, object]:
    """Write one case and prove matrix, obs order, and source rows round-trip."""
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
    obs_index = np.arange(matrix.shape[0], dtype=np.int64)
    source_rows = np.arange(
        source_row_start, source_row_start + matrix.shape[0], dtype=np.int64
    )
    (root / "obs.json").write_text(
        json.dumps({"index": obs_index.tolist(), "source_rows": source_rows.tolist()})
    )

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
    obs_sidecar = json.loads((root / "obs.json").read_text())
    wall_seconds = time.perf_counter() - started
    matrix_parity = (
        roundtrip.shape == matrix.shape
        and roundtrip.nnz == matrix.nnz
        and np.array_equal(roundtrip.data, matrix.data)
        and np.array_equal(roundtrip.indices, matrix.indices)
        and np.array_equal(roundtrip.indptr, matrix.indptr)
    )
    obs_parity = obs_sidecar["index"] == obs_index.tolist()
    source_row_parity = obs_sidecar["source_rows"] == source_rows.tolist()
    return {
        "wall_seconds": wall_seconds,
        "local_rss_bytes": local_rss_bytes(),
        "bytes": stored_bytes(root),
        "matrix_parity": matrix_parity,
        "obs_parity": obs_parity,
        "source_row_parity": source_row_parity,
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
    total_started = time.perf_counter()
    try:
        for n_obs in (5_000, 10_000, 25_000):
            for sparse_format in ("csr", "csc"):
                matrix = matrix_for(n_obs, sparse_format)
                result = write_and_readback(
                    base / f"{sparse_format}-{n_obs}",
                    matrix,
                    sparse_format,
                    source_row_start=0,
                )
                result.update(
                    {
                        "n_obs": n_obs,
                        "n_vars": int(matrix.shape[1]),
                        "nnz": int(matrix.nnz),
                        "format": sparse_format,
                    }
                )
                results.append(result)
        if not all(
            case["matrix_parity"] and case["obs_parity"] and case["source_row_parity"]
            for case in results
        ):
            raise RuntimeError("sparse Zarr benchmark parity failed")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(
                {
                    "host": host,
                    "total_wall_seconds": time.perf_counter() - total_started,
                    "cases": results,
                },
                indent=2,
            )
            + "\n"
        )
    finally:
        if created_base:
            shutil.rmtree(base, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
