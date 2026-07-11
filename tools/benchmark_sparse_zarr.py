#!/usr/bin/env python3
"""Benchmark sparse CSR/CSC Zarr serialization on the designated worker VM.

This script is deliberately local-store only: it never imports Lamin and never
mutates an artifact. Run it on ``pert-gym-worker-eu`` as required by
``logical_sparse_zarr_policy.v1.json``.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import resource
import shutil
import tempfile
import time
from pathlib import Path


def host_name() -> str:
    return os.uname().nodename


def local_rss_bytes() -> int:
    """Return current resident memory in bytes on Linux or macOS."""
    if os.uname().sysname == "Darwin":
        # Darwin exposes process high-water RSS only. The VM-only runner is Linux;
        # retaining the fallback keeps diagnostics available without claiming it is
        # a case-local peak measurement.
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    statm = Path("/proc/self/statm").read_text().split()
    return int(statm[1]) * os.sysconf("SC_PAGE_SIZE")


def process_peak_rss_bytes() -> int:
    """Return the OS-recorded high-water RSS for this isolated case process."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if os.uname().sysname == "Darwin" else peak * 1024


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
    return {
        "write_readback_seconds": time.perf_counter() - started,
        "bytes": stored_bytes(root),
        "matrix_parity": roundtrip.shape == matrix.shape
        and roundtrip.nnz == matrix.nnz
        and np.array_equal(roundtrip.data, matrix.data)
        and np.array_equal(roundtrip.indices, matrix.indices)
        and np.array_equal(roundtrip.indptr, matrix.indptr),
        "obs_parity": obs_sidecar["index"] == obs_index.tolist(),
        "source_row_parity": obs_sidecar["source_rows"] == source_rows.tolist(),
    }


def _run_case_payload(root: Path, n_obs: int, sparse_format: str) -> dict[str, object]:
    """Execute a case entirely inside its own process."""
    started = time.perf_counter()
    baseline_rss = local_rss_bytes()
    matrix = matrix_for(n_obs, sparse_format)
    result = write_and_readback(root, matrix, sparse_format, source_row_start=0)
    peak_rss = max(baseline_rss, process_peak_rss_bytes())
    result.update(
        {
            "n_obs": n_obs,
            "n_vars": int(matrix.shape[1]),
            "nnz": int(matrix.nnz),
            "format": sparse_format,
            "wall_seconds": time.perf_counter() - started,
            "case_rss_baseline_bytes": baseline_rss,
            "case_rss_peak_bytes": peak_rss,
            "case_rss_peak_delta_bytes": peak_rss - baseline_rss,
            "case_rss_peak_measurement": "isolated-process-os-high-water",
        }
    )
    return result


def _case_process_entry(connection, root: Path, n_obs: int, sparse_format: str) -> None:
    try:
        connection.send({"result": _run_case_payload(root, n_obs, sparse_format)})
    except BaseException as error:
        connection.send({"error": f"{type(error).__name__}: {error}"})
    finally:
        connection.close()


def _run_case_in_isolated_process(
    root: Path, n_obs: int, sparse_format: str
) -> dict[str, object]:
    """Run one case in a fresh child so OS high-water RSS is case-local."""
    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_case_process_entry,
        args=(sender, root, n_obs, sparse_format),
    )
    process.start()
    sender.close()
    process.join()
    if not receiver.poll():
        raise RuntimeError(
            f"isolated benchmark case exited without a result (exitcode={process.exitcode})"
        )
    payload = receiver.recv()
    receiver.close()
    if "error" in payload:
        raise RuntimeError(f"isolated benchmark case failed: {payload['error']}")
    if process.exitcode != 0:
        raise RuntimeError(
            f"isolated benchmark case failed with exitcode={process.exitcode}"
        )
    return payload["result"]


def run_case(root: Path, n_obs: int, sparse_format: str) -> dict[str, object]:
    """Measure one case with a case-local OS high-water RSS measurement."""
    return _run_case_in_isolated_process(root, n_obs, sparse_format)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, default=None)
    args = parser.parse_args()

    host = host_name()
    if "pert-gym-worker-eu" not in host:
        raise RuntimeError(
            f"VM-only benchmark refused on host {host!r}; expected pert-gym-worker-eu"
        )

    base = args.workdir or Path(tempfile.mkdtemp(prefix="pert-gym-sparse-zarr-"))
    created_base = args.workdir is None
    total_started = time.perf_counter()
    try:
        cases = [
            run_case(base / f"{sparse_format}-{n_obs}", n_obs, sparse_format)
            for n_obs in (5_000, 10_000, 25_000)
            for sparse_format in ("csr", "csc")
        ]
        if not all(
            case["matrix_parity"] and case["obs_parity"] and case["source_row_parity"]
            for case in cases
        ):
            raise RuntimeError("sparse Zarr benchmark parity failed")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(
                {
                    "host": host,
                    "total_wall_seconds": time.perf_counter() - total_started,
                    "cases": cases,
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
