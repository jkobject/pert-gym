#!/usr/bin/env python3
"""VM-only, requester-pays range migration from one GCS h5ad to GCS sparse-Zarr.

The source is opened through gcsfs/fsspec and h5py; no source file is downloaded
or copied to local disk. Run via ``tools/pert_gym_vm_runner.py`` so its approved
host preflight and existing host-global writer guard cover any optional Lamin
registration step.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import h5py
import numpy as np
from anndata._io.specs import read_elem
from scipy import sparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pert_gym.gcs_native_sparse_zarr import (  # noqa: E402
    DEFAULT_CACHE_CAP_BYTES,
    promote_gcs_native_revision,
    register_gcs_prefix_with_lamin,
    requester_pays_gcs_filesystem,
    write_gcs_native_sparse_revision,
)
from tools.lamin_context import connect_pertdata  # noqa: E402
from tools.pert_gym_vm_runner import require_heavy_vm  # noqa: E402


class GCSH5ADCSR:
    """Range-backed HDF5 CSR adapter exposing only contiguous row slices."""

    format = "csr"

    def __init__(self, h5: h5py.File) -> None:
        group = h5["X"]
        if (
            not isinstance(group, h5py.Group)
            or group.attrs.get("encoding-type") != "csr_matrix"
        ):
            raise ValueError("GCS source X must be HDF5 csr_matrix encoding")
        self._group = group
        self.shape = tuple(int(value) for value in group.attrs["shape"])
        self.nnz = int(group["indptr"][-1])

    def __getitem__(self, selection: slice) -> sparse.csr_matrix:
        if not isinstance(selection, slice) or selection.step not in (None, 1):
            raise TypeError("GCS H5AD source supports contiguous row slices only")
        start, end, _ = selection.indices(self.shape[0])
        indptr = np.asarray(self._group["indptr"][start : end + 1], dtype=np.int64)
        data_start, data_end = int(indptr[0]), int(indptr[-1])
        return sparse.csr_matrix(
            (
                np.asarray(self._group["data"][data_start:data_end]),
                np.asarray(self._group["indices"][data_start:data_end], dtype=np.int64),
                indptr - data_start,
            ),
            shape=(end - start, self.shape[1]),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-gcs-uri", required=True)
    parser.add_argument("--staging-gcs-prefix", required=True)
    parser.add_argument("--logical-key", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--schema-fingerprint", required=True)
    parser.add_argument("--ingestion-run-id", required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--cache-cap-gib", type=float, default=20.0)
    parser.add_argument("--max-rss-gib", type=float, default=4.0)
    parser.add_argument("--min-rows", type=int, default=5_000)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--register-lamin-prefix", action="store_true")
    return parser.parse_args()


def _gcs_key(uri: str) -> str:
    if not uri.startswith("gs://"):
        raise ValueError("GCS URI must start gs://")
    return uri.removeprefix("gs://")


def main() -> int:
    args = parse_args()
    if args.cache_cap_gib <= 0 or args.max_rss_gib <= 0:
        raise ValueError("cache and RSS limits must be positive")
    require_heavy_vm()
    fs = requester_pays_gcs_filesystem("jkobject-1549353370965")
    source_key = _gcs_key(args.source_gcs_uri)
    source_info = fs.info(source_key)
    generation = str(source_info.get("generation", ""))
    if not generation:
        raise RuntimeError("GCS source lacks immutable generation metadata")
    with fs.open(
        source_key, "rb", block_size=8 * 1024**2, cache_type="readahead"
    ) as handle:
        with h5py.File(handle, "r") as h5:
            matrix = GCSH5ADCSR(h5)
            obs, var = read_elem(h5["obs"]), read_elem(h5["var"])
            manifest, metrics = write_gcs_native_sparse_revision(
                fs=fs,
                staging_prefix=_gcs_key(args.staging_gcs_prefix),
                logical_key=args.logical_key,
                revision=args.revision,
                matrix=matrix,
                obs=obs,
                var=var,
                source_uri=args.source_gcs_uri,
                source_generation=generation,
                schema_fingerprint=args.schema_fingerprint,
                ingestion_run_id=args.ingestion_run_id,
                cache_dir=args.cache_dir,
                cache_cap_bytes=int(args.cache_cap_gib * 1024**3),
                max_rss_bytes=int(args.max_rss_gib * 1024**3),
                min_rows=args.min_rows,
                max_rows=args.max_rows,
            )
    result: dict[str, Any] = {"manifest": manifest, "metrics": metrics.__dict__}
    if args.promote:
        result["promotion"] = promote_gcs_native_revision(
            fs=fs,
            staging_prefix=_gcs_key(args.staging_gcs_prefix),
            logical_key=args.logical_key,
            revision=args.revision,
            manifest=manifest,
        )
    if args.register_lamin_prefix:
        prefix = f"gs://{manifest['candidate_prefix']}"
        result["lamin"] = str(
            register_gcs_prefix_with_lamin(ln=connect_pertdata(), prefix_uri=prefix)
        )
    print(json.dumps(result, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
