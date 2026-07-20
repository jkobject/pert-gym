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
import os
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pandas as pd
from anndata._io.specs import read_elem
from anndata._io.specs.methods import read_elem_partial
from scipy import sparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pert_gym.gcs_native_sparse_zarr import (  # noqa: E402
    DEFAULT_CACHE_CAP_BYTES,
    GCSOperationCounter,
    count_gcs_operations,
    promote_gcs_native_revision,
    register_gcs_prefix_with_lamin,
    requester_pays_gcs_filesystem,
    write_gcs_native_sparse_revision,
)
from pert_gym.obs_identity import add_obs_identity, validate_obs_identity  # noqa: E402
from tools.lamin_context import connect_pertdata  # noqa: E402
from tools.pert_gym_vm_runner import (  # noqa: E402
    lamin_writer_lock,
    legacy_lamin_writer_lock_paths,
    preflight,
    vm_global_lamin_writer_lock_path,
)


class GCSH5ADCSR:
    """Range-backed HDF5 CSR adapter exposing only contiguous row slices."""

    format = "csr"

    def __init__(self, h5: h5py.File, *, row_start: int, row_end: int) -> None:
        group = h5["X"]
        if (
            not isinstance(group, h5py.Group)
            or group.attrs.get("encoding-type") != "csr_matrix"
        ):
            raise ValueError("GCS source X must be HDF5 csr_matrix encoding")
        self._group = group
        full_shape = tuple(int(value) for value in group.attrs["shape"])
        if row_start < 0 or row_end <= row_start or row_end > full_shape[0]:
            raise ValueError("source row bounds are outside the source X shape")
        self._row_start = row_start
        self.shape = (row_end - row_start, full_shape[1])
        indptr = group["indptr"]
        self.data_dtype = np.dtype(group["data"].dtype)
        self.index_dtype = np.dtype(group["indices"].dtype)
        self.indptr_dtype = np.dtype(indptr.dtype)
        self.nnz = int(indptr[row_end]) - int(indptr[row_start])

    def __getitem__(self, selection: slice) -> sparse.csr_matrix:
        if not isinstance(selection, slice) or selection.step not in (None, 1):
            raise TypeError("GCS H5AD source supports contiguous row slices only")
        start, end, _ = selection.indices(self.shape[0])
        start += self._row_start
        end += self._row_start
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

    def block_nnz(self, start: int, end: int) -> int:
        """Return exact interval nnz from two bounded indptr reads."""
        if start < 0 or end <= start or end > self.shape[0]:
            raise ValueError("block nnz interval is outside the source range")
        absolute_start = self._row_start + start
        absolute_end = self._row_start + end
        indptr = self._group["indptr"]
        return int(indptr[absolute_end]) - int(indptr[absolute_start])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-gcs-uri", required=True)
    parser.add_argument("--staging-gcs-prefix", required=True)
    parser.add_argument("--logical-key", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--schema-fingerprint", required=True)
    parser.add_argument("--ingestion-run-id", required=True)
    parser.add_argument("--source-checksum", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--canonical-prefix", required=True)
    parser.add_argument("--migration-map-json", type=Path, required=True)
    parser.add_argument("--collection-metadata-json", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--cache-cap-gib", type=float, default=20.0)
    parser.add_argument("--max-rss-gib", type=float, default=4.0)
    parser.add_argument("--min-block-gib", type=float, default=2.0)
    parser.add_argument("--max-block-gib", type=float, default=3.0)
    parser.add_argument(
        "--block-size-exception-json",
        type=Path,
        help="reviewed explicit byte-bound exception; never overrides RSS",
    )
    parser.add_argument("--min-rows", type=int, default=5_000)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--target-object-mib", type=int, default=64)
    parser.add_argument("--forecast-logical-blocks", type=int, required=True)
    parser.add_argument("--launch-task-id", required=True)
    parser.add_argument(
        "--operation-cost-reviewed-by",
        required=True,
        help="reviewer identity bound independently from the exception JSON",
    )
    parser.add_argument(
        "--operation-cost-exception-json",
        type=Path,
        help="task-scoped independently reviewed object/request budget exception",
    )
    parser.add_argument("--row-start", type=int, required=True)
    parser.add_argument("--row-end", type=int, required=True)
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--register-lamin-prefix", action="store_true")
    return parser.parse_args()


def _gcs_key(uri: str) -> str:
    if not uri.startswith("gs://"):
        raise ValueError("GCS URI must start gs://")
    return uri.removeprefix("gs://")


class _CountingRangeReader:
    """File-like proxy that charges every source metadata/range read as Class B."""

    def __init__(self, handle: Any, counter: GCSOperationCounter) -> None:
        self._handle = handle
        self._counter = counter

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)

    def __enter__(self) -> _CountingRangeReader:
        return self

    def __exit__(self, *args: object) -> Any:
        return self._handle.__exit__(*args)

    def info(self) -> Any:
        self._counter.count_class_b()
        return self._handle.info()

    def read(self, *args: object, **kwargs: object) -> Any:
        self._counter.count_class_b()
        return self._handle.read(*args, **kwargs)

    def readinto(self, buffer: Any) -> Any:
        self._counter.count_class_b()
        return self._handle.readinto(buffer)


def open_generation_pinned_source(
    fs: Any,
    source_key: str,
    *,
    operation_counter: GCSOperationCounter | None = None,
) -> tuple[str, Any]:
    """Preflight and open one immutable GCS generation through gcsfs' path API.

    gcsfs 2025.12.0 accepts ``generation=`` in ``GCSFileSystem._open`` but
    drops it while constructing ``GCSFile``. Its documented version-aware path
    (``bucket/object#generation``) survives to every ``cat_file`` range request,
    so use that concrete request path rather than the ineffective kwarg.
    """
    if not bool(getattr(fs, "version_aware", False)):
        raise RuntimeError("GCS source filesystem must enable version-aware paths")
    if "#" in source_key:
        raise RuntimeError(
            "GCS source key must not already contain a generation fragment"
        )
    source_info = fs.info(source_key)
    generation = str(source_info.get("generation", ""))
    if not generation or not generation.isdecimal():
        raise RuntimeError("GCS source lacks a valid immutable generation metadata")
    pinned_key = f"{source_key}#{generation}"
    pinned_info = fs.info(pinned_key)
    if str(pinned_info.get("generation", "")) != generation:
        raise RuntimeError(
            "GCS generation-qualified source did not resolve requested generation"
        )
    handle = fs.open(
        pinned_key,
        "rb",
        block_size=8 * 1024**2,
        cache_type="readahead",
    )
    if operation_counter is not None:
        handle = _CountingRangeReader(handle, operation_counter)
    try:
        opened_info = handle.info()
        if str(opened_info.get("generation", "")) != generation:
            raise RuntimeError("opened GCS source did not resolve requested generation")
    except BaseException:
        handle.close()
        raise
    return generation, handle


def _read_h5ad_dataframe_rows(
    group: h5py.Group, *, row_start: int, row_end: int
) -> pd.DataFrame:
    """Decode a bounded AnnData dataframe window without reading every obs row.

    AnnData's partial reader slices every row-backed index/column dataset. For a
    categorical column it may read the complete categories vocabulary, whose size
    is bounded by category cardinality rather than source row count, while its
    row-aligned codes remain restricted to ``[row_start, row_end)``.
    """
    if row_start < 0 or row_end <= row_start:
        raise ValueError("source row bounds must be non-negative and non-empty")
    value = read_elem_partial(group, indices=(slice(row_start, row_end), slice(None)))
    if not isinstance(value, pd.DataFrame):
        raise ValueError("GCS source obs must be an AnnData dataframe encoding")
    return value


def main() -> int:
    args = parse_args()
    if args.cache_cap_gib <= 0 or args.max_rss_gib <= 0:
        raise ValueError("cache and RSS limits must be positive")
    if args.row_start < 0 or args.row_end <= args.row_start:
        raise ValueError("row bounds must be non-negative and non-empty")
    capacity = preflight()
    lock_metadata = {
        "pid": os.getpid(),
        "run_id": args.ingestion_run_id,
        "host": capacity.hostname,
        "project": capacity.project,
        "zone": capacity.zone,
        "branch": "gcs-native-no-lamin",
        "started_at": time.time(),
    }
    with ExitStack() as writer_locks:
        if os.environ.get("PERT_GYM_VM_RUNNER_LOCK_RUN_ID") != args.ingestion_run_id:
            writer_locks.enter_context(
                lamin_writer_lock(vm_global_lamin_writer_lock_path(), lock_metadata)
            )
            for lock_path in legacy_lamin_writer_lock_paths():
                writer_locks.enter_context(
                    lamin_writer_lock(
                        lock_path, lock_metadata, check_live_metadata=False
                    )
                )
        operation_counter = GCSOperationCounter()
        fs = count_gcs_operations(
            requester_pays_gcs_filesystem("jkobject-1549353370965"),
            operation_counter,
        )
        source_key = _gcs_key(args.source_gcs_uri)
        generation, handle = open_generation_pinned_source(
            fs, source_key, operation_counter=operation_counter
        )
        with handle:
            with h5py.File(handle, "r") as h5:
                matrix = GCSH5ADCSR(h5, row_start=args.row_start, row_end=args.row_end)
                var = read_elem(h5["var"])
                if not isinstance(var, pd.DataFrame):
                    raise ValueError("GCS source var must be a dataframe")
                obs = _read_h5ad_dataframe_rows(
                    h5["obs"], row_start=args.row_start, row_end=args.row_end
                )
                obs = add_obs_identity(
                    obs, dataset_id=args.dataset_id, prefix=args.canonical_prefix
                )
                validate_obs_identity(obs)
                candidate_metadata = {
                    "migration_map": json.loads(
                        args.migration_map_json.read_text("utf-8")
                    ),
                    "collection": json.loads(
                        args.collection_metadata_json.read_text("utf-8")
                    ),
                }
                block_size_exception = (
                    json.loads(args.block_size_exception_json.read_text("utf-8"))
                    if getattr(args, "block_size_exception_json", None)
                    else None
                )
                operation_cost_exception = (
                    json.loads(args.operation_cost_exception_json.read_text("utf-8"))
                    if getattr(args, "operation_cost_exception_json", None)
                    else None
                )
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
                    source_checksum=args.source_checksum,
                    source_row_start=args.row_start,
                    source_row_end=args.row_end,
                    schema_fingerprint=args.schema_fingerprint,
                    ingestion_run_id=args.ingestion_run_id,
                    launch_context={
                        "task_id": args.launch_task_id,
                        "reviewed_by": args.operation_cost_reviewed_by,
                    },
                    cache_dir=args.cache_dir,
                    forecast_logical_blocks=args.forecast_logical_blocks,
                    candidate_metadata=candidate_metadata,
                    cache_cap_bytes=int(args.cache_cap_gib * 1024**3),
                    max_rss_bytes=int(args.max_rss_gib * 1024**3),
                    min_block_bytes=int(getattr(args, "min_block_gib", 2.0) * 1024**3),
                    max_block_bytes=int(getattr(args, "max_block_gib", 3.0) * 1024**3),
                    block_size_exception=block_size_exception,
                    min_rows=args.min_rows,
                    max_rows=args.max_rows,
                    target_object_bytes=int(
                        getattr(args, "target_object_mib", 64) * 1024**2
                    ),
                    operation_cost_exception=operation_cost_exception,
                    operation_counter=operation_counter,
                )
        result: dict[str, Any] = {
            "manifest": manifest,
            "metrics": metrics.__dict__,
        }
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
