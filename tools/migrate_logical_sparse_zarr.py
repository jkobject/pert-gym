#!/usr/bin/env python3
"""VM-only migration of one h5ad source into a local logical sparse-Zarr revision.

This tool writes only an append-only candidate below ``--output-root``.  It never
scans a bucket, promotes a Lamin artifact, or mutates an existing artifact.  Run
it through ``tools/pert_gym_vm_runner.py`` so the VM runner records the durable
heartbeat, child log, and single-writer preflight.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import anndata as ad
from scipy import sparse

from pert_gym.logical_sparse_zarr import write_logical_sparse_revision
from tools.pert_gym_vm_runner import require_heavy_vm


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-h5ad", type=Path, required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--logical-key", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--schema-fingerprint", required=True)
    parser.add_argument("--ingestion-run-id", required=True)
    parser.add_argument("--source-row-start", type=int, default=0)
    parser.add_argument("--max-rss-gib", type=float, default=4.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_rss_gib <= 0:
        raise ValueError("--max-rss-gib must be positive")
    require_heavy_vm()
    if not args.source_h5ad.is_file():
        raise FileNotFoundError(args.source_h5ad)
    source_checksum = f"sha256-file-bytes/v1:{sha256_file(args.source_h5ad)}"
    source = ad.read_h5ad(args.source_h5ad, backed="r")
    try:
        matrix = source.X
        if not sparse.isspmatrix_csr(matrix) and not sparse.isspmatrix_csc(matrix):
            raise TypeError(
                "source X must already be CSR or CSC; conversion is explicit and out of scope"
            )
        manifest = write_logical_sparse_revision(
            root=args.output_root,
            logical_key=args.logical_key,
            revision=args.revision,
            matrix=matrix,
            obs=source.obs.copy(),
            var=source.var.copy(),
            schema_fingerprint=args.schema_fingerprint,
            source_uri=args.source_uri,
            source_checksum=source_checksum,
            source_row_start=args.source_row_start,
            ingestion_run_id=args.ingestion_run_id,
            max_rss_bytes=int(args.max_rss_gib * 1024**3),
        )
    finally:
        source.file.close()
    print(
        json.dumps(
            {
                "revision": args.revision,
                "shape": manifest["shape"],
                "nnz": manifest["nnz"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
