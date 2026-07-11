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
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--source-h5ad", type=Path)
    source.add_argument(
        "--legacy-prefix",
        help="Same-prefix Lamin legacy triplet family, ending before /chunk_NNNN.",
    )
    parser.add_argument("--source-uri")
    parser.add_argument("--legacy-cache-dir", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--logical-key", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--schema-fingerprint", required=True)
    parser.add_argument("--ingestion-run-id", required=True)
    parser.add_argument("--source-row-start", type=int, default=0)
    parser.add_argument("--max-rss-gib", type=float, default=4.0)
    parser.add_argument(
        "--publish-collection-key",
        help="Append-only candidate Collection key; omitted means local candidate only.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_rss_gib <= 0:
        raise ValueError("--max-rss-gib must be positive")
    require_heavy_vm()
    if args.source_h5ad is not None:
        if not args.source_h5ad.is_file():
            raise FileNotFoundError(args.source_h5ad)
        if not args.source_uri:
            raise ValueError("--source-uri is required with --source-h5ad")
        source_checksum = f"sha256-file-bytes/v1:{sha256_file(args.source_h5ad)}"
        source = ad.read_h5ad(args.source_h5ad, backed="r")
        try:
            manifest = write_logical_sparse_revision(
                root=args.output_root,
                logical_key=args.logical_key,
                revision=args.revision,
                matrix=source.X,
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
    else:
        from pert_gym.legacy_triplet_adapter import (
            build_legacy_revision,
            resolve_legacy_triplets,
        )
        from tools.lamin_context import connect_pertdata

        if args.legacy_cache_dir is None:
            raise ValueError("--legacy-cache-dir is required with --legacy-prefix")
        ln = connect_pertdata()
        ln.track(path=__file__)
        triplets = resolve_legacy_triplets(
            ln=ln, prefix=args.legacy_prefix, cache_dir=args.legacy_cache_dir
        )
        manifest = build_legacy_revision(
            root=args.output_root,
            logical_key=args.logical_key,
            revision=args.revision,
            triplets=triplets,
            schema_fingerprint=args.schema_fingerprint,
            ingestion_run_id=args.ingestion_run_id,
            max_rss_bytes=int(args.max_rss_gib * 1024**3),
        )
    publication = None
    if args.publish_collection_key:
        from pert_gym.logical_sparse_publication import publish_candidate
        from tools.lamin_context import connect_pertdata

        publication = publish_candidate(
            ln=connect_pertdata(),
            root=args.output_root,
            logical_key=args.logical_key,
            revision=args.revision,
            collection_key=args.publish_collection_key,
            require_vm=require_heavy_vm,
        )
    print(
        json.dumps(
            {
                "revision": args.revision,
                "shape": manifest["shape"],
                "nnz": manifest["nnz"],
                "publication": publication,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
