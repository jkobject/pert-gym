#!/usr/bin/env python3
"""EU-VM-only local candidate builder for PerturbAI sparse-row parquet files.

This tool performs no remote write unless ``--publish-collection-key`` is given.
That optional path delegates unchanged to PR #53's journaled publication API.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from pert_gym.logical_sparse_publication import publish_candidate
from pert_gym.perturbai_sparse_parquet import (
    REQUESTER_PAYS_PROJECT,
    PerturbAISource,
    build_perturbai_revision,
    requester_pays_storage_options,
)
from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import lamin_writer_lease, require_heavy_vm


def _var(path: Path) -> pd.DataFrame:
    genes = pd.read_parquet(path)
    required = {"gene_token_id", "gene_name", "gene_ids"}
    missing = sorted(required - set(genes.columns))
    if missing:
        raise ValueError(f"gene metadata missing columns: {missing}")
    genes = genes.copy()
    raw_token_ids = genes["gene_token_id"]
    max_token_id = np.iinfo(np.int64).max
    token_ids: list[int] = []
    for value in raw_token_ids:
        if pd.isna(value):
            raise ValueError("gene_token_id must be non-null and finite")
        if isinstance(value, (bool, np.bool_)):
            raise ValueError("gene_token_id must not be boolean")
        if isinstance(value, (int, np.integer)):
            token_id = int(value)
        elif isinstance(value, (float, np.floating)):
            float_value = float(value)
            if not np.isfinite(float_value):
                raise ValueError("gene_token_id must be finite")
            if not float_value.is_integer():
                raise ValueError("gene_token_id must be integer-valued")
            token_id = int(float_value)
        else:
            raise ValueError("gene_token_id must be numeric")
        if token_id < 0:
            raise ValueError("gene_token_id must be non-negative")
        if token_id > max_token_id:
            raise ValueError("gene_token_id outside supported range")
        token_ids.append(token_id)
    if len(set(token_ids)) != len(token_ids):
        raise ValueError("gene_token_id values must be unique; duplicate found")
    token_ids_array = np.asarray(token_ids, dtype=np.int64)
    genes["gene_token_id"] = token_ids_array
    genes = genes.sort_values("gene_token_id")
    if not np.array_equal(genes["gene_token_id"].to_numpy(), np.arange(len(genes))):
        raise ValueError("gene_token_id is not contiguous 0..n_vars-1")
    result = genes.set_index("gene_ids", drop=False)
    result.index.name = "gene_id"
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-parquet", type=Path, action="append", required=True)
    parser.add_argument("--source-uri", action="append", required=True)
    parser.add_argument("--source-object-id", action="append", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--gene-metadata", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--logical-key", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--schema-fingerprint", required=True)
    parser.add_argument("--ingestion-run-id", required=True)
    parser.add_argument("--billing-project", required=True)
    parser.add_argument("--parquet-batch-rows", type=int, default=10_000)
    parser.add_argument("--max-rss-gib", type=float, default=4.0)
    parser.add_argument("--publish-collection-key")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    require_heavy_vm()
    requester_pays_storage_options(args.billing_project)
    if args.max_rss_gib <= 0 or args.parquet_batch_rows <= 0:
        raise ValueError("memory and parquet batch limits must be positive")
    if not (
        len(args.source_parquet) == len(args.source_uri) == len(args.source_object_id)
    ):
        raise ValueError(
            "source parquet, URI, and object-id arguments must have equal counts"
        )
    sources = tuple(
        PerturbAISource(
            stem=path.stem,
            source_uri=uri,
            source_commit=args.source_commit,
            source_object_id=object_id,
            local_path=path,
        )
        for path, uri, object_id in zip(
            args.source_parquet, args.source_uri, args.source_object_id, strict=True
        )
    )
    manifest = build_perturbai_revision(
        root=args.output_root,
        logical_key=args.logical_key,
        revision=args.revision,
        sources=sources,
        var=_var(args.gene_metadata),
        schema_fingerprint=args.schema_fingerprint,
        ingestion_run_id=args.ingestion_run_id,
        max_rss_bytes=int(args.max_rss_gib * 1024**3),
        parquet_batch_rows=args.parquet_batch_rows,
    )
    publication = None
    if args.publish_collection_key:
        with lamin_writer_lease(run_id=args.ingestion_run_id):
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
                "billing_project": REQUESTER_PAYS_PROJECT,
                "shape": manifest["shape"],
                "nnz": manifest["nnz"],
                "publication": publication,
                "revision": args.revision,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
