#!/usr/bin/env python3
"""Read-only real Lamin smoke for the public logical compatibility loader."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
from scipy import sparse

from pert_gym.logical_dataset import open_logical_dataset
from pert_gym.sparse_zarr_contract import LEGACY_FORMAT
from tools.lamin_context import connect_pertdata

DEFAULT_PREFIX = "temporal_pretraining/stomics/hesta/CS12-13_E2_3D/chunk_0000"


def _latest(ln: Any, key: str) -> Any:
    records = list(ln.Artifact.filter(key=key).all())
    if not records:
        raise KeyError(f"missing Lamin artifact {key!r}")
    return sorted(
        records,
        key=lambda artifact: (
            bool(getattr(artifact, "is_latest", False)),
            str(getattr(artifact, "created_at", "")),
            str(getattr(artifact, "uid", "")),
        ),
    )[-1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read two rows from a real tiny jkobject-branch Lamin triplet"
    )
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    ln = connect_pertdata()
    artifacts = {
        role: _latest(ln, f"{args.prefix}/{role}")
        for role in ("obs.parquet", "X.h5ad", "var.parquet")
    }
    paths = {role: Path(artifact.cache()) for role, artifact in artifacts.items()}
    source = ad.read_h5ad(paths["X.h5ad"], backed="r")
    try:
        matrix = source.X
        shape = tuple(matrix.shape)
        sparse_format = getattr(matrix, "format", None)
        if sparse_format not in {"csr", "csc"}:
            probe = matrix[:1]
            if sparse.isspmatrix_csr(probe) or isinstance(probe, np.ndarray):
                sparse_format = "csr"
            elif sparse.isspmatrix_csc(probe):
                sparse_format = "csc"
            else:
                raise TypeError(f"unsupported tiny Lamin X payload: {type(probe)!r}")
        nnz = int(getattr(matrix, "nnz", getattr(matrix, "_indptr", [0])[-1]))
    finally:
        source.file.close()

    manifest = {
        "format": LEGACY_FORMAT,
        "version": 1,
        "n_obs": shape[0],
        "n_vars": shape[1],
        "nnz": nnz,
        "sparse_format": sparse_format,
        "x_key": str(paths["X.h5ad"]),
        "obs_key": str(paths["obs.parquet"]),
        "var_key": str(paths["var.parquet"]),
    }
    dataset = open_logical_dataset(manifest, name=args.prefix)
    batch = dataset.read(rows=slice(0, min(2, shape[0])))
    payload = {
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "dataset": args.prefix,
        "artifact_uids": {
            role: str(artifact.uid) for role, artifact in artifacts.items()
        },
        "artifact_keys": {
            role: str(artifact.key) for role, artifact in artifacts.items()
        },
        "dataset_shape": list(dataset.shape),
        "batch_shape": list(batch.X.shape),
        "batch_nnz": int(batch.X.nnz),
        "obs_index": batch.obs.index.astype(str).tolist(),
        "var_index_head": batch.var.index.astype(str).tolist()[:5],
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
