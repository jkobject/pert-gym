from __future__ import annotations

from pathlib import Path

import anndata as ad
import numpy as np
import pandas as pd
import pytest
from scipy import sparse

from pert_gym.legacy_triplet_adapter import LegacyTriplet, build_legacy_revision
from pert_gym.logical_sparse_zarr import read_logical_sparse_revision


def _triplet(
    root: Path, chunk: int, *, var: pd.DataFrame | None = None
) -> LegacyTriplet:
    prefix = root / f"chunk_{chunk:04d}"
    prefix.mkdir(parents=True)
    obs = pd.DataFrame({"source": [chunk, chunk]}, index=[f"o{chunk}a", f"o{chunk}b"])
    var = (
        var
        if var is not None
        else pd.DataFrame({"kind": ["gene", "gene"]}, index=["g1", "g2"])
    )
    matrix = sparse.csr_matrix(
        np.array([[chunk + 1, 0], [0, chunk + 2]], dtype=np.float32)
    )
    obs.to_parquet(prefix / "obs.parquet")
    ad.AnnData(X=matrix, var=var).write_h5ad(prefix / "X.h5ad")
    var.to_parquet(prefix / "var.parquet")
    return LegacyTriplet(
        chunk_id=chunk,
        obs_path=prefix / "obs.parquet",
        x_path=prefix / "X.h5ad",
        var_path=prefix / "var.parquet",
        obs_artifact_id=f"obs-{chunk}",
        x_artifact_id=f"x-{chunk}",
        var_artifact_id=f"var-{chunk}",
        obs_key=f"family/chunk_{chunk:04d}/obs.parquet",
        x_key=f"family/chunk_{chunk:04d}/X.h5ad",
        var_key=f"family/chunk_{chunk:04d}/var.parquet",
    )


def test_legacy_adapter_orders_triplets_and_binds_identity(tmp_path: Path) -> None:
    late = _triplet(tmp_path, 1)
    early = _triplet(tmp_path, 0)
    manifest = build_legacy_revision(
        root=tmp_path / "out",
        logical_key="family",
        revision="r1",
        triplets=[late, early],
        schema_fingerprint="schema-v1",
        ingestion_run_id="test",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=2,
    )
    _, matrix, obs, _ = read_logical_sparse_revision(tmp_path / "out", "family", "r1")
    assert obs.index.tolist() == ["o0a", "o0b", "o1a", "o1b"]
    assert matrix.toarray().tolist() == [[1.0, 0.0], [0.0, 2.0], [2.0, 0.0], [0.0, 3.0]]
    assert manifest["source_identity"]["sources"][0]["chunk_id"] == 0


def test_legacy_adapter_rejects_missing_ids_and_var_drift(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="contiguous"):
        build_legacy_revision(
            root=tmp_path / "out",
            logical_key="family",
            revision="gap",
            triplets=[_triplet(tmp_path / "gap", 1)],
            schema_fingerprint="schema-v1",
            ingestion_run_id="test",
        )
    first = _triplet(tmp_path, 0)
    different = _triplet(
        tmp_path, 1, var=pd.DataFrame({"kind": ["gene", "other"]}, index=["g1", "g2"])
    )
    with pytest.raises(ValueError, match="var identity"):
        build_legacy_revision(
            root=tmp_path / "out",
            logical_key="family",
            revision="drift",
            triplets=[first, different],
            schema_fingerprint="schema-v1",
            ingestion_run_id="test",
        )
