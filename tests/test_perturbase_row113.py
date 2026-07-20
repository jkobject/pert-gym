from __future__ import annotations

import pandas as pd
import pytest

from tools.ingest_perturbase_row113 import (
    ALLOWED_COMPONENTS,
    localize_archive,
    standardize_obs,
    standardize_var,
    write_triplet,
)


def test_standardize_obs_derives_tf_orf_labels_and_timepoint_minutes() -> None:
    obs = pd.DataFrame(
        {
            "TF": ["TFORF1324-KLF17", "TFORF0816-LHX3"],
            "gene": ["KLF17", "LHX3"],
            "timept": ["day 4", "day 7"],
            "nCount_RNA": [10, 20],
            "nFeature_RNA": [5, 6],
            "percent_mito": [0.1, 0.2],
        },
        index=["cell-a", "cell-b"],
    )

    out = standardize_obs(obs, component="201218_RNA")

    assert out["perturbation"].tolist() == ["KLF17", "LHX3"]
    assert out["perturbation_target"].tolist() == ["KLF17", "LHX3"]
    assert out["guide_id"].tolist() == ["TFORF1324", "TFORF0816"]
    assert out["timepoint"].tolist() == [4 * 24 * 60, 7 * 24 * 60]
    assert out["perturbation_type"].eq("overexpression").all()
    assert out["modality"].eq("scRNA-seq").all()
    assert out["source_component"].eq("201218_RNA").all()
    assert out["cell_id"].tolist() == ["cell-a", "cell-b"]


def test_standardize_obs_preserves_source_gene_controls() -> None:
    obs = pd.DataFrame(
        {
            "TF": ["TFORF0001-SAFE", "TFORF1324-KLF17"],
            "gene": ["CTRL", "KLF17"],
        },
        index=["ctrl-cell", "tf-cell"],
    )

    out = standardize_obs(obs, component="201218_RNA")

    assert out["perturbation"].tolist() == ["CTRL", "KLF17"]
    assert out["perturbation_target"].tolist() == ["CTRL", "KLF17"]
    assert out["is_control"].tolist() == [True, False]


def test_standardize_obs_rejects_missing_perturbation_columns() -> None:
    with pytest.raises(ValueError, match="cannot derive perturbation labels"):
        standardize_obs(pd.DataFrame(index=["cell-a"]), component="201218_RNA")


def test_standardize_var_prefers_ensembl_gene_id_and_keeps_symbols() -> None:
    var = pd.DataFrame({"ENSEMBL": ["ENSG1"], "n_cells": [3]}, index=["AASS"])

    out = standardize_var(var)

    assert out.loc["AASS", "gene_symbol"] == "AASS"
    assert out.loc["AASS", "gene_id"] == "ENSG1"


def test_localize_archive_rejects_non_active_components(tmp_path) -> None:
    assert sorted(ALLOWED_COMPONENTS) == ["201218_RNA", "210322_TFAtlas"]
    with pytest.raises(ValueError, match="not allowed"):
        localize_archive("210715_combinatorial", str(tmp_path), tmp_path)


def test_write_triplet_bypasses_cross_key_hash_dedup_for_x(tmp_path) -> None:
    """An identical X payload still needs a new artifact under the requested prefix."""
    import anndata as ad
    import numpy as np
    from scipy import sparse

    class Features:
        def __init__(self) -> None:
            self.values = {}

        def set_values(self, values) -> None:
            self.values.update(values)

        def get_values(self):
            return self.values

    class Artifact:
        def __init__(self, key: str) -> None:
            self.key = key
            self.features = Features()

        def save(self):
            return self

    class Query:
        def exists(self) -> bool:
            return False

        def all(self):
            return []

    class ArtifactAPI:
        def filter(self, **_kwargs):
            return Query()

        def from_dataframe(self, _frame, *, key, **_kwargs):
            return Artifact(key)

        def from_anndata(self, _path, *, key, skip_hash_lookup=False, **_kwargs):
            return Artifact(key if skip_hash_lookup else "existing/identical/X.h5ad")

    class FakeLamin:
        Artifact = ArtifactAPI()

    chunk = ad.AnnData(
        X=sparse.csr_matrix(np.array([[1, 0]], dtype=np.int64)),
        obs=pd.DataFrame(index=["cell-1"]),
        var=pd.DataFrame(index=["gene-1", "gene-2"]),
    )

    result = write_triplet(
        FakeLamin(), prefix="new/prefix", chunk=chunk, overwrite=False
    )

    assert result["status"] == "ingested"
    assert result["prefix"] == "new/prefix"
