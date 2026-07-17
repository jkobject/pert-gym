from __future__ import annotations

import importlib.util
from pathlib import Path

import anndata as ad
import h5py
import numpy as np
import pandas as pd
from scipy import sparse

ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "tools/materialize_cellxgene_triplets_gcs.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "materialize_cellxgene_triplets_gcs", SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_x_only_h5ad_chunk_copy_preserves_sparse_matrix_and_axes(
    tmp_path: Path,
) -> None:
    module = load_module()
    source = tmp_path / "source.h5ad"
    output = tmp_path / "X.h5ad"
    matrix = sparse.csr_matrix(
        np.array([[0.0, 2.0, 0.0], [1.0, 0.0, 3.0]], dtype=np.float32)
    )
    obs = pd.DataFrame(
        {"nullable": pd.Series([1, None], dtype="Int64")}, index=["c1", "c2"]
    )
    var = pd.DataFrame({"feature_name": ["A", "B", "C"]}, index=["g1", "g2", "g3"])
    ad.AnnData(X=matrix, obs=obs, var=var).write_h5ad(source, compression="gzip")

    shape, size = module.write_x_only_h5ad(source, output)

    assert shape == [2, 3]
    assert size == output.stat().st_size > 0
    result = ad.read_h5ad(output)
    np.testing.assert_array_equal(result.X.toarray(), matrix.toarray())
    assert list(result.obs_names) == ["c1", "c2"]
    assert list(result.var_names) == ["g1", "g2", "g3"]
    assert result.obs.empty
    assert result.var.empty
    with h5py.File(output, "r") as handle:
        assert handle["X"].attrs["encoding-type"] == "csr_matrix"


def test_frame_inventory_records_complete_schema_nulls_and_ordered_index() -> None:
    module = load_module()
    index = pd.Index(["r1", "r2", "r3"], name="cell_id")
    frame = pd.DataFrame(
        {
            "category": pd.Series(pd.Categorical(["a", None, "b"]), index=index),
            "nullable": pd.Series([1, None, 3], dtype="Int64", index=index),
        },
        index=index,
    )

    inventory = module.frame_inventory(frame)

    assert inventory["rows"] == 3
    assert inventory["index_name"] == "cell_id"
    assert inventory["index_unique"] is True
    assert inventory["total_null_count"] == 2
    assert inventory["columns"] == [
        {"name": "category", "dtype": "category", "null_count": 1},
        {"name": "nullable", "dtype": "Int64", "null_count": 1},
    ]
    assert len(inventory["ordered_index_sha256"]) == 64


def test_value_null_parity_allows_categorical_encoding_to_primitive() -> None:
    module = load_module()
    index = pd.Index(["r1", "r2", "r3"])
    source = pd.DataFrame(
        {"feature_length": pd.Categorical([8, None, 13])}, index=index
    )
    physical = pd.DataFrame(
        {"feature_length": pd.Series([8, None, 13], dtype="Int64", index=index)}
    )

    module.assert_frame_value_null_parity(source, physical, "var")


def test_shared_var_identity_binds_ordered_ids_organism_and_namespace() -> None:
    module = load_module()
    var = pd.DataFrame(index=pd.Index(["ENSG2", "ENSG1"], name="feature_id"))

    identity = module.shared_var_identity(
        var,
        organism="NCBITaxon:9606",
        feature_namespace="cellxgene_feature_id",
    )

    assert identity["organism"] == "NCBITaxon:9606"
    assert identity["feature_namespace"] == "cellxgene_feature_id"
    assert identity["ordered_var_identifiers_sha256"] == module.ordered_index_sha256(
        var.index
    )
    assert len(identity["sha256"]) == 64
    reversed_identity = module.shared_var_identity(
        var.iloc[::-1],
        organism="NCBITaxon:9606",
        feature_namespace="cellxgene_feature_id",
    )
    assert reversed_identity["sha256"] != identity["sha256"]
