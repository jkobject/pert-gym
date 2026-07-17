#!/usr/bin/env python3
"""Regression checks for physical sparse encoding inspection."""
from __future__ import annotations

import importlib.util
import tempfile
from pathlib import Path

import anndata as ad
import h5py
import numpy as np
import pandas as pd
from scipy import sparse

ROOT = Path(__file__).parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_matrix(path: Path, matrix: sparse.spmatrix) -> None:
    ad.AnnData(
        X=matrix,
        obs=pd.DataFrame(index=["c0", "c1", "c2"]),
        var=pd.DataFrame(index=["g0", "g1"]),
    ).write_h5ad(path)


def main() -> int:
    builder = load_module("row95_builder", ROOT / "build_component.py")
    verifier = load_module("row95_verifier", ROOT / "verify_component.py")
    dense = np.array([[1, 0], [0, 2], [3, 4]], dtype=np.uint32)
    with tempfile.TemporaryDirectory(prefix="row95-sparse-regression-") as raw_tmp:
        tmp = Path(raw_tmp)
        for expected_format, matrix in (("csr", sparse.csr_matrix(dense)), ("csc", sparse.csc_matrix(dense))):
            path = tmp / f"{expected_format}.h5ad"
            write_matrix(path, matrix)
            producer = builder.matrix_inventory(path)
            consumer = verifier.matrix_inventory(path)
            assert producer == consumer
            assert producer["format"] == expected_format
            assert producer["physical_encoding_type"] == f"{expected_format}_matrix"
            major_axis = dense.shape[0] if expected_format == "csr" else dense.shape[1]
            assert producer["indptr_length"] == major_axis + 1
            assert producer["expected_indptr_length"] == major_axis + 1
        malformed = tmp / "malformed.h5ad"
        write_matrix(malformed, sparse.csc_matrix(dense))
        with h5py.File(malformed, "r+") as handle:
            handle["X"].attrs["encoding-type"] = "unsupported_matrix"
        try:
            verifier.matrix_inventory(malformed)
        except RuntimeError as exc:
            assert "unsupported physical sparse encoding" in str(exc)
        else:
            raise AssertionError("unsupported encoding did not fail closed")
    print("SPARSE_INVENTORY_REGRESSION_PASS csr+csc+unsupported")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
