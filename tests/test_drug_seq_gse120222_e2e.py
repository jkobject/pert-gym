from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import numpy as np
from scipy import sparse

PUBLISHER_PATH = (
    Path(__file__).parents[1]
    / "artifacts/schema_audit/dataset_e2e_20260721/drug_seq_gse120222/t_3d9bf0d8/publish_collections.py"
)
SPEC = importlib.util.spec_from_file_location(
    "drug_seq_gse120222_publisher", PUBLISHER_PATH
)
assert SPEC is not None and SPEC.loader is not None
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


def test_count_nnz_supports_dense_and_sparse_matrices() -> None:
    dense = np.array([[0.0, 2.0], [3.0, 0.0]])

    assert publisher.count_nnz(dense) == 2
    assert publisher.count_nnz(sparse.csr_matrix(dense)) == 2


def test_dataset_description_binds_exact_triplet_and_denominator() -> None:
    description = json.loads(publisher.dataset_description())

    assert description["dataset_id"] == "DRUG-seq/GSE120222"
    assert description["real_dataset_id"] == "drug-seq/GSE120222"
    assert description["source_rows_total"] == 72
    assert description["obs_uid"] == "mKSaEEcH4jyes43Z0002"
    assert description["x_uid"] == "hfeVCMInQu1UKhwp0000"
    assert description["var_uid"] == "vmqp94W72a1Tl2Xw0002"


def test_global_description_binds_exact_predecessor() -> None:
    description = json.loads(publisher.global_description("a" * 64))

    assert description["predecessor_uid"] == "qoTeH7T78kjbmIWA0000"
    assert description["member_count_before"] == 1017
    assert description["member_count_after"] == 1018
    assert description["resulting_membership_sha256"] == "a" * 64
