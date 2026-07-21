from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
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

EVIDENCE_DIR = PUBLISHER_PATH.parent
REVISION_RECEIPT_PATH = EVIDENCE_DIR / "revision_receipt_t_1f13aa0c_r2.json"
REVISION_HANDOFF_PATH = EVIDENCE_DIR / "integrated_handoff_t_1f13aa0c_r2.json"


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


def test_triplet_keys_agree_across_publisher_receipt_and_revision_handoff() -> None:
    receipt = json.loads(REVISION_RECEIPT_PATH.read_text())
    handoff = json.loads(REVISION_HANDOFF_PATH.read_text())
    expected = {
        "obs": "DRUG-seq/GSE120222/obs.parquet",
        "x": "DRUG-seq/GSE120222/X.h5ad",
        "var": "DRUG-seq/GSE120222/var.parquet",
    }

    assert {role: receipt["triplet"][role]["key"] for role in expected} == expected
    assert {role: handoff["triplet"][role]["key"] for role in expected} == expected
    assert {
        "obs": publisher.OBS_KEY,
        "x": publisher.X_KEY,
        "var": publisher.VAR_KEY,
    } == expected
    assert (
        handoff["evidence"]["publisher_sha256"]
        == hashlib.sha256(PUBLISHER_PATH.read_bytes()).hexdigest()
    )
    assert (
        handoff["evidence"]["revision_receipt_sha256"]
        == hashlib.sha256(REVISION_RECEIPT_PATH.read_bytes()).hexdigest()
    )


def test_revision_evidence_preserves_historical_receipts_by_exact_hash() -> None:
    receipt = json.loads(REVISION_RECEIPT_PATH.read_text())

    for historical in receipt["historical_immutable_receipts"].values():
        path = Path(__file__).parents[1] / historical["path"]
        assert historical["modified"] is False
        assert historical["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def _valid_obs() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "control_availability": ["dataset_control_available"] * 2,
            "dose": [0.0, 1.0],
            "modality": ["bulk_RNA"] * 2,
            "perturbation": ["DMSO", "trametinib"],
            "perturbation_type": ["drug"] * 2,
            "source": ["DRUG-seq"] * 2,
            "source_accession": ["GSE120222"] * 2,
            "timepoint": [6.0] * 2,
        }
    )


def test_obs_verifier_records_every_canonical_field_disposition() -> None:
    result = publisher.verify_obs_metadata(_valid_obs())

    assert set(result["field_dispositions"]) == set(publisher.CANONICAL_OBS_FIELDS)
    assert result["field_dispositions"]["source"]["expected"] == ["DRUG-seq"]
    assert result["field_dispositions"]["source_accession"]["expected"] == ["GSE120222"]
    assert result["field_dispositions"]["organism"]["source_expected"] == [
        "Homo sapiens"
    ]
    assert result["field_dispositions"]["cell_line"]["source_expected"] == ["U2OS"]
    assert result["field_dispositions"]["x_semantics"]["disposition"] == "unknown"


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("source", "not-DRUG-seq"),
        ("source_accession", "GSE000000"),
        ("modality", "image"),
        ("perturbation_type", "CRISPRko"),
        ("control_availability", "strict_control_available"),
        ("perturbation", ""),
    ],
)
def test_obs_verifier_rejects_wrong_but_non_null_scientific_metadata(
    field: str, wrong_value: object
) -> None:
    obs = _valid_obs()
    obs.loc[0, field] = wrong_value

    with pytest.raises(AssertionError, match="OBS metadata drift"):
        publisher.verify_obs_metadata(obs)


def _valid_var() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ensembl_gene_id": [
                "ENSG00000123456",
                "ENSG00000123457",
                None,
            ],
            "stable_feature_id": [
                "ENSG00000123456",
                "ENSG00000123457",
                "ERCC-00002",
            ],
            "stable_feature_id_namespace": [
                "Ensembl stable gene ID",
                "Ensembl stable gene ID",
                "ERCC stable spike-in ID",
            ],
            "stable_feature_id_mapping_status": [
                "mapped",
                "mapped",
                "not_applicable",
            ],
            "feature_type": ["gene", "gene", "spike-in"],
            "organism": ["Homo sapiens", "Homo sapiens", "not_applicable"],
        },
        index=["ENSG00000123456", "ENSG00000123457", "ERCC-00002"],
    )


def test_var_verifier_binds_human_ensembl_and_ercc_semantics() -> None:
    result = publisher.verify_var_metadata(
        _valid_var(), expected_ensembl_rows=2, expected_ercc_rows=1
    )

    assert result["human_ensembl_rows"] == 2
    assert result["ercc_rows"] == 1
    assert result["unique_stable_feature_ids"] == 3
    assert result["namespace_disposition"] == {
        "human_ensembl": "Ensembl stable gene ID",
        "ercc": "ERCC stable spike-in ID",
    }
    assert result["organism_disposition"] == {
        "human_ensembl": "Homo sapiens",
        "ercc": "not_applicable",
    }


@pytest.mark.parametrize(
    ("column", "row", "wrong_value"),
    [
        ("ensembl_gene_id", 0, "ENSMUSG00000123456"),
        ("organism", 0, "Mus musculus"),
        (
            "stable_feature_id_namespace",
            0,
            "garbage Ensembl garbage",
        ),
        ("stable_feature_id", 2, "ERCC_BAD"),
        ("stable_feature_id_mapping_status", 0, "not_applicable"),
        ("stable_feature_id_mapping_status", 2, "mapped"),
    ],
)
def test_var_verifier_rejects_malformed_or_wrong_organism_values(
    column: str, row: int, wrong_value: str
) -> None:
    var = _valid_var()
    var.iloc[row, var.columns.get_loc(column)] = wrong_value

    with pytest.raises(AssertionError, match="VAR metadata drift"):
        publisher.verify_var_metadata(
            var, expected_ensembl_rows=2, expected_ercc_rows=1
        )


def test_var_verifier_rejects_duplicate_stable_feature_ids() -> None:
    var = _valid_var()
    var.loc[var.index[1], "stable_feature_id"] = var.loc[
        var.index[0], "stable_feature_id"
    ]

    with pytest.raises(AssertionError, match="VAR metadata drift"):
        publisher.verify_var_metadata(
            var, expected_ensembl_rows=2, expected_ercc_rows=1
        )
