from uuid import NAMESPACE_URL, uuid5

import pandas as pd
import pytest

from pert_gym.obs_identity import add_obs_identity, validate_obs_identity


def test_add_obs_identity_preserves_prism_chunk_original_index_and_stable_uuid():
    obs = pd.DataFrame(
        {
            "cell_line": ["A375", "A549"],
            "drug": ["trametinib", "paclitaxel"],
        },
        index=pd.Index(["well_A01", "well_A02"], name="source_well"),
    )

    with_identity = add_obs_identity(
        obs,
        dataset_id="prism_collection/GSE221321",
        prefix="prism_collection/GSE221321/chunk_0042",
        source_accession="GSE221321",
        chunk_id="chunk_0042",
    )
    rerun = add_obs_identity(
        obs,
        dataset_id="prism_collection/GSE221321",
        prefix="prism_collection/GSE221321/chunk_0042",
        source_accession="GSE221321",
        chunk_id="chunk_0042",
    )

    assert list(with_identity["original_obs_index"]) == ["well_A01", "well_A02"]
    assert with_identity.index.name == "source_well"
    expected_material = "pert-gym.obs.v1:prism_collection/GSE221321:prism_collection/GSE221321/chunk_0042:well_A01"
    assert with_identity["obs_uuid"].iloc[0] == "1433fed0-04e9-59d4-837b-e9a0e99152c9"
    assert with_identity["obs_uuid"].iloc[0] == str(
        uuid5(NAMESPACE_URL, expected_material)
    )
    assert with_identity["obs_uuid"].tolist() == rerun["obs_uuid"].tolist()
    assert with_identity["obs_uuid"].is_unique
    assert with_identity["original_obs_index_is_duplicated"].tolist() == [False, False]
    validate_obs_identity(with_identity)


def test_add_obs_identity_namespaces_rows_by_prefix_not_optional_components():
    obs = pd.DataFrame(
        {
            "cell_barcode": ["AAAC-1", "AAAC-2", "TTTG-1"],
            "sample_id": ["donor1", "donor1", "donor2"],
            "perturbation": ["ctrl", "KRAS", "ctrl"],
        },
        index=[101, 102, 103],
    )

    with_identity = add_obs_identity(
        obs,
        dataset_id="viperturb/genome_wide_filtered",
        prefix="viperturb/genome_wide_filtered/chunk_0001",
        source_accession="VIPerturbSeq",
        sample_column="sample_id",
        barcode_column="cell_barcode",
        chunk_id="chunk_0001",
    )
    different_chunk = add_obs_identity(
        obs,
        dataset_id="viperturb/genome_wide_filtered",
        prefix="viperturb/genome_wide_filtered/chunk_0002",
        source_accession="VIPerturbSeq",
        sample_column="sample_id",
        barcode_column="cell_barcode",
        chunk_id="chunk_0002",
    )

    assert list(with_identity["original_obs_index"]) == ["101", "102", "103"]
    assert with_identity["obs_uuid"].iloc[0] == "aa9b4900-56a3-5315-9f32-c4e5cd4ef7c8"
    assert with_identity["obs_uuid"].is_unique
    assert with_identity["obs_uuid"].tolist() != different_chunk["obs_uuid"].tolist()
    validate_obs_identity(with_identity)


def test_add_obs_identity_supports_non_scrna_table_rows_without_barcodes():
    obs = pd.DataFrame(
        {
            "depmap_id": ["ACH-000001", "ACH-000002"],
            "gene_symbol": ["KRAS", "BRAF"],
            "dependency_score": [-0.8, -0.1],
        }
    )

    with_identity = add_obs_identity(
        obs,
        dataset_id="depmap/avana_2024_q4",
        prefix="depmap/avana_2024_q4/gene_dependency",
        sample_column="depmap_id",
        row_kind="essentiality_row",
    )

    assert list(with_identity["original_obs_index"]) == ["0", "1"]
    assert with_identity["obs_uuid"].iloc[0] == "bdf45d95-4be6-5b71-8310-0acccb7f1e69"
    assert with_identity["obs_uuid"].is_unique
    validate_obs_identity(with_identity)


def test_add_obs_identity_appends_row_position_only_for_duplicate_original_indices():
    obs = pd.DataFrame(
        {"sample_id": ["donor1", "donor1", "donor2"]}, index=["same", "same", "other"]
    )

    with_identity = add_obs_identity(
        obs,
        dataset_id="prism_collection/GSE221321",
        prefix="prism_collection/GSE221321/chunk_0042",
        source_accession="GSE221321",
        sample_column="sample_id",
        chunk_id="chunk_0042",
    )

    expected_duplicate_material = "pert-gym.obs.v1:prism_collection/GSE221321:prism_collection/GSE221321/chunk_0042:same:0"
    expected_unique_material = "pert-gym.obs.v1:prism_collection/GSE221321:prism_collection/GSE221321/chunk_0042:other"
    assert with_identity["original_obs_index"].tolist() == ["same", "same", "other"]
    assert with_identity["original_obs_index_is_duplicated"].tolist() == [
        True,
        True,
        False,
    ]
    assert with_identity["obs_uuid"].iloc[0] == "102e795e-1d59-5c5b-b236-4bc3b5559cba"
    assert with_identity["obs_uuid"].iloc[0] == str(
        uuid5(NAMESPACE_URL, expected_duplicate_material)
    )
    assert with_identity["obs_uuid"].iloc[2] == str(
        uuid5(NAMESPACE_URL, expected_unique_material)
    )
    assert with_identity["obs_uuid"].is_unique


def test_validate_obs_identity_rejects_missing_and_duplicate_values():
    missing = pd.DataFrame({"obs_uuid": ["x"], "value": [1]})
    with pytest.raises(ValueError, match="original_obs_index"):
        validate_obs_identity(missing)

    duplicate = pd.DataFrame(
        {"obs_uuid": ["same", "same"], "original_obs_index": ["0", "1"]}
    )
    with pytest.raises(ValueError, match="unique"):
        validate_obs_identity(duplicate)
