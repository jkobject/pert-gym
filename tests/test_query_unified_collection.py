from pathlib import Path

from tools.query_unified_collection import (
    filter_members,
    find_control_datasets,
    get_dataset_members,
    inspect_harmonization,
    list_datasets,
    load_unified_manifest,
)

FIXTURE_MANIFEST = Path(__file__).parent / "fixtures" / "unified_collection_manifest_minimal.tsv"


def load_fixture_manifest():
    return load_unified_manifest(FIXTURE_MANIFEST)


def test_manifest_filters_public_and_addition_examples():
    manifest = load_fixture_manifest()

    public = filter_members(
        manifest,
        source="DRUG-seq",
        modality="bulk_RNA",
        perturbation_type="drug",
        collection_category="base_public",
    )
    assert len(public) == 1
    assert public.iloc[0].artifact_key == "DRUG-seq/GSE120222/obs.parquet"

    prism = filter_members(
        manifest,
        source="PRISM",
        modality="scRNA-seq",
        perturbation_type="CRISPR",
        collection_category="additions",
        prefix_contains="GSE225775",
    )
    assert len(prism) == 272
    assert prism.n_obs.sum() == 1_356_998


def test_list_datasets_and_dataset_members_chunk_family():
    manifest = load_fixture_manifest()
    datasets = list_datasets(manifest)

    gse225775 = datasets.loc[datasets.dataset_id == "prism_collection/GSE225775"]
    assert len(gse225775) == 1
    assert int(gse225775.iloc[0].members) == 272
    assert int(gse225775.iloc[0].total_obs) == 1_356_998

    members = get_dataset_members("viperturb/genome_wide_binA", manifest)
    assert len(members) > 1
    assert members.iloc[0].artifact_key.endswith("chunk_0000/obs.parquet")
    assert members.chunk_index.is_monotonic_increasing


def test_harmonization_counts_are_manifest_consistent():
    manifest = load_fixture_manifest()
    harmonization = inspect_harmonization(manifest)

    assert harmonization.members.sum() == len(manifest)
    assert set(harmonization.collection_category) == {"additions", "base_public"}
    assert set(manifest.harmonization_level) == {"triplet-integrity-ok"}

    # The 20260621 manifest is conservative: control availability is mostly
    # unknown, and concrete control checks should inspect selected obs payloads.
    controls = find_control_datasets(manifest)
    assert set(controls.artifact_key).issubset(set(manifest.artifact_key))
