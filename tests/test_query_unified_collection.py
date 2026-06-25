from pathlib import Path

import pandas as pd

from tools.query_unified_collection import (
    filter_members,
    find_control_datasets,
    get_dataset_members,
    get_triplet_artifacts,
    inspect_harmonization,
    list_datasets,
    load_unified_manifest,
    load_var_dataframe,
    validate_manifest_var_policy,
    validate_triplet_var_policy,
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

    # The fixture manifest is conservative: control availability is mostly
    # unknown, and concrete control checks should inspect selected obs payloads.
    controls = find_control_datasets(manifest)
    assert set(controls.artifact_key).issubset(set(manifest.artifact_key))


def test_legacy_manifest_gets_same_prefix_var_policy_defaults():
    manifest = load_fixture_manifest()

    assert {"var_key", "var_uid", "var_hash", "var_policy", "var_alias_group"}.issubset(
        manifest.columns
    )
    assert set(manifest.var_policy) == {"same_prefix"}
    assert manifest.same_prefix_var.all()

    row = manifest.loc[manifest.artifact_key == "DRUG-seq/GSE120222/obs.parquet"].iloc[0]
    assert row.var_key == "DRUG-seq/GSE120222/var.parquet"
    assert validate_manifest_var_policy(manifest).empty


def test_shared_alias_policy_allows_non_same_prefix_when_link_is_explicit():
    row = pd.DataFrame(
        [
            {
                "artifact_key": "family/chunk_0000/obs.parquet",
                "prefix": "family/chunk_0000",
                "has_x_var_link": True,
                "same_prefix_var": False,
                "var_policy": "shared_alias",
                "var_key": "family/var.h5ad",
                "var_hash": "",
            }
        ]
    )
    assert validate_manifest_var_policy(row).empty

    missing_link = row.copy()
    missing_link.loc[0, "has_x_var_link"] = False
    assert "missing X->var link" in " ".join(validate_manifest_var_policy(missing_link).reason)

    bad_same_prefix = row.copy()
    bad_same_prefix.loc[0, "var_policy"] = "same_prefix"
    violations = validate_manifest_var_policy(bad_same_prefix)
    assert not violations.empty
    assert "same_prefix_var" in " ".join(violations.reason)


class _FakeFeatures:
    def __init__(self, values):
        self._values = values

    def get_values(self):
        return self._values


class _FakeArtifact:
    def __init__(
        self,
        key,
        *,
        uid=None,
        hash=None,
        is_latest=True,
        created_at="2026-06-25 00:00:00+00:00",
        features=None,
    ):
        self.key = key
        self.uid = uid or key.replace("/", "_")
        self.hash = hash
        self.is_latest = is_latest
        self.created_at = created_at
        self.features = _FakeFeatures(features or {})

    def load(self):
        var = pd.DataFrame(index=["g1", "g2"])
        if self.key.endswith(".h5ad"):
            return type("FakeAnnData", (), {"var": var})()
        return var


class _FakeQuerySet:
    def __init__(self, records):
        self._records = records

    def all(self):
        return self._records


class _FakeArtifactManager:
    def __init__(self, records):
        self._records = records
        self._latest_records = {}
        for record in records:
            if record.is_latest:
                self._latest_records[record.key] = record

    def filter(self, *, key):
        return _FakeQuerySet([record for record in self._records if record.key == key])

    def get(self, *, key):
        return self._latest_records[key]


class _FakeLn:
    def __init__(self, records):
        self.Artifact = _FakeArtifactManager(records)


def test_triplet_resolution_uses_explicit_links_and_latest_same_key_artifact():
    stale_var = _FakeArtifact(
        "family/var.h5ad",
        uid="stale-varuid",
        hash="stale-hash",
        is_latest=False,
        created_at="2026-06-24 00:00:00+00:00",
    )
    latest_var = _FakeArtifact(
        "family/var.h5ad",
        uid="varuid",
        hash="hash1",
        is_latest=True,
        created_at="2026-06-25 00:00:00+00:00",
    )
    x = _FakeArtifact("family/chunk_0000/X.h5ad", features={"var": "family/var.h5ad"})
    obs = _FakeArtifact("family/chunk_0000/obs.parquet", features={"X": x})
    ln = _FakeLn([obs, x, stale_var, latest_var])

    triplet = get_triplet_artifacts(ln, obs.key)
    assert triplet.var.uid == "varuid"
    assert triplet.var.key == "family/var.h5ad"
    assert list(triplet.load_var_dataframe().index) == ["g1", "g2"]
    assert list(load_var_dataframe(latest_var).index) == ["g1", "g2"]

    row = pd.Series(
        {
            "artifact_key": obs.key,
            "prefix": "family/chunk_0000",
            "has_x_var_link": True,
            "same_prefix_var": False,
            "var_policy": "shared_exact_hash",
            "var_key": latest_var.key,
            "var_uid": "varuid",
            "var_hash": "hash1",
        }
    )
    result = validate_triplet_var_policy(ln, row)
    assert result == {
        "obs_key": obs.key,
        "x_key": x.key,
        "var_key": latest_var.key,
        "ok": True,
        "errors": [],
    }
