from __future__ import annotations

import importlib.util
import io
import json
import tarfile
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import zarr
from anndata import AnnData

SCRIPT = Path(
    "artifacts/dataset_completion/temporal__perturbase_gse107185/complete_dataset.py"
)
MANIFEST = SCRIPT.with_name("source_manifest.json")
SEQUENCES = SCRIPT.with_name("orf_sequences.json")
HGNC = SCRIPT.with_name("hgnc_symbol_mappings.json")


def load_module():
    spec = importlib.util.spec_from_file_location("complete_gse107185", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def baseline_obs() -> pd.DataFrame:
    index = pd.Index(["cell-a", "cell-b", "cell-c"], name="obs_id")
    return pd.DataFrame(
        {
            "gene": pd.Series(["CTRL", "KLF4", "ETV2"], index=index, dtype="string"),
            "batch": pd.Series(
                ["sample1", "sample2", "sample2"], index=index, dtype="string"
            ),
            "media": pd.Series(
                ["hPSC", "endothelial", "multilineage"], index=index, dtype="string"
            ),
            "cell_id": pd.Series(index.astype(str), index=index, dtype="string"),
            "n_genes": [1000, 2000, 3000],
            "total_counts": [5000.0, 9000.0, 12000.0],
            "pct_counts_mt": [1.0, 2.0, 3.0],
            "cell_type": pd.Series(
                ["hPSC", "endothelial", "multilineage"], index=index, dtype="string"
            ),
        },
        index=index,
    )


def sequence_payload() -> dict:
    return {
        "used_by_component": ["CTRL", "KLF4", "ETV2"],
        "sequences": {
            "CTRL": {"sequence": "ATGAAA"},
            "KLF4": {"sequence": "ATGCCC"},
            "ETV2": {"sequence": "ATGGGG"},
        },
    }


def test_curate_obs_materializes_source_backed_endpoint_contract() -> None:
    module = load_module()
    module.EXPECTED_N_OBS = 3

    curated, receipt = module.curate_obs(baseline_obs(), sequence_payload())

    assert curated.index.tolist() == ["cell-a", "cell-b", "cell-c"]
    assert curated["timepoint"].tolist() == [7200, 8640, 8640]
    assert curated["timepoint_original_value"].tolist() == [5, 6, 6]
    assert curated["trajectory_id_state"].eq("not_applicable").all()
    assert curated["is_baseline"].eq(False).all()
    assert curated["sex"].eq("male").all()
    assert curated["cell_type"].isna().all()
    assert curated["cell_type_state"].eq("unknown").all()
    assert curated["source_original_cell_type"].tolist() == [
        "hPSC",
        "endothelial",
        "multilineage",
    ]
    assert curated["guide_sequence_state"].eq("not_applicable").all()
    assert curated["molecule_sequence"].tolist() == ["ATGAAA", "ATGCCC", "ATGGGG"]
    assert (
        curated["response_metric"]
        .eq("genotype-level relative fitness log2 fold-change")
        .all()
    )
    assert curated["response_value"].isna().all()
    assert curated["is_low_quality"].eq(False).all()
    assert curated["obs_uuid"].is_unique
    assert receipt["OBS_COMPLETED"] is True
    assert (
        receipt["scientific_modality"]
        == "pooled ORF-overexpression single-cell RNA-seq expression endpoint"
    )
    assert receipt["annotation_level"] == {
        "expression": "cell",
        "perturbation": "cell via PerturBase construct assignment",
        "fitness": "aggregate genotype/sample only; not joined per cell",
    }
    assert (
        receipt["experimental_axes"]["biological_time"]["verdict"]
        == "endpoint_duration_by_media_not_shared_trajectory"
    )
    for field in module.CANONICAL_FIELDS:
        assert field in curated
        assert f"{field}_state" in curated
        assert f"{field}_source" in curated


def test_curate_obs_rejects_incomplete_publication_sequence_join() -> None:
    module = load_module()
    module.EXPECTED_N_OBS = 3
    payload = sequence_payload()
    payload["used_by_component"] = ["CTRL", "KLF4"]

    with pytest.raises(AssertionError, match="perturbation set drift"):
        module.curate_obs(baseline_obs(), payload)


def test_exact_component_membership_and_library_label_are_not_invented() -> None:
    module = load_module()
    payload = json.loads(SEQUENCES.read_text())
    labels = payload["used_by_component"]
    index = pd.Index([f"cell-{i}" for i in range(len(labels))], name="obs_id")
    baseline = pd.DataFrame(
        {
            "gene": pd.Series(labels, index=index, dtype="string"),
            "batch": pd.Series(["sample2"] * len(labels), index=index, dtype="string"),
            "media": pd.Series(["hPSC"] * len(labels), index=index, dtype="string"),
            "cell_id": pd.Series(index.astype(str), index=index, dtype="string"),
            "n_genes": pd.Series([1000] * len(labels), index=index, dtype="Int64"),
            "total_counts": pd.Series([5000.0] * len(labels), index=index),
            "pct_counts_mt": pd.Series([1.0] * len(labels), index=index),
        },
        index=index,
    )
    setattr(module, "EXPECTED_N_OBS", len(labels))

    curated, _ = module.curate_obs(baseline, payload)

    assert len(labels) == 61
    assert labels.count("CTRL") == 1
    assert len(set(labels) - {"CTRL"}) == 60
    assert "HNF4A" not in labels
    assert curated["perturbation_library"].eq(module.PERTURBATION_LIBRARY).all()
    assert (
        module.PERTURBATION_LIBRARY
        == "PerturBase extend_61 component: 60 TF ORFs plus mCherry control; HNF4A absent"
    )


def test_curate_var_preserves_axis_and_disposes_stable_ids() -> None:
    module = load_module()
    module.EXPECTED_N_VARS = 3
    index = pd.Index(["KNOWN", "OLD", "UNRESOLVED"], name="var_id")
    baseline = pd.DataFrame(
        {
            "gene_symbol": pd.Series(index.astype(str), index=index, dtype="string"),
            "ENSEMBL": pd.Series(
                ["ENSG00000000001", pd.NA, pd.NA], index=index, dtype="string"
            ),
            "organism": pd.Series(["Homo sapiens"] * 3, index=index, dtype="string"),
            "feature_namespace": pd.Series(["source"] * 3, index=index, dtype="string"),
        },
        index=index,
    )
    mappings = {
        "source": {"sha256": "abc"},
        "input_unresolved_symbols": 2,
        "unique_mappings": 1,
        "residual_unknown_symbols": ["UNRESOLVED"],
        "ambiguous_symbols": {},
        "mappings": {
            "OLD": {
                "ensembl_gene_id": "ENSG00000000002",
                "approved_symbol": "CURRENT",
            }
        },
    }

    curated, receipt = module.curate_var(baseline, mappings)

    assert curated.index.equals(index)
    assert curated["stable_feature_id"].tolist()[:2] == [
        "ENSG00000000001",
        "ENSG00000000002",
    ]
    assert pd.isna(curated.loc["UNRESOLVED", "stable_feature_id"])
    assert curated["stable_feature_id_state"].tolist() == [
        "present",
        "derived",
        "unknown",
    ]
    assert curated.loc["OLD", "approved_gene_symbol"] == "CURRENT"
    assert receipt["status"] == "PASS"
    assert receipt["source_exact_ensembl"] == 1
    assert receipt["hgnc_recovered"] == 1
    assert receipt["residual_unknown"] == 1


def test_source_manifest_binds_component_supplements_and_axes() -> None:
    manifest = json.loads(MANIFEST.read_text())
    sequences = json.loads(SEQUENCES.read_text())
    hgnc = json.loads(HGNC.read_text())

    assert manifest["task_id"] == "t_ab37edf6"
    assert manifest["source_identity"] == {
        "bioproject": "PRJNA419230",
        "doi": "10.1016/j.cels.2018.10.008",
        "geo_accession": "GSE107185",
        "perturbase_repository_id": "11",
        "pmcid": "PMC6311450",
        "pmid": "30448000",
        "source_component": "extend_61",
    }
    assert manifest["expected"]["n_obs"] == 8428
    assert manifest["expected"]["n_vars"] == 2000
    assert manifest["expected"]["source_exact_ensembl_features"] == 1639
    assert manifest["expected"]["hgnc_recovered_features"] == 81
    assert manifest["expected"]["residual_unknown_features"] == 280
    assert manifest["scientific_contract"] == {
        "annotation_level": {
            "expression": "cell",
            "fitness": "aggregate genotype/sample only; not joined per cell",
            "perturbation": "cell via PerturBase construct assignment",
        },
        "outcomes_endpoints": {
            "expression": "processed PerturBase single-cell matrix",
            "fitness": "known aggregate endpoint; cell-level value unknown because no exact join",
        },
        "scientific_modality": "pooled ORF-overexpression single-cell RNA-seq expression endpoint",
        "temporal_classification": "day 5/6 media-specific endpoint durations; not a shared trajectory",
    }
    assert len(manifest["geo_metadata_supplements"]) == 5
    assert len(manifest["publication_supplements"]) == 6
    assert len(sequences["used_by_component"]) == 61
    assert sequences["unused_source_entries"] == ["HNF4A"]
    assert all(
        item["sequence_length_nt"] == len(item["sequence"])
        for item in sequences["sequences"].values()
    )
    assert hgnc["unique_mappings"] == 81
    assert hgnc["input_unresolved_symbols"] == 361
    assert len(hgnc["residual_unknown_symbols"]) == 279
    assert set(hgnc["ambiguous_symbols"]) == {"LOR"}


def test_lifecycle_lease_requires_exact_task_purpose_and_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_module()
    capacity = SimpleNamespace(
        project="jkobject-1549353370965",
        instance="pert-gym-worker-eu",
        zone="europe-west1-b",
    )
    labels = {
        "owner": "jkobject",
        "project": "pert-gym",
        "purpose": "gse107185-mutate",
        "task": "t-ab37edf6",
        "lease-until": "20991231t235959z",
    }
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps({"labels": labels})),
    )

    assert module.verify_lifecycle_lease(capacity, "mutate") == labels
    labels["task"] = "t-deadbeef"
    with pytest.raises(RuntimeError, match="bounded lifecycle lease mismatch"):
        module.verify_lifecycle_lease(capacity, "mutate")


def test_materialize_artifact_streams_remote_without_lamin_cache(
    tmp_path: Path,
) -> None:
    module = load_module()

    class RemotePath:
        def __str__(self) -> str:
            return "s3://lamindata-eu-central-1/dataset/payload.parquet"

        def open(self, mode: str):
            assert mode == "rb"
            return io.BytesIO(b"fresh remote payload")

    artifact = SimpleNamespace(
        path=RemotePath(),
        cache=lambda: (_ for _ in ()).throw(AssertionError("cache must not be used")),
    )
    destination = tmp_path / "payload.parquet"

    assert module.materialize_artifact(artifact, destination) == destination
    assert destination.read_bytes() == b"fresh remote payload"


def test_ordered_sha256_is_order_sensitive() -> None:
    module = load_module()
    assert module.ordered_sha256(["ab", "c"]) != module.ordered_sha256(["a", "bc"])
    assert module.ordered_sha256(["a", "b"]) != module.ordered_sha256(["b", "a"])


def test_task_revision_adopts_matching_frame_across_helper_changes() -> None:
    module = load_module()
    artifact = SimpleNamespace(
        uid="NMGDnN2AjT75B2lr0001",
        description=(
            "t_ab37edf6: GSE107185 OBS_COMPLETED; "
            "frame_sha256=correct-frame; helper_sha256=old-helper"
        ),
    )

    assert module.is_task_revision(artifact, module.BASELINE_OBS_UID, "obs")
    assert module.is_authorized_revision(
        artifact, module.BASELINE_OBS_UID, "obs", "correct-frame"
    )
    assert not module.is_authorized_revision(
        artifact, module.BASELINE_OBS_UID, "obs", "replacement-frame"
    )


def test_collection_fix_replaces_prior_task_obs_and_replays_without_writes() -> None:
    module = load_module()

    class Query:
        def __init__(self, records):
            self.records = records

        def all(self):
            return list(self.records)

    class Artifact:
        def __init__(self, uid: str, key: str):
            self.uid = uid
            self.key = key

    class CollectionRecord:
        def __init__(self, members, *, uid, key, description, created_at):
            self.uid = uid
            self.key = key
            self.description = description
            self.created_at = created_at
            self.hash = f"hash-{uid}"
            self.is_latest = True
            self.artifacts = Query(members)

        def save(self):
            return self

    class CollectionAPI:
        def __init__(self, records):
            self.records = records

        def filter(self, *, artifacts=None, is_latest=None, key=None):
            records = self.records
            if artifacts is not None:
                records = [
                    record
                    for record in records
                    if artifacts in record.artifacts.records
                ]
            if is_latest is not None:
                records = [
                    record for record in records if record.is_latest == is_latest
                ]
            if key is not None:
                records = [record for record in records if record.key == key]
            return Query(records)

        def __call__(self, members, *, key, description, skip_hash_lookup):
            assert skip_hash_lookup is True
            record = CollectionRecord(
                members,
                uid="successor-uid",
                key=key,
                description=description,
                created_at="2026-08-01T23:00:00Z",
            )

            def save():
                self.records.append(record)
                return record

            record.save = save
            return record

    previous_obs = Artifact("NMGDnN2AjT75B2lr0001", module.OBS_KEY)
    corrected_obs = Artifact("NMGDnN2AjT75B2lr0002", module.OBS_KEY)
    other = Artifact("other-uid", "other/dataset/obs.parquet")
    predecessor = CollectionRecord(
        [previous_obs, other],
        uid="predecessor-uid",
        key="pert-gym/additions/20260730-gse107185-e2e",
        description="prior task publication",
        created_at="2026-07-30T00:00:00Z",
    )
    api = CollectionAPI([predecessor])
    ln = SimpleNamespace(Collection=api)

    successor, created, receipt = module.ensure_successor_collection(
        ln, previous_obs, corrected_obs, allow_create=True
    )

    assert created is True
    assert successor.key == module.SUCCESSOR_COLLECTION_KEY
    assert [item.uid for item in successor.artifacts.all()] == [
        "other-uid",
        corrected_obs.uid,
    ]
    assert receipt["predecessor_uid"] == predecessor.uid
    assert receipt["target_obs_uid"] == corrected_obs.uid
    assert receipt["duplicate_keys"] == 0

    replay, replay_created, replay_receipt = module.ensure_successor_collection(
        ln, previous_obs, corrected_obs, allow_create=False
    )
    assert replay is successor
    assert replay_created is False
    assert replay_receipt == receipt
    assert len(api.records) == 2


def test_ncbi_authority_request_declares_tool_and_contact() -> None:
    module = load_module()

    request = module.authority_request(
        "https://ftp.ncbi.nlm.nih.gov/geo/example/file.txt"
    )

    assert "tool=pert-gym" in request.full_url
    assert "email=jkobject%40gmail.com" in request.full_url
    assert request.headers["User-agent"] == "pert-gym/1.0 (jkobject@gmail.com)"


def test_open_authority_retries_ncbi_rate_limit() -> None:
    module = load_module()
    attempts = iter(
        [
            urllib.error.HTTPError("https://example.test", 403, "rate", None, None),
            SimpleNamespace(),
        ]
    )
    sleeps: list[int] = []

    def opener(request, timeout):
        assert timeout == 180
        result = next(attempts)
        if isinstance(result, Exception):
            raise result
        return result

    response = module.open_authority(
        module.authority_request("https://example.test"),
        opener=opener,
        sleep=sleeps.append,
    )

    assert isinstance(response, SimpleNamespace)
    assert sleeps == [5]


def test_inspect_zarr_x_accepts_external_axis_csr_contract(tmp_path: Path) -> None:
    module = load_module()
    module.EXPECTED_N_OBS = 2
    module.EXPECTED_N_VARS = 3
    path = tmp_path / "X.zarr.zip"
    with zarr.storage.ZipStore(str(path), mode="w") as store:
        root = zarr.group(store=store)
        root.attrs.update(
            {"format": "csr_matrix", "shape": [2, 3], "nnz": 2, "dtype": "float32"}
        )
        root.create_dataset("data", data=[1.0, -2.0], dtype="float32")
        root.create_dataset("indices", data=[0, 2], dtype="int32")
        root.create_dataset("indptr", data=[0, 1, 2], dtype="int32")

    receipt = module.inspect_zarr_x(
        path, {"nnz": 2, "x_dtype": "float32", "x_sum": -1.0}
    )

    assert receipt["status"] == "PASS"
    assert receipt["shape"] == [2, 3]
    assert receipt["checks"]["csr_arrays"] is True
    assert "external" in receipt["axis_contract"]


def test_inspect_source_reads_generation_bound_archive_member(tmp_path: Path) -> None:
    module = load_module()
    module.EXPECTED_N_OBS = 3
    module.EXPECTED_N_VARS = 2
    baseline = baseline_obs()
    var = pd.DataFrame(index=pd.Index(["gene-a", "gene-b"], name="var_id"))
    member_path = tmp_path / "mixscape_hvg_filter.h5ad"
    AnnData(
        X=np.array([[1.0, 0.0], [0.0, -2.0], [3.0, 4.0]], dtype=np.float32),
        obs=baseline[["gene", "batch", "media"]].astype(object),
        var=var,
    ).write_h5ad(member_path)
    archive_path = tmp_path / "extend_61.filter.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(member_path, arcname=member_path.name)
    source_spec = {
        "archive_member": {
            "name": member_path.name,
            "sha256": module.sha256_file(member_path),
            "size": member_path.stat().st_size,
        },
        "member_nnz": 4,
        "member_sum": 6.0,
    }

    receipt = module.inspect_source(archive_path, baseline, var, source_spec)

    assert receipt["status"] == "PASS"
    assert receipt["logical_component"] == "extend_61"
    assert receipt["archive_member"] == "mixscape_hvg_filter.h5ad"
    assert receipt["shape"] == [3, 2]
    assert receipt["nnz"] == 4
