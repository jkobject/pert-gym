from __future__ import annotations

import importlib.util
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

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
