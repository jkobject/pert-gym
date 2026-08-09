from __future__ import annotations

import importlib.util
import io
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

SCRIPT = Path(
    "artifacts/dataset_completion/temporal__organoiddb_odd001111_gse130238/complete_dataset.py"
)
MANIFEST = SCRIPT.with_name("source_manifest.json")


def load_module():
    spec = importlib.util.spec_from_file_location("complete_gse130238", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def baseline_obs() -> pd.DataFrame:
    index = pd.Index(
        [
            "GSM3734294:AAACCTGAGAAACGCC-1",
            "GSM3734295:AAACCTGAGACAAAGG-1",
            "GSM3734296:AAACCTGAGATGTCGG-1",
            "GSM3734297:AAACCTGAGCACCGCT-1",
        ],
        name="cell_id",
    )
    return pd.DataFrame(
        {
            "development_stage": ["1 month", "3 month", "6 month", "10 month"],
            "is_control": [True, True, True, True],
            "sample_accession": [
                "GSM3734294",
                "GSM3734295",
                "GSM3734296",
                "GSM3734297",
            ],
            "sample_title": [
                "One-Month-Old Human Cortical Organoids",
                "Three-Month-Old Human Cortical Organoids",
                "Six-Month-Old Human Cortical Organoids",
                "Ten-Month-Old Human Cortical Organoids",
            ],
            "source_cell_barcode": [
                "AAACCTGAGAAACGCC-1",
                "AAACCTGAGACAAAGG-1",
                "AAACCTGAGATGTCGG-1",
                "AAACCTGAGCACCGCT-1",
            ],
            "source_cell_line": ["WT - iPSC"] * 4,
            "source_passage": ["P7211"] * 4,
            "source_file": [
                "GSM3734294_1M_cortical_organoids_matrix.mtx.gz",
                "GSM3734295_3M_cortical_organoids_matrix.mtx.gz",
                "GSM3734296_6M_cortical_organoids_matrix.mtx.gz",
                "GSM3734297_10M_cortical_organoids_matrix.mtx.gz",
            ],
            "timepoint": pd.Series([1, 3, 6, 10], index=index, dtype="Int64"),
            "timepoint_unit": pd.Series(["month"] * 4, index=index, dtype="string"),
        },
        index=index,
    )


def test_curate_obs_materializes_source_backed_temporal_contract() -> None:
    module = load_module()
    module.EXPECTED_N_OBS = 4

    curated, receipt = module.curate_obs(baseline_obs())

    assert curated.index.tolist() == baseline_obs().index.tolist()
    assert curated["sample"].tolist() == [
        "GSM3734294",
        "GSM3734295",
        "GSM3734296",
        "GSM3734297",
    ]
    assert curated["timepoint"].tolist() == [43830, 131490, 262980, 438300]
    assert curated["timepoint_original_value"].tolist() == [1, 3, 6, 10]
    assert curated["timepoint_original_unit"].eq("month").all()
    assert curated["is_baseline"].tolist() == [True, False, False, False]
    assert curated["trajectory_id"].nunique() == 1
    assert curated["cell_type"].isna().all()
    assert curated["cell_type_state"].eq("unknown").all()
    assert curated["obs_uuid"].is_unique
    assert receipt["OBS_COMPLETED"] is True
    assert receipt["experimental_axes"]["biological_time"]["source_values"] == [
        1,
        3,
        6,
        10,
    ]
    assert (
        receipt["experimental_axes"]["biological_time"]["normalization_convention"]
        == "mean Gregorian month = 365.25/12 days = 43,830 minutes"
    )
    assert receipt["cell_type_evidence"]["state"] == "unknown"
    for field in module.CANONICAL_FIELDS:
        assert field in curated
        assert f"{field}_state" in curated
        assert f"{field}_source" in curated


def test_curate_obs_rejects_incomplete_time_axis() -> None:
    module = load_module()
    module.EXPECTED_N_OBS = 4
    baseline = baseline_obs()
    baseline["timepoint"] = pd.Series([1, 3, 6, 9], index=baseline.index, dtype="Int64")

    with pytest.raises(AssertionError, match="source month axis drift"):
        module.curate_obs(baseline)


def test_var_gate_is_species_and_stable_id_aware() -> None:
    module = load_module()
    module.EXPECTED_N_VARS = 4
    ids = [f"ENSG{i:011d}" for i in range(1, 5)]
    var = pd.DataFrame(
        {
            "feature_id": ids,
            "feature_namespace": ["Ensembl gene ID"] * 4,
            "organism": ["Homo sapiens"] * 4,
        },
        index=pd.Index(ids),
    )

    x_receipt = {
        "shape": [4, 4],
        "var_axis_sha256_ordered": module.ordered_sha256(ids),
    }
    receipt = module.verify_var(var, x_receipt)

    assert receipt["status"] == "PASS"
    assert receipt["VAR_ENSEMBL_SPECIES_COMPLETED"] is True
    var.loc[ids[-1], "organism"] = "Mus musculus"
    with pytest.raises(AssertionError, match="VAR Ensembl/species gate failed"):
        module.verify_var(var, x_receipt)


def test_var_gate_rejects_ordered_axis_drift() -> None:
    module = load_module()
    module.EXPECTED_N_VARS = 2
    ids = ["ENSG00000000001", "ENSG00000000002"]
    var = pd.DataFrame(
        {
            "feature_id": ids,
            "feature_namespace": ["Ensembl"] * 2,
            "organism": ["Homo sapiens"] * 2,
        },
        index=pd.Index(ids),
    )

    with pytest.raises(AssertionError, match="VAR Ensembl/species gate failed"):
        module.verify_var(
            var,
            {
                "shape": [2, 2],
                "var_axis_sha256_ordered": module.ordered_sha256(reversed(ids)),
            },
        )


def test_lifecycle_lease_requires_exact_task_purpose_and_fresh_deadline(
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
        "purpose": "gse130238-mutate",
        "task": "t-9b5c70a6",
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


def test_source_manifest_binds_geo_supplements_and_accepted_axes() -> None:
    payload = json.loads(MANIFEST.read_text())

    assert payload["task_id"] == "t_9b5c70a6"
    assert payload["source_identity"] == {
        "geo_accession": "GSE130238",
        "organoiddb_id": "ODD001111",
        "pmid": "31474560",
        "pmcid": "PMC6778040",
        "doi": "10.1016/j.stem.2019.08.002",
    }
    assert len(payload["supplementary_files"]) == 12
    assert (
        sum(name.endswith("_matrix.mtx.gz") for name in payload["supplementary_files"])
        == 4
    )
    assert payload["expected"]["n_obs"] == sum(
        payload["expected"]["sample_cell_counts"].values()
    )
    assert payload["expected"]["obs_index_sha256_ordered"] == (
        "ef29dab9266c41eee78de1343c07d9f9f78a383157e2ea1c7484cdbee9cd304c"
    )
    assert payload["expected"]["var_index_sha256_ordered"] == (
        "7eb86babb45d8bfedc121c1d964603857d4b54d062de98bd2f84739a284ce304"
    )


def test_ordered_sha256_is_length_delimited_and_order_sensitive() -> None:
    module = load_module()
    assert module.ordered_sha256(["ab", "c"]) != module.ordered_sha256(["a", "bc"])
    assert module.ordered_sha256(["a", "b"]) != module.ordered_sha256(["b", "a"])


def test_obs_axis_gate_rejects_reordering_against_x() -> None:
    module = load_module()
    index = pd.Index(["cell-a", "cell-b"])
    x_receipt = {"obs_axis_sha256_ordered": module.ordered_sha256(index)}

    assert (
        module.verify_obs_x_axis(index, x_receipt)
        == x_receipt["obs_axis_sha256_ordered"]
    )
    with pytest.raises(AssertionError, match="ordered OBS axis"):
        module.verify_obs_x_axis(index[::-1], x_receipt)


def test_recovery_requires_revision_stem_and_exact_authorized_description() -> None:
    module = load_module()
    description = module.expected_obs_description("frame", "helper")
    accepted = SimpleNamespace(
        uid=f"{module.BASELINE_OBS_UID[:16]}0001", description=description
    )

    assert module.is_authorized_obs_revision(accepted, description)
    assert not module.is_authorized_obs_revision(
        SimpleNamespace(uid="foreignStem000000001", description=description),
        description,
    )
    assert not module.is_authorized_obs_revision(
        SimpleNamespace(uid=accepted.uid, description=description + " drift"),
        description,
    )


def test_distributed_lease_heartbeat_renews_and_surfaces_failure() -> None:
    module = load_module()

    class Lease:
        held = True

        def __init__(self, *, fail_after: int | None = None) -> None:
            self.renewals = 0
            self.fail_after = fail_after

        def renew(self) -> None:
            self.renewals += 1
            if self.fail_after is not None and self.renewals >= self.fail_after:
                self.held = False
                raise RuntimeError("lost lease")

    healthy = Lease()
    with module.DistributedLeaseHeartbeat(healthy, interval_seconds=0.01) as heartbeat:
        deadline = time.monotonic() + 1.0
        while healthy.renewals < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        heartbeat.assert_healthy()
    assert healthy.renewals >= 2

    failing = Lease(fail_after=2)
    with pytest.raises(RuntimeError, match="heartbeat failed"):
        with module.DistributedLeaseHeartbeat(
            failing, interval_seconds=0.01
        ) as heartbeat:
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                time.sleep(0.01)
                heartbeat.assert_healthy()
            pytest.fail("heartbeat failure was not surfaced before the deadline")


def test_materialize_artifact_streams_remote_path_not_lamin_cache(
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
    destination = tmp_path / "readback" / "payload.parquet"

    result = module.materialize_artifact(artifact, destination)

    assert result == destination
    assert destination.read_bytes() == b"fresh remote payload"


def test_materialize_artifact_pins_gcs_generation_and_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_module()
    commands: list[list[str]] = []

    def fake_run(command: list[str], *, check: bool) -> None:
        assert check
        commands.append(command)
        Path(command[-1]).write_bytes(b"frozen bytes")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    artifact = SimpleNamespace(path="gs://scperturb/data/payload.parquet")
    destination = tmp_path / "payload.parquet"

    module.materialize_artifact(
        artifact,
        destination,
        {"generation": 123456, "size": len(b"frozen bytes")},
    )

    assert commands == [
        [
            "gcloud",
            "storage",
            "cp",
            "--billing-project",
            module.BILLING_PROJECT,
            "gs://scperturb/data/payload.parquet#123456",
            str(destination),
        ]
    ]

    with pytest.raises(AssertionError, match="generation-pinned artifact size drift"):
        module.materialize_artifact(
            artifact,
            tmp_path / "wrong-size.parquet",
            {"generation": 123456, "size": 1},
        )
