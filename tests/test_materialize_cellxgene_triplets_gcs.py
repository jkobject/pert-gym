from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import anndata as ad
import h5py
import numpy as np
import pandas as pd
import pytest
from scipy import sparse

ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "tools/materialize_cellxgene_triplets_gcs.py"
LEGACY_PLAN = (
    ROOT
    / "artifacts/evidence/human-fetal-retina-temporal-v4-025-t_8b1ae292/execution-plan.json"
)


def load_module():
    spec = importlib.util.spec_from_file_location(
        "materialize_cellxgene_triplets_gcs", SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeBlob:
    def __init__(
        self, bucket: FakeBucket, name: str, generation: int | None = None
    ) -> None:
        self.bucket = bucket
        self.name = name
        self.requested_generation = generation
        self.generation: int | None = generation
        self.size: int | None = None

    def exists(self) -> bool:
        return self.name in self.bucket.objects

    def reload(self) -> None:
        if self.name not in self.bucket.objects:
            raise RuntimeError(f"missing object: {self.name}")
        generation, payload = self.bucket.objects[self.name]
        if (
            self.requested_generation is not None
            and generation != self.requested_generation
        ):
            raise RuntimeError(f"missing generation: {self.requested_generation}")
        self.generation = generation
        self.size = len(payload)

    def upload_from_filename(self, path: Path, **kwargs: object) -> None:
        assert kwargs["if_generation_match"] == 0
        if self.name in self.bucket.objects:
            raise RuntimeError("precondition failed")
        self.bucket.next_generation += 1
        self.bucket.objects[self.name] = (
            self.bucket.next_generation,
            Path(path).read_bytes(),
        )
        self.bucket.uploads.append(self.name)

    def download_to_filename(self, path: Path, **kwargs: object) -> None:
        generation, payload = self.bucket.objects[self.name]
        assert kwargs["if_generation_match"] == generation
        if self.requested_generation is not None:
            assert self.requested_generation == generation
        Path(path).write_bytes(payload)


class FakeBucket:
    name = "test-bucket"

    def __init__(self) -> None:
        self.objects: dict[str, tuple[int, bytes]] = {}
        self.uploads: list[str] = []
        self.next_generation = 100

    def blob(self, name: str, generation: int | None = None) -> FakeBlob:
        return FakeBlob(self, name, generation)


def publication_inputs(tmp_path: Path) -> dict[str, tuple[str, Path]]:
    inputs = {}
    for stage in ("obs", "X", "var", "manifest"):
        path = tmp_path / stage
        path.write_bytes(f"immutable-{stage}".encode())
        inputs[stage] = (f"prefix/{stage}", path)
    return inputs


def test_legacy_recovery_authorization_binds_exact_plan_bytes() -> None:
    module = load_module()
    plan = json.loads(LEGACY_PLAN.read_text())
    plan["logical_key"] = "drifted/logical-key"

    with pytest.raises(RuntimeError, match="writer bytes do not match"):
        module.check_plan(plan, "0" * 64)


@pytest.mark.parametrize("stage", ("obs", "X", "var", "manifest"))
@pytest.mark.parametrize("crash_window", ("remote", "journal"))
def test_publication_resumes_every_stage_without_overwrite(
    tmp_path: Path, stage: str, crash_window: str
) -> None:
    module = load_module()
    bucket = FakeBucket()
    inputs = publication_inputs(tmp_path)
    kwargs = {f"stop_after_{crash_window}_stage": stage}

    with pytest.raises(
        RuntimeError, match=f"intentional crash after {crash_window} {stage}"
    ):
        module.publish_create_only_stages(
            bucket=bucket,
            journal_path=tmp_path / "publication-journal.json",
            identity={"plan_sha256": "a" * 64},
            stage_names=("obs", "X", "var", "manifest"),
            stage_inputs=inputs,
            **kwargs,
        )

    identities = module.publish_create_only_stages(
        bucket=bucket,
        journal_path=tmp_path / "publication-journal.json",
        identity={"plan_sha256": "a" * 64},
        stage_names=("obs", "X", "var", "manifest"),
        stage_inputs=inputs,
    )

    assert bucket.uploads == [
        "prefix/obs",
        "prefix/X",
        "prefix/var",
        "prefix/manifest",
    ]
    assert list(identities) == ["obs", "X", "var", "manifest"]
    journal = json.loads((tmp_path / "publication-journal.json").read_text())
    assert journal["completed_stages"] == ["obs", "X", "var", "manifest"]
    assert set(journal["objects"]) == {"obs", "X", "var", "manifest"}


def test_publication_adopts_matching_legacy_partial_prefix(tmp_path: Path) -> None:
    module = load_module()
    bucket = FakeBucket()
    inputs = publication_inputs(tmp_path)
    for stage in ("obs", "X"):
        name, path = inputs[stage]
        bucket.blob(name).upload_from_filename(path, if_generation_match=0)

    module.publish_create_only_stages(
        bucket=bucket,
        journal_path=tmp_path / "publication-journal.json",
        identity={"plan_sha256": "a" * 64},
        stage_names=("obs", "X", "var", "manifest"),
        stage_inputs=inputs,
    )

    assert bucket.uploads == [
        "prefix/obs",
        "prefix/X",
        "prefix/var",
        "prefix/manifest",
    ]


def test_manifest_last_revalidates_journaled_payloads_without_local_files(
    tmp_path: Path,
) -> None:
    module = load_module()
    bucket = FakeBucket()
    inputs = publication_inputs(tmp_path)
    module.publish_create_only_stages(
        bucket=bucket,
        journal_path=tmp_path / "publication-journal.json",
        identity={"plan_sha256": "a" * 64},
        stage_names=("obs", "X", "var", "manifest"),
        stage_inputs=inputs,
        through_stage="var",
    )
    for stage in ("obs", "X", "var"):
        inputs[stage][1].unlink()

    identities = module.publish_create_only_stages(
        bucket=bucket,
        journal_path=tmp_path / "publication-journal.json",
        identity={"plan_sha256": "a" * 64},
        stage_names=("obs", "X", "var", "manifest"),
        stage_inputs=inputs,
    )

    assert list(identities) == ["obs", "X", "var", "manifest"]
    assert bucket.uploads[-1] == "prefix/manifest"


@pytest.mark.parametrize("conflict", ("identity", "generation", "checksum", "hole"))
def test_publication_drift_and_conflicts_fail_before_new_upload(
    tmp_path: Path, conflict: str
) -> None:
    module = load_module()
    bucket = FakeBucket()
    inputs = publication_inputs(tmp_path)
    with pytest.raises(RuntimeError, match="intentional crash after journal obs"):
        module.publish_create_only_stages(
            bucket=bucket,
            journal_path=tmp_path / "publication-journal.json",
            identity={"plan_sha256": "a" * 64},
            stage_names=("obs", "X", "var", "manifest"),
            stage_inputs=inputs,
            stop_after_journal_stage="obs",
        )
    uploads_before = list(bucket.uploads)
    identity = {"plan_sha256": "a" * 64}
    if conflict == "identity":
        identity = {"plan_sha256": "b" * 64}
    elif conflict == "generation":
        _, payload = bucket.objects["prefix/obs"]
        bucket.objects["prefix/obs"] = (999, payload)
    elif conflict == "checksum":
        generation, _ = bucket.objects["prefix/obs"]
        bucket.objects["prefix/obs"] = (generation, b"tampered")
    else:
        del bucket.objects["prefix/obs"]
        bucket.objects["prefix/X"] = (999, inputs["X"][1].read_bytes())

    with pytest.raises(RuntimeError):
        module.publish_create_only_stages(
            bucket=bucket,
            journal_path=tmp_path / "publication-journal.json",
            identity=identity,
            stage_names=("obs", "X", "var", "manifest"),
            stage_inputs=inputs,
        )

    assert bucket.uploads == uploads_before


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
