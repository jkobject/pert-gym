from __future__ import annotations

import json
from pathlib import Path

import anndata as ad
import numpy as np
import pandas as pd
import pytest
from scipy import sparse

from pert_gym import logical_sparse_zarr
from pert_gym.logical_sparse_zarr import (
    MigrationInterrupted,
    ResourceLimitExceeded,
    read_logical_sparse_revision,
    rollback_to_revision,
    shared_var_identity,
    write_logical_sparse_revision,
)

SHA256 = "a" * 64
SOURCE_CHECKSUM = f"sha256-file-bytes/v1:{SHA256}"


def fixture_data(rows: int = 9) -> tuple[sparse.csr_matrix, pd.DataFrame, pd.DataFrame]:
    matrix = sparse.csr_matrix(
        (
            np.arange(1, rows * 2 + 1, dtype=np.float32),
            (np.repeat(np.arange(rows), 2), np.tile(np.array([0, 2]), rows)),
        ),
        shape=(rows, 3),
    )
    obs = pd.DataFrame(
        {"cell": [f"cell-{index}" for index in range(rows)]},
        index=[f"o{index}" for index in range(rows)],
    )
    var = pd.DataFrame(
        {"feature_type": ["gene", "gene", "gene"]}, index=["g1", "g2", "g3"]
    )
    return matrix, obs, var


def write_candidate(
    root: Path, revision: str, *, matrix: sparse.csr_matrix | None = None
) -> dict[str, object]:
    default_matrix, obs, var = fixture_data()
    return write_logical_sparse_revision(
        root=root,
        logical_key="surfaces/example",
        revision=revision,
        matrix=default_matrix if matrix is None else matrix,
        obs=obs,
        var=var,
        schema_fingerprint="schema-v1",
        source_uri="gs://example/source.h5ad",
        source_checksum=SOURCE_CHECKSUM,
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=4,
    )


def test_shared_var_identity_is_order_sensitive_and_canonical() -> None:
    _, _, var = fixture_data()
    identity = shared_var_identity(var, schema_fingerprint="schema-v1")
    assert identity == shared_var_identity(var.copy(), schema_fingerprint="schema-v1")
    reordered = var.iloc[::-1].copy()
    assert shared_var_identity(reordered, schema_fingerprint="schema-v1") != identity
    assert shared_var_identity(var, schema_fingerprint="schema-v2") != identity


def test_writer_readback_exact_denominator_provenance_and_shared_var(
    tmp_path: Path,
) -> None:
    manifest = write_candidate(tmp_path, "r1")

    assert manifest["shape"] == [9, 3]
    assert manifest["nnz"] == 18
    assert [chunk["shape"][0] for chunk in manifest["chunks"]] == [3, 3, 3]
    assert [
        chunk["obs"]["provenance"]["source_row_start"] for chunk in manifest["chunks"]
    ] == [0, 3, 6]

    surface, matrix, obs, var = read_logical_sparse_revision(
        tmp_path, "surfaces/example", "r1"
    )
    assert surface.shape == (9, 3)
    assert matrix.nnz == 18
    assert obs.index.tolist() == [f"o{index}" for index in range(9)]
    assert var.index.tolist() == ["g1", "g2", "g3"]
    checkpoint = json.loads(
        (tmp_path / "surfaces/example/checkpoints/r1.json").read_text()
    )
    assert checkpoint["status"] == "completed"
    assert checkpoint["completed_chunks"] == [0, 1, 2]


def test_reader_accepts_separate_canonical_var_frame_identity(tmp_path: Path) -> None:
    """Sealed contracts may hash frame rows separately from the ordered var index."""
    write_candidate(tmp_path, "sealed-frame")
    candidate = tmp_path / "surfaces/example/revisions/sealed-frame"
    manifest_path = candidate / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    var = pd.read_parquet(tmp_path / manifest["shared_var"]["key"])
    manifest["shared_var"]["frame_sha256"] = (
        logical_sparse_zarr._canonical_var_frame_sha256(var)
    )
    manifest_path.write_text(json.dumps(manifest))

    surface, matrix, obs, restored_var = read_logical_sparse_revision(
        tmp_path, "surfaces/example", "sealed-frame"
    )

    assert surface.shape == (9, 3)
    assert matrix.nnz == 18
    assert len(obs) == 9
    assert restored_var.index.tolist() == ["g1", "g2", "g3"]


def test_resume_reuses_completed_payload_without_overwrite(tmp_path: Path) -> None:
    matrix, obs, var = fixture_data()
    kwargs = dict(
        root=tmp_path,
        logical_key="surfaces/example",
        revision="resume",
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint="schema-v1",
        source_uri="gs://example/source.h5ad",
        source_checksum=SOURCE_CHECKSUM,
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=4,
    )
    with pytest.raises(MigrationInterrupted):
        write_logical_sparse_revision(**kwargs, stop_after_chunks=1)
    first_chunk = (
        tmp_path / "surfaces/example/revisions/resume/chunks/chunk_000000.zarr/data/0"
    )
    before = first_chunk.read_bytes()
    checkpoint_path = tmp_path / "surfaces/example/checkpoints/resume.json"
    checkpoint = json.loads(checkpoint_path.read_text())
    checkpoint.pop("planning_status")
    checkpoint_path.write_text(json.dumps(checkpoint))

    write_logical_sparse_revision(**kwargs)

    assert first_chunk.read_bytes() == before
    _, restored, _, _ = read_logical_sparse_revision(
        tmp_path, "surfaces/example", "resume"
    )
    assert (restored != matrix).nnz == 0
    with pytest.raises(RuntimeError, match="already completed"):
        write_logical_sparse_revision(**kwargs)


@pytest.mark.parametrize("sparse_format", ["csr", "csc"])
def test_writer_accepts_bounded_backed_sparse_input_without_format_conversion(
    tmp_path: Path, sparse_format: str
) -> None:
    matrix, obs, var = fixture_data()
    source_path = tmp_path / f"source-{sparse_format}.h5ad"
    backed_matrix = matrix if sparse_format == "csr" else matrix.tocsc()
    ad.AnnData(X=backed_matrix, obs=obs, var=var).write_h5ad(source_path)
    source = ad.read_h5ad(source_path, backed="r")
    try:
        assert not sparse.isspmatrix(source.X)
        manifest = write_logical_sparse_revision(
            root=tmp_path / "output",
            logical_key="surfaces/backed",
            revision=sparse_format,
            matrix=source.X,
            obs=source.obs.copy(),
            var=source.var.copy(),
            schema_fingerprint="schema-v1",
            source_uri="file://source.h5ad",
            source_checksum=SOURCE_CHECKSUM,
            ingestion_run_id="backed-test",
            max_rss_bytes=10**12,
            min_rows=1,
            max_rows=4,
        )
    finally:
        source.file.close()

    assert manifest["sparse_format"] == sparse_format
    _, restored, restored_obs, _ = read_logical_sparse_revision(
        tmp_path / "output", "surfaces/backed", sparse_format
    )
    assert restored.format == sparse_format
    assert (restored != backed_matrix).nnz == 0
    assert restored_obs.index.tolist() == obs.index.tolist()


def test_resume_rejects_changed_provenance_and_metadata_identity(
    tmp_path: Path,
) -> None:
    matrix, obs, var = fixture_data()
    kwargs = dict(
        root=tmp_path,
        logical_key="surfaces/example",
        revision="resume-identity",
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint="schema-v1",
        source_uri="gs://example/u1.h5ad",
        source_checksum=SOURCE_CHECKSUM,
        source_row_start=0,
        ingestion_run_id="run1",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=4,
    )
    with pytest.raises(MigrationInterrupted):
        write_logical_sparse_revision(**kwargs, stop_after_chunks=1)

    for changed in (
        {"source_uri": "gs://example/u2.h5ad"},
        {"source_row_start": 100},
        {"ingestion_run_id": "run2"},
        {"schema_fingerprint": "schema-v2"},
        {"var": var.rename(index={"g1": "changed"})},
        {"obs": obs.rename(index={"o0": "changed"})},
        {"obs": obs.assign(cell="changed")},
    ):
        with pytest.raises(RuntimeError, match="checkpoint mismatch"):
            write_logical_sparse_revision(**(kwargs | changed))


def test_resume_rejects_changed_ordered_legacy_source_identity(tmp_path: Path) -> None:
    """A partial legacy-family candidate cannot resume against a changed source list."""
    matrix, obs, var = fixture_data()
    kwargs = dict(
        root=tmp_path,
        logical_key="surfaces/example",
        revision="legacy-source-identity",
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint="schema-v1",
        source_uri="lamin://legacy/family",
        source_checksum=SOURCE_CHECKSUM,
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=4,
        source_identity={
            "kind": "legacy-triplets/v1",
            "sources": [
                {
                    "chunk_id": 0,
                    "obs_artifact_id": "obs-0",
                    "x_artifact_id": "x-0",
                    "var_artifact_id": "var-0",
                    "row_start": 0,
                    "row_end": 9,
                }
            ],
        },
    )
    with pytest.raises(MigrationInterrupted):
        write_logical_sparse_revision(**kwargs, stop_after_chunks=1)

    drifted = kwargs["source_identity"] | {
        "sources": [
            {
                **kwargs["source_identity"]["sources"][0],
                "x_artifact_id": "x-drifted",
            }
        ]
    }
    with pytest.raises(RuntimeError, match="checkpoint mismatch for source_identity"):
        write_logical_sparse_revision(**(kwargs | {"source_identity": drifted}))


def test_writer_rejects_source_or_obs_substitution_before_manifest(
    tmp_path: Path,
) -> None:
    matrix, obs, var = fixture_data()
    kwargs = dict(
        root=tmp_path,
        logical_key="surfaces/example",
        revision="substitution",
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint="schema-v1",
        source_uri="gs://example/source.h5ad",
        source_checksum=SOURCE_CHECKSUM,
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=4,
    )
    with pytest.raises(MigrationInterrupted):
        write_logical_sparse_revision(**kwargs, stop_after_chunks=1)

    substituted = matrix.copy()
    substituted.data *= 7
    with pytest.raises(RuntimeError, match="source/readback"):
        write_logical_sparse_revision(**(kwargs | {"matrix": substituted}))
    assert not (
        tmp_path / "surfaces/example/revisions/substitution/manifest.json"
    ).exists()

    obs_path = (
        tmp_path / "surfaces/example/revisions/substitution/obs/chunk_000000.parquet"
    )
    obs.iloc[:3].iloc[::-1].to_parquet(obs_path)
    with pytest.raises(RuntimeError, match="source/readback"):
        write_logical_sparse_revision(**kwargs)
    assert not (
        tmp_path / "surfaces/example/revisions/substitution/manifest.json"
    ).exists()


def test_resume_recovers_complete_payload_written_before_checkpoint(
    tmp_path: Path,
) -> None:
    matrix, obs, var = fixture_data()
    kwargs = dict(
        root=tmp_path,
        logical_key="surfaces/example",
        revision="crash-recovery",
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint="schema-v1",
        source_uri="gs://example/source.h5ad",
        source_checksum=SOURCE_CHECKSUM,
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=4,
    )
    with pytest.raises(MigrationInterrupted):
        write_logical_sparse_revision(**kwargs, stop_after_chunks=1)
    checkpoint_path = tmp_path / "surfaces/example/checkpoints/crash-recovery.json"
    checkpoint = json.loads(checkpoint_path.read_text())
    checkpoint["completed_chunks"] = []
    checkpoint_path.write_text(json.dumps(checkpoint))

    write_logical_sparse_revision(**kwargs)
    _, restored, _, _ = read_logical_sparse_revision(
        tmp_path, "surfaces/example", "crash-recovery"
    )
    assert (restored != matrix).nnz == 0


def test_shared_var_schema_fingerprint_cannot_escape_storage_identity(
    tmp_path: Path,
) -> None:
    matrix, obs, var = fixture_data()
    manifest = write_logical_sparse_revision(
        root=tmp_path,
        logical_key="surfaces/example",
        revision="safe-schema-key",
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint="../../unsafe/schema",
        source_uri="gs://example/source.h5ad",
        source_checksum=SOURCE_CHECKSUM,
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=4,
    )
    shared_key = manifest["shared_var"]["key"]
    assert ".." not in shared_key
    assert shared_key.count("/") == 2
    assert (tmp_path / shared_key).is_file()


def test_readback_rejects_tampered_sparse_payload(tmp_path: Path) -> None:
    write_candidate(tmp_path, "r1")
    payload = tmp_path / "surfaces/example/revisions/r1/chunks/chunk_000000.zarr/data/0"
    payload.write_bytes(b"tampered")

    with pytest.raises(Exception):
        read_logical_sparse_revision(tmp_path, "surfaces/example", "r1")


def test_promotion_and_rollback_repoint_alias_without_deletion(tmp_path: Path) -> None:
    write_candidate(tmp_path, "r1")
    altered, _, _ = fixture_data()
    altered = altered.copy()
    altered.data *= 2
    write_candidate(tmp_path, "r2", matrix=altered)

    rollback = rollback_to_revision(
        tmp_path, "surfaces/example", "r1", reason="verified fallback"
    )
    alias = json.loads((tmp_path / "surfaces/example/aliases/current.json").read_text())

    assert alias["revision"] == "r1"
    assert json.loads(rollback.read_text())["reason"] == "verified fallback"
    assert (tmp_path / "surfaces/example/revisions/r2/manifest.json").exists()


def test_hard_rss_guard_records_durable_checkpoint_before_candidate_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    matrix, obs, var = fixture_data(rows=2)
    monkeypatch.setattr(logical_sparse_zarr, "_resident_rss_bytes", lambda: 11)

    with pytest.raises(ResourceLimitExceeded, match="candidate_initialization"):
        write_logical_sparse_revision(
            root=tmp_path,
            logical_key="surfaces/example",
            revision="guarded",
            matrix=matrix,
            obs=obs,
            var=var,
            schema_fingerprint="schema-v1",
            source_uri="gs://example/source.h5ad",
            source_checksum=SOURCE_CHECKSUM,
            ingestion_run_id="test-run",
            max_rss_bytes=10,
            min_rows=1,
            max_rows=1,
        )

    checkpoint = json.loads(
        (tmp_path / "surfaces/example/checkpoints/guarded.json").read_text()
    )
    assert checkpoint["status"] == "resource_limit_exceeded"
    assert checkpoint["error"] == {
        "kind": "max_rss_exceeded",
        "phase": "candidate_initialization",
        "chunk_index": None,
        "observed_rss_bytes": 11,
        "max_rss_bytes": 10,
    }
    assert not (tmp_path / "surfaces/example/revisions/guarded/manifest.json").exists()

    monkeypatch.setattr(logical_sparse_zarr, "_resident_rss_bytes", lambda: 0)
    resumed = write_logical_sparse_revision(
        root=tmp_path,
        logical_key="surfaces/example",
        revision="guarded",
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint="schema-v1",
        source_uri="gs://example/source.h5ad",
        source_checksum=SOURCE_CHECKSUM,
        ingestion_run_id="test-run",
        max_rss_bytes=10,
        min_rows=1,
        max_rows=1,
    )
    assert resumed["shape"] == [2, 3]


def test_preplan_materialization_failure_is_checkpointed_and_resumable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    matrix, obs, var = fixture_data(rows=2)
    original_materialize = logical_sparse_zarr._materialize_rows

    def fail_planning_sample(
        matrix: object, start: int, end: int, sparse_format: str
    ) -> sparse.csr_matrix:
        raise MemoryError("synthetic pre-plan allocation failure")

    monkeypatch.setattr(logical_sparse_zarr, "_materialize_rows", fail_planning_sample)
    kwargs = dict(
        root=tmp_path,
        logical_key="surfaces/example",
        revision="preplan-guarded",
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint="schema-v1",
        source_uri="gs://example/source.h5ad",
        source_checksum=SOURCE_CHECKSUM,
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=1,
    )

    with pytest.raises(ResourceLimitExceeded, match="plan materialization"):
        write_logical_sparse_revision(**kwargs)

    checkpoint_path = tmp_path / "surfaces/example/checkpoints/preplan-guarded.json"
    checkpoint = json.loads(checkpoint_path.read_text())
    assert checkpoint["status"] == "resource_limit_exceeded"
    assert checkpoint["error"] == {
        "kind": "materialization_memory_error",
        "phase": "plan_materialization",
        "chunk_index": None,
        "max_rss_bytes": 10**12,
    }
    candidate = tmp_path / "surfaces/example/revisions/preplan-guarded"
    assert not (candidate / "manifest.json").exists()
    assert not (candidate / "chunks").exists()

    monkeypatch.setattr(logical_sparse_zarr, "_materialize_rows", original_materialize)
    resumed = write_logical_sparse_revision(**kwargs)
    assert resumed["shape"] == [2, 3]
    assert (
        read_logical_sparse_revision(tmp_path, "surfaces/example", "preplan-guarded")[
            1
        ].nnz
        == matrix.nnz
    )
