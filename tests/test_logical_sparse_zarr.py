from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy import sparse

from pert_gym.logical_sparse_zarr import (
    MigrationInterrupted,
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

    write_logical_sparse_revision(**kwargs)

    assert first_chunk.read_bytes() == before
    _, restored, _, _ = read_logical_sparse_revision(
        tmp_path, "surfaces/example", "resume"
    )
    assert (restored != matrix).nnz == 0
    with pytest.raises(RuntimeError, match="already completed"):
        write_logical_sparse_revision(**kwargs)


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
