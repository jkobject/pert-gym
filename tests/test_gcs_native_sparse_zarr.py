from __future__ import annotations

import json
from pathlib import Path

import fsspec
import numpy as np
import pandas as pd
import pytest
from scipy import sparse

from pert_gym.gcs_native_sparse_zarr import (
    GCSNativeWriterError,
    assert_cache_budget,
    promote_gcs_native_revision,
    register_gcs_prefix_with_lamin,
    write_gcs_native_sparse_revision,
)


def source() -> tuple[sparse.csr_matrix, pd.DataFrame, pd.DataFrame]:
    matrix = sparse.csr_matrix(
        (
            np.arange(1, 13, dtype=np.float32),
            (np.repeat(np.arange(6), 2), np.tile(np.array([0, 2]), 6)),
        ),
        shape=(6, 3),
    )
    obs = pd.DataFrame(
        {"cell": [f"cell-{i}" for i in range(6)]}, index=[f"o{i}" for i in range(6)]
    )
    var = pd.DataFrame({"kind": ["gene"] * 3}, index=["g1", "g2", "g3"])
    return matrix, obs, var


def memory_filesystem() -> object:
    fs = fsspec.filesystem("memory")
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]
    return fs


def write(
    fs: object, cache_dir: Path, **changes: object
) -> tuple[dict[str, object], object]:
    matrix, obs, var = source()
    arguments = {
        "fs": fs,
        "staging_prefix": "bucket/staging",
        "logical_key": "family/example",
        "revision": "r1",
        "matrix": matrix,
        "obs": obs,
        "var": var,
        "source_uri": "gs://source-bucket/immutable.h5ad",
        "source_generation": "12345",
        "schema_fingerprint": "schema-v1",
        "ingestion_run_id": "test-run",
        "cache_dir": cache_dir,
        "cache_cap_bytes": 1024,
        "cache_safety_reserve_bytes": 0,
        "min_rows": 1,
        "max_rows": 2,
    } | changes
    return write_gcs_native_sparse_revision(**arguments)


def test_remote_writer_resumes_direct_object_store_chunks_and_promotes_last(
    tmp_path: Path,
) -> None:
    fs = memory_filesystem()
    with pytest.raises(GCSNativeWriterError, match="intentional interruption"):
        write(fs, tmp_path / "cache", stop_after_chunks=1)

    manifest, metrics = write(fs, tmp_path / "cache")
    assert metrics.chunk_count == 3
    assert metrics.bytes_read > 0
    assert metrics.bytes_written > 0
    assert metrics.cache_bytes_after_cleanup == 0
    assert not (tmp_path / "cache").exists()
    assert fs.exists(
        "bucket/staging/family/example/temporary-revisions/r1/manifest.json"
    )
    assert not fs.exists("bucket/staging/family/example/promotions/r1.json")
    assert [record["source_generation"] for record in manifest["chunks"]] == [
        "12345"
    ] * 3
    assert all(record["checksums"]["data_sha256"] for record in manifest["chunks"])

    marker = promote_gcs_native_revision(
        fs=fs,
        staging_prefix="bucket/staging",
        logical_key="family/example",
        revision="r1",
        manifest=manifest,
    )
    assert marker["promotion_key"].endswith("promotions/r1.json")
    assert json.loads(fs.cat(marker["promotion_key"]))["manifest_key"].endswith(
        "manifest.json"
    )


def test_remote_writer_rejects_resume_source_generation_drift_and_orphan(
    tmp_path: Path,
) -> None:
    fs = memory_filesystem()
    with pytest.raises(GCSNativeWriterError, match="intentional interruption"):
        write(fs, tmp_path / "cache", stop_after_chunks=1)
    with pytest.raises(GCSNativeWriterError, match="remote plan identity mismatch"):
        write(fs, tmp_path / "cache", source_generation="changed")

    other = memory_filesystem()
    other.pipe(
        "bucket/staging/family/example/temporary-revisions/r1/chunks/chunk_000000.zarr/orphan",
        b"x",
    )
    with pytest.raises(GCSNativeWriterError, match="orphan or partial"):
        write(other, tmp_path / "other-cache")


def test_cache_budget_refuses_unsafe_cap_and_lamin_prefix_reference_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Usage:
        free = 30

    monkeypatch.setattr(
        "pert_gym.gcs_native_sparse_zarr.shutil.disk_usage", lambda _: Usage()
    )
    with pytest.raises(GCSNativeWriterError, match="safe headroom"):
        assert_cache_budget(
            tmp_path / "cache", cache_cap_bytes=20, safety_reserve_bytes=20
        )

    with pytest.raises(GCSNativeWriterError, match="register_gcs_prefix"):
        register_gcs_prefix_with_lamin(ln=object(), prefix_uri="gs://bucket/prefix")


def test_manifest_or_promotion_is_immutable(tmp_path: Path) -> None:
    fs = memory_filesystem()
    manifest, _ = write(fs, tmp_path / "cache")
    promote_gcs_native_revision(
        fs=fs,
        staging_prefix="bucket/staging",
        logical_key="family/example",
        revision="r1",
        manifest=manifest,
    )
    with pytest.raises(GCSNativeWriterError, match="already completed"):
        write(fs, tmp_path / "cache")
    with pytest.raises(GCSNativeWriterError, match="refusing overwrite"):
        promote_gcs_native_revision(
            fs=fs,
            staging_prefix="bucket/staging",
            logical_key="family/example",
            revision="r1",
            manifest=manifest,
        )
