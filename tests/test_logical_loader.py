from __future__ import annotations

import io
from pathlib import Path

import anndata as ad
import fsspec
import numpy as np
import pandas as pd
import pytest
from scipy import sparse

from pert_gym.gcs_native_sparse_zarr import (
    promote_gcs_native_revision,
    write_gcs_native_sparse_revision,
)
from pert_gym.logical_dataset import (
    GIB,
    PRODUCTION_BLOCK_MAX_BYTES,
    PRODUCTION_BLOCK_MIN_BYTES,
    PRODUCTION_BLOCK_TARGET_BYTES,
    LogicalCollection,
    open_logical_dataset,
    plan_production_blocks,
)
from pert_gym.logical_sparse_zarr import write_logical_sparse_revision
from pert_gym.sparse_zarr_contract import LEGACY_FORMAT

SOURCE_CHECKSUM = f"sha256-file-bytes/v1:{'a' * 64}"


def _frames(rows: int = 9) -> tuple[sparse.csr_matrix, pd.DataFrame, pd.DataFrame]:
    matrix = sparse.csr_matrix(
        (
            np.arange(1, rows * 2 + 1, dtype=np.float32),
            (np.repeat(np.arange(rows), 2), np.tile([0, 2], rows)),
        ),
        shape=(rows, 3),
    )
    obs = pd.DataFrame(
        {
            "cell_id": [f"cell-{index}" for index in range(rows)],
            "target": ["control", "A", "B"] * (rows // 3),
            "target_split": ["train", "val", "test"] * (rows // 3),
        },
        index=[f"obs-{index}" for index in range(rows)],
    )
    var = pd.DataFrame({"feature_type": ["gene"] * 3}, index=["g0", "g1", "g2"])
    return matrix, obs, var


def _write_logical(tmp_path: Path, *, rows: int = 9) -> tuple[Path, sparse.csr_matrix]:
    matrix, obs, var = _frames(rows)
    write_logical_sparse_revision(
        root=tmp_path,
        logical_key="datasets/example",
        revision="r1",
        matrix=matrix,
        obs=obs,
        var=var,
        schema_fingerprint="genes-v1",
        source_uri="gs://example/source.h5ad",
        source_checksum=SOURCE_CHECKSUM,
        ingestion_run_id="loader-test",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=3,
    )
    return (
        tmp_path / "datasets/example/revisions/r1/manifest.json",
        matrix,
    )


def test_open_is_lazy_and_slice_reads_only_overlapping_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path, expected = _write_logical(tmp_path)
    import pert_gym.logical_dataset as loader

    opened: list[str] = []
    original = loader._read_zarr_matrix

    def recording_reader(*args, **kwargs):
        opened.append(str(args[1]))
        return original(*args, **kwargs)

    monkeypatch.setattr(loader, "_read_zarr_matrix", recording_reader)
    dataset = open_logical_dataset(manifest_path)

    assert opened == []
    assert dataset.name == "datasets/example"
    assert dataset.shape == (9, 3)
    assert dataset.X.shape == (9, 3)

    batch = dataset.read(rows=slice(2, 5))

    assert len(opened) == 2
    assert batch.obs.index.tolist() == ["obs-2", "obs-3", "obs-4"]
    assert batch.obs["target_split"].tolist() == ["test", "train", "val"]
    assert np.array_equal(batch.X.toarray(), expected[2:5].toarray())
    assert batch.var.index.tolist() == ["g0", "g1", "g2"]


def test_unrestricted_read_fails_before_opening_any_matrix_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path, _ = _write_logical(tmp_path)
    import pert_gym.logical_dataset as loader

    opened: list[str] = []
    monkeypatch.setattr(
        loader,
        "_read_zarr_matrix",
        lambda *_args, **_kwargs: opened.append("opened"),
    )
    dataset = open_logical_dataset(manifest_path)

    with pytest.raises(ValueError, match="bounded row or block selection"):
        dataset.read()
    with pytest.raises(ValueError, match="full-dataset materialization"):
        dataset.read(rows=slice(None))
    with pytest.raises(ValueError, match="full-dataset materialization"):
        dataset.read(blocks=list(range(dataset.block_count)))
    with pytest.raises(ValueError, match="bounded row or block selection"):
        dataset.read_obs()

    assert opened == []


def test_read_rejects_noncontiguous_blocks_before_opening_payloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path, _ = _write_logical(tmp_path)
    import pert_gym.logical_dataset as loader

    opened: list[str] = []
    dataset = open_logical_dataset(manifest_path)
    monkeypatch.setattr(
        loader,
        "_read_zarr_matrix",
        lambda *_args, **_kwargs: opened.append("matrix"),
    )
    monkeypatch.setattr(
        loader,
        "_read_parquet",
        lambda *_args, **_kwargs: opened.append("parquet"),
    )

    with pytest.raises(ValueError, match="contiguous blocks"):
        dataset.read(blocks=[0, 2])

    assert opened == []


@pytest.mark.parametrize("row_end", [6, 8])
def test_near_full_read_fails_before_opening_or_stacking_payloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, row_end: int
) -> None:
    manifest_path, _ = _write_logical(tmp_path)
    import pert_gym.logical_dataset as loader

    opened: list[str] = []
    stacked: list[str] = []
    dataset = open_logical_dataset(manifest_path)
    monkeypatch.setattr(
        loader,
        "_read_zarr_matrix",
        lambda *_args, **_kwargs: opened.append("matrix"),
    )
    monkeypatch.setattr(
        loader,
        "_read_parquet",
        lambda *_args, **_kwargs: opened.append("parquet"),
    )
    monkeypatch.setattr(
        loader.sparse,
        "vstack",
        lambda *_args, **_kwargs: stacked.append("vstack"),
    )

    with pytest.raises(ValueError, match="bounded selection"):
        dataset.read(rows=slice(0, row_end))

    assert opened == []
    assert stacked == []


def test_cross_chunk_read_coordinates_match_materialized_rows(
    tmp_path: Path,
) -> None:
    manifest_path, _ = _write_logical(tmp_path)
    dataset = open_logical_dataset(manifest_path)

    batch = dataset.read(rows=slice(2, 5))

    assert (batch.start, batch.end) == (2, 5)
    assert batch.end - batch.start == batch.X.shape[0] == len(batch.obs)
    assert batch.block_indexes == (0, 1)


def test_block_selection_reuses_one_verified_dataset_level_var(tmp_path: Path) -> None:
    manifest_path, _ = _write_logical(tmp_path)
    dataset = open_logical_dataset(manifest_path)

    blocks = list(dataset.iter_blocks(blocks=[0, 2]))

    assert [(block.start, block.end) for block in blocks] == [(0, 3), (6, 9)]
    assert blocks[0].var is blocks[1].var
    assert dataset.var is blocks[0].var
    assert blocks[0].obs.index.tolist() == ["obs-0", "obs-1", "obs-2"]
    assert blocks[1].obs.index.tolist() == ["obs-6", "obs-7", "obs-8"]


def test_shared_var_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    manifest_path, _ = _write_logical(tmp_path)
    dataset = open_logical_dataset(manifest_path)
    var_path = next(tmp_path.glob("vars/*/var.parquet"))
    changed = pd.read_parquet(var_path).rename(index={"g0": "changed"})
    changed.to_parquet(var_path)

    with pytest.raises(ValueError, match="shared var identity"):
        _ = dataset.var


def test_legacy_h5ad_triplet_is_backed_and_sliceable(tmp_path: Path) -> None:
    matrix, obs, var = _frames()
    ad.AnnData(X=matrix).write_h5ad(tmp_path / "X.h5ad")
    obs.to_parquet(tmp_path / "obs.parquet")
    var.to_parquet(tmp_path / "var.parquet")
    manifest = {
        "format": LEGACY_FORMAT,
        "version": 1,
        "n_obs": len(obs),
        "n_vars": len(var),
        "nnz": matrix.nnz,
        "sparse_format": "csr",
        "x_key": "X.h5ad",
        "obs_key": "obs.parquet",
        "var_key": "var.parquet",
    }

    dataset = open_logical_dataset(manifest, root=tmp_path)
    batch = dataset.read(rows=slice(1, 4))

    assert batch.obs.index.tolist() == ["obs-1", "obs-2", "obs-3"]
    assert np.array_equal(batch.X.toarray(), matrix[1:4].toarray())
    assert batch.var.index.tolist() == var.index.tolist()


def test_legacy_h5ad_reads_only_the_selected_source_row_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    matrix, obs, var = _frames()
    obs.to_parquet(tmp_path / "obs.parquet")
    var.to_parquet(tmp_path / "var.parquet")
    import pert_gym.logical_dataset as loader

    selections: list[object] = []

    class MatrixSource:
        def __getitem__(self, selection: object) -> sparse.csr_matrix:
            selections.append(selection)
            if selection == slice(None):
                raise AssertionError("full legacy matrix access is forbidden")
            return matrix[selection]

    class File:
        def close(self) -> None:
            return None

    class Backed:
        X = MatrixSource()
        file = File()

    monkeypatch.setattr(loader.ad, "read_h5ad", lambda *_args, **_kwargs: Backed())
    dataset = open_logical_dataset(
        {
            "format": LEGACY_FORMAT,
            "version": 1,
            "n_obs": len(obs),
            "n_vars": len(var),
            "nnz": matrix.nnz,
            "sparse_format": "csr",
            "x_key": "X.h5ad",
            "obs_key": "obs.parquet",
            "var_key": "var.parquet",
        },
        root=tmp_path,
    )

    batch = dataset.read(rows=slice(2, 5))

    assert selections == [slice(2, 5)]
    assert np.array_equal(batch.X.toarray(), matrix[2:5].toarray())


def test_small_dense_legacy_h5ad_is_exposed_as_sparse_without_full_open(
    tmp_path: Path,
) -> None:
    matrix, obs, var = _frames()
    dense = matrix.toarray()
    ad.AnnData(X=dense).write_h5ad(tmp_path / "X.h5ad")
    obs.to_parquet(tmp_path / "obs.parquet")
    var.to_parquet(tmp_path / "var.parquet")
    dataset = open_logical_dataset(
        {
            "format": LEGACY_FORMAT,
            "version": 1,
            "n_obs": len(obs),
            "n_vars": len(var),
            "nnz": int(np.count_nonzero(dense)),
            "sparse_format": "csr",
            "x_key": "X.h5ad",
            "obs_key": "obs.parquet",
            "var_key": "var.parquet",
        },
        root=tmp_path,
    )

    assert np.array_equal(dataset.X[2:4].toarray(), dense[2:4])


def test_collection_selects_datasets_without_opening_payloads(tmp_path: Path) -> None:
    first, _ = _write_logical(tmp_path / "one")
    second, _ = _write_logical(tmp_path / "two")
    collection = LogicalCollection({"one": first, "two": second})

    assert collection.names == ("one", "two")
    assert collection.select(["two"]).names == ("two",)
    assert collection["one"].read(rows=slice(0, 1)).obs.index.tolist() == ["obs-0"]


def test_production_block_policy_targets_two_to_three_gib_with_one_tail() -> None:
    assert PRODUCTION_BLOCK_MIN_BYTES == 2 * GIB
    assert PRODUCTION_BLOCK_TARGET_BYTES == int(2.5 * GIB)
    assert PRODUCTION_BLOCK_MAX_BYTES == 3 * GIB

    row_bytes = [256 * 1024**2] * 25
    blocks = plan_production_blocks(row_bytes)
    sizes = [sum(row_bytes[start:end]) for start, end in blocks]

    assert blocks[-1][1] == len(row_bytes)
    assert all(
        PRODUCTION_BLOCK_MIN_BYTES <= size <= PRODUCTION_BLOCK_MAX_BYTES
        for size in sizes[:-1]
    )
    assert sizes[-1] <= PRODUCTION_BLOCK_MAX_BYTES
    assert len([size for size in sizes if size < PRODUCTION_BLOCK_MIN_BYTES]) <= 1


def test_genuinely_small_dataset_is_one_policy_exception() -> None:
    assert plan_production_blocks([128 * 1024**2] * 4) == ((0, 4),)


def test_remote_promotion_marker_opens_verified_manifest_and_bounded_slice(
    tmp_path: Path,
) -> None:
    fs = fsspec.filesystem("memory")
    fs.store.clear()
    matrix, obs, var = _frames()
    manifest, _ = write_gcs_native_sparse_revision(
        fs=fs,
        staging_prefix="bucket/staging",
        logical_key="datasets/remote",
        revision="r1",
        matrix=matrix,
        obs=obs,
        var=var,
        source_uri="gs://source/immutable.h5ad",
        source_generation="123",
        source_row_start=0,
        source_row_end=len(obs),
        schema_fingerprint="genes-v1",
        ingestion_run_id="remote-loader-test",
        cache_dir=tmp_path / "cache",
        cache_cap_bytes=1024,
        cache_safety_reserve_bytes=0,
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=3,
    )
    marker = promote_gcs_native_revision(
        fs=fs,
        staging_prefix="bucket/staging",
        logical_key="datasets/remote",
        revision="r1",
        manifest=manifest,
    )

    dataset = open_logical_dataset(marker["promotion_key"], filesystem=fs)
    batch = dataset.read(rows=slice(2, 5))

    assert dataset.shape == matrix.shape
    assert batch.obs.index.tolist() == ["obs-2", "obs-3", "obs-4"]
    assert np.array_equal(batch.X.toarray(), matrix[2:5].toarray())


def test_loader_block_selection_uses_measured_production_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pert_gym.gcs_native_sparse_zarr as writer
    import pert_gym.logical_dataset as loader

    fs = fsspec.filesystem("memory")
    fs.store.clear()
    matrix, obs, var = _frames(rows=6)
    original_writer = writer._write_remote_matrix

    def measured_writer(fs, key, payload, sparse_format):
        original_writer(fs, key, payload, sparse_format)
        return 100

    monkeypatch.setattr(writer, "_write_remote_matrix", measured_writer)
    manifest, _ = write_gcs_native_sparse_revision(
        fs=fs,
        staging_prefix="bucket/staging",
        logical_key="datasets/blocks",
        revision="r1",
        matrix=matrix,
        obs=obs,
        var=var,
        source_uri="gs://source/immutable.h5ad",
        source_generation="123",
        source_row_start=0,
        source_row_end=len(obs),
        schema_fingerprint="genes-v1",
        ingestion_run_id="block-loader-test",
        cache_dir=tmp_path / "cache",
        cache_cap_bytes=1024,
        cache_safety_reserve_bytes=0,
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=2,
        production_block_min_bytes=150,
        production_block_target_bytes=200,
        production_block_max_bytes=250,
    )
    opened: list[str] = []
    original_reader = loader._read_zarr_matrix

    def recording_reader(fs, key, sparse_format):
        opened.append(key)
        return original_reader(fs, key, sparse_format)

    monkeypatch.setattr(loader, "_read_zarr_matrix", recording_reader)

    dataset = open_logical_dataset(manifest, filesystem=fs)
    batches = list(dataset.iter_blocks(blocks=[1]))

    assert dataset.block_count == 2
    assert [(batch.start, batch.end, batch.block_indexes) for batch in batches] == [
        (4, 6, (1,))
    ]
    assert opened == [manifest["chunks"][2]["matrix_key"]]

    opened_before_rejected_read = list(opened)
    with pytest.raises(ValueError, match="1 measured production block"):
        dataset.read(rows=slice(0, 5))
    assert opened == opened_before_rejected_read


def test_loader_rejects_manifest_block_layout_that_violates_measured_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pert_gym.gcs_native_sparse_zarr as writer

    fs = fsspec.filesystem("memory")
    fs.store.clear()
    matrix, obs, var = _frames(rows=6)
    original_writer = writer._write_remote_matrix

    def measured_writer(fs, key, payload, sparse_format):
        original_writer(fs, key, payload, sparse_format)
        return 100

    monkeypatch.setattr(writer, "_write_remote_matrix", measured_writer)
    manifest, _ = write_gcs_native_sparse_revision(
        fs=fs,
        staging_prefix="bucket/staging",
        logical_key="datasets/invalid-blocks",
        revision="r1",
        matrix=matrix,
        obs=obs,
        var=var,
        source_uri="gs://source/immutable.h5ad",
        source_generation="123",
        source_row_start=0,
        source_row_end=len(obs),
        schema_fingerprint="genes-v1",
        ingestion_run_id="invalid-block-loader-test",
        cache_dir=tmp_path / "cache",
        cache_cap_bytes=1024,
        cache_safety_reserve_bytes=0,
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=2,
        production_block_min_bytes=150,
        production_block_target_bytes=200,
        production_block_max_bytes=250,
    )
    manifest["blocks"] = [
        {
            "index": 0,
            "start": 0,
            "end": 6,
            "chunk_indexes": [0, 1, 2],
            "compressed_bytes": 300,
            "var": manifest["var"],
        }
    ]

    with pytest.raises(ValueError, match="production block layout"):
        open_logical_dataset(manifest, filesystem=fs)


def test_remote_parquet_generation_uses_version_aware_path() -> None:
    import pert_gym.logical_dataset as loader

    buffer = io.BytesIO()
    expected = pd.DataFrame({"value": [1]}, index=["row"])
    expected.to_parquet(buffer)

    class VersionAwareFilesystem:
        version_aware = True

        def __init__(self) -> None:
            self.opened: list[str] = []

        def info(self, key: str) -> dict[str, str]:
            assert key == "bucket/obs.parquet#123"
            return {"generation": "123"}

        def open(self, key: str, mode: str):
            assert mode == "rb"
            self.opened.append(key)
            return io.BytesIO(buffer.getvalue())

    fs = VersionAwareFilesystem()
    observed = loader._read_parquet(fs, "bucket/obs.parquet", generation="123")

    assert observed.equals(expected)
    assert fs.opened == ["bucket/obs.parquet#123"]
