from __future__ import annotations

import ctypes
import importlib.util
import json
import weakref
from pathlib import Path
from typing import Any, TypedDict, cast

import fsspec
import gcsfs
import h5py
import numpy as np
import pandas as pd
import pytest
from anndata import AnnData
from fsspec.implementations.memory import MemoryFileSystem
from scipy import sparse
from typing_extensions import Unpack

import pert_gym.gcs_native_sparse_zarr as gcs_native_sparse_zarr
from pert_gym.gcs_native_sparse_zarr import (
    BlockPlanConflict,
    GCSNativeMetrics,
    GCSNativeWriterError,
    assert_cache_budget,
    calibrated_block_plan,
    promote_gcs_native_revision,
    register_gcs_prefix_with_lamin,
    requester_pays_gcs_filesystem,
    validate_measured_block,
    write_gcs_native_sparse_revision,
)

TOOL_PATH = Path(__file__).parents[1] / "tools" / "migrate_gcs_native_sparse_zarr.py"
TOOL_SPEC = importlib.util.spec_from_file_location(
    "gcs_native_migration_tool", TOOL_PATH
)
assert TOOL_SPEC is not None and TOOL_SPEC.loader is not None
gcs_native_migration_tool = importlib.util.module_from_spec(TOOL_SPEC)
TOOL_SPEC.loader.exec_module(gcs_native_migration_tool)


class NativeChunk(TypedDict):
    checksums: dict[str, str]
    end: int
    matrix_key: str
    peak_rss_bytes: int
    runtime_seconds: float
    source_generation: str
    source_checksum: str
    start: int


class NativeSource(TypedDict):
    checksum: str
    row_end: int
    row_start: int


class NativeVar(TypedDict):
    frame_sha256: str
    index_sha256: str


class NativeManifest(TypedDict):
    chunks: list[NativeChunk]
    source: NativeSource
    var: NativeVar


class PromotionMarker(TypedDict):
    promotion_key: str


class WriteOverrides(TypedDict, total=False):
    block_size_exception: dict[str, str]
    matrix: object
    max_block_bytes: int
    max_rss_bytes: int
    max_rows: int
    min_block_bytes: int
    obs: pd.DataFrame
    peak_rss_reader: Any
    source_generation: str
    source_row_end: int | None
    source_row_start: int | None
    stop_after_chunks: int | None


class WriteArguments(TypedDict):
    cache_cap_bytes: int
    cache_dir: Path
    cache_safety_reserve_bytes: int
    fs: Any
    ingestion_run_id: str
    logical_key: str
    matrix: object
    max_block_bytes: int
    max_rows: int
    min_block_bytes: int
    min_rows: int
    obs: pd.DataFrame
    revision: str
    schema_fingerprint: str
    source_generation: str
    source_row_end: int | None
    source_row_start: int | None
    source_uri: str
    staging_prefix: str
    var: pd.DataFrame


def source() -> tuple[sparse.csr_matrix, pd.DataFrame, pd.DataFrame]:
    matrix = sparse.csr_matrix(
        (
            np.arange(1, 13, dtype=np.float32),
            (np.repeat(np.arange(6), 2), np.tile(np.array([0, 2]), 6)),
        ),
        shape=(6, 3),
    )
    obs = pd.DataFrame(
        {"cell": [f"cell-{i}" for i in range(6)]},
        index=pd.Index([f"o{i}" for i in range(6)]),
    )
    var = pd.DataFrame({"kind": ["gene"] * 3}, index=pd.Index(["g1", "g2", "g3"]))
    return matrix, obs, var


def memory_filesystem() -> MemoryFileSystem:
    fs = fsspec.filesystem("memory")
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]
    return cast(MemoryFileSystem, fs)


def test_requester_pays_gcs_filesystem_enables_concrete_version_aware_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    def filesystem(protocol: str, **kwargs: object) -> object:
        created.update({"protocol": protocol, **kwargs})
        return object()

    monkeypatch.setattr("pert_gym.gcs_native_sparse_zarr.fsspec.filesystem", filesystem)
    requester_pays_gcs_filesystem("jkobject-1549353370965")

    assert created == {
        "protocol": "gcs",
        "project": "jkobject-1549353370965",
        "requester_pays": True,
        "version_aware": True,
    }


def test_release_block_memory_collects_and_best_effort_trims_linux(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class Trim:
        argtypes: list[object] = []
        restype: object = None

        def __call__(self, value: int) -> None:
            events.append(("trim", value))

    class Libc:
        malloc_trim = Trim()

    monkeypatch.setattr(
        gcs_native_sparse_zarr.gc, "collect", lambda: events.append("gc")
    )
    monkeypatch.setattr(gcs_native_sparse_zarr.sys, "platform", "linux")
    monkeypatch.setattr(gcs_native_sparse_zarr.ctypes, "CDLL", lambda _: Libc())

    gcs_native_sparse_zarr._release_block_memory()

    assert events == ["gc", ("trim", 0)]
    assert Libc.malloc_trim.argtypes == [ctypes.c_size_t]
    assert Libc.malloc_trim.restype is ctypes.c_int

    monkeypatch.setattr(gcs_native_sparse_zarr.ctypes, "CDLL", lambda _: object())
    gcs_native_sparse_zarr._release_block_memory()
    assert events[-1] == "gc"


def write(
    fs: MemoryFileSystem, cache_dir: Path, **changes: Unpack[WriteOverrides]
) -> tuple[NativeManifest, GCSNativeMetrics]:
    matrix, obs, var = source()
    arguments = cast(
        WriteArguments,
        {
            "fs": fs,
            "staging_prefix": "bucket/staging",
            "logical_key": "family/example",
            "revision": "r1",
            "matrix": matrix,
            "obs": obs,
            "var": var,
            "source_uri": "gs://source-bucket/immutable.h5ad",
            "source_generation": "12345",
            "source_checksum": "sha256-file-bytes/v1:" + "a" * 64,
            "source_row_start": 0,
            "source_row_end": 6,
            "schema_fingerprint": "schema-v1",
            "ingestion_run_id": "test-run",
            "cache_dir": cache_dir,
            "cache_cap_bytes": 1024,
            "cache_safety_reserve_bytes": 0,
            "min_block_bytes": 1,
            "max_block_bytes": 10_000,
            "min_rows": 1,
            "max_rows": 2,
        }
        | changes,
    )
    manifest, metrics = write_gcs_native_sparse_revision(**arguments)
    return cast(NativeManifest, manifest), metrics


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
    assert all(
        record["source_checksum"].endswith("a" * 64) for record in manifest["chunks"]
    )
    assert all(record["runtime_seconds"] >= 0 for record in manifest["chunks"])
    assert all(record["peak_rss_bytes"] > 0 for record in manifest["chunks"])
    assert manifest["source"]["checksum"].endswith("a" * 64)
    assert manifest["var"]["index_sha256"]
    assert manifest["var"]["frame_sha256"]

    marker = cast(
        PromotionMarker,
        promote_gcs_native_revision(
            fs=fs,
            staging_prefix="bucket/staging",
            logical_key="family/example",
            revision="r1",
            manifest=manifest,
        ),
    )
    assert marker["promotion_key"].endswith("promotions/r1.json")
    assert json.loads(fs.cat(marker["promotion_key"]))["manifest_key"].endswith(
        "manifest.json"
    )


def test_remote_writer_releases_previous_block_only_after_record_is_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    refs_by_block: list[tuple[weakref.ReferenceType[object], ...]] = []
    original_materialize = gcs_native_sparse_zarr._materialize_rows
    original_parity = gcs_native_sparse_zarr._assert_source_readback_parity
    original_release = gcs_native_sparse_zarr._release_block_memory
    original_write_exclusive = gcs_native_sparse_zarr._write_exclusive

    def materialize(matrix: object, start: int, end: int, sparse_format: str) -> Any:
        if refs_by_block:
            assert all(ref() is None for ref in refs_by_block[-1])
        return original_materialize(matrix, start, end, sparse_format)

    def parity(
        source_matrix: Any,
        remote_matrix: Any,
        source_frame: pd.DataFrame,
        remote_frame: pd.DataFrame,
    ) -> None:
        values = (source_matrix, remote_matrix, source_frame, remote_frame)
        original_parity(*values)
        refs_by_block.append(tuple(weakref.ref(value) for value in values))

    def write_exclusive(fs: Any, key: str, payload: bytes) -> dict[str, str | int]:
        if "/chunk-records/" in key:
            assert refs_by_block
            assert all(ref() is not None for ref in refs_by_block[-1])
        return original_write_exclusive(fs, key, payload)

    def release() -> None:
        assert refs_by_block
        assert all(ref() is None for ref in refs_by_block[-1])
        original_release()

    monkeypatch.setattr(gcs_native_sparse_zarr, "_materialize_rows", materialize)
    monkeypatch.setattr(
        gcs_native_sparse_zarr, "_assert_source_readback_parity", parity
    )
    monkeypatch.setattr(gcs_native_sparse_zarr, "_release_block_memory", release)
    monkeypatch.setattr(gcs_native_sparse_zarr, "_write_exclusive", write_exclusive)

    manifest, _ = write(memory_filesystem(), tmp_path / "cache")

    assert len(manifest["chunks"]) == 3
    assert len(refs_by_block) == 4


def test_remote_writer_rejects_resume_source_drift_and_recovers_partial_orphan(
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
    manifest, _ = write(other, tmp_path / "other-cache")

    assert (
        other.cat(
            "bucket/staging/family/example/temporary-revisions/r1/chunks/chunk_000000.zarr/orphan"
        )
        == b"x"
    )
    assert manifest["chunks"][0]["matrix_key"].endswith(
        "recovery/chunk_000000/attempt_000000/matrix.zarr"
    )


def test_remote_writer_skips_partial_recovery_attempt_without_overwrite(
    tmp_path: Path,
) -> None:
    fs = memory_filesystem()
    base = "bucket/staging/family/example/temporary-revisions/r1"
    canonical_orphan = f"{base}/chunks/chunk_000000.zarr/orphan"
    first_recovery_orphan = (
        f"{base}/recovery/chunk_000000/attempt_000000/matrix.zarr/orphan"
    )
    fs.pipe(canonical_orphan, b"canonical")
    fs.pipe(first_recovery_orphan, b"recovery-0")

    manifest, _ = write(fs, tmp_path / "cache")

    assert fs.cat(canonical_orphan) == b"canonical"
    assert fs.cat(first_recovery_orphan) == b"recovery-0"
    assert manifest["chunks"][0]["matrix_key"].endswith(
        "recovery/chunk_000000/attempt_000001/matrix.zarr"
    )


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


def test_remote_writer_records_absolute_bounded_source_rows_and_rejects_resume_drift(
    tmp_path: Path,
) -> None:
    fs = memory_filesystem()
    matrix, obs, _ = source()
    manifest, _ = write(
        fs,
        tmp_path / "cache",
        matrix=matrix[2:5],
        obs=obs.iloc[2:5],
        source_row_start=2,
        source_row_end=5,
    )

    assert manifest["source"]["row_start"] == 2
    assert manifest["source"]["row_end"] == 5
    assert [(record["start"], record["end"]) for record in manifest["chunks"]] == [
        (2, 4),
        (4, 5),
    ]
    with pytest.raises(GCSNativeWriterError, match="remote plan identity mismatch"):
        write(
            fs,
            tmp_path / "cache",
            matrix=matrix[1:4],
            obs=obs.iloc[1:4],
            source_row_start=1,
            source_row_end=4,
        )


@pytest.mark.parametrize(
    ("row_start", "row_end"),
    [(None, 3), (0, None), (-1, 2), (2, 2), (2, 7)],
)
def test_remote_writer_rejects_invalid_or_ambiguous_source_row_bounds(
    tmp_path: Path, row_start: int | None, row_end: int | None
) -> None:
    with pytest.raises(ValueError, match="source row bounds"):
        write(
            memory_filesystem(),
            tmp_path / "cache",
            source_row_start=row_start,
            source_row_end=row_end,
        )


def test_generation_pinned_source_uses_gcsfs_versioned_range_request() -> None:
    """Reject the old false-green ``generation=`` path that returned current bytes.

    This uses an actual gcsfs ``GCSFile``. The mocked backend intentionally
    emulates 2025.12.0's bug: a ``generation=`` kwarg can report the old version
    during info preflight while an unversioned ``cat_file`` read returns new bytes.
    Only the documented ``#generation`` path reaches the versioned range request.
    """
    fs = gcsfs.GCSFileSystem(token="anon", version_aware=True)
    source_key = "bucket/source.h5ad"
    pinned_key = f"{source_key}#123456"
    info_calls: list[tuple[str, dict[str, object]]] = []
    reads: list[str] = []

    def info(path: str, **kwargs: object) -> dict[str, object]:
        info_calls.append((path, kwargs))
        if path not in {source_key, pinned_key}:
            raise AssertionError(f"unexpected info path: {path}")
        return {"generation": "123456", "size": 20}

    def cat_file(path: str, **_kwargs: object) -> bytes:
        reads.append(path)
        return b"old-generation-bytes" if path == pinned_key else b"current-bytes"

    fs.info = info  # type: ignore[method-assign]
    fs.cat_file = cat_file  # type: ignore[method-assign]
    generation, handle = gcs_native_migration_tool.open_generation_pinned_source(
        fs, source_key
    )
    try:
        assert generation == "123456"
        assert handle.read() == b"old-generation-bytes"
    finally:
        handle.close()

    assert info_calls == [
        (source_key, {}),
        (pinned_key, {}),
        (pinned_key, {"generation": "123456"}),
    ]
    assert reads == [pinned_key]


def test_generation_pinned_source_rejects_generation_fragment_in_source_key() -> None:
    fs = type("VersionAwareFS", (), {"version_aware": True})()
    with pytest.raises(RuntimeError, match="must not already contain"):
        gcs_native_migration_tool.open_generation_pinned_source(
            fs, "bucket/source.h5ad#123456"
        )


def test_generation_pinned_source_rejects_non_version_aware_filesystem() -> None:
    with pytest.raises(RuntimeError, match="must enable version-aware"):
        gcs_native_migration_tool.open_generation_pinned_source(
            gcsfs.GCSFileSystem(token="anon"), "bucket/source.h5ad"
        )


def test_generation_pinned_source_rejects_resolved_generation_drift() -> None:
    class DriftingFS:
        version_aware = True

        def info(self, path: str, **_kwargs: object) -> dict[str, str]:
            return {"generation": "456" if "#" in path else "123"}

    with pytest.raises(RuntimeError, match="did not resolve requested generation"):
        gcs_native_migration_tool.open_generation_pinned_source(
            DriftingFS(), "bucket/source.h5ad"
        )


def test_bounded_obs_decode_never_reads_rows_outside_requested_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "source.h5ad"
    obs = pd.DataFrame(
        {
            "number": np.arange(8),
            "category": pd.Categorical(["a", "b"] * 4, ordered=True),
        },
        index=pd.Index([f"cell-{row}" for row in range(8)], name="cell_id"),
    )
    AnnData(X=sparse.eye(8, format="csr"), obs=obs).write_h5ad(path)
    reads: dict[str, list[object]] = {}
    original_getitem = h5py.Dataset.__getitem__

    def record_getitem(dataset: h5py.Dataset, selection: object) -> object:
        if dataset.name in {"/obs/cell_id", "/obs/number", "/obs/category/codes"}:
            reads.setdefault(dataset.name, []).append(selection)
        return original_getitem(dataset, selection)

    monkeypatch.setattr(h5py.Dataset, "__getitem__", record_getitem)
    with h5py.File(path, "r") as h5:
        result = gcs_native_migration_tool._read_h5ad_dataframe_rows(
            h5["obs"], row_start=2, row_end=5
        )

    pd.testing.assert_frame_equal(result, obs.iloc[2:5])
    assert reads == {
        "/obs/cell_id": [slice(2, 5, None)],
        "/obs/number": [slice(2, 5, None)],
        "/obs/category/codes": [slice(2, 5, None)],
    }


def test_promotion_remote_io_occurs_before_every_writer_lock_exits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    events: list[str] = []
    held: set[str] = set()

    class Lock:
        def __init__(self, name: str) -> None:
            self.name = name

        def __enter__(self) -> None:
            held.add(self.name)
            events.append(f"enter:{self.name}")

        def __exit__(self, *_: object) -> None:
            events.append(f"exit:{self.name}")
            held.remove(self.name)

    class Handle:
        def __enter__(self) -> object:
            return object()

        def __exit__(self, *_: object) -> None:
            return None

    class H5:
        def __enter__(self) -> dict[str, object]:
            return {"obs": object(), "var": object()}

        def __exit__(self, *_: object) -> None:
            return None

    class Metrics:
        __dict__ = {"chunk_count": 1}

    args = type(
        "Args",
        (),
        {
            "cache_cap_gib": 1.0,
            "max_rss_gib": 1.0,
            "row_start": 0,
            "row_end": 1,
            "ingestion_run_id": "test-run",
            "source_checksum": "sha256-file-bytes/v1:" + "a" * 64,
            "dataset_id": "family/example",
            "canonical_prefix": "family/example",
            "source_gcs_uri": "gs://bucket/source.h5ad",
            "staging_gcs_prefix": "gs://bucket/staging",
            "logical_key": "family/example",
            "revision": "r1",
            "schema_fingerprint": "schema",
            "cache_dir": tmp_path / "cache",
            "min_rows": 1,
            "max_rows": 1,
            "promote": True,
            "register_lamin_prefix": False,
            "migration_map_json": tmp_path / "migration.json",
            "collection_metadata_json": tmp_path / "collection.json",
        },
    )()
    args.migration_map_json.write_text("{}", encoding="utf-8")
    args.collection_metadata_json.write_text("{}", encoding="utf-8")
    capacity = type(
        "Capacity", (), {"hostname": "host", "project": "project", "zone": "zone"}
    )()
    monkeypatch.setattr(gcs_native_migration_tool, "parse_args", lambda: args)
    monkeypatch.setattr(gcs_native_migration_tool, "preflight", lambda: capacity)
    monkeypatch.setattr(
        gcs_native_migration_tool, "vm_global_lamin_writer_lock_path", lambda: "global"
    )
    monkeypatch.setattr(
        gcs_native_migration_tool, "legacy_lamin_writer_lock_paths", lambda: ("legacy",)
    )
    monkeypatch.setattr(
        gcs_native_migration_tool,
        "lamin_writer_lock",
        lambda path, *_args, **_kwargs: Lock(path),
    )
    monkeypatch.setattr(
        gcs_native_migration_tool, "requester_pays_gcs_filesystem", lambda _: object()
    )
    monkeypatch.setattr(
        gcs_native_migration_tool,
        "open_generation_pinned_source",
        lambda *_args: ("123", Handle()),
    )
    monkeypatch.setattr(gcs_native_migration_tool.h5py, "File", lambda *_args: H5())
    monkeypatch.setattr(
        gcs_native_migration_tool, "GCSH5ADCSR", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        gcs_native_migration_tool,
        "read_elem",
        lambda _: pd.DataFrame(index=pd.Index(["g"])),
    )
    monkeypatch.setattr(
        gcs_native_migration_tool,
        "_read_h5ad_dataframe_rows",
        lambda *_args, **_kwargs: pd.DataFrame(index=pd.Index(["cell"])),
    )
    monkeypatch.setattr(
        gcs_native_migration_tool,
        "write_gcs_native_sparse_revision",
        lambda **_kwargs: ({"candidate_prefix": "bucket/staging/candidate"}, Metrics()),
    )

    def promote(**_kwargs: object) -> dict[str, str]:
        assert held == {"global", "legacy"}
        events.append("remote:promotion")
        return {"promotion_key": "promotion"}

    monkeypatch.setattr(
        gcs_native_migration_tool, "promote_gcs_native_revision", promote
    )
    assert gcs_native_migration_tool.main() == 0
    assert events == [
        "enter:global",
        "enter:legacy",
        "remote:promotion",
        "exit:legacy",
        "exit:global",
    ]


def test_calibration_selects_feasible_measured_intervals_and_records_identity() -> None:
    plan = calibrated_block_plan(
        identity={"source_generation": "123", "source_checksum": "checksum"},
        n_obs=1_000,
        probe_rows=100,
        measured_bytes=250,
        measured_peak_rss_bytes=400,
        min_block_bytes=500,
        max_block_bytes=750,
        max_rss_bytes=1_000,
        max_rows=500,
        calibration_objects=[{"key": "probe", "generation": "9", "size": 250}],
    )

    assert plan["planned_chunks"] == [[0, 250], [250, 500], [500, 750], [750, 1000]]
    assert plan["calibration"]["identity"]["source_generation"] == "123"
    assert plan["calibration"]["objects"][0]["generation"] == "9"
    assert plan["thresholds"] == {
        "min_block_bytes": 500,
        "max_block_bytes": 750,
        "max_rss_bytes": 1_000,
    }


def test_calibration_fails_closed_when_byte_minimum_conflicts_with_rss() -> None:
    with pytest.raises(BlockPlanConflict) as caught:
        calibrated_block_plan(
            identity={"source_generation": "123"},
            n_obs=1_000,
            probe_rows=100,
            measured_bytes=100,
            measured_peak_rss_bytes=900,
            min_block_bytes=500,
            max_block_bytes=750,
            max_rss_bytes=1_000,
            max_rows=1_000,
            calibration_objects=[],
        )

    assert caught.value.evidence["kind"] == "byte_rss_conflict"
    assert caught.value.evidence["rows_required_for_min_bytes"] == 500
    assert caught.value.evidence["rows_allowed_by_rss"] == 111

    with pytest.raises(BlockPlanConflict, match="cannot satisfy") as row_cap:
        calibrated_block_plan(
            identity={"source_generation": "123"},
            n_obs=1_000,
            probe_rows=100,
            measured_bytes=100,
            measured_peak_rss_bytes=100,
            min_block_bytes=500,
            max_block_bytes=750,
            max_rss_bytes=10_000,
            max_rows=400,
            calibration_objects=[],
        )
    assert row_cap.value.evidence["rows_required_for_min_bytes"] == 500
    assert row_cap.value.evidence["max_rows"] == 400

    granular = calibrated_block_plan(
        identity={"source_generation": "123"},
        n_obs=10,
        probe_rows=3,
        measured_bytes=1_000,
        measured_peak_rss_bytes=100,
        min_block_bytes=500,
        max_block_bytes=750,
        max_rss_bytes=10_000,
        max_rows=10,
        calibration_objects=[],
    )
    assert granular["chosen_rows"] == 2

    with pytest.raises(BlockPlanConflict, match="cannot satisfy"):
        calibrated_block_plan(
            identity={"source_generation": "123"},
            n_obs=115,
            probe_rows=36,
            measured_bytes=99_309,
            measured_peak_rss_bytes=40_301,
            min_block_bytes=898,
            max_block_bytes=1_673,
            max_rss_bytes=204_683,
            max_rows=86,
            calibration_objects=[],
        )

    with pytest.raises(BlockPlanConflict) as hard_rss:
        calibrated_block_plan(
            identity={"source_generation": "123"},
            n_obs=100,
            probe_rows=100,
            measured_bytes=100,
            measured_peak_rss_bytes=1_001,
            min_block_bytes=500,
            max_block_bytes=750,
            max_rss_bytes=1_000,
            max_rows=100,
            calibration_objects=[],
            explicit_exception={"id": "cannot-waive-rss"},
        )
    assert hard_rss.value.evidence["kind"] == "calibration_rss_conflict"


def test_whole_small_calibration_cannot_waive_modeled_rss_ceiling() -> None:
    with pytest.raises(
        BlockPlanConflict,
        match="whole dataset below byte minimum exceeds hard constraints: rss_row_ceiling",
    ) as caught:
        calibrated_block_plan(
            identity={"source_generation": "captured-generation"},
            n_obs=6_721,
            probe_rows=2_547,
            measured_bytes=33_293,
            measured_peak_rss_bytes=58_303,
            min_block_bytes=99_032,
            max_block_bytes=173_636,
            max_rss_bytes=116_955,
            max_rows=6_721,
            calibration_objects=[],
        )

    assert caught.value.evidence["kind"] == "whole_small_hard_limit_conflict"
    assert caught.value.evidence["active_constraints"] == ["rss_row_ceiling"]
    assert caught.value.evidence["whole_dataset_below_minimum"] is True
    assert caught.value.evidence["rows_allowed_by_rss"] == 5_109


def test_whole_small_calibration_cannot_waive_max_rows() -> None:
    with pytest.raises(
        BlockPlanConflict,
        match="whole dataset below byte minimum exceeds hard constraints: max_rows",
    ) as caught:
        calibrated_block_plan(
            identity={"source_generation": "captured-generation"},
            n_obs=200,
            probe_rows=100,
            measured_bytes=200,
            measured_peak_rss_bytes=300,
            min_block_bytes=500,
            max_block_bytes=750,
            max_rss_bytes=1_000_000,
            max_rows=199,
            calibration_objects=[],
        )

    assert caught.value.evidence["kind"] == "whole_small_hard_limit_conflict"
    assert caught.value.evidence["active_constraints"] == ["max_rows"]
    assert caught.value.evidence["whole_dataset_below_minimum"] is True
    assert caught.value.evidence["max_rows"] == 199
    assert caught.value.evidence["rows_allowed_by_rss"] == 333_333


def test_whole_small_calibration_reports_combined_hard_constraints() -> None:
    with pytest.raises(
        BlockPlanConflict,
        match=(
            "whole dataset below byte minimum exceeds hard constraints: "
            "max_rows, rss_row_ceiling"
        ),
    ) as caught:
        calibrated_block_plan(
            identity={"source_generation": "captured-generation"},
            n_obs=6_721,
            probe_rows=2_547,
            measured_bytes=33_293,
            measured_peak_rss_bytes=58_303,
            min_block_bytes=99_032,
            max_block_bytes=173_636,
            max_rss_bytes=116_955,
            max_rows=1_228,
            calibration_objects=[],
        )

    assert caught.value.evidence["kind"] == "whole_small_hard_limit_conflict"
    assert caught.value.evidence["active_constraints"] == [
        "max_rows",
        "rss_row_ceiling",
    ]
    assert caught.value.evidence["max_rows"] == 1_228
    assert caught.value.evidence["rows_allowed_by_rss"] == 5_109


def test_whole_small_calibration_accepts_one_block_within_hard_constraints() -> None:
    plan = calibrated_block_plan(
        identity={"source_generation": "fitting-generation"},
        n_obs=200,
        probe_rows=100,
        measured_bytes=200,
        measured_peak_rss_bytes=300,
        min_block_bytes=500,
        max_block_bytes=750,
        max_rss_bytes=1_000,
        max_rows=200,
        calibration_objects=[],
    )

    assert plan["chosen_rows"] == 200
    assert plan["planned_chunks"] == [[0, 200]]
    assert plan["exception"] == {"kind": "whole_dataset_below_minimum"}


def test_measured_block_validation_rejects_small_non_tail_and_hard_rss() -> None:
    with pytest.raises(BlockPlanConflict, match="measured block contract") as small:
        validate_measured_block(
            start=0,
            end=100,
            n_obs=300,
            measured_bytes=499,
            measured_peak_rss_bytes=900,
            min_block_bytes=500,
            max_block_bytes=750,
            max_rss_bytes=1_000,
        )
    assert small.value.evidence["violations"] == ["bytes_below_minimum"]

    with pytest.raises(BlockPlanConflict) as rss:
        validate_measured_block(
            start=0,
            end=100,
            n_obs=300,
            measured_bytes=600,
            measured_peak_rss_bytes=1_001,
            min_block_bytes=500,
            max_block_bytes=750,
            max_rss_bytes=1_000,
            explicit_exception={"id": "reviewed-byte-exception"},
        )
    assert rss.value.evidence["violations"] == ["rss_above_maximum"]


def test_measured_block_validation_records_tail_small_and_explicit_exceptions() -> None:
    tail = validate_measured_block(
        start=200,
        end=250,
        n_obs=250,
        measured_bytes=100,
        measured_peak_rss_bytes=400,
        min_block_bytes=500,
        max_block_bytes=750,
        max_rss_bytes=1_000,
    )
    small = validate_measured_block(
        start=0,
        end=100,
        n_obs=100,
        measured_bytes=100,
        measured_peak_rss_bytes=400,
        min_block_bytes=500,
        max_block_bytes=750,
        max_rss_bytes=1_000,
    )
    explicit = validate_measured_block(
        start=0,
        end=100,
        n_obs=300,
        measured_bytes=100,
        measured_peak_rss_bytes=400,
        min_block_bytes=500,
        max_block_bytes=750,
        max_rss_bytes=1_000,
        explicit_exception={"id": "approved-1", "reason": "reviewed"},
    )

    assert tail["exception"]["kind"] == "final_tail_below_minimum"
    assert small["exception"]["kind"] == "whole_dataset_below_minimum"
    assert explicit["exception"]["id"] == "approved-1"


def test_hct116_observed_records_regress_as_byte_and_rss_contract_failure() -> None:
    evidence = json.loads(
        (
            Path(__file__).parent / "fixtures" / "hct116_observed_block_metrics.json"
        ).read_text()
    )

    assert evidence["record_count"] == 10
    assert evidence["bytes_written_max"] < 2 * 1024**3
    assert evidence["peak_rss_bytes_max"] > evidence["configured_max_rss_bytes"]


def test_writer_calibrates_before_immutable_plan_and_refuses_plan_drift(
    tmp_path: Path,
) -> None:
    fs = memory_filesystem()
    with pytest.raises(GCSNativeWriterError, match="intentional interruption"):
        write(
            fs,
            tmp_path / "cache",
            min_block_bytes=1,
            max_block_bytes=10_000,
            stop_after_chunks=1,
        )
    base = "bucket/staging/family/example/temporary-revisions/r1"
    plan = json.loads(fs.cat(f"{base}/plan.json"))

    assert fs.exists(f"{base}/calibration/probe.json")
    assert plan["calibration"]["measured_bytes"] > 0
    with pytest.raises(GCSNativeWriterError, match="immutable plan"):
        write(
            fs,
            tmp_path / "cache",
            min_block_bytes=2,
            max_block_bytes=10_000,
        )


def test_writer_persists_terminal_evidence_and_stops_before_next_block(
    tmp_path: Path,
) -> None:
    fs = memory_filesystem()
    rss = iter([100, 200, 200])

    with pytest.raises(BlockPlanConflict, match="measured block contract"):
        write(
            fs,
            tmp_path / "cache",
            min_block_bytes=1,
            max_block_bytes=10_000,
            max_rows=2,
            max_rss_bytes=150,
            peak_rss_reader=lambda: next(rss),
        )

    base = "bucket/staging/family/example/temporary-revisions/r1"
    record = json.loads(fs.cat(f"{base}/chunk-records/chunk_000000.json"))
    failure = json.loads(fs.cat(f"{base}/failure.json"))
    assert record["block_validation"]["status"] == "failed"
    assert record["block_validation"]["violations"] == ["rss_above_maximum"]
    assert failure["requires_new_revision"] is True
    assert not fs.exists(f"{base}/chunks/chunk_000001.zarr")
    assert not fs.exists(f"{base}/manifest.json")
