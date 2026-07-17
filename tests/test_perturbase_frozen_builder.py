from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

BUILDER_PATH = (
    Path(__file__).parents[1]
    / "artifacts"
    / "evidence"
    / "remote-perturbase-gse216481-t_d84e0d14"
    / "build_perturbase_gse216481_component.py"
)


def load_builder(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setitem(sys.modules, "anndata", types.ModuleType("anndata"))
    monkeypatch.setitem(sys.modules, "zarr", types.ModuleType("zarr"))
    runner = types.ModuleType("tools.pert_gym_vm_runner")
    setattr(runner, "lamin_writer_lock", object())
    setattr(runner, "legacy_lamin_writer_lock_paths", object())
    setattr(runner, "vm_global_lamin_writer_lock_path", object())
    monkeypatch.setitem(sys.modules, "tools.pert_gym_vm_runner", runner)
    ingest = types.ModuleType("tools.ingest_perturbase_row113")
    setattr(ingest, "standardize_obs", object())
    setattr(ingest, "standardize_var", object())
    monkeypatch.setitem(sys.modules, "tools.ingest_perturbase_row113", ingest)
    lamin = types.ModuleType("tools.lamin_context")
    setattr(lamin, "connect_pertdata", object())
    monkeypatch.setitem(sys.modules, "tools.lamin_context", lamin)
    spec = importlib.util.spec_from_file_location(
        "perturbase_frozen_builder", BUILDER_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_normalize_gcs_description_accepts_gcloud_storage_snake_case(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = load_builder(monkeypatch)

    observed = builder.normalize_gcs_description(
        "gs://bucket/source.tar.gz#123",
        {
            "generation": "123",
            "size": 456,
            "crc32c_hash": "crc-value",
            "md5_hash": "md5-value",
            "update_time": "2026-07-16T17:00:00+0000",
        },
    )

    assert observed == {
        "uri": "gs://bucket/source.tar.gz",
        "generation": "123",
        "size": 456,
        "md5_base64": "md5-value",
        "crc32c_base64": "crc-value",
        "updated": "2026-07-16T17:00:00+0000",
    }


def test_normalize_gcs_description_accepts_json_api_camel_case(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = load_builder(monkeypatch)

    observed = builder.normalize_gcs_description(
        "gs://bucket/source.tar.gz",
        {
            "generation": 123,
            "size": "456",
            "crc32c": "crc-value",
            "md5Hash": "md5-value",
            "updateTime": "2026-07-16T17:00:00Z",
        },
    )

    assert observed["generation"] == "123"
    assert observed["size"] == 456
    assert observed["crc32c_base64"] == "crc-value"
    assert observed["md5_base64"] == "md5-value"
    assert observed["updated"] == "2026-07-16T17:00:00Z"
