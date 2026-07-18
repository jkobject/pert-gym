from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd
import pytest

from tools import migrate_perturbai_sparse_parquet as perturbai
from tools import pert_gym_vm_runner as runner


def _identity() -> tuple[str, str, str, str]:
    hostname = sorted(runner.ALLOWED_HEAVY_HOSTS)[0]
    return (
        hostname,
        runner.EXPECTED_GCE_PROJECT,
        runner.EXPECTED_ZONE,
        hostname,
    )


def _writer_metadata(run_id: str) -> dict[str, object]:
    hostname, project, zone, _ = _identity()
    return {
        "run_id": run_id,
        "pid": os.getpid(),
        "host": hostname,
        "project": project,
        "zone": zone,
        "branch": "test",
        "started_at": time.time(),
    }


def _valid_preflight() -> runner.Preflight:
    hostname, project, zone, instance = _identity()
    return runner.Preflight(
        hostname=hostname,
        project=project,
        zone=zone,
        instance=instance,
        free_disk_bytes=100 * 1024**3,
        available_memory_bytes=32 * 1024**3,
        billing_project=runner.BILLING_PROJECT,
    )


def _configure_perturbai_cli(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(perturbai, "require_heavy_vm", _identity)
    monkeypatch.setattr(runner, "preflight", _valid_preflight)
    monkeypatch.setattr(runner, "_git_branch", lambda: "fix/test")
    monkeypatch.setattr(
        perturbai, "requester_pays_storage_options", lambda _project: {}
    )
    monkeypatch.setattr(perturbai, "_var", lambda _path: pd.DataFrame())
    monkeypatch.setattr(
        perturbai,
        "build_perturbai_revision",
        lambda **_kwargs: {"shape": [1, 1], "nnz": 1},
    )
    monkeypatch.setattr(
        perturbai,
        "parse_args",
        lambda: argparse.Namespace(
            source_parquet=[tmp_path / "source.parquet"],
            source_uri=["gs://example/source.parquet"],
            source_object_id=["source.parquet"],
            source_commit="a" * 40,
            gene_metadata=tmp_path / "genes.parquet",
            output_root=tmp_path / "output",
            logical_key="perturbai/test",
            revision="r1",
            schema_fingerprint="schema",
            ingestion_run_id="run-1",
            billing_project=runner.BILLING_PROJECT,
            parquet_batch_rows=10_000,
            max_rss_gib=4.0,
            publish_collection_key="perturbai/test",
        ),
    )


def test_perturbai_direct_writer_rejects_the_runner_global_lease(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PERT_GYM_LAMIN_WRITER_LOCK_DIR", str(tmp_path / "locks"))
    _configure_perturbai_cli(monkeypatch, tmp_path)
    publication_started = False

    def publish(**_kwargs: object) -> dict[str, object]:
        nonlocal publication_started
        publication_started = True
        return {"unexpected": True}

    monkeypatch.setattr(perturbai, "connect_pertdata", lambda: object())
    monkeypatch.setattr(perturbai, "publish_candidate", publish)

    lock_path = runner.vm_global_lamin_writer_lock_path()
    with runner.lamin_writer_lock(lock_path, _writer_metadata("runner")):
        with pytest.raises(RuntimeError, match="another Lamin writer"):
            perturbai.main()

    assert not publication_started


def test_perturbai_direct_writer_rejects_a_legacy_runner_lease(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    global_lock_dir = tmp_path / "locks"
    legacy_lock = tmp_path / "legacy-worktree" / "lamin-writer.lock"
    monkeypatch.setenv("PERT_GYM_LAMIN_WRITER_LOCK_DIR", str(global_lock_dir))
    monkeypatch.setattr(
        runner, "legacy_lamin_writer_lock_paths", lambda: (legacy_lock,)
    )
    _configure_perturbai_cli(monkeypatch, tmp_path)
    connected = False

    def connect() -> object:
        nonlocal connected
        connected = True
        return object()

    monkeypatch.setattr(perturbai, "connect_pertdata", connect)
    monkeypatch.setattr(
        perturbai, "publish_candidate", lambda **_kwargs: {"unexpected": True}
    )

    with runner.lamin_writer_lock(legacy_lock, _writer_metadata("legacy-runner")):
        with pytest.raises(
            RuntimeError, match=f"another Lamin writer holds {legacy_lock}"
        ):
            perturbai.main()

    assert not connected


def test_perturbai_direct_writer_releases_all_locks_after_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    global_lock = tmp_path / "locks" / "lamin-writer.lock"
    legacy_locks = (
        tmp_path / "legacy-a" / "lamin-writer.lock",
        tmp_path / "legacy-b" / "lamin-writer.lock",
    )
    monkeypatch.setenv("PERT_GYM_LAMIN_WRITER_LOCK_DIR", str(global_lock.parent))
    monkeypatch.setattr(runner, "legacy_lamin_writer_lock_paths", lambda: legacy_locks)
    _configure_perturbai_cli(monkeypatch, tmp_path)
    monkeypatch.setattr(perturbai, "connect_pertdata", lambda: object())

    def fail_publish(**_kwargs: object) -> dict[str, object]:
        raise ValueError("publication failed")

    monkeypatch.setattr(perturbai, "publish_candidate", fail_publish)

    with pytest.raises(ValueError, match="publication failed"):
        perturbai.main()

    with runner.lamin_writer_lease(
        run_id="next-writer", preflight_result=_valid_preflight()
    ):
        for lock_path in (global_lock, *legacy_locks):
            acquired = json.loads(lock_path.read_text(encoding="utf-8"))
            assert acquired["run_id"] == "next-writer"
            assert acquired["status"] == "acquired"
