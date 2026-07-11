from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from tools import pert_gym_vm_runner as runner
from tools import stage_to_gcs


def test_require_heavy_vm_rejects_darwin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner.platform, "system", lambda: "Darwin")

    with pytest.raises(RuntimeError, match="Darwin"):
        runner.require_heavy_vm()


def test_require_heavy_vm_rejects_wrong_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(runner.socket, "gethostname", lambda: "untrusted-host")

    with pytest.raises(RuntimeError, match="untrusted-host"):
        runner.require_heavy_vm()


def test_require_heavy_vm_requires_pinned_gce_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        runner.socket, "gethostname", lambda: f"{runner.EXPECTED_HEAVY_HOST}.internal"
    )
    metadata = {
        "project/project-id": runner.EXPECTED_GCE_PROJECT,
        "instance/zone": f"projects/1/zones/{runner.EXPECTED_ZONE}",
        "instance/name": runner.EXPECTED_HEAVY_HOST,
    }
    monkeypatch.setattr(runner, "_metadata_value", metadata.__getitem__)

    assert runner.require_heavy_vm() == (
        runner.EXPECTED_HEAVY_HOST,
        runner.EXPECTED_GCE_PROJECT,
        runner.EXPECTED_ZONE,
        runner.EXPECTED_HEAVY_HOST,
    )

    metadata["instance/name"] = "lookalike-worker"
    with pytest.raises(RuntimeError, match="unpinned GCE identity"):
        runner.require_heavy_vm()


def test_writer_lock_rejects_duplicate_writer(tmp_path: Path) -> None:
    lock_path = tmp_path / "lamin-writer.lock"
    with runner.lamin_writer_lock(lock_path, {"pid": 1}):
        with pytest.raises(RuntimeError, match="another Lamin writer"):
            with runner.lamin_writer_lock(lock_path, {"pid": 2}):
                pass


def test_bounded_smoke_10k_and_25k_leave_checkpoints(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    small = runner.run_bounded_smoke(run_dir=run_dir, cells=10_000, chunk_size=4_000)
    large = runner.run_bounded_smoke(run_dir=run_dir, cells=25_000, chunk_size=4_000)

    assert small["chunk_count"] == 3
    assert large["chunk_count"] == 7
    assert small["max_chunk_cells"] == large["max_chunk_cells"] == 4_000
    assert small["lamin_writes"] == large["lamin_writes"] == 0
    assert (
        json.loads((run_dir / "checkpoints" / "smoke_25000.json").read_text())[
            "last_completed_end"
        ]
        == 25_000
    )
    assert (run_dir / "heartbeat.json").exists()


def test_bounded_smoke_rejects_unbounded_size(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly"):
        runner.run_bounded_smoke(run_dir=tmp_path, cells=9_999)


def test_child_environment_sets_requester_pays_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PERT_GYM_GCS_USER_PROJECT", raising=False)

    environment = runner._child_env()

    assert environment["GOOGLE_CLOUD_PROJECT"] == runner.BILLING_PROJECT
    assert environment["GCLOUD_PROJECT"] == runner.BILLING_PROJECT
    assert environment["PERT_GYM_GCS_USER_PROJECT"] == runner.BILLING_PROJECT


def test_requester_pays_urls_include_billing_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, str] = {}

    class Response:
        headers = {"Location": "https://upload.example/session"}

        def raise_for_status(self) -> None:
            return None

    def post(url: str, **kwargs: object) -> Response:
        seen["post"] = url
        return Response()

    monkeypatch.setattr(stage_to_gcs.requests, "post", post)

    upload_url = stage_to_gcs.start_resumable_upload(
        "scperturb", "pert-gym/a file.h5ad", "token", runner.BILLING_PROJECT
    )

    assert upload_url == "https://upload.example/session"
    assert "userProject=jkobject-1549353370965" in seen["post"]


def _valid_preflight() -> runner.Preflight:
    return runner.Preflight(
        hostname=runner.EXPECTED_HEAVY_HOST,
        project=runner.EXPECTED_GCE_PROJECT,
        zone=runner.EXPECTED_ZONE,
        instance=runner.EXPECTED_HEAVY_HOST,
        free_disk_bytes=100 * 1024**3,
        available_memory_bytes=32 * 1024**3,
        billing_project=runner.BILLING_PROJECT,
    )


def test_cli_rejects_user_host_and_resource_gate_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(runner.socket, "gethostname", lambda: "untrusted-host")

    for override in (
        ("--expected-host", "untrusted-host"),
        ("--min-free-disk-gb", "0"),
        ("--min-available-memory-gb", "-1"),
    ):
        with pytest.raises(SystemExit, match="2"):
            runner.main([*override, "--smoke", "10000"])
        assert not (tmp_path / "artifacts" / "vm_runs").exists()


def test_cli_production_command_publishes_periodic_progress_during_partial_stdout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "preflight", lambda: _valid_preflight())
    monkeypatch.setattr(runner, "PRODUCTION_HEARTBEAT_SECONDS", 0.01)
    writes: list[tuple[Path, object]] = []
    original_write_json = runner._write_json

    def record_write(path: Path, value: object) -> None:
        writes.append((path, value))
        original_write_json(path, value)

    monkeypatch.setattr(runner, "_write_json", record_write)

    assert (
        runner.main(
            [
                "--run-id",
                "production-test",
                "--allow-lamin-writes",
                "--command",
                sys.executable,
                "-c",
                "import sys, time; sys.stdout.write('partial'); sys.stdout.flush(); time.sleep(0.06)",
            ]
        )
        == 0
    )

    run_dir = tmp_path / "artifacts" / "vm_runs" / "production-test"
    assert (run_dir / "logs" / "runner.log").exists()
    checkpoint = json.loads((run_dir / "checkpoints" / "production.json").read_text())
    assert checkpoint["status"] == "completed"
    assert checkpoint["run_id"] == "production-test"
    assert (run_dir / "logs" / "runner.log").read_text() == "partial"
    heartbeat_writes = [
        value
        for path, value in writes
        if path == run_dir / "heartbeat.json"
        and isinstance(value, dict)
        and value.get("status") == "running"
    ]
    checkpoint_writes = [
        value
        for path, value in writes
        if path == run_dir / "checkpoints" / "production.json"
        and isinstance(value, dict)
        and value.get("status") == "running"
    ]
    assert len(heartbeat_writes) >= 3
    assert len(checkpoint_writes) >= 3
