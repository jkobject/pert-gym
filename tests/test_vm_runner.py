from __future__ import annotations

import json
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
