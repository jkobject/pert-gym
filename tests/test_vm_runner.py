from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from tools import pert_gym_vm_runner as runner
from tools import stage_to_gcs


def test_require_heavy_vm_rejects_darwin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner.platform, "system", lambda: "Darwin")

    with pytest.raises(RuntimeError, match="Darwin"):
        runner.require_heavy_vm()


@pytest.mark.parametrize(
    "hostname",
    [
        "pert-gym-worker-eu-v2",
        "pert-gym-capacity-eu-v2-lookalike",
        "untrusted-host",
    ],
)
def test_require_heavy_vm_rejects_lookalike_or_unknown_host(
    monkeypatch: pytest.MonkeyPatch, hostname: str
) -> None:
    monkeypatch.setattr(runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(runner.socket, "gethostname", lambda: hostname)
    metadata = {
        "project/project-id": runner.EXPECTED_GCE_PROJECT,
        "instance/zone": f"projects/1/zones/{runner.EXPECTED_ZONE}",
        "instance/name": hostname,
    }
    monkeypatch.setattr(runner, "_metadata_value", metadata.__getitem__)

    with pytest.raises(RuntimeError, match=hostname):
        runner.require_heavy_vm()


@pytest.mark.parametrize("hostname", sorted(runner.ALLOWED_HEAVY_HOSTS))
def test_require_heavy_vm_requires_pinned_gce_identity_for_approved_host(
    monkeypatch: pytest.MonkeyPatch,
    hostname: str,
) -> None:
    monkeypatch.setattr(runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(runner.socket, "gethostname", lambda: f"{hostname}.internal")
    metadata = {
        "project/project-id": runner.EXPECTED_GCE_PROJECT,
        "instance/zone": f"projects/1/zones/{runner.EXPECTED_ZONE}",
        "instance/name": hostname,
    }
    monkeypatch.setattr(runner, "_metadata_value", metadata.__getitem__)

    assert runner.require_heavy_vm() == (
        hostname,
        runner.EXPECTED_GCE_PROJECT,
        runner.EXPECTED_ZONE,
        hostname,
    )

    metadata["instance/name"] = "lookalike-worker"
    with pytest.raises(RuntimeError, match="unpinned GCE identity"):
        runner.require_heavy_vm()


def _writer_metadata(*, run_id: str, pid: int | None = None) -> dict[str, object]:
    return {
        "run_id": run_id,
        "pid": os.getpid() if pid is None else pid,
        "host": sorted(runner.ALLOWED_HEAVY_HOSTS)[0],
        "project": runner.EXPECTED_GCE_PROJECT,
        "zone": runner.EXPECTED_ZONE,
        "branch": "fix/test",
        "started_at": time.time(),
    }


@dataclass
class _MemoryLeaseObject:
    generation: int
    payload: dict[str, object]


class _MemoryGcsLeaseBackend:
    """Deterministic GCS-generation model used only by distributed-lease tests."""

    def __init__(self) -> None:
        self.object: _MemoryLeaseObject | None = None
        self.next_generation = 1
        self.unavailable = False
        self.fail_replace = False
        self.after_delete: object | None = None

    def read(self) -> _MemoryLeaseObject | None:
        if self.unavailable:
            raise OSError("backend unavailable")
        return self.object

    def create(self, payload: dict[str, object]) -> _MemoryLeaseObject:
        if self.unavailable:
            raise OSError("backend unavailable")
        if self.object is not None:
            raise runner.LeaseGenerationConflict("already exists")
        self.object = _MemoryLeaseObject(self.next_generation, payload)
        self.next_generation += 1
        return self.object

    def replace(
        self, generation: int, payload: dict[str, object]
    ) -> _MemoryLeaseObject:
        if self.unavailable or self.fail_replace:
            raise OSError("backend unavailable")
        if self.object is None or self.object.generation != generation:
            raise runner.LeaseGenerationConflict("generation changed")
        self.object = _MemoryLeaseObject(self.next_generation, payload)
        self.next_generation += 1
        return self.object

    def delete(self, generation: int) -> None:
        if self.unavailable:
            raise OSError("backend unavailable")
        if self.object is None or self.object.generation != generation:
            raise runner.LeaseGenerationConflict("generation changed")
        self.object = None
        if self.after_delete is not None:
            self.after_delete()  # type: ignore[operator]


class _NoopDistributedLease:
    def renew(self) -> None:
        return None

    def __enter__(self) -> "_NoopDistributedLease":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None


@pytest.fixture(autouse=True)
def _avoid_live_gcs_for_runner_main(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner,
        "distributed_lamin_writer_lease",
        lambda metadata: _NoopDistributedLease(),
    )


def _distributed_lease(
    backend: _MemoryGcsLeaseBackend, *, host: str, now: float = 100.0
) -> object:
    return runner.DistributedLaminWriterLease(
        backend=backend,
        metadata={**_writer_metadata(run_id=f"run-{host}"), "host": host},
        ttl_seconds=30.0,
        clock=lambda: now,
    )


def test_distributed_lease_allows_exactly_one_of_two_distinct_hosts() -> None:
    backend = _MemoryGcsLeaseBackend()
    first = _distributed_lease(backend, host="pert-gym-worker-eu")
    second = _distributed_lease(backend, host="pert-gym-capacity-eu-v2")

    first.acquire()  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError, match="another distributed Lamin writer"):
        second.acquire()  # type: ignore[attr-defined]

    assert backend.object is not None
    assert backend.object.payload["host"] == "pert-gym-worker-eu"


def test_distributed_lease_recovers_only_expired_owner() -> None:
    backend = _MemoryGcsLeaseBackend()
    backend.object = _MemoryLeaseObject(
        7,
        {**_writer_metadata(run_id="expired"), "lease_id": "old", "expires_at": 99.0},
    )
    lease = _distributed_lease(backend, host="pert-gym-capacity-eu-v2")

    lease.acquire()  # type: ignore[attr-defined]

    assert backend.object is not None
    assert backend.object.payload["lease_id"] == lease.lease_id  # type: ignore[attr-defined]
    assert backend.object.payload["expires_at"] == 130.0


def test_distributed_lease_fails_closed_when_stale_recovery_loses_generation_race() -> (
    None
):
    backend = _MemoryGcsLeaseBackend()
    backend.object = _MemoryLeaseObject(
        7,
        {**_writer_metadata(run_id="expired"), "lease_id": "old", "expires_at": 99.0},
    )

    def competing_owner() -> None:
        backend.create(
            {
                **_writer_metadata(run_id="winner"),
                "lease_id": "winner",
                "expires_at": 130.0,
            }
        )

    backend.after_delete = competing_owner
    lease = _distributed_lease(backend, host="pert-gym-capacity-eu-v2")

    with pytest.raises(
        RuntimeError, match="could not prove distributed Lamin writer lease"
    ):
        lease.acquire()  # type: ignore[attr-defined]

    assert backend.object is not None
    assert backend.object.payload["lease_id"] == "winner"


def test_distributed_lease_renewal_failure_revokes_local_ownership() -> None:
    backend = _MemoryGcsLeaseBackend()
    lease = _distributed_lease(backend, host="pert-gym-worker-eu")
    lease.acquire()  # type: ignore[attr-defined]
    backend.fail_replace = True

    with pytest.raises(RuntimeError, match="renewal could not be proven"):
        lease.renew()  # type: ignore[attr-defined]

    assert not lease.held  # type: ignore[attr-defined]


def test_run_command_terminates_live_child_when_renewal_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    real_popen = subprocess.Popen
    children: list[Any] = []

    def observe_child(*args: Any, **kwargs: Any) -> Any:
        child = real_popen(*args, **kwargs)
        children.append(child)
        return child

    renewals = 0

    def fail_second_renewal() -> None:
        nonlocal renewals
        renewals += 1
        if renewals == 2:
            assert children and children[0].poll() is None
            raise RuntimeError("renewal could not be proven")

    monkeypatch.setattr(runner.subprocess, "Popen", observe_child)
    monkeypatch.setattr(runner, "PRODUCTION_HEARTBEAT_SECONDS", 0.01)

    with pytest.raises(RuntimeError, match="renewal could not be proven"):
        runner.run_command(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            run_dir=tmp_path,
            run_id="renewal-termination-test",
            renew_writer_lease=fail_second_renewal,
        )

    assert renewals == 2
    assert len(children) == 1
    assert children[0].poll() is not None


def test_distributed_lease_release_never_deletes_replaced_owner() -> None:
    backend = _MemoryGcsLeaseBackend()
    lease = _distributed_lease(backend, host="pert-gym-worker-eu")
    lease.acquire()  # type: ignore[attr-defined]
    assert backend.object is not None
    backend.object = _MemoryLeaseObject(
        backend.object.generation + 1,
        {**_writer_metadata(run_id="other"), "lease_id": "other", "expires_at": 130.0},
    )

    with pytest.raises(RuntimeError, match="release ownership could not be proven"):
        lease.release()  # type: ignore[attr-defined]

    assert backend.object is not None
    assert backend.object.payload["lease_id"] == "other"


def test_distributed_lease_backend_unreachable_fails_closed() -> None:
    backend = _MemoryGcsLeaseBackend()
    backend.unavailable = True
    lease = _distributed_lease(backend, host="pert-gym-worker-eu")

    with pytest.raises(
        RuntimeError, match="could not prove distributed Lamin writer lease"
    ):
        lease.acquire()  # type: ignore[attr-defined]


def test_writer_lock_rejects_duplicate_writer(tmp_path: Path) -> None:
    lock_path = tmp_path / "lamin-writer.lock"
    with runner.lamin_writer_lock(lock_path, _writer_metadata(run_id="first")):
        with pytest.raises(RuntimeError, match="another Lamin writer"):
            with runner.lamin_writer_lock(lock_path, _writer_metadata(run_id="second")):
                pass


def test_vm_global_writer_lock_is_independent_of_worktree(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lock_dir = tmp_path / "host-locks"
    monkeypatch.setenv("PERT_GYM_LAMIN_WRITER_LOCK_DIR", str(lock_dir))
    first_worktree = tmp_path / "worktree-a"
    second_worktree = tmp_path / "worktree-b"

    first = runner.vm_global_lamin_writer_lock_path(first_worktree)
    second = runner.vm_global_lamin_writer_lock_path(second_worktree)

    assert first == second == lock_dir / "lamin-writer.lock"
    with runner.lamin_writer_lock(first, _writer_metadata(run_id="first")):
        with pytest.raises(RuntimeError, match="another Lamin writer"):
            with runner.lamin_writer_lock(second, _writer_metadata(run_id="second")):
                pass


def test_new_writer_refuses_live_legacy_lock_on_distinct_inode(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    legacy_root = tmp_path / "legacy-worktree"
    global_lock = tmp_path / "host-locks" / "lamin-writer.lock"
    legacy_lock = runner.legacy_lamin_writer_lock_path(legacy_root)
    assert legacy_lock != global_lock
    legacy_lock.parent.mkdir(parents=True)
    entered = tmp_path / "command-entered"

    monkeypatch.setattr(runner, "ROOT", legacy_root)
    monkeypatch.setattr(
        runner,
        "legacy_lamin_writer_lock_paths",
        lambda: (legacy_lock,),
    )
    monkeypatch.setenv("PERT_GYM_LAMIN_WRITER_LOCK_DIR", str(global_lock.parent))
    monkeypatch.setattr(runner, "preflight", lambda: _valid_preflight())

    with legacy_lock.open("a+", encoding="utf-8") as legacy_handle:
        fcntl.flock(legacy_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="another Lamin writer holds"):
            runner.main(
                [
                    "--run-id",
                    "migration-lock-test",
                    "--allow-lamin-writes",
                    "--command",
                    sys.executable,
                    "-c",
                    f"from pathlib import Path; Path({str(entered)!r}).touch()",
                ]
            )
        assert not entered.exists()
        assert legacy_lock.read_text(encoding="utf-8") == ""
        fcntl.flock(legacy_handle.fileno(), fcntl.LOCK_UN)

    assert global_lock.exists()
    assert legacy_lock.exists()
    assert not os.path.samefile(global_lock, legacy_lock)


def test_new_writer_refuses_live_legacy_lock_in_another_worktree(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    legacy_worktree = tmp_path / "legacy-worktree"
    command_worktree = tmp_path / "command-worktree"
    global_lock = tmp_path / "host-locks" / "lamin-writer.lock"
    legacy_lock = runner.legacy_lamin_writer_lock_path(legacy_worktree)
    entered = tmp_path / "command-entered"

    monkeypatch.setattr(runner, "ROOT", command_worktree)
    monkeypatch.setattr(runner, "_git_branch", lambda: "fix/test")

    def registered_worktrees(
        command: list[str], **kwargs: object
    ) -> runner.subprocess.CompletedProcess[str]:
        assert command == [
            "git",
            "-C",
            str(command_worktree),
            "worktree",
            "list",
            "--porcelain",
        ]
        return runner.subprocess.CompletedProcess(
            command,
            0,
            f"worktree {legacy_worktree}\n\nworktree {command_worktree}\n",
            "",
        )

    monkeypatch.setattr(runner.subprocess, "run", registered_worktrees)
    monkeypatch.setenv("PERT_GYM_LAMIN_WRITER_LOCK_DIR", str(global_lock.parent))
    monkeypatch.setattr(runner, "preflight", lambda: _valid_preflight())
    legacy_lock.parent.mkdir(parents=True)

    with legacy_lock.open("a+", encoding="utf-8") as legacy_handle:
        fcntl.flock(legacy_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="another Lamin writer holds"):
            runner.main(
                [
                    "--run-id",
                    "cross-worktree-migration-lock-test",
                    "--allow-lamin-writes",
                    "--command",
                    sys.executable,
                    "-c",
                    f"from pathlib import Path; Path({str(entered)!r}).touch()",
                ]
            )
        assert not entered.exists()
        assert legacy_lock.read_text(encoding="utf-8") == ""
        fcntl.flock(legacy_handle.fileno(), fcntl.LOCK_UN)

    assert global_lock.exists()
    assert legacy_lock.exists()
    assert not os.path.samefile(global_lock, legacy_lock)


def test_process_start_ticks_treats_zombie_as_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def zombie_stat(path: Path, *, encoding: str) -> str:
        assert str(path) == "/proc/123/stat"
        return "123 (unreaped child) Z " + "0 " * 18 + "4242\n"

    monkeypatch.setattr(Path, "read_text", zombie_stat)

    assert runner._process_start_ticks(123) is None
    assert not runner._is_live_owner(
        {"status": "acquired", "pid": 123, "process_start_ticks": "4242"}
    )


def test_writer_lock_recovers_dead_owner_only_after_atomic_acquisition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lock_path = tmp_path / "lamin-writer.lock"
    dead_owner = _writer_metadata(run_id="dead", pid=999_999)
    dead_owner["status"] = "acquired"
    dead_owner["process_start_ticks"] = "old-process"
    lock_path.write_text(json.dumps(dead_owner) + "\n", encoding="utf-8")
    monkeypatch.setattr(runner, "_process_start_ticks", lambda pid: None)

    with runner.lamin_writer_lock(lock_path, _writer_metadata(run_id="recovered")):
        acquired = json.loads(lock_path.read_text(encoding="utf-8"))
        assert acquired["run_id"] == "recovered"
        assert acquired["status"] == "acquired"

    released = json.loads(lock_path.read_text(encoding="utf-8"))
    assert released["status"] == "released"


def test_writer_lock_refuses_to_steal_live_owner_metadata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lock_path = tmp_path / "lamin-writer.lock"
    live_owner = _writer_metadata(run_id="live", pid=123)
    live_owner["status"] = "acquired"
    live_owner["process_start_ticks"] = "same-process"
    lock_path.write_text(json.dumps(live_owner) + "\n", encoding="utf-8")
    monkeypatch.setattr(runner, "_process_start_ticks", lambda pid: "same-process")

    with pytest.raises(RuntimeError, match="live owner"):
        with runner.lamin_writer_lock(lock_path, _writer_metadata(run_id="new")):
            pass


def test_writer_lock_treats_pid_reuse_as_stale_owner(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lock_path = tmp_path / "lamin-writer.lock"
    old_owner = _writer_metadata(run_id="old", pid=123)
    old_owner["status"] = "acquired"
    old_owner["process_start_ticks"] = "old-process"
    lock_path.write_text(json.dumps(old_owner) + "\n", encoding="utf-8")
    monkeypatch.setattr(runner, "_process_start_ticks", lambda pid: "reused-process")

    with runner.lamin_writer_lock(lock_path, _writer_metadata(run_id="new")):
        assert json.loads(lock_path.read_text(encoding="utf-8"))["run_id"] == "new"


def test_writer_lock_releases_after_exception(tmp_path: Path) -> None:
    lock_path = tmp_path / "lamin-writer.lock"

    with pytest.raises(ValueError, match="boom"):
        with runner.lamin_writer_lock(lock_path, _writer_metadata(run_id="failed")):
            raise ValueError("boom")

    with runner.lamin_writer_lock(lock_path, _writer_metadata(run_id="next")):
        assert json.loads(lock_path.read_text(encoding="utf-8"))["run_id"] == "next"


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
        hostname=sorted(runner.ALLOWED_HEAVY_HOSTS)[0],
        project=runner.EXPECTED_GCE_PROJECT,
        zone=runner.EXPECTED_ZONE,
        instance=sorted(runner.ALLOWED_HEAVY_HOSTS)[0],
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
    monkeypatch.setenv("PERT_GYM_LAMIN_WRITER_LOCK_DIR", str(tmp_path / "host-locks"))
    monkeypatch.setattr(runner, "preflight", lambda: _valid_preflight())
    monkeypatch.setattr(
        runner,
        "legacy_lamin_writer_lock_paths",
        lambda: (runner.legacy_lamin_writer_lock_path(),),
    )
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
    lock_metadata = json.loads(
        (tmp_path / "host-locks" / "lamin-writer.lock").read_text(encoding="utf-8")
    )
    assert lock_metadata["status"] == "released"
    assert {"run_id", "pid", "host", "project", "zone", "branch", "started_at"} <= (
        lock_metadata.keys()
    )
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
