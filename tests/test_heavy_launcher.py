from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest


def _launcher():
    from tools import launch_pert_gym_heavy

    return launch_pert_gym_heavy


def completed(command: list[str], *, returncode: int = 0, stdout: str = ""):
    return subprocess.CompletedProcess(command, returncode, stdout, "")


class FakeGcloud:
    def __init__(self, *, initial_status: str = "TERMINATED") -> None:
        self.calls: list[list[str]] = []
        self.instance = {
            "id": "3715582979673213789",
            "name": "pert-gym-worker-eu",
            "zone": "https://www.googleapis.com/compute/v1/projects/"
            "jkobject-1549353370965/zones/europe-west1-b",
            "status": initial_status,
            "labels": {"active-wave": "true", "do-not-stop": "true"},
        }

    def __call__(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(command)
        if "describe" in command:
            return completed(command, stdout=json.dumps(self.instance))
        if "add-labels" in command:
            labels_arg = command[command.index("--labels") + 1]
            self.instance["labels"].update(
                item.split("=", maxsplit=1) for item in labels_arg.split(",")
            )
            return completed(command)
        if "start" in command:
            self.instance["status"] = "RUNNING"
            return completed(command)
        if "ssh" in command:
            return completed(command)
        if "stop" in command:
            self.instance["status"] = "TERMINATED"
            return completed(command)
        raise AssertionError(f"unexpected command: {command}")


def test_launcher_publishes_exact_bounded_lease_before_start_and_payload(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud()
    local_lease = tmp_path / "leases" / "pert-gym-worker-eu.json"
    now = datetime(2026, 7, 16, 17, 0, tzinfo=timezone.utc)

    assert (
        launcher.launch_heavy_command(
            task="t_f8501514",
            eta_hours=5,
            command=["uv", "run", "python", "heavy.py"],
            local_lease_path=local_lease,
            now=now,
            run=fake,
        )
        == 0
    )

    expected_labels = {
        "owner": "jkobject",
        "project": "pert-gym",
        "purpose": "pert-gym-longrun",
        "task": "t-f8501514",
        "lease-until": "20260717t010000z",
    }
    assert all(
        fake.instance["labels"][key] == value for key, value in expected_labels.items()
    )
    local = json.loads(local_lease.read_text(encoding="utf-8"))
    assert local["allowed_instances"] == ["pert-gym-worker-eu"]
    assert local["expires_at"] == "2026-07-17T01:00:00Z"
    assert local["instance_expires_at"] == {
        "pert-gym-worker-eu": "2026-07-17T01:00:00Z"
    }
    assert local["task"] == "t_f8501514"

    operations = [
        next(
            operation
            for operation in ("describe", "add-labels", "start", "ssh", "stop")
            if operation in call
        )
        for call in fake.calls
    ]
    assert operations == [
        "describe",
        "add-labels",
        "describe",
        "describe",
        "start",
        "ssh",
        "stop",
    ]
    assert not local_lease.with_suffix(".json.tmp").exists()


def test_launcher_refuses_readback_mismatch_before_start_or_payload(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud()

    def corrupting_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        result = fake(command)
        if "add-labels" in command:
            fake.instance["labels"]["task"] = "wrong-task"
        return result

    local_lease = tmp_path / "lease.json"
    with pytest.raises(RuntimeError, match="lease label readback mismatch"):
        launcher.launch_heavy_command(
            task="t_f8501514",
            eta_hours=6,
            command=["heavy"],
            local_lease_path=local_lease,
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            run=corrupting_run,
        )

    assert not local_lease.exists()
    assert not any("start" in call or "ssh" in call for call in fake.calls)


def test_launcher_stops_only_after_clean_terminal_payload(tmp_path: Path) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="RUNNING")

    def failing_payload(command: list[str]) -> subprocess.CompletedProcess[str]:
        result = fake(command)
        if "ssh" in command:
            return completed(command, returncode=7)
        return result

    assert (
        launcher.launch_heavy_command(
            task="t_f8501514",
            eta_hours=8,
            command=["heavy"],
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            run=failing_payload,
        )
        == 7
    )
    assert not any("stop" in call for call in fake.calls)
    assert fake.instance["labels"]["lease-until"] == "20260716t100000z"


def test_payload_uses_long_running_transport_separate_from_bounded_gcloud(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="RUNNING")
    payload_calls: list[list[str]] = []

    def bounded_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        if "ssh" in command:
            raise AssertionError("payload must not use the bounded control transport")
        return fake(command)

    def payload_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        payload_calls.append(command)
        return completed(command, returncode=7)

    assert (
        launcher.launch_heavy_command(
            task="t_f8501514",
            eta_hours=8,
            command=["heavy"],
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            run=bounded_run,
            payload_run=payload_run,
        )
        == 7
    )
    assert len(payload_calls) == 1
    assert "ssh" in payload_calls[0]


def test_default_payload_transport_streams_without_python_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = _launcher()
    observed: dict[str, object] = {}

    def fake_subprocess_run(command: list[str], **kwargs: object):
        observed["command"] = command
        observed.update(kwargs)
        return completed(command)

    monkeypatch.setattr(launcher.subprocess, "run", fake_subprocess_run)
    assert launcher._run_payload(["gcloud", "compute", "ssh"]).returncode == 0
    assert observed["command"] == ["gcloud", "compute", "ssh"]
    assert observed.get("capture_output") is None
    assert observed.get("timeout") is None


def test_clean_terminal_stop_allows_next_task_to_replace_stale_labels(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud()
    now = datetime(2026, 7, 16, tzinfo=timezone.utc)

    assert (
        launcher.launch_heavy_command(
            task="t_f8501514",
            eta_hours=1,
            command=["first"],
            local_lease_path=tmp_path / "first.json",
            now=now,
            run=fake,
        )
        == 0
    )
    assert fake.instance["status"] == "TERMINATED"
    assert (
        launcher.launch_heavy_command(
            task="t_deadbeef",
            eta_hours=1,
            command=["second"],
            local_lease_path=tmp_path / "second.json",
            now=now,
            run=fake,
        )
        == 0
    )
    assert fake.instance["labels"]["task"] == "t-deadbeef"


def test_live_same_task_lease_beyond_policy_blocks_without_shortening(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="RUNNING")
    fake.instance["labels"].update(
        {"task": "t-f8501514", "lease-until": "20990101t000000z"}
    )

    with pytest.raises(RuntimeError, match="exceeds the 14 hour lease policy"):
        launcher.launch_heavy_command(
            task="t_f8501514",
            eta_hours=1,
            command=["heavy"],
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            run=fake,
        )

    assert fake.instance["labels"]["lease-until"] == "20990101t000000z"
    assert not any("add-labels" in call or "ssh" in call for call in fake.calls)


def test_lease_duration_has_eight_hour_floor_and_fourteen_hour_cap() -> None:
    launcher = _launcher()
    now = datetime(2026, 7, 16, tzinfo=timezone.utc)

    assert launcher.lease_deadline(now, eta_hours=1) == datetime(
        2026, 7, 16, 8, tzinfo=timezone.utc
    )
    assert launcher.lease_deadline(now, eta_hours=12) == datetime(
        2026, 7, 16, 14, tzinfo=timezone.utc
    )
    with pytest.raises(ValueError, match="exceeds the 14 hour lease policy"):
        launcher.lease_deadline(now, eta_hours=12.1)


@pytest.mark.parametrize("task", ["", "wrong", "t_ABCDEF12", "t_123"])
def test_task_identity_is_exact_and_fail_closed(task: str) -> None:
    launcher = _launcher()

    with pytest.raises(ValueError, match="task id"):
        launcher.task_label(task)
