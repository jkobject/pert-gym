from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


def _launcher():
    from tools import launch_pert_gym_heavy

    return launch_pert_gym_heavy


def completed(
    command: list[str],
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
):
    return subprocess.CompletedProcess(command, returncode, stdout, stderr)


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
        if "remove-labels" in command:
            keys = command[command.index("--labels") + 1].split(",")
            for key in keys:
                self.instance["labels"].pop(key, None)
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
        "describe",
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


def test_terminated_instance_with_active_foreign_lease_blocks_before_publication(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    fake.instance["labels"].update(
        {"task": "t-deadbeef", "lease-until": "20260716t080000z"}
    )

    with pytest.raises(RuntimeError, match="active lease owned by another task"):
        launcher.launch_heavy_command(
            task="t_f8501514",
            eta_hours=1,
            command=["heavy"],
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            run=fake,
        )

    assert not any(
        operation in call
        for call in fake.calls
        for operation in ("add-labels", "start", "ssh", "stop")
    )


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


def test_expired_lease_replaced_during_payload_prevents_old_owner_stop(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="RUNNING")

    def replacing_payload(command: list[str]) -> subprocess.CompletedProcess[str]:
        result = fake(command)
        fake.instance["labels"].update(
            {"task": "t-deadbeef", "lease-until": "20260717t000000z"}
        )
        return result

    with pytest.raises(RuntimeError, match="lease label readback mismatch"):
        launcher.launch_heavy_command(
            task="t_f8501514",
            eta_hours=1,
            command=["heavy"],
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            run=fake,
            payload_run=replacing_payload,
        )

    assert fake.instance["status"] == "RUNNING"
    assert fake.instance["labels"]["task"] == "t-deadbeef"
    assert not any("stop" in call for call in fake.calls)


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


def test_default_verify_control_transport_uses_only_remaining_absolute_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = _launcher()
    observed: dict[str, object] = {}
    clock = ExactClock()
    clock.value = 4.0

    def fake_subprocess_run(command: list[str], **kwargs: object):
        observed["command"] = command
        observed.update(kwargs)
        return completed(command)

    monkeypatch.setattr(launcher.subprocess, "run", fake_subprocess_run)
    bounded = launcher._bounded_control_run(
        launcher._run,
        absolute_deadline=6.0,
        monotonic=clock.monotonic,
    )

    assert bounded(["gcloud", "compute", "ssh"]).returncode == 0
    assert observed["timeout"] == pytest.approx(2.0)


def test_clean_terminal_stop_allows_next_task_to_replace_expired_labels(
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
            now=now + timedelta(hours=9),
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


class FakePayloadProcess:
    def __init__(self, polls_before_exit: int, returncode: int = 0) -> None:
        self.polls_before_exit = polls_before_exit
        self.returncode = returncode
        self.killed = False

    def poll(self) -> int | None:
        if self.killed:
            return 124
        if self.polls_before_exit > 0:
            self.polls_before_exit -= 1
            return None
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 124 if self.killed else self.returncode


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += max(seconds, 300)


class ExactClock:
    def __init__(self, *, on_sleep=None) -> None:
        self.value = 0.0
        self.on_sleep = on_sleep

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds
        if self.on_sleep is not None:
            self.on_sleep()


def test_verify_only_waits_for_delayed_ssh_readiness_before_pid_operations(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    process = FakePayloadProcess(polls_before_exit=0)
    readiness_attempts = 0
    operations: list[str] = []

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal readiness_attempts
        if "ssh" in command and command[-1] == "true":
            operations.append("readiness")
            readiness_attempts += 1
            if readiness_attempts < 3:
                return completed(
                    command,
                    returncode=255,
                    stderr="ssh: connect to host 1.2.3.4 port 22: Connection refused",
                )
            return completed(command)
        if "ssh" in command and "rm -f --" in command[-1]:
            operations.append("pid-cleanup")
        if "ssh" in command and "cat --" in command[-1]:
            return completed(command, stdout="4242\n")
        return fake(command)

    def payload_start(command: list[str]) -> FakePayloadProcess:
        operations.append("payload-start")
        return process

    assert (
        launcher.launch_heavy_command(
            task="t_24eb37f7",
            eta_hours=0.1,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=5,
            absolute_max_minutes=10,
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            payload_start=payload_start,
            monotonic=ExactClock().monotonic,
            sleep=lambda seconds: None,
        )
        == 0
    )
    assert readiness_attempts == 3
    assert operations == [
        "readiness",
        "readiness",
        "readiness",
        "pid-cleanup",
        "payload-start",
    ]


def test_verify_only_readiness_timeout_stops_and_clears_exact_lease(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    clock = ExactClock()
    local_lease = tmp_path / "lease.json"
    payload_started = False

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        if "ssh" in command and command[-1] == "true":
            return completed(command, returncode=255, stderr="Connection refused")
        return fake(command)

    def payload_start(command: list[str]) -> FakePayloadProcess:
        nonlocal payload_started
        payload_started = True
        return FakePayloadProcess(0)

    with pytest.raises(RuntimeError, match="SSH readiness timeout exhausted"):
        launcher.launch_heavy_command(
            task="t_24eb37f7",
            eta_hours=0.1,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=5,
            absolute_max_minutes=10,
            readiness_timeout_seconds=10,
            local_lease_path=local_lease,
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            payload_start=payload_start,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert payload_started is False
    assert not any("rm -f --" in call[-1] for call in fake.calls if "ssh" in call)
    assert fake.instance["status"] == "TERMINATED"
    assert fake.instance["labels"] == {"active-wave": "true", "do-not-stop": "true"}
    assert not local_lease.exists()


@pytest.mark.parametrize(
    "error",
    [
        "Permission denied (publickey).",
        "Host key verification failed.",
    ],
)
def test_verify_only_readiness_does_not_retry_auth_or_host_key_failure(
    tmp_path: Path, error: str
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    attempts = 0

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal attempts
        if "ssh" in command and command[-1] == "true":
            attempts += 1
            return completed(
                command,
                returncode=255,
                stderr=error,
            )
        return fake(command)

    with pytest.raises(RuntimeError, match="non-retryable SSH readiness failure"):
        launcher.launch_heavy_command(
            task="t_24eb37f7",
            eta_hours=0.1,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=5,
            absolute_max_minutes=10,
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            sleep=lambda seconds: None,
        )
    assert attempts == 1
    assert fake.instance["status"] == "TERMINATED"


def test_verify_only_readiness_rechecks_exact_lease_ownership_before_retry(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    attempts = 0

    def replace_owner() -> None:
        fake.instance["labels"]["task"] = "t-deadbeef"

    clock = ExactClock(on_sleep=replace_owner)

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal attempts
        if "ssh" in command and command[-1] == "true":
            attempts += 1
            return completed(command, returncode=255, stderr="Connection refused")
        return fake(command)

    with pytest.raises(RuntimeError, match="lease label readback mismatch"):
        launcher.launch_heavy_command(
            task="t_24eb37f7",
            eta_hours=0.1,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=5,
            absolute_max_minutes=10,
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
    assert attempts == 1
    assert fake.instance["status"] == "RUNNING"
    assert fake.instance["labels"]["task"] == "t-deadbeef"
    assert not any("rm -f --" in call[-1] for call in fake.calls if "ssh" in call)


def test_verify_only_readiness_never_crosses_absolute_ceiling(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    clock = ExactClock()

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        if "ssh" in command and command[-1] == "true":
            return completed(command, returncode=255, stderr="Connection timed out")
        return fake(command)

    with pytest.raises(RuntimeError, match="SSH readiness absolute ceiling exhausted"):
        launcher.launch_heavy_command(
            task="t_24eb37f7",
            eta_hours=0.1,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=0.05,
            absolute_max_minutes=0.1,
            readiness_timeout_seconds=60,
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert clock.value == pytest.approx(6.0)
    assert fake.instance["status"] == "TERMINATED"
    assert fake.instance["labels"] == {"active-wave": "true", "do-not-stop": "true"}


def test_verify_only_readiness_success_after_absolute_ceiling_never_dispatches(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    clock = ExactClock()
    payload_started = False

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        if "ssh" in command and command[-1] == "true":
            clock.value = 7.0
            return completed(command)
        return fake(command)

    def payload_start(command: list[str]) -> FakePayloadProcess:
        nonlocal payload_started
        payload_started = True
        return FakePayloadProcess(0)

    with pytest.raises(RuntimeError, match="SSH readiness absolute ceiling exhausted"):
        launcher.launch_heavy_command(
            task="t_24eb37f7",
            eta_hours=0.1,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=0.05,
            absolute_max_minutes=0.1,
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            payload_start=payload_start,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert payload_started is False
    assert fake.instance["status"] == "TERMINATED"
    assert fake.instance["labels"] == {"active-wave": "true", "do-not-stop": "true"}


def test_verify_only_absolute_ceiling_includes_lease_publication(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    clock = ExactClock()
    payload_started = False

    def delayed_publication(command: list[str]) -> subprocess.CompletedProcess[str]:
        result = fake(command)
        if "add-labels" in command:
            clock.value = 7.0
        return result

    def payload_start(command: list[str]) -> FakePayloadProcess:
        nonlocal payload_started
        payload_started = True
        return FakePayloadProcess(0)

    with pytest.raises(RuntimeError, match="verify-only absolute ceiling exhausted"):
        launcher.launch_heavy_command(
            task="t_24eb37f7",
            eta_hours=0.1,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=0.05,
            absolute_max_minutes=0.1,
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=delayed_publication,
            payload_start=payload_start,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert payload_started is False
    assert fake.instance["status"] == "TERMINATED"
    assert fake.instance["labels"] == {"active-wave": "true", "do-not-stop": "true"}


def test_verify_only_pid_read_crossing_absolute_ceiling_fails_closed(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    clock = ExactClock()
    process = FakePayloadProcess(polls_before_exit=10)

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        if "ssh" in command and "cat --" in command[-1]:
            clock.value = 7.0
            return completed(command, stdout="4242\n")
        return fake(command)

    with pytest.raises(RuntimeError, match="verify-only absolute ceiling exhausted"):
        launcher.launch_heavy_command(
            task="t_24eb37f7",
            eta_hours=0.1,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=0.05,
            absolute_max_minutes=0.1,
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            payload_start=lambda command: process,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert fake.instance["status"] == "TERMINATED"
    assert fake.instance["labels"] == {"active-wave": "true", "do-not-stop": "true"}


def test_verify_only_start_failure_cleans_valid_owner_lease(tmp_path: Path) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    local_lease = tmp_path / "lease.json"

    def failing_start(command: list[str]) -> subprocess.CompletedProcess[str]:
        result = fake(command)
        if "start" in command:
            return completed(command, returncode=1, stderr="control response lost")
        return result

    with pytest.raises(RuntimeError, match="instance start failed"):
        launcher.launch_heavy_command(
            task="t_24eb37f7",
            eta_hours=0.1,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=5,
            absolute_max_minutes=10,
            local_lease_path=local_lease,
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=failing_start,
        )

    assert fake.instance["status"] == "TERMINATED"
    assert fake.instance["labels"] == {"active-wave": "true", "do-not-stop": "true"}
    assert not local_lease.exists()


def test_verify_only_launcher_uses_bounded_purpose_and_clears_terminal_lease(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    local_lease = tmp_path / "verify-lease.json"
    process = FakePayloadProcess(polls_before_exit=1)

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        if "ssh" in command and "cat --" in command[-1]:
            return completed(command, stdout="4242\n")
        if "ssh" in command and "kill -0" in command[-1]:
            return completed(command)
        return fake(command)

    assert (
        launcher.launch_heavy_command(
            task="t_eb3a96ca",
            eta_hours=0.5,
            command=["uv", "run", "python", "verify.py"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=60,
            absolute_max_minutes=90,
            local_lease_path=local_lease,
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            payload_start=lambda command: process,
            sleep=lambda seconds: None,
        )
        == 0
    )

    published = next(call for call in fake.calls if "add-labels" in call)
    labels = published[published.index("--labels") + 1]
    assert "purpose=review-pr104-drugseq-verify-only" in labels
    assert "lease-until=20260721t130000z" in labels
    assert fake.instance["status"] == "TERMINATED"
    assert (
        not {
            "owner",
            "project",
            "purpose",
            "task",
            "lease-until",
        }
        & fake.instance["labels"].keys()
    )
    assert not local_lease.exists()


def test_verify_only_policy_rejects_unbounded_lifecycle_before_publication(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud()
    base = {
        "task": "t_eb3a96ca",
        "eta_hours": 0.5,
        "command": ["verify"],
        "purpose": "review-pr104-drugseq-verify-only",
        "verify_only": True,
        "local_lease_path": tmp_path / "lease.json",
        "run": fake,
    }

    with pytest.raises(ValueError, match="at most 60 minutes"):
        launcher.launch_heavy_command(
            **base,
            lease_minutes=61,
            absolute_max_minutes=90,
        )
    with pytest.raises(ValueError, match="at most 90 minutes"):
        launcher.launch_heavy_command(
            **base,
            lease_minutes=60,
            absolute_max_minutes=91,
        )
    with pytest.raises(ValueError, match="exact verify-only purpose"):
        launcher.launch_heavy_command(
            **{**base, "purpose": launcher.PURPOSE},
            lease_minutes=60,
            absolute_max_minutes=90,
        )

    assert not fake.calls


def test_verify_only_renewal_requires_exact_live_pid_and_honors_absolute_ceiling(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="RUNNING")
    process = FakePayloadProcess(polls_before_exit=20)
    clock = FakeClock()

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        if "ssh" in command and "cat --" in command[-1] and "kill" not in command[-1]:
            return completed(command, stdout="4242\n")
        if "ssh" in command and "kill -0" in command[-1]:
            return completed(command)
        return fake(command)

    assert (
        launcher.launch_heavy_command(
            task="t_eb3a96ca",
            eta_hours=0.25,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=10,
            absolute_max_minutes=20,
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            payload_start=lambda command: process,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
        == 124
    )

    publications = [call for call in fake.calls if "add-labels" in call]
    assert len(publications) >= 2
    lease_values = [
        dict(
            item.split("=", maxsplit=1)
            for item in call[call.index("--labels") + 1].split(",")
        )["lease-until"]
        for call in publications
    ]
    assert max(lease_values) == "20260721t122000z"
    assert fake.instance["status"] == "TERMINATED"
    assert fake.instance["labels"] == {
        "active-wave": "true",
        "do-not-stop": "true",
    }


def test_verify_only_refuses_renewal_after_exact_payload_pid_dies(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="RUNNING")
    process = FakePayloadProcess(polls_before_exit=5)
    clock = FakeClock()

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        if "ssh" in command and "cat --" in command[-1] and "kill" not in command[-1]:
            return completed(command, stdout="4242\n")
        if "ssh" in command and "kill -0" in command[-1]:
            return completed(command, returncode=1)
        return fake(command)

    with pytest.raises(RuntimeError, match="exact live payload PID"):
        launcher.launch_heavy_command(
            task="t_eb3a96ca",
            eta_hours=0.25,
            command=["verify"],
            purpose="review-pr104-drugseq-verify-only",
            verify_only=True,
            lease_minutes=10,
            absolute_max_minutes=20,
            local_lease_path=tmp_path / "lease.json",
            now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            payload_start=lambda command: process,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert len([call for call in fake.calls if "add-labels" in call]) == 1
    assert fake.instance["status"] == "TERMINATED"
    assert fake.instance["labels"] == {
        "active-wave": "true",
        "do-not-stop": "true",
    }


def test_bounded_writer_accepts_approved_three_hour_lifecycle(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud(initial_status="TERMINATED")
    local_lease = tmp_path / "writer-lease.json"
    process = FakePayloadProcess(polls_before_exit=1)

    def control_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        if "ssh" in command and "cat --" in command[-1]:
            return completed(command, stdout="4242\n")
        if "ssh" in command and "kill -0" in command[-1]:
            return completed(command)
        return fake(command)

    assert (
        launcher.launch_heavy_command(
            task="t_79ff033e",
            eta_hours=2,
            command=["uv", "run", "python", "curate_gse132080.py"],
            purpose="gse132080-obs-var-curation",
            lease_minutes=150,
            absolute_max_minutes=180,
            local_lease_path=local_lease,
            now=datetime(2026, 7, 22, 12, 0, tzinfo=timezone.utc),
            run=control_run,
            payload_start=lambda command: process,
            sleep=lambda seconds: None,
        )
        == 0
    )

    published = next(call for call in fake.calls if "add-labels" in call)
    labels = published[published.index("--labels") + 1]
    assert "purpose=gse132080-obs-var-curation" in labels
    assert "lease-until=20260722t143000z" in labels
    assert fake.instance["status"] == "TERMINATED"
    assert fake.instance["labels"] == {
        "active-wave": "true",
        "do-not-stop": "true",
    }
    assert not local_lease.exists()


def test_bounded_writer_rejects_lifecycle_over_six_hours_before_publication(
    tmp_path: Path,
) -> None:
    launcher = _launcher()
    fake = FakeGcloud()

    with pytest.raises(ValueError, match="at most 360 minutes"):
        launcher.launch_heavy_command(
            task="t_79ff033e",
            eta_hours=2,
            command=["writer"],
            purpose="gse132080-obs-var-curation",
            lease_minutes=150,
            absolute_max_minutes=361,
            local_lease_path=tmp_path / "lease.json",
            run=fake,
        )

    assert not fake.calls


def test_minute_lifecycle_options_must_be_provided_together(tmp_path: Path) -> None:
    launcher = _launcher()
    fake = FakeGcloud()

    with pytest.raises(ValueError, match="must be provided together"):
        launcher.launch_heavy_command(
            task="t_79ff033e",
            eta_hours=2,
            command=["writer"],
            purpose="gse132080-obs-var-curation",
            lease_minutes=150,
            local_lease_path=tmp_path / "lease.json",
            run=fake,
        )

    assert not fake.calls
