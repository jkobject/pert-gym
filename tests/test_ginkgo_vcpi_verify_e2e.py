from __future__ import annotations

import hashlib
import importlib.util
import io
import subprocess
import tarfile
from pathlib import Path

import pytest

VERIFIER_PATH = (
    Path(__file__).parents[1]
    / "artifacts/schema_audit/dataset_e2e_20260721/ginkgo_vcpi/t_a8c96b03/verify_e2e.py"
)
SPEC = importlib.util.spec_from_file_location("ginkgo_vcpi_verify_e2e", VERIFIER_PATH)
assert SPEC is not None and SPEC.loader is not None
verifier = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verifier)


def _archive(path: Path, member: tarfile.TarInfo, content: bytes = b"") -> None:
    with tarfile.open(path, "w:gz") as archive:
        archive.addfile(member, io.BytesIO(content) if member.isfile() else None)


def test_safely_extract_payload_hashes_and_extracts_regular_file(
    tmp_path: Path,
) -> None:
    payload = tmp_path / "payload.tar.gz"
    member = tarfile.TarInfo("safe/data.txt")
    member.size = len(b"accepted")
    _archive(payload, member, b"accepted")

    observed_sha256 = verifier.safely_extract_payload(payload, tmp_path / "out")

    assert observed_sha256 == hashlib.sha256(payload.read_bytes()).hexdigest()
    assert (tmp_path / "out/safe/data.txt").read_bytes() == b"accepted"


@pytest.mark.parametrize("name", ["../outside.txt", "/tmp/outside.txt"])
def test_safely_extract_payload_rejects_escaping_paths(
    tmp_path: Path, name: str
) -> None:
    payload = tmp_path / "payload.tar.gz"
    member = tarfile.TarInfo(name)
    member.size = 1
    _archive(payload, member, b"x")

    with pytest.raises(AssertionError, match="unsafe archive member path"):
        verifier.safely_extract_payload(payload, tmp_path / "out")


@pytest.mark.parametrize("member_type", [tarfile.SYMTYPE, tarfile.LNKTYPE])
def test_safely_extract_payload_rejects_links(
    tmp_path: Path, member_type: bytes
) -> None:
    payload = tmp_path / "payload.tar.gz"
    member = tarfile.TarInfo("safe/link")
    member.type = member_type
    member.linkname = "../../outside.txt"
    _archive(payload, member)

    with pytest.raises(AssertionError, match="archive links are forbidden"):
        verifier.safely_extract_payload(payload, tmp_path / "out")


def test_resolve_git_state_observes_head_and_rejects_tracked_drift(
    tmp_path: Path,
) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.name", "Test"], check=True
    )
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.email", "test@example.com"],
        check=True,
    )
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(tmp_path), "add", "tracked.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "commit", "-q", "-m", "test"], check=True
    )
    head = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert verifier.resolve_git_state(tmp_path, expected_head=head) == {
        "root": str(tmp_path.resolve()),
        "expected_head": head,
        "head": head,
        "tracked_dirty": False,
    }

    tracked.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="tracked changes"):
        verifier.resolve_git_state(tmp_path, expected_head=head)


def test_resolve_git_state_requires_expected_revision(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)

    with pytest.raises(AssertionError, match="PERT_GYM_EXPECTED_GIT_HEAD is required"):
        verifier.resolve_git_state(tmp_path)
