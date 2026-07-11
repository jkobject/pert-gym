from __future__ import annotations

from pathlib import Path

import pytest

from pert_gym.legacy_triplet_adapter import build_legacy_revision
from pert_gym.logical_sparse_publication import (
    publish_candidate,
    record_rollback,
)
from tests.test_legacy_triplet_adapter import _triplet


class _Records:
    def __init__(self, records: list[object]):
        self.records = records

    def all(self) -> list[object]:
        return self.records


class _Artifact:
    def __init__(self, path: Path, *, key: str, description: str):
        self.path = path
        self.key = key
        self.description = description
        self.uid = f"uid-{key}"

    def save(self) -> _Artifact:
        return self

    def cache(self) -> Path:
        return self.path


class _ArtifactManager:
    def __init__(self, existing: list[_Artifact] | None = None):
        self.existing = existing or []
        self.created: list[_Artifact] = []

    def filter(self, *, key__in: list[str]) -> _Records:
        return _Records([item for item in self.existing if item.key in key__in])

    def __call__(self, path: Path, *, key: str, description: str) -> _Artifact:
        artifact = _Artifact(path, key=key, description=description)
        self.created.append(artifact)
        return artifact


class _Collection:
    def __init__(self, artifacts: list[_Artifact], *, key: str, description: str):
        self.artifacts = artifacts
        self.key = key
        self.description = description
        self.uid = "collection-uid"

    def save(self) -> _Collection:
        return self


class _Ln:
    def __init__(self, *, branch: str = "jkobject", existing: list[_Artifact] | None = None):
        self.Artifact = _ArtifactManager(existing)
        self.Collection = _Collection
        self.setup = type(
            "Setup",
            (),
            {"settings": type("Settings", (), {"instance": type("I", (), {"slug": "laminlabs/pertdata"})(), "branch": type("B", (), {"name": branch})()})()},
        )()


def _candidate(tmp_path: Path) -> tuple[Path, str, str]:
    triplet = _triplet(tmp_path / "legacy", 0)
    build_legacy_revision(
        root=tmp_path / "out",
        logical_key="family",
        revision="r1",
        triplets=[triplet],
        schema_fingerprint="schema-v1",
        ingestion_run_id="test",
        min_rows=1,
        max_rows=2,
        max_rss_bytes=10**12,
    )
    return tmp_path / "out", "family", "r1"


def test_publish_candidate_is_append_only_and_readback_checked(tmp_path: Path) -> None:
    root, logical_key, revision = _candidate(tmp_path)
    ln = _Ln()
    result = publish_candidate(
        ln=ln,
        root=root,
        logical_key=logical_key,
        revision=revision,
        collection_key="pert-gym/additions/test-r1",
        require_vm=lambda: None,
    )
    assert result["branch"] == "jkobject"
    assert result["payload_key"].endswith("payload.tar.gz")
    assert result["promotion_key"].endswith("promotions/r1.json")
    assert len(ln.Artifact.created) == 5

    ln.Artifact.existing.append(
        _Artifact(Path(__file__), key=str(result["manifest_key"]), description="old")
    )
    with pytest.raises(FileExistsError, match="no-overwrite"):
        publish_candidate(
            ln=ln,
            root=root,
            logical_key=logical_key,
            revision=revision,
            collection_key="pert-gym/additions/test-r1",
            require_vm=lambda: None,
        )


def test_publication_refuses_main_and_rollback_is_append_only(tmp_path: Path) -> None:
    root, logical_key, revision = _candidate(tmp_path)
    with pytest.raises(RuntimeError, match="jkobject"):
        publish_candidate(
            ln=_Ln(branch="main"),
            root=root,
            logical_key=logical_key,
            revision=revision,
            collection_key="pert-gym/additions/test-r1",
            require_vm=lambda: None,
        )
    result = record_rollback(
        ln=_Ln(), logical_key=logical_key, revision=revision, reason="contract test"
    )
    assert "/rollbacks/" in result["key"]
