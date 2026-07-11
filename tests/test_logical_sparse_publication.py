from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from pert_gym.legacy_triplet_adapter import build_legacy_revision
from pert_gym.logical_sparse_publication import publish_candidate, record_rollback
from pert_gym.logical_sparse_zarr import read_logical_sparse_revision
from tests.test_legacy_triplet_adapter import _triplet


class _Records:
    def __init__(self, records: list[object]):
        self.records = records

    def all(self) -> list[object]:
        return self.records


class _Artifact:
    def __init__(self, manager: _ArtifactManager, path: Path, *, key: str, description: str):
        self.manager = manager
        self.path = path
        self.key = key
        self.description = description
        self.uid = f"uid-{key}"

    def save(self) -> _Artifact:
        target = self.manager.remote_root / self.key.replace("/", "__")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.path, target)
        self.remote_path = target
        self.manager.existing.append(self)
        return self

    def cache(self) -> Path:
        assert not self.path.exists(), "readback must not reuse the upload source"
        return self.remote_path


class _ArtifactManager:
    def __init__(self, remote_root: Path, existing: list[_Artifact] | None = None):
        self.existing = existing or []
        self.created: list[_Artifact] = []
        self.remote_root = remote_root

    def filter(self, *, key__in: list[str]) -> _Records:
        return _Records([item for item in self.existing if item.key in key__in])

    def __call__(self, path: Path, *, key: str, description: str) -> _Artifact:
        artifact = _Artifact(self, path, key=key, description=description)
        self.created.append(artifact)
        return artifact


class _Collection:
    existing: list[_Collection] = []

    def __init__(self, artifacts: list[_Artifact], *, key: str, description: str):
        self.artifacts = artifacts
        self.key = key
        self.description = description
        self.uid = f"collection-{key}"

    @classmethod
    def filter(cls, *, key__in: list[str]) -> _Records:
        return _Records([item for item in cls.existing if item.key in key__in])

    def save(self) -> _Collection:
        type(self).existing.append(self)
        return self


class _Ln:
    def __init__(self, tmp_path: Path, *, branch: str = "jkobject", existing: list[_Artifact] | None = None):
        self.Artifact = _ArtifactManager(tmp_path / "remote", existing)
        self.Collection = _Collection
        self.Collection.existing = []
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


def _payload(ln: _Ln, key: str) -> _Artifact:
    return next(artifact for artifact in ln.Artifact.created if artifact.key == key)


def test_publish_candidate_is_self_contained_append_only_and_remote_readback(tmp_path: Path) -> None:
    root, logical_key, revision = _candidate(tmp_path)
    ln = _Ln(tmp_path)
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
    assert result["shared_var_key"].startswith("vars/")
    assert len(ln.Artifact.created) == 6

    restored = tmp_path / "restored"
    shutil.unpack_archive(_payload(ln, str(result["payload_key"])).remote_path, restored)
    _surface, matrix, _obs, var = read_logical_sparse_revision(restored, logical_key, revision)
    assert matrix.shape == (2, 2)
    assert var.index.tolist() == ["g1", "g2"]

    ln.Artifact.existing.append(
        _Artifact(ln.Artifact, Path(__file__), key=str(result["manifest_key"]), description="old")
    )
    with pytest.raises(RuntimeError, match="duplicate artifacts"):
        publish_candidate(
            ln=ln,
            root=root,
            logical_key=logical_key,
            revision=revision,
            collection_key="pert-gym/additions/test-r1",
            require_vm=lambda: None,
        )


@pytest.mark.parametrize(
    "stage", ["shared-var", "payload", "manifest", "migration-map", "collection-manifest", "collection", "promotion"]
)
def test_publish_resumes_every_exact_partial_stage_and_rejects_drift(tmp_path: Path, stage: str) -> None:
    root, logical_key, revision = _candidate(tmp_path)
    ln = _Ln(tmp_path)
    with pytest.raises(RuntimeError, match=f"intentional crash after {stage}"):
        publish_candidate(
            ln=ln,
            root=root,
            logical_key=logical_key,
            revision=revision,
            collection_key="pert-gym/additions/test-r1",
            require_vm=lambda: None,
            stop_after_stage=stage,
        )
    result = publish_candidate(
        ln=ln,
        root=root,
        logical_key=logical_key,
        revision=revision,
        collection_key="pert-gym/additions/test-r1",
        require_vm=lambda: None,
    )
    assert result["promotion_key"].endswith("promotions/r1.json")

    manifest = root / logical_key / "revisions" / revision / "manifest.json"
    manifest.write_text(manifest.read_text().replace("legacy-triplets/v1", "drifted-source/v1"))
    with pytest.raises(RuntimeError, match="publication journal identity mismatch"):
        publish_candidate(
            ln=ln,
            root=root,
            logical_key=logical_key,
            revision=revision,
            collection_key="pert-gym/additions/test-r1",
            require_vm=lambda: None,
        )


def test_publication_refuses_collection_collision(tmp_path: Path) -> None:
    root, logical_key, revision = _candidate(tmp_path)
    ln = _Ln(tmp_path)
    ln.Collection.existing = [_Collection([], key="pert-gym/additions/test-r1", description="old")]
    with pytest.raises(FileExistsError, match="Collection"):
        publish_candidate(
            ln=ln,
            root=root,
            logical_key=logical_key,
            revision=revision,
            collection_key="pert-gym/additions/test-r1",
            require_vm=lambda: None,
        )


def test_publication_refuses_main_and_rollback_is_vm_gated_and_non_colliding(tmp_path: Path) -> None:
    root, logical_key, revision = _candidate(tmp_path)
    with pytest.raises(RuntimeError, match="jkobject"):
        publish_candidate(
            ln=_Ln(tmp_path, branch="main"),
            root=root,
            logical_key=logical_key,
            revision=revision,
            collection_key="pert-gym/additions/test-r1",
            require_vm=lambda: None,
        )
    with pytest.raises(RuntimeError, match="jkobject"):
        record_rollback(
            ln=_Ln(tmp_path, branch="main"),
            logical_key=logical_key,
            revision=revision,
            reason="main refusal",
            require_vm=lambda: None,
        )

    calls = 0

    def require_vm() -> None:
        nonlocal calls
        calls += 1

    ln = _Ln(tmp_path)
    first = record_rollback(
        ln=ln,
        logical_key=logical_key,
        revision=revision,
        reason="contract test",
        require_vm=require_vm,
    )
    second = record_rollback(
        ln=ln,
        logical_key=logical_key,
        revision=revision,
        reason="different reason",
        require_vm=require_vm,
    )
    assert "/rollbacks/" in first["key"]
    assert first["key"] != second["key"]
    assert calls == 2
    with pytest.raises(RuntimeError, match="host guard"):
        record_rollback(
            ln=_Ln(tmp_path),
            logical_key=logical_key,
            revision=revision,
            reason="refuse host",
            require_vm=lambda: (_ for _ in ()).throw(RuntimeError("host guard")),
        )
