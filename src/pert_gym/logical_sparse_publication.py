"""VM-gated, append-only publication of logical sparse-Zarr candidates.

A candidate is only consumable after its promotion marker.  The local publication
journal is identity-bound to the verified candidate manifest and lets an exact
interrupted attempt resume without revising artifacts or Collections.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, cast

from pert_gym.logical_sparse_zarr import read_logical_sparse_revision

_STAGE_ORDER = (
    "shared-var",
    "payload",
    "manifest",
    "migration-map",
    "collection-manifest",
    "collection",
    "promotion",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sha256_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def _safe_key(value: str) -> str:
    candidate = Path(value)
    if not value or candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("publication key must be a non-empty relative path")
    return candidate.as_posix()


def _artifact_id(artifact: Any) -> str:
    return str(getattr(artifact, "uid", None) or getattr(artifact, "id", None) or "")


def _exact_artifacts(ln: Any, keys: Iterable[str]) -> list[Any]:
    requested = list(keys)
    records = list(ln.Artifact.filter(key__in=requested).all())
    returned = [str(getattr(record, "key", "")) for record in records]
    if len(returned) != len(set(returned)):
        raise RuntimeError("duplicate artifacts returned during no-overwrite preflight")
    if set(returned) - set(requested):
        raise RuntimeError("artifact query returned unexpected key during preflight")
    return records


def _exact_collections(ln: Any, keys: Iterable[str]) -> list[Any]:
    requested = list(keys)
    collection_type = ln.Collection
    if not hasattr(collection_type, "filter"):
        raise RuntimeError("Lamin Collection API must support exact key preflight")
    records = list(collection_type.filter(key__in=requested).all())
    returned = [str(getattr(record, "key", "")) for record in records]
    if len(returned) != len(set(returned)) or set(returned) - set(requested):
        raise RuntimeError(
            "invalid Collection records returned during no-overwrite preflight"
        )
    return records


def assert_jkobject_branch(ln: Any) -> None:
    settings = ln.setup.settings
    if settings.instance.slug != "laminlabs/pertdata":
        raise RuntimeError("refusing publication outside laminlabs/pertdata")
    if settings.branch.name != "jkobject":
        raise RuntimeError("refusing publication outside pertdata jkobject branch")


def _verify_remote_file(artifact: Any, path: Path, *, key: str) -> None:
    """Checksum a remote readback while the original upload path is absent."""
    expected = _sha256_file(path)
    parked = path.with_name(f".{path.name}.upload-source")
    if parked.exists():
        raise RuntimeError(f"temporary publication source collision: {parked}")
    path.replace(parked)
    try:
        cached = Path(artifact.cache())
        if not cached.is_file() or _sha256_file(cached) != expected:
            raise RuntimeError(f"remote-backed payload checksum mismatch for {key}")
    finally:
        if parked.exists() and not path.exists():
            parked.replace(path)
        elif parked.exists():
            parked.unlink()


def _save_file(ln: Any, path: Path, *, key: str, description: str) -> Any:
    """Save then checksum a readback while the original upload path is absent.

    ``Artifact.cache()`` is allowed to recreate the original pathname from remote
    storage, but cannot merely return an extant local source file.  This keeps the
    adapter compatible with Lamin's generic immutable-file constructor while
    rejecting the same-local-file pseudo-readback that would mask an upload error.
    """
    artifact = ln.Artifact(path, key=key, description=description).save()
    _verify_remote_file(artifact, path, key=key)
    return artifact


def _reconcile_artifact(
    artifact: Any, path: Path, *, key: str, description: str
) -> Any:
    """Adopt one exact unjournaled Artifact, never saving or revising it."""
    if str(getattr(artifact, "key", "")) != key:
        raise RuntimeError("existing artifact key does not match publication identity")
    if getattr(artifact, "description", None) != description:
        raise RuntimeError(
            "existing artifact identity does not match publication identity"
        )
    _verify_remote_file(artifact, path, key=key)
    return artifact


def _collection_member_keys(collection: Any) -> list[str]:
    members = getattr(collection, "artifacts", None)
    if members is None:
        raise RuntimeError("existing Collection lacks membership contract")
    if hasattr(members, "all"):
        members = members.all()
    return [str(getattr(artifact, "key", "")) for artifact in members]


def _reconcile_collection(
    collection: Any, *, key: str, description: str, member_keys: Iterable[str]
) -> Any:
    """Adopt one exact unjournaled Collection, never saving or revising it."""
    if str(getattr(collection, "key", "")) != key:
        raise RuntimeError(
            "existing Collection key does not match publication identity"
        )
    if getattr(collection, "description", None) != description:
        raise RuntimeError(
            "existing Collection identity does not match publication identity"
        )
    if _collection_member_keys(collection) != list(member_keys):
        raise RuntimeError(
            "existing Collection membership does not match publication identity"
        )
    if getattr(collection, "revises", None) is not None:
        raise RuntimeError(
            "existing Collection revision does not match publication identity"
        )
    return collection


def _crash_after_remote_save(stage: str, stop_after_save_stage: str | None) -> None:
    if stop_after_save_stage == stage:
        raise RuntimeError(f"intentional crash after remote {stage} save")


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


def _journal_path(root: Path, logical_key: str, revision: str) -> Path:
    return root / logical_key / "revisions" / revision / "publication-journal.json"


def _load_or_create_journal(
    path: Path, identity: Mapping[str, object]
) -> dict[str, object]:
    if path.exists():
        journal = json.loads(path.read_text(encoding="utf-8"))
        if journal.get("identity") != identity:
            raise RuntimeError(
                "publication journal identity mismatch; refusing drifted resume"
            )
        if not isinstance(journal.get("completed_stages"), list):
            raise RuntimeError("publication journal completed_stages is invalid")
        return journal
    journal: dict[str, object] = {
        "format": "pert-gym.logical-sparse-zarr.publication-journal/v1",
        "identity": dict(identity),
        "completed_stages": [],
    }
    _write_json(path, journal)
    return journal


def _complete_stage(
    *,
    journal_path: Path,
    journal: dict[str, object],
    stage: str,
    stop_after_stage: str | None,
) -> None:
    completed = cast(list[str], journal["completed_stages"])
    if stage not in completed:
        completed.append(stage)
        _write_json(journal_path, journal)
    if stop_after_stage == stage:
        raise RuntimeError(f"intentional crash after {stage}")


def _build_payload(
    *, root: Path, logical_key: str, revision: str, target: Path, shared_key: str
) -> None:
    candidate = root / logical_key / "revisions" / revision
    shared = root / shared_key
    if not shared.is_file():
        raise RuntimeError("candidate manifest references a missing shared var")

    def normalized_member(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
        if info.name.endswith("/publication-journal.json"):
            return None
        info.mtime = 0
        info.uid = 0
        info.gid = 0
        info.uname = ""
        info.gname = ""
        return info

    with target.open("wb") as handle:
        with gzip.GzipFile(
            fileobj=handle, mode="wb", mtime=0, filename=""
        ) as gzip_handle:
            with tarfile.open(fileobj=gzip_handle, mode="w") as archive:
                archive.add(
                    candidate,
                    arcname=f"{logical_key}/revisions/{revision}",
                    filter=normalized_member,
                )
                archive.add(shared, arcname=shared_key, filter=normalized_member)


def publish_candidate(
    *,
    ln: Any,
    root: Path,
    logical_key: str,
    revision: str,
    collection_key: str,
    require_vm: Callable[[], object],
    stop_after_stage: str | None = None,
    stop_after_save_stage: str | None = None,
) -> dict[str, object]:
    """Publish a verified local candidate with exact, recoverable append-only stages.

    Fault injection arguments are test-only; the VM CLI never exposes them.
    """
    if stop_after_stage is not None and stop_after_stage not in _STAGE_ORDER:
        raise ValueError("unknown publication stage")
    if stop_after_save_stage is not None and stop_after_save_stage not in _STAGE_ORDER:
        raise ValueError("unknown publication stage")
    require_vm()
    assert_jkobject_branch(ln)
    logical_key = _safe_key(logical_key)
    collection_key = _safe_key(collection_key)
    surface, _matrix, _obs, _var = read_logical_sparse_revision(
        root, logical_key, revision
    )
    candidate = root / logical_key / "revisions" / revision
    manifest_path = candidate / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    shared_key = str(manifest.get("shared_var", {}).get("key", ""))
    if not shared_key or not (root / shared_key).is_file():
        raise RuntimeError("verified candidate lacks its manifest-required shared var")

    prefix = f"{logical_key}/revisions/{revision}"
    artifact_paths = {
        "shared-var": shared_key,
        "payload": f"{prefix}/payload.tar.gz",
        "manifest": f"{prefix}/manifest.json",
        "migration-map": f"{prefix}/migration-map.json",
        "collection-manifest": f"{prefix}/collection-manifest.json",
        "promotion": f"{logical_key}/promotions/{revision}.json",
    }
    migration_map = {
        "format": "pert-gym.legacy-to-logical-map/v1",
        "logical_key": logical_key,
        "revision": revision,
        "source_identity": manifest.get("source_identity"),
        "chunks": [
            {"chunk": index, "start": chunk.start, "end": chunk.end, "key": chunk.key}
            for index, chunk in enumerate(surface.chunks)
        ],
        "rollback": {"strategy": "append-only", "rejected_candidate_retained": True},
    }
    collection_manifest = {
        "format": "pert-gym.logical-sparse-zarr.collection/v1",
        "collection_key": collection_key,
        "logical_key": logical_key,
        "revision": revision,
        "manifest_key": artifact_paths["manifest"],
        "migration_map_key": artifact_paths["migration-map"],
        "shape": list(surface.shape),
        "nnz": surface.nnz,
    }
    identity = {
        "logical_key": logical_key,
        "revision": revision,
        "collection_key": collection_key,
        "manifest_sha256": _sha256_file(manifest_path),
        "shared_var_key": shared_key,
        "shared_var_sha256": _sha256_file(root / shared_key),
        "migration_map_sha256": _sha256_json(migration_map),
        "collection_manifest_sha256": _sha256_json(collection_manifest),
    }
    journal_path = _journal_path(root, logical_key, revision)
    journal = _load_or_create_journal(journal_path, identity)
    completed = set(cast(list[str], journal["completed_stages"]))
    artifact_existing = {
        record.key: record for record in _exact_artifacts(ln, artifact_paths.values())
    }
    collection_existing = _exact_collections(ln, [collection_key])
    for stage, key in artifact_paths.items():
        if stage in completed and key not in artifact_existing:
            raise RuntimeError(
                f"publication journal says {stage} completed but artifact is missing"
            )
    if "collection" in completed and not collection_existing:
        raise RuntimeError(
            "publication journal says collection completed but Collection is missing"
        )

    with tempfile.TemporaryDirectory(prefix="logical_sparse_publish_") as temporary:
        temporary_root = Path(temporary)
        payload_path = temporary_root / "payload.tar.gz"
        migration_path = temporary_root / "migration-map.json"
        collection_manifest_path = temporary_root / "collection-manifest.json"
        promotion_path = temporary_root / "promotion.json"
        _build_payload(
            root=root,
            logical_key=logical_key,
            revision=revision,
            target=payload_path,
            shared_key=shared_key,
        )
        _write_json(migration_path, migration_map)
        _write_json(collection_manifest_path, collection_manifest)

        stage_inputs = {
            "shared-var": (
                root / shared_key,
                artifact_paths["shared-var"],
                "logical sparse-Zarr shared var",
            ),
            "payload": (
                payload_path,
                artifact_paths["payload"],
                "logical sparse-Zarr immutable payload",
            ),
            "manifest": (
                manifest_path,
                artifact_paths["manifest"],
                "logical sparse-Zarr candidate manifest",
            ),
            "migration-map": (
                migration_path,
                artifact_paths["migration-map"],
                "legacy to logical sparse migration map",
            ),
            "collection-manifest": (
                collection_manifest_path,
                artifact_paths["collection-manifest"],
                "logical sparse candidate collection manifest",
            ),
            "promotion": (
                promotion_path,
                artifact_paths["promotion"],
                "atomic logical sparse candidate promotion marker",
            ),
        }
        for stage in _STAGE_ORDER:
            if stage == "collection":
                member_keys = [
                    artifact_paths[name]
                    for name in (
                        "shared-var",
                        "payload",
                        "manifest",
                        "migration-map",
                        "collection-manifest",
                    )
                ]
                description = "append-only logical sparse-Zarr candidate metadata"
                if stage not in completed:
                    if collection_existing:
                        collection = _reconcile_collection(
                            collection_existing[0],
                            key=collection_key,
                            description=description,
                            member_keys=member_keys,
                        )
                    else:
                        collection = ln.Collection(
                            [artifact_existing[key] for key in member_keys],
                            key=collection_key,
                            description=description,
                        )
                        if getattr(collection, "revises", None) is not None:
                            raise RuntimeError(
                                "refusing implicit Collection revision during publication"
                            )
                        collection = collection.save()
                        _crash_after_remote_save(stage, stop_after_save_stage)
                    collection_existing = [collection]
                    _complete_stage(
                        journal_path=journal_path,
                        journal=journal,
                        stage=stage,
                        stop_after_stage=stop_after_stage,
                    )
                continue
            path, key, description = stage_inputs[stage]
            if stage == "promotion":
                _write_json(
                    promotion_path,
                    {
                        "format": "pert-gym.logical-sparse-zarr.promotion/v1",
                        "logical_key": logical_key,
                        "revision": revision,
                        "candidate_collection_key": collection_key,
                        "candidate_collection_uid": _artifact_id(
                            collection_existing[0]
                        ),
                        "manifest_key": artifact_paths["manifest"],
                        "rollback_to": None,
                        "state": "candidate-complete",
                    },
                )
            existing = artifact_existing.get(key)
            if existing is not None:
                artifact_existing[key] = _reconcile_artifact(
                    existing, path, key=key, description=description
                )
            elif stage not in completed:
                artifact_existing[key] = _save_file(
                    ln, path, key=key, description=description
                )
                _crash_after_remote_save(stage, stop_after_save_stage)
            if stage not in completed:
                _complete_stage(
                    journal_path=journal_path,
                    journal=journal,
                    stage=stage,
                    stop_after_stage=stop_after_stage,
                )

    collection = collection_existing[0]
    promotion = artifact_existing[artifact_paths["promotion"]]
    return {
        "shared_var_key": artifact_paths["shared-var"],
        "manifest_key": artifact_paths["manifest"],
        "migration_map_key": artifact_paths["migration-map"],
        "payload_key": artifact_paths["payload"],
        "collection_manifest_key": artifact_paths["collection-manifest"],
        "promotion_key": artifact_paths["promotion"],
        "collection_key": collection_key,
        "collection_uid": _artifact_id(collection),
        "promotion_uid": _artifact_id(promotion),
        "branch": "jkobject",
    }


def record_rollback(
    *,
    ln: Any,
    logical_key: str,
    revision: str,
    reason: str,
    require_vm: Callable[[], object],
) -> dict[str, str]:
    """Append one VM-gated rollback event, never deleting or revising a candidate."""
    require_vm()
    assert_jkobject_branch(ln)
    if not reason:
        raise ValueError("rollback reason must be non-empty")
    logical_key = _safe_key(logical_key)
    event = {
        "format": "pert-gym.logical-sparse-zarr.rollback/v1",
        "revision": revision,
        "reason": reason,
        "strategy": "retain-candidate",
    }
    event_id = _sha256_json(event)[:20]
    key = f"{logical_key}/rollbacks/{revision}-{event_id}.json"
    if _exact_artifacts(ln, [key]):
        raise FileExistsError("refusing duplicate/no-overwrite rollback event")
    with tempfile.TemporaryDirectory(prefix="logical_sparse_rollback_") as temporary:
        path = Path(temporary) / "rollback.json"
        _write_json(path, event)
        artifact = _save_file(
            ln, path, key=key, description="logical sparse rollback record"
        )
    return {"key": key, "uid": _artifact_id(artifact)}
