"""VM-gated append-only registration for logical sparse-Zarr candidates.

The promotion marker is written last.  Consumers must resolve the marker rather
than list a prefix, so an interrupted upload remains an unpromoted candidate.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterable

from pert_gym.logical_sparse_zarr import read_logical_sparse_revision


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


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


def assert_jkobject_branch(ln: Any) -> None:
    settings = ln.setup.settings
    if settings.instance.slug != "laminlabs/pertdata":
        raise RuntimeError("refusing publication outside laminlabs/pertdata")
    if settings.branch.name != "jkobject":
        raise RuntimeError("refusing publication outside pertdata jkobject branch")


def _save_file(ln: Any, path: Path, *, key: str, description: str) -> Any:
    """Use Lamin's generic immutable file registration API and read it back."""
    artifact = ln.Artifact(path, key=key, description=description).save()
    cached = Path(artifact.cache())
    if _sha256_file(cached) != _sha256_file(path):
        raise RuntimeError(f"uploaded payload checksum mismatch for {key}")
    return artifact


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def publish_candidate(
    *,
    ln: Any,
    root: Path,
    logical_key: str,
    revision: str,
    collection_key: str,
    require_vm: Callable[[], object],
) -> dict[str, object]:
    """Register a verified candidate without mutating legacy artifacts or aliases.

    ``require_vm`` is injected for metadata-only contract tests; production passes
    ``tools.pert_gym_vm_runner.require_heavy_vm``.
    """
    require_vm()
    assert_jkobject_branch(ln)
    logical_key = _safe_key(logical_key)
    collection_key = _safe_key(collection_key)
    surface, _matrix, _obs, _var = read_logical_sparse_revision(root, logical_key, revision)
    candidate = root / logical_key / "revisions" / revision
    manifest_path = candidate / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError("verified candidate manifest is missing")
    prefix = f"{logical_key}/revisions/{revision}"
    manifest_key = f"{prefix}/manifest.json"
    migration_key = f"{prefix}/migration-map.json"
    payload_key = f"{prefix}/payload.tar.gz"
    promotion_key = f"{logical_key}/promotions/{revision}.json"
    collection_manifest_key = f"{prefix}/collection-manifest.json"
    requested = [
        manifest_key,
        migration_key,
        payload_key,
        promotion_key,
        collection_manifest_key,
    ]
    existing = _exact_artifacts(ln, requested)
    if existing:
        raise FileExistsError(
            "refusing duplicate/no-overwrite publication: "
            + ", ".join(sorted(str(record.key) for record in existing))
        )

    source_identity = json.loads(manifest_path.read_text(encoding="utf-8")).get(
        "source_identity"
    )
    migration_map = {
        "format": "pert-gym.legacy-to-logical-map/v1",
        "logical_key": logical_key,
        "revision": revision,
        "source_identity": source_identity,
        "chunks": [
            {"chunk": index, "start": chunk.start, "end": chunk.end, "key": chunk.key}
            for index, chunk in enumerate(surface.chunks)
        ],
        "rollback": {"strategy": "append-only", "rejected_candidate_retained": True},
    }
    with tempfile.TemporaryDirectory(prefix="logical_sparse_publish_") as temporary:
        temporary_root = Path(temporary)
        migration_path = temporary_root / "migration-map.json"
        collection_manifest_path = temporary_root / "collection-manifest.json"
        payload_path = Path(
            shutil.make_archive(
                str(temporary_root / "candidate-payload"), "gztar", root_dir=candidate
            )
        )
        _write_json(migration_path, migration_map)
        _write_json(
            collection_manifest_path,
            {
                "format": "pert-gym.logical-sparse-zarr.collection/v1",
                "collection_key": collection_key,
                "logical_key": logical_key,
                "revision": revision,
                "manifest_key": manifest_key,
                "migration_map_key": migration_key,
                "shape": list(surface.shape),
                "nnz": surface.nnz,
            },
        )
        artifacts = [
            _save_file(
                ln,
                payload_path,
                key=payload_key,
                description="logical sparse-Zarr immutable payload",
            ),
            _save_file(ln, manifest_path, key=manifest_key, description="logical sparse-Zarr candidate manifest"),
            _save_file(ln, migration_path, key=migration_key, description="legacy to logical sparse migration map"),
            _save_file(ln, collection_manifest_path, key=collection_manifest_key, description="logical sparse candidate collection manifest"),
        ]
        collection = ln.Collection(
            artifacts,
            key=collection_key,
            description="append-only logical sparse-Zarr candidate metadata",
        ).save()
        promotion_path = temporary_root / "promotion.json"
        _write_json(
            promotion_path,
            {
                "format": "pert-gym.logical-sparse-zarr.promotion/v1",
                "logical_key": logical_key,
                "revision": revision,
                "candidate_collection_key": collection_key,
                "candidate_collection_uid": _artifact_id(collection),
                "manifest_key": manifest_key,
                "rollback_to": None,
                "state": "candidate-complete",
            },
        )
        promotion = _save_file(
            ln, promotion_path, key=promotion_key, description="atomic logical sparse candidate promotion marker"
        )
    return {
        "manifest_key": manifest_key,
        "migration_map_key": migration_key,
        "payload_key": payload_key,
        "collection_manifest_key": collection_manifest_key,
        "promotion_key": promotion_key,
        "collection_key": collection_key,
        "collection_uid": _artifact_id(collection),
        "promotion_uid": _artifact_id(promotion),
        "branch": "jkobject",
    }


def record_rollback(*, ln: Any, logical_key: str, revision: str, reason: str) -> dict[str, str]:
    """Append a rollback record; never delete or rewrite a candidate."""
    assert_jkobject_branch(ln)
    if not reason:
        raise ValueError("rollback reason must be non-empty")
    logical_key = _safe_key(logical_key)
    with tempfile.TemporaryDirectory(prefix="logical_sparse_rollback_") as temporary:
        path = Path(temporary) / "rollback.json"
        key = f"{logical_key}/rollbacks/{revision}-{_sha256_file(Path(__file__))[:12]}.json"
        _write_json(path, {"revision": revision, "reason": reason, "strategy": "retain-candidate"})
        artifact = _save_file(ln, path, key=key, description="logical sparse rollback record")
    return {"key": key, "uid": _artifact_id(artifact)}
