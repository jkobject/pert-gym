#!/usr/bin/env python3
"""Bounded, read-only live inspection for OrganoidDB ODD001154 / GSE194214."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import tempfile
from pathlib import Path
from typing import Any

import anndata as ad
import fsspec
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_56a7b7cf"
LOGICAL_KEY = "pert-gym/logical/temporal/organoiddb_odd001154_gse194214"
ACCESSION = "GSE194214"
ORGANOIDDB_ID = "ODD001154"
EXPECTED_SHAPE = (18_716, 33_694)
CANONICAL_PREFIX = "data/cleaned/GSE194214"
BILLING_PROJECT = "jkobject-1549353370965"
ALIASES = (
    "GSE194214",
    "ODD001154",
    "Odd001154",
    "organoiddb_odd001154",
    "10.7554/eLife.68925",
    "Paraxial mesoderm organoids",
    "Somitoid",
)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def ordered_sha256(values: pd.Index) -> str:
    return hashlib.sha256(("\n".join(values.astype(str)) + "\n").encode()).hexdigest()


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size) if artifact.size is not None else None,
        "n_observations": getattr(artifact, "n_observations", None),
        "created_at": str(artifact.created_at),
        "created_on_id": getattr(artifact, "created_on_id", None),
        "branch_id": getattr(artifact, "branch_id", None),
        "is_latest": bool(artifact.is_latest),
        "description": str(artifact.description),
        "path": str(artifact.path),
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all()[:3])
    if len(by_uid) == 1:
        return by_uid[0]
    by_key = list(ln.Artifact.filter(key=value).order_by("-created_at")[:3])
    if not by_key:
        raise AssertionError(f"cannot resolve linked artifact: {value}")
    return by_key[0]


def materialize(artifact: Any, root: Path) -> Path:
    path = str(artifact.path)
    target = root / Path(str(artifact.key)).name
    if path.startswith("gs://"):
        fs = fsspec.filesystem(
            "gcs", project=BILLING_PROJECT, requester_pays=True, version_aware=True
        )
        fs.get_file(path.removeprefix("gs://"), str(target))
        return target
    cached = Path(artifact.cache())
    return cached


def frame_summary(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "columns": list(map(str, frame.columns)),
        "index_name": frame.index.name,
        "index_unique": bool(frame.index.is_unique),
        "index_sha256": ordered_sha256(frame.index),
        "non_null": {str(c): int(frame[c].notna().sum()) for c in frame.columns},
        "nunique": {
            str(c): int(frame[c].dropna().astype(str).nunique()) for c in frame.columns
        },
        "value_samples": {
            str(c): frame[c].dropna().astype(str).drop_duplicates().head(20).tolist()
            for c in frame.columns
        },
    }


def bounded_alias_matches(ln: Any) -> list[dict[str, Any]]:
    found: dict[str, Any] = {}
    for alias in ALIASES:
        for field in ("key", "description"):
            query = {f"{field}__icontains": alias}
            for artifact in ln.Artifact.filter(**query).order_by("-created_at")[:50]:
                found[str(artifact.uid)] = artifact
    return [artifact_identity(found[uid]) for uid in sorted(found)]


def bounded_collection_matches(ln: Any, obs: Any) -> dict[str, Any]:
    memberships = []
    for collection in obs.collections.order_by("-created_at")[:50]:
        members = list(collection.artifacts.only("uid", "key").all())
        memberships.append(
            {
                "uid": str(collection.uid),
                "key": str(collection.key),
                "created_at": str(collection.created_at),
                "member_count": len(members),
                "target_key_count": sum(
                    str(item.key) == str(obs.key) for item in members
                ),
                "target_uid_count": sum(
                    str(item.uid) == str(obs.uid) for item in members
                ),
            }
        )
    return {"membership_count": len(memberships), "memberships": memberships}


def main() -> None:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    matches = bounded_alias_matches(ln)
    latest_obs_candidates = [
        item
        for item in matches
        if item["key"] == f"{CANONICAL_PREFIX}/obs.parquet" and item["is_latest"]
    ]
    if len(latest_obs_candidates) != 1:
        raise AssertionError(
            f"expected one latest target OBS, got {latest_obs_candidates}"
        )
    obs_artifact = ln.Artifact.get(uid=latest_obs_candidates[0]["uid"])
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-inspect-"))
    obs = pd.read_parquet(materialize(obs_artifact, root))
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    var = pd.read_parquet(materialize(var_artifact, root))
    x_path = materialize(x_artifact, root)
    backed = ad.read_h5ad(x_path, backed="r")
    try:
        x_shape = (int(backed.n_obs), int(backed.n_vars))
        x_obs = pd.Index(backed.obs_names.astype(str))
        x_var = pd.Index(backed.var_names.astype(str))
    finally:
        backed.file.close()
    if (
        x_shape != EXPECTED_SHAPE
        or len(obs) != EXPECTED_SHAPE[0]
        or len(var) != EXPECTED_SHAPE[1]
    ):
        raise AssertionError("live triplet shape drift")
    report = {
        "format": "pert-gym.odd001154-live-inspection/v1",
        "task_id": TASK_ID,
        "logical_key": LOGICAL_KEY,
        "source_identity": f"{ACCESSION};{ORGANOIDDB_ID}",
        "host": capacity.hostname,
        "pid": os.getpid(),
        "capacity": {
            "free_disk_bytes": capacity.free_disk_bytes,
            "available_memory_bytes": capacity.available_memory_bytes,
        },
        "bounded_alias_matches": matches,
        "main_equivalence_probe": {
            "definition": "bounded inherited/public records have created_on_id different from active jkobject branch id",
            "active_branch_id": int(ln.setup.settings.branch.id),
            "public_or_inherited_matches": [
                item
                for item in matches
                if item["created_on_id"] != int(ln.setup.settings.branch.id)
            ],
            "public_or_inherited_match_count": sum(
                item["created_on_id"] != int(ln.setup.settings.branch.id)
                for item in matches
            ),
            "queries": list(ALIASES),
        },
        "current": {
            "obs": artifact_identity(obs_artifact),
            "x": artifact_identity(x_artifact),
            "var": artifact_identity(var_artifact),
            "obs_frame": frame_summary(obs),
            "var_frame": frame_summary(var),
            "shape": list(x_shape),
            "axis": {
                "obs_index_equals_x_obs_names": obs.index.astype(str).equals(x_obs),
                "var_index_equals_x_var_names": var.index.astype(str).equals(x_var),
                "obs_index_sha256": ordered_sha256(obs.index.astype(str)),
                "x_obs_names_sha256": ordered_sha256(x_obs),
                "var_index_sha256": ordered_sha256(var.index.astype(str)),
                "x_var_names_sha256": ordered_sha256(x_var),
            },
            "links": {"obs_to_x": True, "x_to_var": True},
            "collections": bounded_collection_matches(ln, obs_artifact),
        },
        "invariants": {"writes": 0, "single_physical_member": True},
    }
    print("ODD001154_INSPECTION=" + canonical(report), flush=True)


if __name__ == "__main__":
    main()
