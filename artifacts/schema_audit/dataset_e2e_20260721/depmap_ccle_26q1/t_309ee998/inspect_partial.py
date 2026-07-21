#!/usr/bin/env python3
"""Read-only diagnosis of the DepMap E2E Collection publication state."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from tools.lamin_context import connect_pertdata


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def snapshot(collection: Any, predecessor_uids: set[str]) -> dict[str, object]:
    members = list(collection.artifacts.all().only("uid", "key"))
    uids = [member.uid for member in members]
    keys = [str(member.key) for member in members]
    return {
        "uid": collection.uid,
        "key": collection.key,
        "hash": collection.hash,
        "description": collection.description,
        "member_count": len(members),
        "unique_uid_count": len(set(uids)),
        "unique_key_count": len(set(keys)),
        "obs_key_count": sum(key.endswith("/obs.parquet") for key in keys),
        "membership_sha256": hashlib.sha256(
            canonical(sorted(uids)).encode()
        ).hexdigest(),
        "contains_obs": "kCNSxyUJoJJKRSgE0004" in uids,
        "depmap_key_members": sorted(
            member.uid
            for member in members
            if str(member.key) == "depmap_ccle/26q1/obs.parquet"
        ),
        "added_vs_predecessor": sorted(set(uids) - predecessor_uids),
    }


ln = connect_pertdata()
out = {"collections": {}, "run": {}, "counts": {}}
predecessor_uids = set(
    ln.Collection.get(uid="WBFxVN9Alr8zFt9T0000").artifacts.values_list(
        "uid", flat=True
    )
)
for key in (
    "pert-gym/additions/20260719-temporal-v4-059-drosophila-dorsal-ventral",
    "pert-gym/dataset/depmap_ccle/26q1/20260721-e2e",
    "pert-gym/additions/20260721-depmap-ccle-26q1-e2e",
):
    out["collections"][key] = [
        snapshot(item, predecessor_uids) for item in ln.Collection.filter(key=key).all()
    ]
run = ln.Run.get(uid="Lz66ZdHZIrVyfIgn")
out["run"] = {
    "uid": run.uid,
    "started_at": run.started_at,
    "finished_at": run.finished_at,
    "output_artifacts": list(run.output_artifacts.values_list("uid", flat=True)),
}
out["counts"] = {
    "artifacts": ln.Artifact.filter().count(),
    "collections": ln.Collection.filter().count(),
    "runs": ln.Run.filter().count(),
    "transforms": ln.Transform.filter().count(),
}
print("HERMES_DIAG=" + canonical(out))
