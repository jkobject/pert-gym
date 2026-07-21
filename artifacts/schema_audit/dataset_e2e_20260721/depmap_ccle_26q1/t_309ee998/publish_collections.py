#!/usr/bin/env python3
"""Idempotently publish DepMap 26Q1 dataset and global Collection membership."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

from tools.lamin_context import connect_pertdata

TASK_ID = "t_309ee998"
DATASET_ID = "depmap_ccle/26q1"
OBS_UID = "kCNSxyUJoJJKRSgE0004"
PREDECESSOR_OBS_UID = "kCNSxyUJoJJKRSgE0000"
OBS_KEY = "depmap_ccle/26q1/obs.parquet"
OBS_HASH = "Zm-yc0UfSwnYI1DnDfoccQ"
X_UID = "fUSYT9ArHdQye5qv0001"
X_KEY = "depmap_ccle/26q1/X.h5ad"
X_HASH = "I1DppOQzGK8jczy2Lh_J9O"
VAR_UID = "0S0wAPqgigynI4Av0003"
VAR_KEY = "depmap_ccle/26q1/var.parquet"
VAR_HASH = "5wjqSsaFA7D0kcZSts--ig"
PAYLOAD_MANIFEST = (
    "gs://scperturb/pert-gym/staging/pert-gym/logical/depmap_ccle26q1/"
    "revisions/depmap-ccle26q1-default-models-wave01-960f9db1b737f306/"
    "manifest.json#1784226218253256"
)
PAYLOAD_MANIFEST_SHA256 = (
    "ad3d220f2a0550d63d76ff944e93454a658dfa16efe4a3b3be7239ff0e492ecc"
)
PREDECESSOR_UID = "WBFxVN9Alr8zFt9T0000"
PREDECESSOR_KEY = (
    "pert-gym/additions/20260719-temporal-v4-059-drosophila-dorsal-ventral"
)
PREDECESSOR_HASH = "JtWQXByhDd6wVBbNI0_VRw"
PREDECESSOR_MEMBER_COUNT = 1016
PREDECESSOR_MEMBERSHIP_SHA256 = (
    "796581b630386a90f8cc2b4da1df8f46ea7fe6316322e93832c32c245fcf8096"
)
DATASET_COLLECTION_KEY = "pert-gym/dataset/depmap_ccle/26q1/20260721-e2e"
REJECTED_GLOBAL_KEY = "pert-gym/additions/20260721-depmap-ccle-26q1-e2e"
REJECTED_GLOBAL_UID = "aH1RWvlWo87xGpsb0000"
REJECTED_GLOBAL_HASH = "z6blPVzHnMTIpoEpgdtusg"
REJECTED_GLOBAL_MEMBERSHIP_SHA256 = (
    "89eaf5e0d08034520b63a584e155331d12ff94b4d0153d09db11f2be6d4d5bae"
)
GLOBAL_SUCCESSOR_KEY = "pert-gym/additions/20260721-depmap-ccle-26q1-e2e-r2"
LOCK_ROOT = Path("/tmp/pert-gym")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def membership_sha256(uids: list[str]) -> str:
    return hashlib.sha256(canonical(sorted(uids)).encode()).hexdigest()


def feature_key(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "key", value))


def collection_snapshot(collection: Any) -> tuple[list[Any], dict[str, Any]]:
    members = list(collection.artifacts.all().only("uid", "key"))
    uids = [str(member.uid) for member in members]
    keys = [str(member.key) for member in members]
    return members, {
        "uid": str(collection.uid),
        "key": str(collection.key),
        "hash": str(collection.hash),
        "description": str(collection.description),
        "member_count": len(members),
        "unique_uid_count": len(set(uids)),
        "unique_key_count": len(set(keys)),
        "obs_key_count": sum(key.endswith("/obs.parquet") for key in keys),
        "membership_sha256": membership_sha256(uids),
    }


def exact_artifact(ln: Any, *, uid: str, key: str, hash_: str) -> Any:
    artifact = ln.Artifact.get(uid=uid)
    if (
        artifact.key != key
        or artifact.hash != hash_
        or not artifact.is_latest
        or ln.Artifact.filter(key=key, is_latest=True).count() != 1
    ):
        raise AssertionError(f"current Artifact identity drift for {key}")
    return artifact


def exact_collection(ln: Any, key: str) -> Any | None:
    matches = list(ln.Collection.filter(key=key).all())
    if len(matches) > 1:
        raise AssertionError(f"duplicate Collection key: {key}")
    return matches[0] if matches else None


def dataset_description() -> str:
    return canonical(
        {
            "format": "pert-gym.dataset-e2e-collection/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "source": "DepMap/CCLE",
            "source_accession": "26q1",
            "source_evidence": "https://depmap.org/portal/download/all/",
            "source_rows_total": 1775,
            "selected_default_model_rows": 1719,
            "non_default_source_rows": 56,
            "n_vars": 19215,
            "obs_uid": OBS_UID,
            "x_uid": X_UID,
            "var_uid": VAR_UID,
            "payload_manifest": PAYLOAD_MANIFEST,
            "payload_manifest_sha256": PAYLOAD_MANIFEST_SHA256,
            "membership_rule": "canonical OBS only; follow OBS->X->VAR feature links",
            "rollback": "select immutable Artifacts by exact predecessor UIDs",
        }
    )


def global_description(result_membership_sha256: str) -> str:
    return canonical(
        {
            "format": "pert-gym.append-only-dataset-e2e-successor/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "added_obs_uid": OBS_UID,
            "replaced_obs_uid": PREDECESSOR_OBS_UID,
            "member_count_before": PREDECESSOR_MEMBER_COUNT,
            "member_count_after": PREDECESSOR_MEMBER_COUNT,
            "predecessor_uid": PREDECESSOR_UID,
            "predecessor_key": PREDECESSOR_KEY,
            "predecessor_membership_sha256": PREDECESSOR_MEMBERSHIP_SHA256,
            "resulting_membership_sha256": result_membership_sha256,
            "membership_rule": (
                "immutable predecessor with same-key obsolete OBS replaced by exact "
                "current OBS; no duplicate artifact keys"
            ),
            "rejected_predecessor_successor_uid": REJECTED_GLOBAL_UID,
            "rollback": f"select immutable predecessor Collection {PREDECESSOR_UID}",
        }
    )


def acquire(path: Path) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    handle.seek(0)
    handle.truncate()
    handle.write(
        canonical({"task_id": TASK_ID, "pid": os.getpid(), "at": int(time.time())})
        + "\n"
    )
    handle.flush()
    os.fsync(handle.fileno())
    return handle


def acquire_locks() -> tuple[list[Any], list[str]]:
    family = hashlib.sha256(DATASET_ID.encode()).hexdigest()
    predecessor = hashlib.sha256(PREDECESSOR_UID.encode()).hexdigest()
    paths = [
        LOCK_ROOT / "lamin-writer.lock",
        LOCK_ROOT / "current-legacy-writer.lock",
        LOCK_ROOT / "families" / f"{family}.lock",
        LOCK_ROOT / "collections" / f"{predecessor}.lock",
    ]
    handles: list[Any] = []
    try:
        for path in paths:
            handles.append(acquire(path))
    except Exception:
        release_locks(handles)
        raise
    return handles, [str(path) for path in paths]


def release_locks(handles: list[Any]) -> None:
    for handle in reversed(handles):
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def preflight(ln: Any) -> dict[str, Any]:
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise AssertionError("unexpected Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("unexpected Lamin branch")
    obs = exact_artifact(ln, uid=OBS_UID, key=OBS_KEY, hash_=OBS_HASH)
    x = exact_artifact(ln, uid=X_UID, key=X_KEY, hash_=X_HASH)
    var = exact_artifact(ln, uid=VAR_UID, key=VAR_KEY, hash_=VAR_HASH)
    if feature_key(obs.features.get_values().get("X")) != X_KEY:
        raise AssertionError("OBS -> X link drift")
    if feature_key(x.features.get_values().get("var")) != VAR_KEY:
        raise AssertionError("X -> VAR link drift")
    if (obs.n_observations, x.n_observations, var.n_observations) != (
        1719,
        1719,
        19215,
    ):
        raise AssertionError("OBS/X/VAR denominator drift")
    predecessor = ln.Collection.get(uid=PREDECESSOR_UID)
    predecessor_members, predecessor_snapshot = collection_snapshot(predecessor)
    expected_predecessor = {
        "uid": PREDECESSOR_UID,
        "key": PREDECESSOR_KEY,
        "hash": PREDECESSOR_HASH,
        "member_count": PREDECESSOR_MEMBER_COUNT,
        "unique_uid_count": PREDECESSOR_MEMBER_COUNT,
        "unique_key_count": PREDECESSOR_MEMBER_COUNT,
        "obs_key_count": PREDECESSOR_MEMBER_COUNT,
        "membership_sha256": PREDECESSOR_MEMBERSHIP_SHA256,
    }
    for key, expected in expected_predecessor.items():
        if predecessor_snapshot[key] != expected:
            raise AssertionError(f"predecessor Collection drift for {key}")
    predecessor_uids = {member.uid for member in predecessor_members}
    if OBS_UID in predecessor_uids:
        raise AssertionError("target OBS is already in predecessor")
    predecessor_same_key = [
        member.uid for member in predecessor_members if str(member.key) == OBS_KEY
    ]
    if predecessor_same_key != [PREDECESSOR_OBS_UID]:
        raise AssertionError(
            "accepted predecessor does not have the exact obsolete OBS"
        )
    rejected = ln.Collection.get(uid=REJECTED_GLOBAL_UID)
    rejected_members, rejected_snapshot = collection_snapshot(rejected)
    rejected_same_key = sorted(
        member.uid for member in rejected_members if str(member.key) == OBS_KEY
    )
    if (
        rejected_snapshot["key"] != REJECTED_GLOBAL_KEY
        or rejected_snapshot["hash"] != REJECTED_GLOBAL_HASH
        or rejected_snapshot["member_count"] != 1017
        or rejected_snapshot["unique_uid_count"] != 1017
        or rejected_snapshot["unique_key_count"] != 1016
        or rejected_snapshot["membership_sha256"] != REJECTED_GLOBAL_MEMBERSHIP_SHA256
        or rejected_same_key != [PREDECESSOR_OBS_UID, OBS_UID]
    ):
        raise AssertionError("rejected first successor identity drift")
    intervening = []
    for collection in ln.Collection.filter(key__startswith="pert-gym/additions/").all():
        if collection.uid == PREDECESSOR_UID:
            continue
        try:
            description = json.loads(collection.description or "{}")
        except json.JSONDecodeError:
            continue
        if description.get("predecessor_uid") == PREDECESSOR_UID:
            intervening.append(collection.uid)
    allowed = {REJECTED_GLOBAL_UID}
    existing_successor = exact_collection(ln, GLOBAL_SUCCESSOR_KEY)
    if existing_successor is not None:
        allowed.add(existing_successor.uid)
    unexpected = sorted(set(intervening) - allowed)
    if unexpected:
        raise AssertionError(
            f"intervening successor(s) from accepted predecessor: {unexpected}"
        )
    return {
        "obs": obs,
        "x": x,
        "var": var,
        "predecessor": predecessor,
        "predecessor_members": predecessor_members,
        "predecessor_snapshot": predecessor_snapshot,
        "rejected_successor_snapshot": rejected_snapshot,
    }


def reconcile(ln: Any, state: dict[str, Any], *, allow_create: bool) -> dict[str, Any]:
    obs = state["obs"]
    writes = 0
    created: list[str] = []
    dataset = exact_collection(ln, DATASET_COLLECTION_KEY)
    if dataset is None:
        if not allow_create:
            raise AssertionError("dataset Collection is absent")
        dataset = ln.Collection(
            [obs], key=DATASET_COLLECTION_KEY, description=dataset_description()
        )
        dataset.save()
        dataset.refresh_from_db()
        writes += 1
        created.append("dataset_collection")
    dataset_members, dataset_snapshot = collection_snapshot(dataset)
    if (
        dataset_snapshot["key"] != DATASET_COLLECTION_KEY
        or dataset_snapshot["description"] != dataset_description()
        or [member.uid for member in dataset_members] != [OBS_UID]
        or dataset_snapshot["unique_uid_count"] != 1
    ):
        raise AssertionError("dataset Collection identity or membership drift")

    successor_members = [
        member
        for member in state["predecessor_members"]
        if member.uid != PREDECESSOR_OBS_UID
    ] + [obs]
    successor_uids = [member.uid for member in successor_members]
    successor_keys = [str(member.key) for member in successor_members]
    result_hash = membership_sha256(successor_uids)
    if (
        len(successor_uids) != 1016
        or len(set(successor_uids)) != 1016
        or len(set(successor_keys)) != 1016
    ):
        raise AssertionError("planned global successor is not 1016 unique UIDs/keys")
    successor = exact_collection(ln, GLOBAL_SUCCESSOR_KEY)
    if successor is None:
        if not allow_create:
            raise AssertionError("global successor Collection is absent")
        successor = ln.Collection(
            successor_members,
            key=GLOBAL_SUCCESSOR_KEY,
            description=global_description(result_hash),
        )
        successor.save()
        successor.refresh_from_db()
        writes += 1
        created.append("global_successor_collection")
    successor_members_readback, successor_snapshot = collection_snapshot(successor)
    readback_uids = [member.uid for member in successor_members_readback]
    predecessor_uids = {member.uid for member in state["predecessor_members"]}
    if (
        successor_snapshot["key"] != GLOBAL_SUCCESSOR_KEY
        or successor_snapshot["description"] != global_description(result_hash)
        or successor_snapshot["member_count"] != 1016
        or successor_snapshot["unique_uid_count"] != 1016
        or successor_snapshot["unique_key_count"] != 1016
        or successor_snapshot["obs_key_count"] != 1016
        or successor_snapshot["membership_sha256"] != result_hash
        or sorted(set(readback_uids) - predecessor_uids) != [OBS_UID]
        or sorted(predecessor_uids - set(readback_uids)) != [PREDECESSOR_OBS_UID]
    ):
        raise AssertionError("global successor identity, membership, or drift mismatch")
    _, predecessor_after = collection_snapshot(ln.Collection.get(uid=PREDECESSOR_UID))
    if predecessor_after != state["predecessor_snapshot"]:
        raise AssertionError("predecessor mutated")
    return {
        "writes": writes,
        "created": created,
        "dataset_collection": dataset_snapshot,
        "global_successor": successor_snapshot,
        "predecessor": predecessor_after,
        "added_uids": sorted(set(readback_uids) - predecessor_uids),
        "removed_uids": sorted(predecessor_uids - set(readback_uids)),
        "duplicate_count": len(readback_uids) - len(set(readback_uids)),
    }


def finish_tracking(ln: Any) -> None:
    try:
        ln.finish()
    except AttributeError:
        ln.context.finish()


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in {"mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} mutate|verify")
    mode = sys.argv[1]
    helper_sha256 = os.environ.get("HERMES_HELPER_SHA256", "")
    if not re.fullmatch(r"[0-9a-f]{64}", helper_sha256):
        raise AssertionError("missing exact helper SHA-256 binding")
    ln = connect_pertdata()
    handles: list[Any] = []
    tracked = False
    try:
        state = preflight(ln)
        first = reconcile(ln, state, allow_create=False) if mode == "verify" else None
        lock_paths: list[str] = []
        if mode == "mutate":
            handles, lock_paths = acquire_locks()
            state = preflight(ln)
            if (
                exact_collection(ln, DATASET_COLLECTION_KEY) is None
                or exact_collection(ln, GLOBAL_SUCCESSOR_KEY) is None
            ):
                ln.track(
                    key="pert-gym/dataset-e2e/depmap-ccle-26q1-r2",
                    kind="script",
                    params={"task_id": TASK_ID, "helper_sha256": helper_sha256},
                    new_run=True,
                    pypackages=False,
                    stream_tracking=False,
                )
                tracked = True
            first = reconcile(ln, state, allow_create=True)
            if tracked:
                finish_tracking(ln)
        assert first is not None
        counts_after_first = {
            "artifact": ln.Artifact.filter().count(),
            "collection": ln.Collection.filter().count(),
        }
        replay_state = preflight(ln)
        replay = reconcile(ln, replay_state, allow_create=False)
        counts_after_replay = {
            "artifact": ln.Artifact.filter().count(),
            "collection": ln.Collection.filter().count(),
        }
        if replay["writes"] != 0 or counts_after_replay != counts_after_first:
            raise AssertionError("exact replay was not a no-op")
        release_locks(handles)
        handles = []
        receipt = {
            "format": "pert-gym.dataset-e2e-receipt/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "status": "PASS",
            "mode": mode,
            "host": os.uname().nodename,
            "instance": ln.setup.settings.instance.slug,
            "branch": ln.setup.settings.branch.name,
            "helper_sha256": helper_sha256,
            "source": {
                "evidence_url": "https://depmap.org/portal/download/all/",
                "release": "26q1",
                "source_rows_total": 1775,
                "selected_default_model_rows": 1719,
                "non_default_source_rows": 56,
            },
            "payload": {
                "manifest": PAYLOAD_MANIFEST,
                "manifest_sha256": PAYLOAD_MANIFEST_SHA256,
                "n_obs": 1719,
                "n_vars": 19215,
                "obs_uid": OBS_UID,
                "x_uid": X_UID,
                "var_uid": VAR_UID,
            },
            "rejected_first_successor": state["rejected_successor_snapshot"],
            "first_pass": first,
            "replay": {
                "writes": replay["writes"],
                "duplicate_count": replay["duplicate_count"],
                "added_uids": replay["added_uids"],
                "removed_uids": replay["removed_uids"],
                "counts_stable": counts_after_replay == counts_after_first,
            },
            "locks": {"paths": lock_paths, "released": True},
            "writes": {
                "collections": first["writes"],
                "artifacts": 0,
                "feature_links": 0,
                "gcs": 0,
                "deletions": 0,
            },
            "completed_at": int(time.time()),
        }
        receipt["canonical_sha256"] = hashlib.sha256(
            canonical(receipt).encode()
        ).hexdigest()
        print("HERMES_RECEIPT=" + canonical(receipt))
    finally:
        release_locks(handles)


if __name__ == "__main__":
    main()
