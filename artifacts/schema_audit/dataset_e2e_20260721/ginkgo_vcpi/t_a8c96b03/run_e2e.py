#!/usr/bin/env python3
"""Append-only integrated E2E publisher for ginkgo-datapoints/vcpi."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import sys
import threading
import time
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from pert_gym.legacy_triplet_adapter import LegacyTriplet, build_legacy_revision
from pert_gym.logical_sparse_publication import publish_candidate
from pert_gym.logical_sparse_zarr import read_logical_sparse_revision
from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    legacy_lamin_writer_lock_paths,
    require_heavy_vm,
    vm_global_lamin_writer_lock_path,
)

TASK_ID = "t_a8c96b03"
DATASET_ID = "ginkgo-datapoints/vcpi"
REAL_DATASET_ID = "ginkgo/vcpi"
OBS_UID = "Q7Qaj6dz0CzyQQ9i0002"
OBS_KEY = f"{DATASET_ID}/obs.parquet"
OBS_HASH = "zzCErT_TrE3EEDaiTBcq7w"
OBS_SHA256 = "8f316a04ccd84f634ffaeeae865c90a8491a235319ff68af0958442318f5bb3d"
X_UID = "72CMoQ6GfgZuTNdL0000"
X_KEY = f"{DATASET_ID}/X.h5ad"
X_HASH = "oM7hvGHydZS_XUc2XoPqIS"
X_SHA256 = "174f6bb42e9a3d130f8f11b57e0fff3829c44ad2203d1754158f63e8fef5e172"
VAR_UID = "sDYMNbN7DkmFB7Dx0001"
VAR_KEY = f"{DATASET_ID}/var.parquet"
VAR_HASH = "wybVhkvLWvejs9fvxszi2Q"
VAR_SHA256 = "3d997ab104242ee6c78ed41d33cab3cddf4464e3f77439d89f64f51b8096ec56"
N_OBS = 11_808
N_VARS = 59_427
NNZ = 157_793_388

SOURCE_ARTIFACTS = {
    "counts": {
        "uid": "2VyibSgHlpBSmaJn0000",
        "key": f"{DATASET_ID}/vcpi_GDPx2_counts.parquet",
        "hash": "qCaJCpc-Ri2Kp9AJ3BpQUg",
        "sha256": "11c64133a1e2a4269e21f1595b0941c06c00ce959f72496adf3e808763a4e1bf",
        "size": 411_808_920,
        "url": "https://ginkgo-datapoints-public.s3.us-east-2.amazonaws.com/datasets/vcpi/vcpi_GDPx2_counts.parquet",
    },
    "meta": {
        "uid": "vVkF13lDcCA7fJdd0000",
        "key": f"{DATASET_ID}/vcpi_GDPx2_meta.csv",
        "hash": "zfhHAvVB3-1OLSnQ6a8nBA",
        "sha256": "4102ee42ae30db37fbbf3b5bae80e8423d051d53adca2e86352728c7a091b934",
        "size": 3_560_321,
        "url": "https://ginkgo-datapoints-public.s3.us-east-2.amazonaws.com/datasets/vcpi/vcpi_GDPx2_meta.csv",
    },
    "compounds": {
        "uid": "53cXKxjkWFbp4Bmc0000",
        "key": f"{DATASET_ID}/compounds-GDPx2-2026-02-09.csv",
        "hash": "5m4JhBL3dJEknwrHrr04sQ",
        "sha256": "1f6e69e8a7f2f372b1cd948062f12c0e5de4bbcf101752cc7f51f140ce4c1e22",
        "size": 13_818,
        "url": None,
    },
}

LOGICAL_KEY = "pert-gym/logical/ginkgo-datapoints/vcpi"
REVISION = "ginkgo-vcpi-e2e-t-a8c96b03-r1"
SCHEMA_FINGERPRINT = (
    "ginkgo-vcpi-var/v1:sDYMNbN7DkmFB7Dx0001:"
    "3d997ab104242ee6c78ed41d33cab3cddf4464e3f77439d89f64f51b8096ec56"
)
CANDIDATE_COLLECTION_KEY = (
    "pert-gym/candidates/logical-sparse-zarr/20260721/ginkgo-datapoints-vcpi-t-a8c96b03"
)
DATASET_COLLECTION_KEY = "pert-gym/dataset/ginkgo-datapoints/vcpi/20260721-e2e"
GLOBAL_COLLECTION_KEY = "pert-gym/additions/20260721-ginkgo-vcpi-e2e"
PREDECESSOR_UID = "B9N5cXbu8Cm0RZSj0000"
PREDECESSOR_KEY = "pert-gym/additions/20260721-depmap-ccle-26q1-e2e-r2"
PREDECESSOR_HASH = "ImySHsnSBRg8WsSXmRcGzw"
PREDECESSOR_MEMBER_COUNT = 1016
PREDECESSOR_MEMBERSHIP_SHA256 = (
    "9052ec0f3df6f680819c9c2db059bf1a943dd643d8192f4f6b4bd30694cda957"
)
ROOT = Path("/tmp/pert-gym") / TASK_ID / "e2e-product"
HEARTBEAT_PATH = Path("/tmp/pert-gym") / TASK_ID / "product-heartbeat.jsonl"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def membership_sha256(uids: list[str]) -> str:
    return hashlib.sha256(canonical(sorted(uids)).encode()).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def feature_key(value: Any) -> str | None:
    return None if value is None else str(getattr(value, "key", value))


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


def exact_collection(ln: Any, key: str) -> Any | None:
    records = list(ln.Collection.filter(key=key).all())
    if len(records) > 1:
        raise AssertionError(f"duplicate Collection key: {key}")
    return records[0] if records else None


def exact_artifact(ln: Any, *, uid: str, key: str, hash_: str) -> Any:
    artifact = ln.Artifact.get(uid=uid)
    history = list(ln.Artifact.filter(key=key).order_by("created_at"))
    if (
        not history
        or str(history[-1].uid) != uid
        or str(artifact.key) != key
        or str(artifact.hash) != hash_
        or not bool(artifact.is_latest)
    ):
        raise AssertionError(f"current Artifact identity drift for {key}")
    return artifact


def available_memory() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise AssertionError("MemAvailable absent")


def process_conflicts() -> list[dict[str, Any]]:
    ancestors = {os.getpid()}
    cursor = os.getpid()
    while cursor > 1:
        try:
            cursor = int(
                Path(f"/proc/{cursor}/stat").read_text().rsplit(")", 1)[1].split()[1]
            )
        except (FileNotFoundError, IndexError, ValueError):
            break
        ancestors.add(cursor)
    needles = ("migrate_logical_sparse_zarr", "publish_candidate", DATASET_ID)
    matches = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal() or int(entry.name) in ancestors:
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode()
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if command and any(needle in command for needle in needles):
            matches.append({"pid": int(entry.name), "command": command})
    return matches


class ProductHeartbeat:
    def __init__(self) -> None:
        self.phase = "preflight"
        self.current = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def payload(self) -> dict[str, Any]:
        return {
            "product_execution": {
                "host": os.uname().nodename,
                "pid": os.getpid(),
                "phase": self.phase,
                "payload_heartbeat_at": int(time.time()),
                "metric": "dataset_e2e_current",
                "current": self.current,
                "denominator": 1,
                "unit": "logical_dataset",
            }
        }

    def emit(self) -> None:
        payload = self.payload()
        HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with HEARTBEAT_PATH.open("a", encoding="utf-8") as handle:
            handle.write(canonical(payload) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        print("PRODUCT_EXECUTION=" + canonical(payload), flush=True)

    def transition(self, phase: str, current: int | None = None) -> None:
        self.phase = phase
        if current is not None:
            self.current = current
        self.emit()

    def _loop(self) -> None:
        while not self._stop.wait(300):
            self.emit()

    def start(self) -> None:
        self.emit()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)


def acquire(path: Path) -> Any:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    handle = path.open("a+")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    handle.seek(0)
    handle.truncate()
    handle.write(canonical({"task_id": TASK_ID, "pid": os.getpid()}) + "\n")
    handle.flush()
    os.fsync(handle.fileno())
    return handle


def acquire_locks() -> tuple[list[Any], list[str]]:
    family = hashlib.sha256(DATASET_ID.encode()).hexdigest()
    predecessor = hashlib.sha256(PREDECESSOR_UID.encode()).hexdigest()
    paths = [
        vm_global_lamin_writer_lock_path(),
        *legacy_lamin_writer_lock_paths(),
        Path("/tmp/pert-gym/families") / f"{family}.lock",
        Path("/tmp/pert-gym/collections") / f"{predecessor}.lock",
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


def dataset_description(publication: dict[str, object]) -> str:
    return canonical(
        {
            "format": "pert-gym.dataset-e2e-collection/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "real_dataset_id": REAL_DATASET_ID,
            "source": "Ginkgo VCPI / GDPx2",
            "source_rows_total": N_OBS,
            "n_vars": N_VARS,
            "source_counts_uid": SOURCE_ARTIFACTS["counts"]["uid"],
            "source_counts_sha256": SOURCE_ARTIFACTS["counts"]["sha256"],
            "obs_uid": OBS_UID,
            "x_uid": X_UID,
            "var_uid": VAR_UID,
            "logical_manifest_key": publication["manifest_key"],
            "logical_promotion_uid": publication["promotion_uid"],
            "membership_rule": "canonical OBS only; follow OBS->X->VAR feature links",
            "rollback": "select exact immutable predecessor Artifacts and Collection",
        }
    )


def global_description(resulting_membership_sha256: str) -> str:
    return canonical(
        {
            "format": "pert-gym.append-only-dataset-e2e-successor/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "added_obs_uid": OBS_UID,
            "member_count_before": PREDECESSOR_MEMBER_COUNT,
            "member_count_after": PREDECESSOR_MEMBER_COUNT + 1,
            "predecessor_uid": PREDECESSOR_UID,
            "predecessor_key": PREDECESSOR_KEY,
            "predecessor_membership_sha256": PREDECESSOR_MEMBERSHIP_SHA256,
            "resulting_membership_sha256": resulting_membership_sha256,
            "membership_rule": "immutable predecessor union exact current dataset OBS",
            "rollback": f"select immutable predecessor Collection {PREDECESSOR_UID}",
        }
    )


def preflight(ln: Any) -> dict[str, Any]:
    require_heavy_vm()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise AssertionError("unexpected Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("unexpected Lamin branch")
    if available_memory() < 8 * 1024**3:
        raise AssertionError("MemAvailable is below 8 GiB")
    conflicts = process_conflicts()
    if conflicts:
        raise AssertionError(f"conflicting writer processes: {conflicts}")

    obs = exact_artifact(ln, uid=OBS_UID, key=OBS_KEY, hash_=OBS_HASH)
    x = exact_artifact(ln, uid=X_UID, key=X_KEY, hash_=X_HASH)
    var = exact_artifact(ln, uid=VAR_UID, key=VAR_KEY, hash_=VAR_HASH)
    if feature_key(obs.features.get_values().get("X")) != X_KEY:
        raise AssertionError("OBS -> X link drift")
    if feature_key(x.features.get_values().get("var")) != VAR_KEY:
        raise AssertionError("X -> VAR link drift")
    if (x.n_observations, var.n_observations) != (N_OBS, N_VARS):
        raise AssertionError("X/VAR denominator drift")

    sources = {}
    for role, identity in SOURCE_ARTIFACTS.items():
        artifact = exact_artifact(
            ln,
            uid=str(identity["uid"]),
            key=str(identity["key"]),
            hash_=str(identity["hash"]),
        )
        if int(artifact.size) != identity["size"]:
            raise AssertionError(f"source size drift for {role}")
        sources[role] = artifact

    predecessor = ln.Collection.get(uid=PREDECESSOR_UID)
    predecessor_members, predecessor_snapshot = collection_snapshot(predecessor)
    expected = {
        "uid": PREDECESSOR_UID,
        "key": PREDECESSOR_KEY,
        "hash": PREDECESSOR_HASH,
        "member_count": PREDECESSOR_MEMBER_COUNT,
        "unique_uid_count": PREDECESSOR_MEMBER_COUNT,
        "unique_key_count": PREDECESSOR_MEMBER_COUNT,
        "membership_sha256": PREDECESSOR_MEMBERSHIP_SHA256,
    }
    for key, value in expected.items():
        if predecessor_snapshot[key] != value:
            raise AssertionError(f"predecessor Collection drift for {key}")
    predecessor_uids = {str(member.uid) for member in predecessor_members}
    predecessor_keys = {str(member.key) for member in predecessor_members}
    if OBS_UID in predecessor_uids or OBS_KEY in predecessor_keys:
        raise AssertionError("target OBS already exists in global predecessor")

    existing_target = exact_collection(ln, GLOBAL_COLLECTION_KEY)
    intervening = []
    for collection in ln.Collection.filter(key__startswith="pert-gym/additions/").all():
        try:
            description = json.loads(collection.description or "{}")
        except json.JSONDecodeError:
            continue
        if description.get("predecessor_uid") == PREDECESSOR_UID:
            intervening.append(str(collection.uid))
    allowed = {str(existing_target.uid)} if existing_target is not None else set()
    if set(intervening) - allowed:
        raise AssertionError(f"unexpected successor(s) of predecessor: {intervening}")

    return {
        "obs": obs,
        "x": x,
        "var": var,
        "sources": sources,
        "predecessor": predecessor,
        "predecessor_members": predecessor_members,
        "predecessor_snapshot": predecessor_snapshot,
        "mem_available_bytes": available_memory(),
        "disk_free_bytes": shutil.disk_usage(ROOT.parent).free,
    }


def materialize_triplet(state: dict[str, Any]) -> LegacyTriplet:
    obs_path = Path(state["obs"].cache())
    x_path = Path(state["x"].cache())
    var_path = Path(state["var"].cache())
    checksums = {
        "obs": sha256_file(obs_path),
        "x": sha256_file(x_path),
        "var": sha256_file(var_path),
    }
    expected = {"obs": OBS_SHA256, "x": X_SHA256, "var": VAR_SHA256}
    if checksums != expected:
        raise AssertionError(f"triplet SHA-256 drift: {checksums}")
    return LegacyTriplet(
        chunk_id=0,
        obs_path=obs_path,
        x_path=x_path,
        var_path=var_path,
        obs_artifact_id=OBS_UID,
        x_artifact_id=X_UID,
        var_artifact_id=VAR_UID,
        obs_key=OBS_KEY,
        x_key=X_KEY,
        var_key=VAR_KEY,
    )


def build_or_read(triplet: LegacyTriplet) -> dict[str, Any]:
    manifest_path = ROOT / LOGICAL_KEY / "revisions" / REVISION / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = build_legacy_revision(
            root=ROOT,
            logical_key=LOGICAL_KEY,
            revision=REVISION,
            triplets=[triplet],
            schema_fingerprint=SCHEMA_FINGERPRINT,
            ingestion_run_id=TASK_ID,
            max_rss_bytes=4 * 1024**3,
        )
    if manifest.get("shape") != [N_OBS, N_VARS] or manifest.get("nnz") != NNZ:
        raise AssertionError("logical manifest shape/nnz drift")
    return manifest


def verify_local_parity(triplet: LegacyTriplet) -> dict[str, Any]:
    surface, logical_x, logical_obs, logical_var = read_logical_sparse_revision(
        ROOT, LOGICAL_KEY, REVISION
    )
    source_obs = pd.read_parquet(triplet.obs_path)
    source_var = pd.read_parquet(triplet.var_path)
    assert_frame_equal(logical_obs, source_obs, check_categorical=True)
    assert_frame_equal(logical_var, source_var, check_categorical=True)
    source = ad.read_h5ad(triplet.x_path, backed="r")
    checked_rows = 0
    checked_nnz = 0
    max_rows = 0
    try:
        for chunk in surface.chunks:
            expected = source.X[chunk.start : chunk.end].tocsr()
            actual = logical_x[chunk.start : chunk.end].tocsr()
            if (
                expected.shape != actual.shape
                or not np.array_equal(expected.indptr, actual.indptr)
                or not np.array_equal(expected.indices, actual.indices)
                or not np.array_equal(expected.data, actual.data)
            ):
                raise AssertionError(
                    f"matrix parity mismatch at {chunk.start}:{chunk.end}"
                )
            rows = chunk.end - chunk.start
            checked_rows += rows
            checked_nnz += int(actual.nnz)
            max_rows = max(max_rows, rows)
    finally:
        source.file.close()
    if (checked_rows, checked_nnz) != (N_OBS, NNZ):
        raise AssertionError("matrix parity denominator mismatch")
    return {
        "rows_checked": checked_rows,
        "nnz_checked": checked_nnz,
        "logical_blocks": len(surface.chunks),
        "max_loader_rows": max_rows,
        "obs_rows_checked": len(logical_obs),
        "var_rows_checked": len(logical_var),
        "mismatch_count": 0,
    }


def save_collection(
    ln: Any, *, key: str, members: list[Any], description: str, allow_create: bool
) -> tuple[Any, int]:
    existing = exact_collection(ln, key)
    writes = 0
    if existing is None:
        if not allow_create:
            raise AssertionError(f"required Collection absent: {key}")
        existing = ln.Collection(members, key=key, description=description)
        existing.save()
        existing.refresh_from_db()
        writes = 1
    readback_members, snapshot = collection_snapshot(existing)
    expected_uids = sorted(str(member.uid) for member in members)
    if (
        snapshot["key"] != key
        or snapshot["description"] != description
        or sorted(str(member.uid) for member in readback_members) != expected_uids
        or snapshot["unique_uid_count"] != len(expected_uids)
    ):
        raise AssertionError(f"Collection identity/membership drift: {key}")
    return existing, writes


def reconcile_collections(
    ln: Any,
    state: dict[str, Any],
    publication: dict[str, object],
    *,
    allow_create: bool,
) -> dict[str, Any]:
    dataset, dataset_writes = save_collection(
        ln,
        key=DATASET_COLLECTION_KEY,
        members=[state["obs"]],
        description=dataset_description(publication),
        allow_create=allow_create,
    )
    successor_members = list(state["predecessor_members"]) + [state["obs"]]
    successor_uids = [str(member.uid) for member in successor_members]
    successor_keys = [str(member.key) for member in successor_members]
    if (
        len(successor_uids) != PREDECESSOR_MEMBER_COUNT + 1
        or len(set(successor_uids)) != len(successor_uids)
        or len(set(successor_keys)) != len(successor_keys)
    ):
        raise AssertionError("global successor plan is not unique by UID and key")
    result_hash = membership_sha256(successor_uids)
    successor, successor_writes = save_collection(
        ln,
        key=GLOBAL_COLLECTION_KEY,
        members=successor_members,
        description=global_description(result_hash),
        allow_create=allow_create,
    )
    _, dataset_snapshot = collection_snapshot(dataset)
    successor_members_readback, successor_snapshot = collection_snapshot(successor)
    readback_uids = {str(member.uid) for member in successor_members_readback}
    predecessor_uids = {str(member.uid) for member in state["predecessor_members"]}
    if (
        successor_snapshot["member_count"] != PREDECESSOR_MEMBER_COUNT + 1
        or successor_snapshot["unique_uid_count"] != PREDECESSOR_MEMBER_COUNT + 1
        or successor_snapshot["unique_key_count"] != PREDECESSOR_MEMBER_COUNT + 1
        or sorted(readback_uids - predecessor_uids) != [OBS_UID]
        or predecessor_uids - readback_uids
    ):
        raise AssertionError("global successor readback mismatch")
    _, predecessor_after = collection_snapshot(ln.Collection.get(uid=PREDECESSOR_UID))
    if predecessor_after != state["predecessor_snapshot"]:
        raise AssertionError("predecessor Collection mutated")
    return {
        "writes": dataset_writes + successor_writes,
        "dataset_collection": dataset_snapshot,
        "global_successor": successor_snapshot,
        "predecessor": predecessor_after,
        "added_uids": sorted(readback_uids - predecessor_uids),
        "removed_uids": sorted(predecessor_uids - readback_uids),
        "duplicate_uid_count": len(successor_members_readback) - len(readback_uids),
        "duplicate_key_count": len(successor_members_readback)
        - len({str(member.key) for member in successor_members_readback}),
    }


def finish_tracking(ln: Any) -> None:
    try:
        ln.finish()
    except AttributeError:
        ln.context.finish()


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} mutate|verify")
    mode = sys.argv[1]
    helper_sha256 = os.environ.get("HERMES_HELPER_SHA256", "")
    if len(helper_sha256) != 64:
        raise AssertionError("missing exact helper SHA-256 binding")
    heartbeat = ProductHeartbeat()
    handles: list[Any] = []
    tracked = False
    ln: Any | None = None
    heartbeat.start()
    try:
        ln = connect_pertdata()
        state = preflight(ln)
        handles, lock_paths = acquire_locks()
        state = preflight(ln)
        heartbeat.transition("writing")
        triplet = materialize_triplet(state)
        manifest = build_or_read(triplet)
        parity = verify_local_parity(triplet)

        product_keys = [
            CANDIDATE_COLLECTION_KEY,
            DATASET_COLLECTION_KEY,
            GLOBAL_COLLECTION_KEY,
        ]
        absent_before = [
            key for key in product_keys if exact_collection(ln, key) is None
        ]
        if mode == "verify" and absent_before:
            raise AssertionError(f"verify-only product is incomplete: {absent_before}")
        if mode == "mutate" and absent_before:
            ln.track(
                key="pert-gym/dataset-e2e/ginkgo-vcpi-t-a8c96b03",
                kind="script",
                params={"task_id": TASK_ID, "helper_sha256": helper_sha256},
                new_run=True,
                pypackages=False,
                stream_tracking=False,
            )
            tracked = True
        counts_before = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
        }
        publication = publish_candidate(
            ln=ln,
            root=ROOT,
            logical_key=LOGICAL_KEY,
            revision=REVISION,
            collection_key=CANDIDATE_COLLECTION_KEY,
            require_vm=require_heavy_vm,
        )
        first = reconcile_collections(
            ln, state, publication, allow_create=mode == "mutate"
        )
        if tracked:
            finish_tracking(ln)
            tracked = False
        counts_after_first = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
        }

        replay_state = preflight(ln)
        replay_publication = publish_candidate(
            ln=ln,
            root=ROOT,
            logical_key=LOGICAL_KEY,
            revision=REVISION,
            collection_key=CANDIDATE_COLLECTION_KEY,
            require_vm=require_heavy_vm,
        )
        replay = reconcile_collections(
            ln, replay_state, replay_publication, allow_create=False
        )
        counts_after_replay = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
        }
        if replay["writes"] != 0 or counts_after_replay != counts_after_first:
            raise AssertionError("replay was not an exact no-op")
        heartbeat.transition("checkpointing", current=1)
        release_locks(handles)
        handles = []
        receipt = {
            "format": "pert-gym.dataset-e2e-receipt/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "real_dataset_id": REAL_DATASET_ID,
            "status": "PASS",
            "mode": mode,
            "helper_sha256": helper_sha256,
            "source": {
                role: {
                    key: value
                    for key, value in identity.items()
                    if key in {"uid", "key", "sha256", "size", "url"}
                }
                for role, identity in SOURCE_ARTIFACTS.items()
            },
            "accepted_triplet": {
                "obs_uid": OBS_UID,
                "x_uid": X_UID,
                "var_uid": VAR_UID,
                "shape": [N_OBS, N_VARS],
                "nnz": NNZ,
                "obs_sha256": OBS_SHA256,
                "x_sha256": X_SHA256,
                "var_sha256": VAR_SHA256,
                "accepted_obs_fields_written": [
                    "control_availability",
                    "dose",
                    "modality",
                    "perturbation",
                    "perturbation_type",
                    "source",
                    "source_accession",
                ],
                "x_semantics": "unknown; explicitly not written without evidence",
            },
            "logical_surface": {
                "logical_key": LOGICAL_KEY,
                "revision": REVISION,
                "schema_fingerprint": SCHEMA_FINGERPRINT,
                "manifest_sha256": sha256_file(
                    ROOT / LOGICAL_KEY / "revisions" / REVISION / "manifest.json"
                ),
                "manifest_shape": manifest["shape"],
                "manifest_nnz": manifest["nnz"],
                "publication": publication,
                "parity": parity,
                "rollback": "retain legacy triplet and select it by exact UIDs",
            },
            "first_pass": first,
            "replay": {
                "writes": replay["writes"],
                "counts_stable": counts_after_replay == counts_after_first,
                "duplicate_uid_count": replay["duplicate_uid_count"],
                "duplicate_key_count": replay["duplicate_key_count"],
                "added_uids": replay["added_uids"],
                "removed_uids": replay["removed_uids"],
            },
            "counts": {
                "before": counts_before,
                "after_first": counts_after_first,
                "after_replay": counts_after_replay,
            },
            "host": {
                "hostname": os.uname().nodename,
                "mem_available_preflight": state["mem_available_bytes"],
                "disk_free_preflight": state["disk_free_bytes"],
            },
            "locks": {"paths": lock_paths, "released": True},
            "writes": {
                "collection_writes": first["writes"],
                "deletions": 0,
                "legacy_triplet_rewrites": 0,
            },
            "completed_at": int(time.time()),
        }
        receipt["canonical_sha256"] = hashlib.sha256(
            canonical(receipt).encode()
        ).hexdigest()
        print("HERMES_RECEIPT=" + canonical(receipt), flush=True)
        return 0
    except BaseException:
        heartbeat.transition("failed")
        raise
    finally:
        if tracked and ln is not None:
            finish_tracking(ln)
        release_locks(handles)
        heartbeat.stop()


if __name__ == "__main__":
    raise SystemExit(main())
