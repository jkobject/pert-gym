#!/usr/bin/env python3
"""Append-only dataset/global Collection publisher for DRUG-seq/GSE120222."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    legacy_lamin_writer_lock_paths,
    require_heavy_vm,
    vm_global_lamin_writer_lock_path,
)

TASK_ID = "t_3d9bf0d8"
DATASET_ID = "DRUG-seq/GSE120222"
REAL_DATASET_ID = "drug-seq/GSE120222"
OBS_UID = "mKSaEEcH4jyes43Z0002"
OBS_KEY = f"{DATASET_ID}/obs.parquet"
OBS_HASH = "hRHOdB9ldVKBI2WH6dhudg"
OBS_SHA256 = "bfd125492d869a784f6f938a440b9a0e2cb54a8beafe2136d0ceb4f391893975"
X_UID = "hfeVCMInQu1UKhwp0000"
X_KEY = f"{DATASET_ID}/X.h5ad"
X_HASH = "SVX9BmtCqGpiRNoYzBAUIA"
X_SHA256 = "a1e1d9b5640082d8c599dd53bcca24a0d778c2721f6dd925c5c2ee72b152ce84"
VAR_UID = "vmqp94W72a1Tl2Xw0002"
VAR_KEY = f"{DATASET_ID}/var.parquet"
VAR_HASH = "-aVjf8X-KyvpMcr0TyVeXQ"
VAR_SHA256 = "35bb21aa696af1213cd3d03bb14180cd149599e3f3ffc6880412d57ddf501637"
N_OBS = 72
N_VARS = 60_371
NNZ = 1_747_564
PREDECESSOR_UID = "qoTeH7T78kjbmIWA0000"
PREDECESSOR_KEY = "pert-gym/additions/20260721-ginkgo-vcpi-e2e"
PREDECESSOR_HASH = "lsA-KQnpRV66FuOawrA6uA"
PREDECESSOR_MEMBER_COUNT = 1_017
PREDECESSOR_MEMBERSHIP_SHA256 = (
    "27845039a3e3e6cb833cf2f2068f1279fb60a6ab222e42d74ce73d9e07b15517"
)
DATASET_COLLECTION_KEY = "pert-gym/dataset/DRUG-seq/GSE120222/20260721-e2e"
GLOBAL_COLLECTION_KEY = "pert-gym/additions/20260721-drug-seq-gse120222-e2e"
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


def available_memory() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise AssertionError("MemAvailable absent")


def count_nnz(matrix: Any) -> int:
    return int(matrix.nnz if hasattr(matrix, "nnz") else np.count_nonzero(matrix))


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
    matches = list(ln.Collection.filter(key=key).all())
    if len(matches) > 1:
        raise AssertionError(f"duplicate Collection key: {key}")
    return matches[0] if matches else None


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
    needles = ("publish_collections.py", "run_e2e.py", DATASET_ID)
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
        HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = self.payload()
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


def dataset_description() -> str:
    return canonical(
        {
            "format": "pert-gym.dataset-e2e-collection/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "real_dataset_id": REAL_DATASET_ID,
            "source": "DRUG-seq",
            "source_accession": "GSE120222",
            "source_rows_total": N_OBS,
            "n_vars": N_VARS,
            "source_evidence": [
                "https://doi.org/10.1038/s41467-018-06500-x",
                "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE120222",
                "https://pubmed.ncbi.nlm.nih.gov/30333485/",
            ],
            "obs_uid": OBS_UID,
            "obs_sha256": OBS_SHA256,
            "x_uid": X_UID,
            "x_sha256": X_SHA256,
            "var_uid": VAR_UID,
            "var_sha256": VAR_SHA256,
            "representation": (
                "accepted single triplet retained; 37,790,360-byte X and 72 rows do "
                "not materially require recompaction"
            ),
            "membership_rule": "canonical OBS only; follow OBS->X->VAR feature links",
            "rollback": "select exact immutable predecessor Artifacts and Collection",
        }
    )


def global_description(result_membership_sha256: str) -> str:
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
            "resulting_membership_sha256": result_membership_sha256,
            "membership_rule": "immutable predecessor union exact current dataset OBS",
            "rollback": f"select immutable predecessor Collection {PREDECESSOR_UID}",
        }
    )


def verify_triplet_payload(state: dict[str, Any]) -> dict[str, Any]:
    paths = {role: Path(state[role].cache()) for role in ("obs", "x", "var")}
    observed_sha256 = {role: sha256_file(path) for role, path in paths.items()}
    expected_sha256 = {"obs": OBS_SHA256, "x": X_SHA256, "var": VAR_SHA256}
    if observed_sha256 != expected_sha256:
        raise AssertionError(f"triplet SHA-256 drift: {observed_sha256}")
    obs = pd.read_parquet(paths["obs"])
    var = pd.read_parquet(paths["var"])
    x = ad.read_h5ad(paths["x"], backed="r")
    try:
        matrix = x.X[:]
        shape = [int(x.n_obs), int(x.n_vars)]
        nnz = count_nnz(matrix)
        dtype = str(x.X.dtype)
    finally:
        x.file.close()
    if (
        shape != [N_OBS, N_VARS]
        or len(obs) != N_OBS
        or len(var) != N_VARS
        or nnz != NNZ
    ):
        raise AssertionError("OBS/X/VAR payload denominator or parity drift")
    canonical_fields = [
        "control_availability",
        "dose",
        "modality",
        "perturbation",
        "perturbation_type",
        "source",
        "source_accession",
        "timepoint",
    ]
    if any(
        field not in obs or not obs[field].notna().all() for field in canonical_fields
    ):
        raise AssertionError("accepted canonical OBS field coverage drift")
    if "x_semantics" in obs:
        raise AssertionError("unsupported x_semantics was unexpectedly materialized")
    ensembl_rows = int(var["ensembl_gene_id"].notna().sum())
    ercc_rows = int((var["stable_feature_id_namespace"] == "ERCC").sum())
    if (ensembl_rows, ercc_rows) != (60_279, 92):
        raise AssertionError("species-correct VAR disposition drift")
    return {
        "shape": shape,
        "nnz": nnz,
        "dtype": dtype,
        "sha256": observed_sha256,
        "obs_rows_checked": len(obs),
        "var_rows_checked": len(var),
        "canonical_fields_non_null": canonical_fields,
        "x_semantics": "unknown; absent rather than invented",
        "human_ensembl_rows": ensembl_rows,
        "ercc_rows": ercc_rows,
        "mismatch_count": 0,
        "bounded_loader": "backed H5AD metadata plus one complete 72-row matrix slice",
    }


def preflight(ln: Any) -> dict[str, Any]:
    require_heavy_vm()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise AssertionError("unexpected Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("unexpected Lamin branch")
    if available_memory() < 8 * 1024**3:
        raise AssertionError("MemAvailable below 8 GiB")
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
    if (obs.n_observations, x.n_observations, var.n_observations) != (
        N_OBS,
        N_OBS,
        N_VARS,
    ):
        raise AssertionError("OBS/X/VAR registry denominator drift")

    predecessor = ln.Collection.get(uid=PREDECESSOR_UID)
    predecessor_members, predecessor_snapshot = collection_snapshot(predecessor)
    expected = {
        "uid": PREDECESSOR_UID,
        "key": PREDECESSOR_KEY,
        "hash": PREDECESSOR_HASH,
        "member_count": PREDECESSOR_MEMBER_COUNT,
        "unique_uid_count": PREDECESSOR_MEMBER_COUNT,
        "unique_key_count": PREDECESSOR_MEMBER_COUNT,
        "obs_key_count": PREDECESSOR_MEMBER_COUNT,
        "membership_sha256": PREDECESSOR_MEMBERSHIP_SHA256,
    }
    for key, value in expected.items():
        if predecessor_snapshot[key] != value:
            raise AssertionError(f"predecessor Collection drift for {key}")
    predecessor_uids = {str(member.uid) for member in predecessor_members}
    predecessor_keys = {str(member.key) for member in predecessor_members}
    if OBS_UID in predecessor_uids or OBS_KEY in predecessor_keys:
        raise AssertionError("target OBS already exists in predecessor")

    existing_target = exact_collection(ln, GLOBAL_COLLECTION_KEY)
    successors = []
    for collection in ln.Collection.filter(key__startswith="pert-gym/additions/").all():
        try:
            description = json.loads(collection.description or "{}")
        except json.JSONDecodeError:
            continue
        if description.get("predecessor_uid") == PREDECESSOR_UID:
            successors.append(str(collection.uid))
    allowed = {str(existing_target.uid)} if existing_target is not None else set()
    if set(successors) - allowed:
        raise AssertionError(f"unexpected successor(s) of predecessor: {successors}")
    return {
        "obs": obs,
        "x": x,
        "var": var,
        "predecessor": predecessor,
        "predecessor_members": predecessor_members,
        "predecessor_snapshot": predecessor_snapshot,
        "mem_available_bytes": available_memory(),
    }


def save_collection(
    ln: Any, *, key: str, members: list[Any], description: str, allow_create: bool
) -> tuple[Any, int]:
    collection = exact_collection(ln, key)
    writes = 0
    if collection is None:
        if not allow_create:
            raise AssertionError(f"required Collection absent: {key}")
        collection = ln.Collection(members, key=key, description=description)
        collection.save()
        collection.refresh_from_db()
        writes = 1
    readback_members, snapshot = collection_snapshot(collection)
    expected_uids = sorted(str(member.uid) for member in members)
    if (
        snapshot["key"] != key
        or snapshot["description"] != description
        or sorted(str(member.uid) for member in readback_members) != expected_uids
        or snapshot["unique_uid_count"] != len(expected_uids)
        or snapshot["unique_key_count"] != len(expected_uids)
    ):
        raise AssertionError(f"Collection identity/membership drift: {key}")
    return collection, writes


def reconcile(ln: Any, state: dict[str, Any], *, allow_create: bool) -> dict[str, Any]:
    dataset, dataset_writes = save_collection(
        ln,
        key=DATASET_COLLECTION_KEY,
        members=[state["obs"]],
        description=dataset_description(),
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
        or successor_snapshot["obs_key_count"] != PREDECESSOR_MEMBER_COUNT + 1
        or sorted(readback_uids - predecessor_uids) != [OBS_UID]
        or predecessor_uids - readback_uids
    ):
        raise AssertionError("global successor identity, membership, or drift mismatch")
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
    if not re.fullmatch(r"[0-9a-f]{64}", helper_sha256):
        raise AssertionError("missing exact helper SHA-256 binding")
    heartbeat = ProductHeartbeat()
    heartbeat.start()
    handles: list[Any] = []
    tracked = False
    ln: Any | None = None
    try:
        ln = connect_pertdata()
        state = preflight(ln)
        handles, lock_paths = acquire_locks()
        state = preflight(ln)
        heartbeat.transition("writing")
        triplet = verify_triplet_payload(state)
        absent_before = [
            key
            for key in (DATASET_COLLECTION_KEY, GLOBAL_COLLECTION_KEY)
            if exact_collection(ln, key) is None
        ]
        if mode == "verify" and absent_before:
            raise AssertionError(f"verify-only product is incomplete: {absent_before}")
        if mode == "mutate" and absent_before:
            ln.track(
                key="pert-gym/dataset-e2e/drug-seq-gse120222-t-3d9bf0d8",
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
        first = reconcile(ln, state, allow_create=mode == "mutate")
        if tracked:
            finish_tracking(ln)
            tracked = False
        counts_after_first = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
        }
        replay_state = preflight(ln)
        replay = reconcile(ln, replay_state, allow_create=False)
        counts_after_replay = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
        }
        if replay["writes"] != 0 or counts_after_replay != counts_after_first:
            raise AssertionError("replay was not an exact no-op")
        if mode == "verify" and counts_before != counts_after_replay:
            raise AssertionError("verify-only registry counts changed")
        heartbeat.transition("checkpointing", current=1)
        release_locks(handles)
        handles = []
        receipt = {
            "format": "pert-gym.drug-seq-gse120222-dataset-e2e-receipt/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "real_dataset_id": REAL_DATASET_ID,
            "status": "PASS",
            "mode": mode,
            "helper_sha256": helper_sha256,
            "source_publication": {
                "accession": "GSE120222",
                "doi": "10.1038/s41467-018-06500-x",
                "pubmed": "30333485",
                "source_rows_total": N_OBS,
                "source_members_total": 1,
            },
            "triplet": triplet,
            "representation": {
                "status": "not_materially_needed",
                "reason": (
                    "accepted X is one 37,790,360-byte payload with 72 rows; "
                    "bounded backed loading and exact OBS/X/VAR parity passed"
                ),
                "legacy_triplet_retained": True,
                "rollback": "select exact immutable OBS/X/VAR UIDs",
            },
            "first_pass": first,
            "replay": replay,
            "counts": {
                "before": counts_before,
                "after_first": counts_after_first,
                "after_replay": counts_after_replay,
            },
            "host": {
                "hostname": os.uname().nodename,
                "mem_available_preflight": state["mem_available_bytes"],
            },
            "locks": {"paths": lock_paths, "released": True},
            "writes": {
                "collection_writes": first["writes"],
                "artifact_writes": 0,
                "deletions": 0,
                "triplet_rewrites": 0,
                "gcs_mutations": 0,
            },
            "completed_at": int(time.time()),
        }
        receipt["canonical_sha256"] = hashlib.sha256(
            canonical(receipt).encode()
        ).hexdigest()
        heartbeat.transition("accepted", current=1)
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
