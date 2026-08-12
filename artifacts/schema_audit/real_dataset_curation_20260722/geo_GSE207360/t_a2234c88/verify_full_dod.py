#!/usr/bin/env python3
"""Zero-write live full-DoD readback for geo/GSE207360."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Any

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import verify_only_preflight

TASK_ID = "t_f2593783"
DATASET_ID = "geo/GSE207360"
PREFIX = "prism_collection/GSE207360"
OBS_UID = "KSAkP0NJF5P5g1mJ0004"
X_UID = "4IOEQEw4ylx0Zx4c0000"
VAR_UID = "U8OeHI58YG9Y9Nsb0002"
SOURCE_SHA256 = "b54a754f26aeb6082de7480ac15622c1696b5f753e036e36b4346c98021bdba1"
SOURCE_PATH = Path("/var/tmp/pert-gym-gse207360/GSE207360_Human_Mouse_filtered.rds.gz")
BILLING_PROJECT = "jkobject-1549353370965"
LEGACY_STAGING_URI = (
    "gs://scperturb/pert-gym/staging/data/main/prism_collection/GSE207360.h5ad"
)
CLEANED_PREFIX = "gs://scperturb/data/cleaned/GSE207360/"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(16 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    by_key = list(ln.Artifact.filter(key=value).all())
    latest = [item for item in by_key if bool(item.is_latest)]
    if len(latest) != 1:
        raise AssertionError(f"cannot resolve linked artifact {value!r}")
    return latest[0]


def gcs_exists(uri: str) -> bool:
    result = subprocess.run(
        [
            "gcloud",
            "storage",
            "ls",
            f"--billing-project={BILLING_PROJECT}",
            uri,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode not in {0, 1}:
        raise RuntimeError(f"GCS exact-scope readback failed: {result.stderr[-500:]}")
    return result.returncode == 0 and bool(result.stdout.strip())


def main() -> int:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = verify_only_preflight()
    if not SOURCE_PATH.is_file():
        raise AssertionError("immutable filtered source is absent")
    source_size = SOURCE_PATH.stat().st_size
    source_sha256 = sha256_file(SOURCE_PATH)
    if source_size != 4_174_159_639 or source_sha256 != SOURCE_SHA256:
        raise AssertionError("immutable filtered source identity drift")

    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    obs_matches = list(ln.Artifact.filter(uid=OBS_UID).all())
    if len(obs_matches) != 1:
        raise AssertionError("exact OBS successor absent")
    obs_artifact = obs_matches[0]
    if str(obs_artifact.key) != f"{PREFIX}/obs.parquet" or not bool(
        obs_artifact.is_latest
    ):
        raise AssertionError("exact OBS is not the latest canonical key revision")
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    if str(x_artifact.uid) != X_UID or str(var_artifact.uid) != VAR_UID:
        raise AssertionError("explicit obs -> X -> var identity drift")
    organism = obs["organism"].astype("string").value_counts().to_dict()
    strata = {
        "Homo sapiens": int(organism.get("Homo sapiens", 0)),
        "Mus musculus": int(organism.get("Mus musculus", 0)),
    }
    if len(obs) != 12_487 or strata != {
        "Homo sapiens": 10_984,
        "Mus musculus": 1_503,
    }:
        raise AssertionError("OBS denominator or mixed-species strata drift")
    if not obs["source_filtered_rds_sha256"].eq(SOURCE_SHA256).all():
        raise AssertionError("OBS source checksum provenance drift")

    collection_rows: list[dict[str, Any]] = []
    exact_current_memberships: list[dict[str, Any]] = []
    for collection in ln.Collection.filter().all():
        members = list(collection.artifacts.only("uid", "key").all())
        target = [
            {"uid": str(member.uid), "key": str(member.key)}
            for member in members
            if str(member.key) == f"{PREFIX}/obs.parquet"
        ]
        if target:
            row = {
                "uid": str(collection.uid),
                "key": str(collection.key),
                "member_count": len(members),
                "target_members": target,
            }
            collection_rows.append(row)
            if target == [{"uid": OBS_UID, "key": f"{PREFIX}/obs.parquet"}]:
                exact_current_memberships.append(row)
    if not collection_rows:
        raise AssertionError("no Collection contains the canonical GSE207360 key")

    legacy_staging_exists = gcs_exists(LEGACY_STAGING_URI)
    cleaned_payload_exists = gcs_exists(CLEANED_PREFIX)
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    if counts_after != counts_before:
        raise AssertionError("registry drift during zero-write verification")

    decommission_gates = {
        "accepted_parity_and_readback": True,
        "executable_processing_notebook": True,
        "immutable_source_and_rollback": True,
        "exact_deletion_scope_present": legacy_staging_exists,
        "canonical_data_cleaned_payload": cleaned_payload_exists,
        "accepted_current_collection_membership": bool(exact_current_memberships),
        "independent_review_of_this_receipt": False,
    }
    deletion_eligible = all(decommission_gates.values())
    if deletion_eligible:
        raise AssertionError("deletion requires a separately reviewed approved path")

    receipt = {
        "format": "pert-gym.gse207360-full-dod-live-readback/v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "status": "PASS",
        "mode": "verify-only",
        "code_head": os.environ.get("PERT_GYM_VERIFY_HEAD", "unknown"),
        "source": {
            "path": str(SOURCE_PATH),
            "size": source_size,
            "sha256": source_sha256,
        },
        "artifacts": {
            "obs": {"uid": str(obs_artifact.uid), "key": str(obs_artifact.key)},
            "x": {"uid": str(x_artifact.uid), "key": str(x_artifact.key)},
            "var": {"uid": str(var_artifact.uid), "key": str(var_artifact.key)},
            "explicit_links": True,
            "observations": len(obs),
            "species_strata": strata,
        },
        "collections_with_target_key": collection_rows,
        "exact_current_collection_memberships": exact_current_memberships,
        "registry_counts": {"before": counts_before, "after": counts_after},
        "writes": {
            "artifact_writes": 0,
            "collection_writes": 0,
            "deletions": 0,
        },
        "replay_noop": True,
        "rollback": {
            "predecessor_obs_uid": "KSAkP0NJF5P5g1mJ0003",
            "x_uid": X_UID,
            "var_uid": VAR_UID,
        },
        "gcs_decommission": {
            "legacy_staging_uri": LEGACY_STAGING_URI,
            "legacy_staging_exists": legacy_staging_exists,
            "canonical_cleaned_prefix": CLEANED_PREFIX,
            "canonical_cleaned_payload_exists": cleaned_payload_exists,
            "gates": decommission_gates,
            "eligible": False,
            "action": "preserved_no_deletion",
            "unmet_gates": [
                key for key, passed in decommission_gates.items() if not passed
            ],
        },
        "host": {
            "hostname": capacity.hostname,
            "free_disk_bytes": capacity.free_disk_bytes,
            "available_memory_bytes": capacity.available_memory_bytes,
        },
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = hashlib.sha256(
        canonical(receipt).encode()
    ).hexdigest()
    print("GSE207360_FULL_DOD_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
