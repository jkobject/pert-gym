#!/usr/bin/env python3
"""Zero-write, two-pass live full-DoD readback for geo/GSE207360."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import verify_only_preflight

TASK_ID = "t_f2593783"
DATASET_ID = "geo/GSE207360"
PREFIX = "prism_collection/GSE207360"
RECEIPT_FORMAT = "pert-gym.gse207360-full-dod-live-readback/v2"
SOURCE_SHA256 = "b54a754f26aeb6082de7480ac15622c1696b5f753e036e36b4346c98021bdba1"
SOURCE_PATH = Path("/var/tmp/pert-gym-gse207360/GSE207360_Human_Mouse_filtered.rds.gz")
BILLING_PROJECT = "jkobject-1549353370965"
LEGACY_STAGING_URI = "gs://scperturb/pert-gym/staging/data/main/prism_collection/GSE207360.h5ad"
CLEANED_PREFIX = "gs://scperturb/data/cleaned/GSE207360/"
EXPECTED_BRANCH = "pert-gym/t_f2593783-complete-gse207360-live-dod-after-pr-117"
EXPECTED_ARTIFACTS = {
    "obs": {
        "uid": "KSAkP0NJF5P5g1mJ0004",
        "key": f"{PREFIX}/obs.parquet",
        "hash": "HZm19cFnhQVE6TPO4kL0zg",
        "size": 2_531_291,
        "n_observations": 12_487,
        "version": "0004",
        "is_latest": True,
    },
    "x": {
        "uid": "4IOEQEw4ylx0Zx4c0000",
        "key": f"{PREFIX}/X.h5ad",
        "hash": "rLTZFYwmtPyrsHhVQ6_kp-",
        "size": 453_049_912,
        "n_observations": 12_487,
        "version": "0000",
        "is_latest": True,
        "shape": [12_487, 60_736],
        "stored_nonzero": 56_169_414,
        "obs_names_sha256": "65de59c0c005cbdbbdd3a08d6aba68efb7a0667bd12f7a393cd396a80eeb193b",
        "var_names_sha256": "aa592b3ef2d217646eb95395c0207af7ae0a42c7b27c363710b722982cc1ffb3",
    },
    "var": {
        "uid": "U8OeHI58YG9Y9Nsb0002",
        "key": f"{PREFIX}/var.parquet",
        "hash": "wv2BwlQShhowaM7AYyu4uQ",
        "size": 4_083_025,
        "n_observations": 60_736,
        "version": "0002",
        "is_latest": True,
        "rows": 60_736,
        "human_ensg": 32_738,
        "mouse_ensmusg": 27_998,
    },
    "explicit_links": True,
    "species_strata": {"Homo sapiens": 10_984, "Mus musculus": 1_503},
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(16 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_sha256(values: Any) -> str:
    return hashlib.sha256("\n".join(map(str, values)).encode()).hexdigest()


def receipt_sha256(receipt: dict[str, Any]) -> str:
    unsigned = {key: value for key, value in receipt.items() if key != "canonical_sha256"}
    return hashlib.sha256(canonical(unsigned).encode()).hexdigest()


def verifier_sha256() -> str:
    return sha256_file(Path(__file__))


def resolve_launch_branch(actual_branch: str, expected_launch_branch: str) -> str:
    """Bind detached exact-head execution to the explicitly declared PR branch."""
    if expected_launch_branch != EXPECTED_BRANCH:
        raise RuntimeError("exact PERT_GYM_VERIFY_BRANCH launch branch is required")
    if actual_branch and actual_branch != expected_launch_branch:
        raise RuntimeError("checked-out branch does not match immutable launch binding")
    return expected_launch_branch


def gcloud_access_token() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    token = (result.stdout or "").strip()
    if result.returncode or not token:
        detail = ((result.stdout or "") + (result.stderr or ""))[-500:]
        raise RuntimeError(f"GCS access token acquisition failed: {detail}")
    return token


def gcs_prefix_probe(uri: str) -> dict[str, Any]:
    """Return a structured exact-scope listing; every HTTP/transport error raises."""
    if not uri.startswith("gs://"):
        raise ValueError("GCS URI required")
    bucket, _, prefix = uri[5:].partition("/")
    if not bucket or not prefix:
        raise ValueError("bounded non-empty GCS prefix required")
    query = urllib.parse.urlencode(
        {
            "prefix": prefix,
            "maxResults": 2,
            "fields": "items(name,generation,size,md5Hash),nextPageToken",
            "userProject": BILLING_PROJECT,
        }
    )
    request = urllib.request.Request(
        f"https://storage.googleapis.com/storage/v1/b/{urllib.parse.quote(bucket, safe='')}/o?{query}",
        headers={"Authorization": f"Bearer {gcloud_access_token()}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"GCS exact-scope listing failed with HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"GCS exact-scope listing transport/response failure: {exc}") from exc
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise RuntimeError("GCS exact-scope listing returned invalid items")
    identities = [
        {
            "name": str(item["name"]),
            "generation": str(item["generation"]),
            "size": int(item["size"]),
            "md5Hash": str(item.get("md5Hash", "")),
        }
        for item in items
    ]
    return {
        "uri": uri,
        "http_status": 200,
        "exists": bool(identities),
        "object_count_lower_bound": len(identities),
        "truncated": bool(payload.get("nextPageToken")),
        "objects": identities,
    }


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size),
        "n_observations": int(artifact.n_observations),
        "version": str(artifact.version),
        "is_latest": bool(artifact.is_latest),
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    latest = [item for item in ln.Artifact.filter(key=value).all() if bool(item.is_latest)]
    if len(latest) != 1:
        raise AssertionError(f"cannot resolve linked artifact {value!r}")
    return latest[0]


def registry_snapshot(ln: Any) -> dict[str, Any]:
    artifacts = sorted(
        (
            str(item.uid),
            str(item.key),
            str(item.hash),
            bool(item.is_latest),
            int(item.size),
        )
        for item in ln.Artifact.filter().only(
            "uid", "key", "hash", "is_latest", "size"
        )
    )
    collections = []
    for collection in ln.Collection.filter().all():
        members = sorted(str(item.uid) for item in collection.artifacts.only("uid").all())
        collections.append(
            (str(collection.uid), str(collection.key), str(collection.hash), tuple(members))
        )
    collections.sort()
    payload = {"artifacts": artifacts, "collections": collections}
    return {
        "artifact_count": len(artifacts),
        "collection_count": len(collections),
        "artifact_identities_sha256": hashlib.sha256(canonical(artifacts).encode()).hexdigest(),
        "collection_identities_and_memberships_sha256": hashlib.sha256(
            canonical(collections).encode()
        ).hexdigest(),
        "canonical_sha256": hashlib.sha256(canonical(payload).encode()).hexdigest(),
    }


def live_artifacts(ln: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    obs_artifact = ln.Artifact.get(uid=EXPECTED_ARTIFACTS["obs"]["uid"])
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    x = x_artifact.load()
    var = var_artifact.load()
    x_matrix = x.X
    observed: dict[str, Any] = {
        "obs": artifact_identity(obs_artifact),
        "x": {
            **artifact_identity(x_artifact),
            "shape": list(x.shape),
            "stored_nonzero": int(x_matrix.nnz),
            "obs_names_sha256": ordered_sha256(x.obs_names),
            "var_names_sha256": ordered_sha256(x.var_names),
        },
        "var": {
            **artifact_identity(var_artifact),
            "rows": len(var),
            "human_ensg": int(var["stable_feature_id"].astype("string").str.fullmatch(r"ENSG\d+", na=False).sum()),
            "mouse_ensmusg": int(var["stable_feature_id"].astype("string").str.fullmatch(r"ENSMUSG\d+", na=False).sum()),
        },
        "explicit_links": True,
        "species_strata": {
            str(key): int(value)
            for key, value in obs["organism"].astype("string").value_counts().items()
        },
    }
    if observed != EXPECTED_ARTIFACTS:
        raise AssertionError(f"artifact identity or dimensional parity drift: {observed!r}")
    if not obs["source_filtered_rds_sha256"].eq(SOURCE_SHA256).all():
        raise AssertionError("OBS source checksum provenance drift")
    memberships = []
    for collection in ln.Collection.filter().all():
        targets = sorted(
            (
                {"uid": str(member.uid), "key": str(member.key)}
                for member in collection.artifacts.only("uid", "key").all()
                if str(member.key) == f"{PREFIX}/obs.parquet"
            ),
            key=lambda item: item["uid"],
        )
        if targets:
            memberships.append(
                {
                    "uid": str(collection.uid),
                    "key": str(collection.key),
                    "hash": str(collection.hash),
                    "target_members": targets,
                }
            )
    if not memberships:
        raise AssertionError("no Collection contains the canonical GSE207360 key")
    return observed, memberships


def validate_receipt(
    receipt: dict[str, Any],
    *,
    expected_head: str,
    expected_run_id: str,
    expected_verifier_sha256: str,
) -> None:
    if receipt.get("canonical_sha256") != receipt_sha256(receipt):
        raise AssertionError("receipt canonical digest mismatch")
    if receipt.get("format") != RECEIPT_FORMAT or receipt.get("task_id") != TASK_ID:
        raise AssertionError("receipt dataset/task identity mismatch")
    if receipt.get("run_id") != expected_run_id:
        raise AssertionError("receipt run identity mismatch")
    code = receipt.get("code", {})
    if code.get("head") != expected_head or code.get("branch") != EXPECTED_BRANCH:
        raise AssertionError("receipt code head or branch mismatch")
    if code.get("verifier_sha256") != expected_verifier_sha256:
        raise AssertionError("receipt verifier lineage mismatch")
    if receipt.get("command", {}).get("exit_code") != 0:
        raise AssertionError("receipt command did not exit successfully")
    if receipt.get("source") != {"sha256": SOURCE_SHA256, "size": 4_174_159_639}:
        raise AssertionError("receipt source identity mismatch")
    if receipt.get("artifacts") != EXPECTED_ARTIFACTS:
        raise AssertionError("receipt artifact identity mismatch")
    snapshots = receipt.get("snapshots", {})
    if snapshots.get("before") != snapshots.get("after") or receipt.get("registry_drift") != 0:
        raise AssertionError("receipt registry drift detected")
    if receipt.get("replay_noop") is not True:
        raise AssertionError("receipt replay is not a no-op")
    lifecycle = receipt.get("lifecycle", {})
    expected_lifecycle = {
        "payload_exit_code": 0,
        "terminal_vm_status": "TERMINATED",
        "task_scoped_labels_cleared": True,
        "local_lease_absent": True,
    }
    if any(lifecycle.get(key) != value for key, value in expected_lifecycle.items()):
        raise AssertionError("receipt lifecycle evidence is incomplete")
    decommission = receipt.get("gcs_decommission", {})
    if decommission.get("eligible") is not False or decommission.get("action") != "preserved_no_deletion":
        raise AssertionError("receipt decommission disposition is unsafe")


def main() -> int:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = verify_only_preflight()
    expected_head = os.environ.get("PERT_GYM_VERIFY_HEAD", "")
    run_id = os.environ.get("PERT_GYM_KANBAN_RUN_ID", "")
    expected_branch = os.environ.get("PERT_GYM_VERIFY_BRANCH", "")
    if len(expected_head) != 40 or not run_id or not expected_branch:
        raise RuntimeError("exact head, branch, and Kanban run launch bindings are required")
    actual_head = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    actual_branch = subprocess.run(
        ["git", "branch", "--show-current"], check=True, capture_output=True, text=True
    ).stdout.strip()
    branch = resolve_launch_branch(actual_branch, expected_branch)
    if actual_head != expected_head:
        raise RuntimeError("checked-out code head does not match immutable launch binding")
    if not SOURCE_PATH.is_file() or SOURCE_PATH.stat().st_size != 4_174_159_639:
        raise AssertionError("immutable filtered source is absent or has wrong size")
    if sha256_file(SOURCE_PATH) != SOURCE_SHA256:
        raise AssertionError("immutable filtered source checksum drift")

    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata" or ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin target")
    before = registry_snapshot(ln)
    artifacts, memberships = live_artifacts(ln)
    legacy = gcs_prefix_probe(LEGACY_STAGING_URI)
    cleaned = gcs_prefix_probe(CLEANED_PREFIX)
    after = registry_snapshot(ln)
    if before != after:
        raise AssertionError("registry identity/membership drift during zero-write replay")
    current_memberships = [
        row
        for row in memberships
        if row["target_members"] == [{"uid": EXPECTED_ARTIFACTS["obs"]["uid"], "key": EXPECTED_ARTIFACTS["obs"]["key"]}]
    ]
    decommission_gates = {
        "accepted_parity_and_readback": True,
        "executable_processing_notebook": True,
        "immutable_source_and_rollback": True,
        "exact_deletion_scope_present": legacy["exists"],
        "canonical_data_cleaned_payload": cleaned["exists"],
        "accepted_current_collection_membership": bool(current_memberships),
        "independent_review_of_this_receipt": False,
    }
    receipt: dict[str, Any] = {
        "format": RECEIPT_FORMAT,
        "task_id": TASK_ID,
        "run_id": run_id,
        "dataset_id": DATASET_ID,
        "code": {
            "head": actual_head,
            "branch": branch,
            "pr": 138,
            "verifier_sha256": verifier_sha256(),
        },
        "command": {"argv": sys.argv, "exit_code": 0},
        "source": {"sha256": SOURCE_SHA256, "size": 4_174_159_639},
        "artifacts": artifacts,
        "collections_with_target_key": memberships,
        "exact_current_collection_memberships": current_memberships,
        "snapshots": {"before": before, "after": after},
        "registry_drift": 0,
        "replay_noop": True,
        "gcs_decommission": {
            "legacy_staging": legacy,
            "canonical_cleaned": cleaned,
            "gates": decommission_gates,
            "eligible": False,
            "action": "preserved_no_deletion",
            "unmet_gates": [key for key, value in decommission_gates.items() if not value],
        },
        "lifecycle": {
            "payload_exit_code": 0,
            "terminal_vm_status": "PENDING_CONTROL_PLANE_READBACK",
            "task_scoped_labels_cleared": False,
            "local_lease_absent": False,
        },
        "host": {
            "hostname": capacity.hostname,
            "free_disk_bytes": capacity.free_disk_bytes,
            "available_memory_bytes": capacity.available_memory_bytes,
        },
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = receipt_sha256(receipt)
    print("GSE207360_FULL_DOD_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
