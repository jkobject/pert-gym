#!/usr/bin/env python3
"""Zero-write live full-DoD readback for DepMap/CCLE Public 26Q1."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import require_heavy_vm

TASK_ID = "t_c90d1146"
DATASET_ID = "depmap_ccle/26q1"
PREFIX = "depmap_ccle/26q1"
RECEIPT_FORMAT = "pert-gym.depmap-ccle-26q1-full-dod-live-readback/v1"
EXPECTED_BRANCH = "pert-gym/t_c90d1146-complete-fixed-18-depmap_ccle-26q1-full"
BILLING_PROJECT = "jkobject-1549353370965"
STAGING_MANIFEST_URI = (
    "gs://scperturb/pert-gym/staging/pert-gym/logical/depmap_ccle26q1/"
    "revisions/depmap-ccle26q1-default-models-wave01-960f9db1b737f306/"
    "manifest.json#1784226218253256"
)
STAGING_MANIFEST_SHA256 = (
    "ad3d220f2a0550d63d76ff944e93454a658dfa16efe4a3b3be7239ff0e492ecc"
)
STAGING_REVISION_PREFIX = (
    "gs://scperturb/pert-gym/staging/pert-gym/logical/depmap_ccle26q1/"
    "revisions/depmap-ccle26q1-default-models-wave01-960f9db1b737f306/"
)
CANONICAL_CLEANED_PREFIX = "gs://scperturb/data/cleaned/depmap_ccle/26q1/"
EXPECTED_ARTIFACTS = {
    "obs": {
        "uid": "kCNSxyUJoJJKRSgE0004",
        "key": f"{PREFIX}/obs.parquet",
        "hash": "Zm-yc0UfSwnYI1DnDfoccQ",
        "n_observations": 1719,
    },
    "x": {
        "uid": "fUSYT9ArHdQye5qv0001",
        "key": f"{PREFIX}/X.h5ad",
        "hash": "I1DppOQzGK8jczy2Lh_J9O",
        "n_observations": 1719,
    },
    "var": {
        "uid": "0S0wAPqgigynI4Av0003",
        "key": f"{PREFIX}/var.parquet",
        "hash": "5wjqSsaFA7D0kcZSts--ig",
        "n_observations": 19215,
    },
}
EXPECTED_DATASET_COLLECTION = {
    "uid": "6bVd5NhvcNOXom0s0000",
    "key": "pert-gym/dataset/depmap_ccle/26q1/20260721-e2e",
    "hash": "WTrCJAmC8y3rJupQb9Xn_w",
    "membership_sha256": "4c9ea52a66181a2a8dc9b515b9b49225a9487d00ac34d7b58a58fb9fc63439cd",
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def ordered_sha256(values: Any) -> str:
    return sha256_bytes("\n".join(map(str, values)).encode())


def verifier_sha256() -> str:
    return sha256_bytes(Path(__file__).read_bytes())


def verify_only_capacity() -> dict[str, Any]:
    """Measure the approved VM without applying the writer's 50-GiB disk floor."""
    hostname, project, zone, instance = require_heavy_vm()
    meminfo = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        key, value = line.split(":", maxsplit=1)
        meminfo[key] = int(value.strip().split()[0]) * 1024
    return {
        "hostname": hostname,
        "socket_hostname": socket.gethostname(),
        "project": project,
        "zone": zone,
        "instance": instance,
        "free_disk_bytes": shutil.disk_usage(Path.cwd()).free,
        "available_memory_bytes": meminfo["MemAvailable"],
    }


def receipt_sha256(receipt: dict[str, Any]) -> str:
    unsigned = {
        key: value for key, value in receipt.items() if key != "canonical_sha256"
    }
    return sha256_bytes(canonical(unsigned).encode())


def access_token() -> str:
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


def parse_gs_uri(uri: str) -> tuple[str, str, str | None]:
    if not uri.startswith("gs://"):
        raise ValueError("GCS URI required")
    bucket, separator, path = uri[5:].partition("/")
    if not bucket or not separator or not path:
        raise ValueError("bounded GCS object or prefix required")
    name, marker, generation = path.rpartition("#")
    return bucket, name if marker else path, generation if marker else None


def gcs_get_exact(uri: str) -> dict[str, Any]:
    bucket, name, generation = parse_gs_uri(uri)
    if generation is None:
        raise ValueError("generation-qualified GCS object required")
    quoted_bucket = urllib.parse.quote(bucket, safe="")
    quoted_name = urllib.parse.quote(name, safe="")
    base = (
        f"https://storage.googleapis.com/storage/v1/b/{quoted_bucket}/o/{quoted_name}"
    )
    common = {"generation": generation, "userProject": BILLING_PROJECT}
    headers = {"Authorization": f"Bearer {access_token()}"}
    metadata_request = urllib.request.Request(
        base + "?" + urllib.parse.urlencode(common), headers=headers
    )
    media_request = urllib.request.Request(
        base + "?" + urllib.parse.urlencode({**common, "alt": "media"}), headers=headers
    )
    try:
        with urllib.request.urlopen(metadata_request, timeout=120) as response:
            metadata = json.loads(response.read())
        with urllib.request.urlopen(media_request, timeout=120) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"GCS exact object read failed with HTTP {exc.code}"
        ) from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"GCS exact object transport/response failure: {exc}"
        ) from exc
    observed = {
        "uri": uri,
        "name": str(metadata["name"]),
        "generation": str(metadata["generation"]),
        "size": int(metadata["size"]),
        "md5Hash": str(metadata.get("md5Hash", "")),
        "sha256": sha256_bytes(payload),
        "json": json.loads(payload),
    }
    if observed["sha256"] != STAGING_MANIFEST_SHA256:
        raise AssertionError("generation-pinned staging manifest checksum drift")
    return observed


def gcs_prefix_probe(uri: str) -> dict[str, Any]:
    bucket, prefix, generation = parse_gs_uri(uri)
    if generation is not None:
        raise ValueError("prefix must not include a generation")
    query = urllib.parse.urlencode(
        {
            "prefix": prefix,
            "maxResults": 100,
            "fields": "items(name,generation,size,md5Hash),nextPageToken",
            "userProject": BILLING_PROJECT,
        }
    )
    request = urllib.request.Request(
        "https://storage.googleapis.com/storage/v1/b/"
        f"{urllib.parse.quote(bucket, safe='')}/o?{query}",
        headers={"Authorization": f"Bearer {access_token()}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"GCS prefix listing failed with HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"GCS prefix transport/response failure: {exc}") from exc
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise RuntimeError("GCS prefix listing returned invalid items")
    objects = [
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
        "exists": bool(objects),
        "object_count": len(objects),
        "truncated": bool(payload.get("nextPageToken")),
        "objects": objects,
    }


def feature_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    latest = [
        item for item in ln.Artifact.filter(key=value).all() if bool(item.is_latest)
    ]
    if len(latest) != 1:
        raise AssertionError(f"cannot resolve linked Artifact {value!r}")
    return latest[0]


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size),
        "n_observations": int(artifact.n_observations),
        "version": str(artifact.version),
        "is_latest": bool(artifact.is_latest),
        "path": str(artifact.path),
    }


def membership_sha256(uids: list[str]) -> str:
    return sha256_bytes(canonical(sorted(uids)).encode())


def collection_identity(collection: Any) -> dict[str, Any]:
    members = list(collection.artifacts.only("uid", "key").all())
    uids = [str(member.uid) for member in members]
    return {
        "uid": str(collection.uid),
        "key": str(collection.key),
        "hash": str(collection.hash),
        "member_count": len(members),
        "unique_uid_count": len(set(uids)),
        "unique_key_count": len({str(member.key) for member in members}),
        "membership_sha256": membership_sha256(uids),
        "target_members": sorted(
            (
                {"uid": str(member.uid), "key": str(member.key)}
                for member in members
                if str(member.key) == f"{PREFIX}/obs.parquet"
            ),
            key=lambda item: (item["uid"], item["key"]),
        ),
    }


def scoped_registry_snapshot(ln: Any) -> dict[str, Any]:
    histories = {}
    for role in ("obs", "X", "var"):
        key = f"{PREFIX}/{role}.parquet" if role != "X" else f"{PREFIX}/X.h5ad"
        histories[role] = sorted(
            (artifact_identity(item) for item in ln.Artifact.filter(key=key).all()),
            key=lambda item: item["uid"],
        )
    memberships = []
    for collection in ln.Collection.filter().all():
        identity = collection_identity(collection)
        if identity["target_members"]:
            memberships.append(identity)
    return {
        "artifact_count": int(ln.Artifact.filter().count()),
        "collection_count": int(ln.Collection.filter().count()),
        "target_histories": histories,
        "target_memberships": sorted(memberships, key=lambda item: item["uid"]),
    }


def live_payload(ln: Any) -> dict[str, Any]:
    obs_artifact = ln.Artifact.get(uid=EXPECTED_ARTIFACTS["obs"]["uid"])
    x_artifact = feature_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = feature_artifact(ln, x_artifact.features.get_values()["var"])
    observed_base = {
        "obs": {
            key: artifact_identity(obs_artifact)[key]
            for key in EXPECTED_ARTIFACTS["obs"]
        },
        "x": {
            key: artifact_identity(x_artifact)[key] for key in EXPECTED_ARTIFACTS["x"]
        },
        "var": {
            key: artifact_identity(var_artifact)[key]
            for key in EXPECTED_ARTIFACTS["var"]
        },
    }
    if observed_base != EXPECTED_ARTIFACTS:
        raise AssertionError(f"Artifact identity drift: {observed_base!r}")
    obs = obs_artifact.load()
    x = x_artifact.load()
    var = var_artifact.load()
    if list(x.shape) != [1719, 19215] or len(obs) != 1719 or len(var) != 19215:
        raise AssertionError("OBS/X/VAR dimensional parity drift")
    if ordered_sha256(obs.index) != ordered_sha256(x.obs_names):
        raise AssertionError("OBS/X observation order drift")
    stable_ids = var["stable_feature_id"].astype("string")
    x_var_digest = ordered_sha256(x.var_names)
    axis_candidates = {"var.index": ordered_sha256(var.index)}
    axis_candidates.update(
        {
            f"var.{column}": ordered_sha256(var[column].astype("string"))
            for column in var.columns
            if bool(var[column].astype("string").is_unique)
        }
    )
    matching_axis_sources = sorted(
        source for source, digest in axis_candidates.items() if digest == x_var_digest
    )
    if "var.index" not in matching_axis_sources:
        raise AssertionError(
            "X/VAR ordered-axis identity is absent from canonical VAR index: "
            f"matches={matching_axis_sources!r}"
        )
    axis_source = "var.index"
    canonical_fields = [
        "modality",
        "perturbation",
        "perturbation_type",
        "source",
        "source_accession",
        "x_semantics",
    ]
    missing = [field for field in canonical_fields if field not in obs.columns]
    if missing:
        raise AssertionError(f"accepted OBS fields are missing: {missing}")
    coverage = {
        field: {
            "non_null": int(obs[field].notna().sum()),
            "values": {
                str(key): int(value)
                for key, value in obs[field]
                .astype("string")
                .value_counts(dropna=False)
                .items()
            },
        }
        for field in canonical_fields
    }
    return {
        "artifacts": {
            "obs": artifact_identity(obs_artifact),
            "x": artifact_identity(x_artifact),
            "var": artifact_identity(var_artifact),
        },
        "shape": [1719, 19215],
        "matrix_dtype": str(x.X.dtype),
        "matrix_stored_nonzero": int(
            x.X.nnz if hasattr(x.X, "nnz") else (x.X != 0).sum()
        ),
        "obs_index_sha256": ordered_sha256(obs.index),
        "x_obs_names_sha256": ordered_sha256(x.obs_names),
        "stable_feature_id_sha256": ordered_sha256(stable_ids),
        "x_var_names_sha256": x_var_digest,
        "var_axis_identity_source": axis_source,
        "var_axis_identity_aliases": matching_axis_sources,
        "var_axis_identity_sha256": axis_candidates[axis_source],
        "obs_columns": list(map(str, obs.columns)),
        "var_columns": list(map(str, var.columns)),
        "accepted_obs_field_coverage": coverage,
        "var_identity": {
            "stable_feature_id_unique": bool(stable_ids.is_unique),
            "human_ensembl_release_116_unique": int(
                stable_ids.str.fullmatch(r"ENSG\d+", na=False).sum()
            ),
            "explicit_unresolved": int(
                len(stable_ids) - stable_ids.str.fullmatch(r"ENSG\d+", na=False).sum()
            ),
        },
    }


def main() -> int:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = verify_only_capacity()
    expected_head = os.environ.get("PERT_GYM_VERIFY_HEAD", "")
    run_id = os.environ.get("PERT_GYM_KANBAN_RUN_ID", "")
    if len(expected_head) != 40 or not run_id:
        raise RuntimeError(
            "exact PERT_GYM_VERIFY_HEAD and PERT_GYM_KANBAN_RUN_ID are required"
        )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    branch = subprocess.run(
        ["git", "branch", "--show-current"], check=True, capture_output=True, text=True
    ).stdout.strip()
    if head != expected_head or branch != EXPECTED_BRANCH:
        raise RuntimeError(
            "checked-out code head/branch differs from immutable launch binding"
        )

    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise AssertionError("wrong Lamin instance")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin branch")
    before = scoped_registry_snapshot(ln)
    payload = live_payload(ln)
    dataset_collection = collection_identity(
        ln.Collection.get(uid=EXPECTED_DATASET_COLLECTION["uid"])
    )
    for key, value in EXPECTED_DATASET_COLLECTION.items():
        if dataset_collection[key] != value:
            raise AssertionError(f"dataset Collection identity drift for {key}")
    if dataset_collection["target_members"] != [
        {"uid": EXPECTED_ARTIFACTS["obs"]["uid"], "key": f"{PREFIX}/obs.parquet"}
    ]:
        raise AssertionError("dataset Collection target membership drift")
    manifest = gcs_get_exact(STAGING_MANIFEST_URI)
    staging = gcs_prefix_probe(STAGING_REVISION_PREFIX)
    cleaned = gcs_prefix_probe(CANONICAL_CLEANED_PREFIX)
    after = scoped_registry_snapshot(ln)
    if before != after:
        raise AssertionError("registry drift during zero-write verification")
    current_memberships = [
        row
        for row in after["target_memberships"]
        if row["target_members"]
        == [{"uid": EXPECTED_ARTIFACTS["obs"]["uid"], "key": f"{PREFIX}/obs.parquet"}]
    ]
    decommission_gates = {
        "accepted_payload_and_source_parity": True,
        "executable_processing_notebook": False,
        "immutable_upstream_checksum_or_retained_raw_source": False,
        "canonical_data_cleaned_payload": cleaned["exists"],
        "accepted_current_collection_membership": bool(current_memberships),
        "reviewed_gcs_decommission_ready_manifest": False,
        "independent_review_of_this_receipt": False,
    }
    receipt: dict[str, Any] = {
        "format": RECEIPT_FORMAT,
        "task_id": TASK_ID,
        "run_id": run_id,
        "dataset_id": DATASET_ID,
        "status": "PASS_ZERO_WRITE_FULL_DOD_INCOMPLETE",
        "code": {"head": head, "branch": branch, "verifier_sha256": verifier_sha256()},
        "command": {"argv": sys.argv, "exit_code": 0},
        "source_release": {
            "name": "DepMap Public 26Q1",
            "portal": "https://depmap.org/portal/download/all/",
            "source_rows_total": 1775,
            "selected_default_model_rows": 1719,
            "excluded_non_default_rows": 56,
            "acquisition_checksum_status": "unknown",
        },
        "payload": payload,
        "staging_manifest": manifest,
        "dataset_collection": dataset_collection,
        "current_collection_memberships": current_memberships,
        "snapshots": {"before": before, "after": after},
        "registry_drift": 0,
        "replay_noop": True,
        "writes": {
            "artifacts": 0,
            "collections": 0,
            "feature_links": 0,
            "gcs": 0,
            "deletions": 0,
        },
        "gcs_decommission": {
            "staging_revision": staging,
            "canonical_cleaned": cleaned,
            "gates": decommission_gates,
            "eligible": False,
            "action": "preserved_no_deletion",
            "unmet_gates": [
                key for key, value in decommission_gates.items() if not value
            ],
        },
        "host": {
            **capacity,
        },
        "lifecycle": {
            "payload_exit_code": 0,
            "terminal_vm_status": "PENDING_CONTROL_PLANE_READBACK",
            "task_scoped_labels_cleared": False,
            "local_lease_absent": False,
        },
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = receipt_sha256(receipt)
    print("DEPMAP_CCLE_26Q1_FULL_DOD_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
