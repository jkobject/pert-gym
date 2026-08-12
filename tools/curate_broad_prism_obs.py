#!/usr/bin/env python3
"""Append-only Broad PRISM 24Q2 OBS curation with exact legacy-axis parity.

The current 22,316,860-row OBS surface was produced by treating the long-form
PRISM source columns as perturbations and adding one synthetic control row per
source record. This tool does not rewrite X or compact that structural surface.
Instead, it preserves every existing row and its order while joining each row
back to the immutable source record, treatment metadata, and the publication's
cell-line table. Only ``write`` mode mutates Lamin, and it requires an exact
plan authorization generated after the no-write ``plan`` mode.

Run only on ``pert-gym-worker-eu`` through ``tools/launch_pert_gym_heavy.py``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from tools.lamin_context import connect_pertdata

TASK_ID = "t_a15f5366"
DATASET_ID = "broad_prism_repurposing"
OBS_KEY = f"{DATASET_ID}/obs.parquet"
X_KEY = f"{DATASET_ID}/X.h5ad"
VAR_KEY = f"{DATASET_ID}/var.parquet"
EXPECTED_PREDECESSOR_OBS_UID = "eKrJkcFDb9TEDbte0000"
EXPECTED_VAR_UID = "T3YpaJB1Rt51Ef4U0000"
EXPECTED_OBS_ROWS = 22_316_860
EXPECTED_SOURCE_ROWS = 4_463_372
MATERIALIZATION_BATCH_ROWS = 50_000
MIN_MEM_AVAILABLE_BYTES = 8 * 1024**3
SOURCE_FIELDS = ("profile_id", "LFC", "LFC_cb", "PASS")
SOURCE_RELEASE = "DepMap PRISM Primary Repurposing Public 24Q2"
BILLING_PROJECT = "jkobject-1549353370965"
LFC_URI = (
    "gs://scperturb/pert-gym/staging/data/main/broad_prism/"
    "Repurposing_Public_24Q2_LFC.csv"
)
LFC_NAME = "pert-gym/staging/data/main/broad_prism/Repurposing_Public_24Q2_LFC.csv"
LFC_GENERATION = "1781122336979534"
LFC_BYTES = 479_352_899
LFC_SHA256 = "824149f9b9f3821eb520b385a5976e1a9977d86b21caf5d22171763800a40523"
TREATMENT_URI = (
    "gs://scperturb/pert-gym/staging/data/main/broad_prism/"
    "Repurposing_Public_24Q2_Treatment_Meta_Data.csv"
)
TREATMENT_NAME = (
    "pert-gym/staging/data/main/broad_prism/"
    "Repurposing_Public_24Q2_Treatment_Meta_Data.csv"
)
TREATMENT_GENERATION = "1781122337401878"
TREATMENT_BYTES = 1_510_417
TREATMENT_SHA256 = "6be6422ba804ad0775e78b457677bdf088707b9354746a03e110ae63f5eb2061"
CELL_INFO_URL = "https://ndownloader.figshare.com/files/20237718"
CELL_INFO_FIGSHARE_ID = 20237718
CELL_INFO_BYTES = 46_849
CELL_INFO_MD5 = "580b9f0b5118d44f473ffa4efac7e0c2"
CELL_INFO_SHA256 = "c17174f48264fea0adb3eb4b2b1f2daf61b83999b0db6e4965338e3bd6a9fde2"
PAPER_DOI = "10.1038/s43018-019-0018-6"
PAPER_PMID = "32613204"
PAPER_PMCID = "PMC7328899"
FIGSHARE_DOI = "10.6084/m9.figshare.9393293.v4"
INCORRECT_CARD_DOI = "10.1038/s41591-019-0404-8"
SOURCE_REQUIRED_COLUMNS = ("row_id", "profile_id", "LFC", "LFC_cb", "PASS")
TREATMENT_REQUIRED_COLUMNS = (
    "profile_id",
    "prism_replicate",
    "perturbation_well",
    "culture",
    "perturbation_type",
    "dose",
    "broad_id",
    "name",
    "compound_plate",
    "rep",
    "screen",
)
CELL_REQUIRED_COLUMNS = (
    "depmap_id",
    "ccle_name",
    "primary_tissue",
    "secondary_tissue",
    "tertiary_tissue",
    "passed_str_profiling",
)
RUN_ROOT_DEFAULT = Path.home() / "pert-gym-runs" / TASK_ID

FIELD_DISPOSITIONS = {
    "obs_uuid": "present: preserved predecessor deterministic identity",
    "original_obs_index": "present: preserved predecessor source-derived identity",
    "dataset": "present: dataset-wide constant",
    "sample": "present: exact raw row_id",
    "cell_id": "not_applicable: pooled cell-line viability, not single-cell",
    "donor_id": "not_applicable: immortalized cancer cell lines",
    "batch": "present: treatment metadata prism_replicate",
    "cell_type": "not_applicable: cell-line response screen",
    "cell_line": "present: ACH DepMap ID plus source ccle_name when joined",
    "disease": "present: cancer, supported for all screened human cancer cell lines",
    "tissue_type": "present where Figshare cell-line metadata joins; otherwise unknown",
    "organism": "present: Homo sapiens",
    "sex": "unknown: not in the release-bound named sources",
    "age": "not_applicable: donor age is not a property of the response row",
    "ethnicity": "not_applicable: donor ethnicity is not a response-row dimension",
    "sequencer": "not_applicable: Luminex barcode viability readout",
    "technology": "present: PRISM pooled molecular-barcoding viability assay",
    "assay": "present: PRISM multiplexed pooled-cell-line viability",
    "modality": "present: drug_response",
    "media": "unknown: no row-joinable medium field in named release sources",
    "is_bulk": "not_applicable: response table, not bulk RNA",
    "is_pseudobulk": "not_applicable: response table",
    "perturbation": "present: treatment metadata name",
    "perturbation_type": "present: source treatment class normalized to drug",
    "perturbation_technology": "present: PRISM pooled viability screen",
    "perturbation_library": "present: Broad Drug Repurposing Hub",
    "guide_id": "not_applicable: chemical perturbation",
    "guide_sequence": "not_applicable: chemical perturbation",
    "perturbation_target": "unknown: not supplied as a row-joinable release field",
    "perturbation_target_id": "unknown: not supplied as a row-joinable release field",
    "is_control": "present: exact treatment type ctl_vehicle",
    "dose": "present: treatment metadata dose",
    "dose_unit": "present: micromolar, publication/Figshare screen protocol",
    "timepoint": "present where encoded in profile_id (minutes)",
    "trajectory_id": "not_applicable: non-temporal response design",
    "pseudotime": "not_applicable: non-single-cell response screen",
    "is_baseline": "not_applicable: no measured baseline expression in this triplet",
    "sensitivity": "present only for direct LFC rows; alias of response_value",
    "response_metric": "present only for direct LFC rows",
    "response_value": "present only for direct LFC rows",
    "response_source": "present only for direct LFC rows",
    "n_counts": "not_applicable: no RNA counts",
    "n_genes": "not_applicable: empty non-gene X/VAR axis",
    "pct_mito": "not_applicable: no RNA counts",
    "pct_ribo": "not_applicable: no RNA counts",
    "is_low_quality": "present: inverse of exact source PASS for direct LFC rows",
}


def utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_available_memory() -> int | None:
    """Fail closed on the Linux worker before allocating another bounded batch."""
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return None
    fields = {}
    for line in meminfo.read_text(encoding="utf-8").splitlines():
        key, value = line.split(":", maxsplit=1)
        fields[key] = int(value.strip().split()[0]) * 1024
    available = fields.get("MemAvailable")
    if available is None or available < MIN_MEM_AVAILABLE_BYTES:
        raise RuntimeError(
            f"MemAvailable preflight failed: {available!r} < {MIN_MEM_AVAILABLE_BYTES}"
        )
    return available


def update_ordered_identity_digest(digest: Any, table: pa.Table) -> None:
    for name in ("obs_uuid", "original_obs_index"):
        if name not in table.column_names:
            raise RuntimeError(f"identity column missing: {name}")
        digest.update(name.encode() + b"\0")
        for value in table[name].to_pylist():
            encoded = b"<NULL>" if value is None else str(value).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)


def ordered_identity_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(
        batch_size=MATERIALIZATION_BATCH_ROWS,
        columns=["obs_uuid", "original_obs_index"],
    ):
        update_ordered_identity_digest(digest, pa.Table.from_batches([batch]))
    return digest.hexdigest()


def md5_file(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json_sha256(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def seal_evidence(payload: dict[str, Any], digest_field: str) -> dict[str, Any]:
    sealed = {key: value for key, value in payload.items() if key != digest_field}
    sealed[digest_field] = canonical_json_sha256(sealed)
    return sealed


def validate_sealed_evidence(
    payload: dict[str, Any], *, digest_field: str, label: str
) -> None:
    digest = payload.get(digest_field)
    if not isinstance(digest, str) or not digest:
        raise RuntimeError(f"{label} is missing {digest_field}")
    unsealed = {key: value for key, value in payload.items() if key != digest_field}
    if canonical_json_sha256(unsealed) != digest:
        raise RuntimeError(f"{label} digest mismatch")


def current_code_identity() -> dict[str, str]:
    return {
        "code_commit": run_checked(["git", "rev-parse", "HEAD"]).strip(),
        "code_script_sha256": sha256_file(Path(__file__).resolve()),
    }


def expected_source_provenance() -> dict[str, dict[str, object]]:
    """Return every immutable identity field for the three source objects."""
    return {
        "lfc": {
            "uri": LFC_URI,
            "name": LFC_NAME,
            "generation": LFC_GENERATION,
            "bytes": LFC_BYTES,
            "sha256": LFC_SHA256,
        },
        "treatment_metadata": {
            "uri": TREATMENT_URI,
            "name": TREATMENT_NAME,
            "generation": TREATMENT_GENERATION,
            "bytes": TREATMENT_BYTES,
            "sha256": TREATMENT_SHA256,
        },
        "cell_line_metadata": {
            "url": CELL_INFO_URL,
            "figshare_file_id": CELL_INFO_FIGSHARE_ID,
            "bytes": CELL_INFO_BYTES,
            "md5": CELL_INFO_MD5,
            "sha256": CELL_INFO_SHA256,
        },
    }


def validate_plan_integrity(
    plan: dict[str, Any], *, current_identity: dict[str, str] | None = None
) -> None:
    if plan.get("format") != "pert-gym.broad-prism-obs-plan.v1":
        raise RuntimeError("unsupported Broad PRISM plan format")
    plan_digest = plan.get("plan_sha256")
    if (
        not isinstance(plan_digest, str)
        or canonical_json_sha256(
            {key: value for key, value in plan.items() if key != "plan_sha256"}
        )
        != plan_digest
    ):
        raise RuntimeError("plan bytes/content digest mismatch")
    identity = current_identity or current_code_identity()
    expected_identity = {
        "code_commit": plan.get("code_commit"),
        "code_script_sha256": plan.get("code_script_sha256"),
    }
    if identity != expected_identity:
        raise RuntimeError(
            f"code provenance mismatch: expected {expected_identity!r}, got {identity!r}"
        )
    sources = plan.get("sources")
    expected_sources = expected_source_provenance()
    if not isinstance(sources, dict):
        raise RuntimeError("source provenance is missing")
    mismatches: dict[str, dict[str, object]] = {}
    for source_name, expected_fields in expected_sources.items():
        actual = sources.get(source_name)
        if not isinstance(actual, dict):
            mismatches[source_name] = {"expected": expected_fields, "actual": actual}
            continue
        for field, expected in expected_fields.items():
            if actual.get(field) != expected:
                mismatches[f"{source_name}.{field}"] = {
                    "expected": expected,
                    "actual": actual.get(field),
                }
    if mismatches:
        raise RuntimeError(f"source provenance mismatch: {mismatches!r}")


def emit_heartbeat(phase: str, epoch: int, current: int, denominator: int) -> None:
    heartbeat_path = os.environ.get("PERT_GYM_PAYLOAD_HEARTBEAT_PATH")
    if heartbeat_path:
        path = Path(heartbeat_path)
        temporary = path.with_suffix(path.suffix + ".partial")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(f"{os.getpid()} {int(time.time())}\n", encoding="ascii")
        temporary.replace(path)
    print(
        "PRODUCT_EXECUTION "
        + json.dumps(
            {
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "phase": phase,
                "epoch": epoch,
                "metric": "obs_rows",
                "current": current,
                "denominator": denominator,
                "timestamp": utc_now(),
            },
            sort_keys=True,
        ),
        flush=True,
    )


def require_eu_worker() -> None:
    if socket.gethostname() != "pert-gym-worker-eu":
        raise RuntimeError(
            "Broad PRISM bulk curation is allowed only on pert-gym-worker-eu"
        )


def run_checked(command: list[str]) -> str:
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(
            f"command failed rc={result.returncode}: {' '.join(command)}\n"
            + (result.stdout or "")[-1000:]
            + (result.stderr or "")[-1000:]
        )
    return result.stdout


def describe_gcs(uri: str) -> dict[str, Any]:
    output = run_checked(
        [
            "gcloud",
            "storage",
            "objects",
            "describe",
            uri,
            f"--billing-project={BILLING_PROJECT}",
            "--format=json(name,size,generation)",
        ]
    )
    payload = json.loads(output)
    return {
        "name": payload["name"],
        "size": int(payload["size"]),
        "generation": str(payload["generation"]),
    }


def materialize_gcs_source(
    uri: str,
    destination: Path,
    *,
    expected_generation: str,
    expected_size: int,
    expected_sha256: str,
) -> dict[str, Any]:
    before = describe_gcs(uri)
    if before["generation"] != expected_generation or before["size"] != expected_size:
        raise RuntimeError(f"source generation/size drift for {uri}: {before}")
    existing_is_valid = (
        destination.exists()
        and destination.stat().st_size == expected_size
        and sha256_file(destination) == expected_sha256
    )
    if not existing_is_valid:
        partial = destination.with_suffix(destination.suffix + ".partial")
        partial.unlink(missing_ok=True)
        destination.unlink(missing_ok=True)
        run_checked(
            [
                "gcloud",
                "storage",
                "cp",
                uri,
                str(partial),
                f"--billing-project={BILLING_PROJECT}",
            ]
        )
        if partial.stat().st_size != expected_size:
            raise RuntimeError(f"downloaded source size mismatch for {uri}")
        partial.replace(destination)
    digest = sha256_file(destination)
    if digest != expected_sha256:
        raise RuntimeError(f"source SHA-256 mismatch for {uri}")
    after = describe_gcs(uri)
    if after != before:
        raise RuntimeError(f"source identity changed during materialization for {uri}")
    return {
        "uri": uri,
        "name": before["name"],
        "generation": before["generation"],
        "bytes": before["size"],
        "sha256": digest,
        "local_path": str(destination),
    }


def materialize_http_source(
    url: str,
    destination: Path,
    *,
    expected_sha256: str,
    expected_md5: str,
) -> dict[str, Any]:
    existing_is_valid = (
        destination.exists()
        and sha256_file(destination) == expected_sha256
        and md5_file(destination) == expected_md5
    )
    if not existing_is_valid:
        partial = destination.with_suffix(destination.suffix + ".partial")
        partial.unlink(missing_ok=True)
        destination.unlink(missing_ok=True)
        with (
            urllib.request.urlopen(url, timeout=120) as response,
            partial.open("wb") as out,
        ):
            shutil.copyfileobj(response, out)
        partial.replace(destination)
    if (
        sha256_file(destination) != expected_sha256
        or md5_file(destination) != expected_md5
    ):
        raise RuntimeError(f"HTTP source checksum mismatch for {url}")
    return {
        "url": url,
        "figshare_file_id": CELL_INFO_FIGSHARE_ID,
        "bytes": destination.stat().st_size,
        "md5": expected_md5,
        "sha256": expected_sha256,
        "local_path": str(destination),
    }


def require_columns(
    path: Path, reader: csv.DictReader, required: Iterable[str]
) -> None:
    missing = sorted(set(required) - set(reader.fieldnames or ()))
    if missing:
        raise RuntimeError(f"{path} is missing required columns: {missing}")


def load_unique_csv(
    path: Path, *, key: str, required: tuple[str, ...]
) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        require_columns(path, reader, required)
        for line_number, row in enumerate(reader, start=2):
            value = (row.get(key) or "").strip()
            if not value:
                raise RuntimeError(f"{path}:{line_number} has empty {key}")
            if value in rows:
                raise RuntimeError(f"{path}:{line_number} duplicates {key}={value}")
            rows[value] = {name: (raw or "").strip() for name, raw in row.items()}
    return rows


def build_source_index(path: Path, database_path: Path) -> dict[str, Any]:
    temporary = database_path.with_suffix(".sqlite.partial")
    temporary.unlink(missing_ok=True)
    database = sqlite3.connect(temporary)
    finite_lfc = 0
    pass_true = 0
    try:
        database.execute(
            "CREATE TABLE source_rows ("
            "source_index INTEGER PRIMARY KEY, row_id TEXT NOT NULL UNIQUE, "
            "profile_id TEXT NOT NULL, lfc TEXT, lfc_cb TEXT, pass_value TEXT NOT NULL)"
        )
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            require_columns(path, reader, SOURCE_REQUIRED_COLUMNS)
            batch: list[tuple[int, str, str, str, str, str]] = []
            for source_index, row in enumerate(reader):
                row_id = (row.get("row_id") or "").strip()
                profile_id = (row.get("profile_id") or "").strip()
                if not row_id or not profile_id:
                    raise RuntimeError(
                        f"empty source identity at data row {source_index + 1}"
                    )
                raw_lfc = (row.get("LFC") or "").strip()
                try:
                    if math.isfinite(float(raw_lfc)):
                        finite_lfc += 1
                except ValueError:
                    pass
                pass_value = (row.get("PASS") or "").strip()
                if pass_value.lower() not in {"true", "false"}:
                    raise RuntimeError(
                        f"unknown PASS value {pass_value!r} at CSV row {source_index + 2}"
                    )
                pass_true += pass_value.lower() == "true"
                batch.append(
                    (
                        source_index,
                        row_id,
                        profile_id,
                        raw_lfc,
                        (row.get("LFC_cb") or "").strip(),
                        pass_value,
                    )
                )
                if len(batch) >= 50_000:
                    database.executemany(
                        "INSERT INTO source_rows VALUES (?, ?, ?, ?, ?, ?)", batch
                    )
                    database.commit()
                    batch.clear()
                    emit_heartbeat(
                        "indexing_source",
                        source_index // 50_000 + 1,
                        source_index + 1,
                        EXPECTED_SOURCE_ROWS,
                    )
            if batch:
                database.executemany(
                    "INSERT INTO source_rows VALUES (?, ?, ?, ?, ?, ?)", batch
                )
                database.commit()
        count = int(database.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0])
        if count != EXPECTED_SOURCE_ROWS:
            raise RuntimeError(
                f"source row denominator {count} != {EXPECTED_SOURCE_ROWS}"
            )
        database.execute("CREATE INDEX source_rows_profile ON source_rows(profile_id)")
        database.commit()
    finally:
        database.close()
    temporary.replace(database_path)
    return {
        "source_rows": count,
        "finite_lfc_rows": finite_lfc,
        "pass_true_rows": pass_true,
        "exact_source_duplicate_count": 0,
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    if hasattr(value, "uid"):
        return value
    if not isinstance(value, str):
        raise RuntimeError(f"unsupported artifact feature value: {value!r}")
    try:
        return ln.Artifact.get(key=value)
    except Exception:
        return ln.Artifact.get(uid=value)


def collection_memberships(ln: Any, artifact: Any) -> list[dict[str, str]]:
    memberships: list[dict[str, str]] = []
    for collection in ln.Collection.filter().all():
        if collection.artifacts.filter(uid=artifact.uid).exists():
            memberships.append(collection_identity(collection))
    return sorted(memberships, key=lambda item: (item["name"], item["uid"]))


def artifact_identity(artifact: Any) -> dict[str, object]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size or -1),
        "path": str(artifact.path),
    }


def collection_identity(collection: Any) -> dict[str, str]:
    return {
        "uid": str(collection.uid),
        "name": str(collection.name),
        "version": str(collection.version or ""),
        "hash": str(collection.hash),
    }


def artifact_registry_inventory(ln: Any) -> list[dict[str, object]]:
    return sorted(
        (artifact_identity(artifact) for artifact in ln.Artifact.filter().all()),
        key=lambda item: (str(item["uid"]), str(item["key"])),
    )


def collection_registry_inventory(ln: Any) -> list[dict[str, object]]:
    inventory: list[dict[str, object]] = []
    for collection in ln.Collection.filter().all():
        artifacts = sorted(str(artifact.uid) for artifact in collection.artifacts.all())
        inventory.append(
            {
                **collection_identity(collection),
                "artifact_uids": artifacts,
            }
        )
    return sorted(inventory, key=lambda item: (str(item["uid"]), str(item["name"])))


def registry_snapshot(ln: Any) -> dict[str, object]:
    artifacts = artifact_registry_inventory(ln)
    collections = collection_registry_inventory(ln)
    return {
        "artifacts": artifacts,
        "artifact_registry_sha256": canonical_json_sha256({"artifacts": artifacts}),
        "collections": collections,
        "collection_registry_sha256": canonical_json_sha256(
            {"collections": collections}
        ),
    }


def require_artifact_identity(
    expected: dict[str, object], actual: dict[str, object], *, label: str
) -> None:
    fields = ("uid", "key", "hash", "size", "path")
    mismatches = {
        field: {"expected": expected.get(field), "actual": actual.get(field)}
        for field in fields
        if expected.get(field) != actual.get(field)
    }
    if mismatches:
        raise RuntimeError(f"{label} artifact identity mismatch: {mismatches!r}")


def validate_expected_predecessor(snapshot: dict[str, Any]) -> None:
    expected = {
        "obs": {"uid": EXPECTED_PREDECESSOR_OBS_UID, "key": OBS_KEY},
        "var": {"uid": EXPECTED_VAR_UID, "key": VAR_KEY},
    }
    for role, identity in expected.items():
        actual = snapshot.get(role)
        if not isinstance(actual, dict) or any(
            actual.get(field) != value for field, value in identity.items()
        ):
            raise RuntimeError(
                f"expected predecessor {role.upper()} identity mismatch: "
                f"expected {identity!r}, got {actual!r}"
            )


def inspect_lamin(ln: Any) -> dict[str, Any]:
    obs = ln.Artifact.get(key=OBS_KEY)
    x = resolve_artifact(ln, obs.features.get_values()["X"])
    var = resolve_artifact(ln, x.features.get_values()["var"])
    if x.key != X_KEY or var.key != VAR_KEY:
        raise RuntimeError(f"triplet link drift: {x.key=} {var.key=}")
    obs_path = Path(obs.cache())
    parquet = pq.ParquetFile(obs_path)
    if parquet.metadata.num_rows != EXPECTED_OBS_ROWS:
        raise RuntimeError("live OBS row denominator drift")
    var_frame = var.load()
    if len(var_frame) != 0:
        raise RuntimeError("Broad PRISM VAR is no longer an empty non-gene axis")
    if int(x.n_observations or -1) != EXPECTED_OBS_ROWS:
        raise RuntimeError("X n_observations no longer matches OBS")
    registries = registry_snapshot(ln)
    return {
        "obs_artifact": obs,
        "x_artifact": x,
        "var_artifact": var,
        "obs_path": obs_path,
        "snapshot": {
            "instance": ln.setup.settings.instance.slug,
            "branch": ln.setup.settings.branch.name,
            "obs": {
                "uid": str(obs.uid),
                "key": str(obs.key),
                "path": str(obs.path),
                "hash": str(obs.hash),
                "size": int(obs.size or -1),
                "rows": parquet.metadata.num_rows,
                "row_groups": parquet.num_row_groups,
                "columns": parquet.schema_arrow.names,
            },
            "X": {
                "uid": str(x.uid),
                "key": str(x.key),
                "path": str(x.path),
                "hash": str(x.hash),
                "size": int(x.size or -1),
                "n_observations": int(x.n_observations or -1),
            },
            "var": {
                "uid": str(var.uid),
                "key": str(var.key),
                "path": str(var.path),
                "hash": str(var.hash),
                "size": int(var.size or -1),
                "rows": len(var_frame),
                "verdict": "not_applicable",
                "feature_class": "non_gene_empty_response_axis",
                "species": "Homo sapiens",
            },
            "collection_count": int(ln.Collection.filter().count()),
            "predecessor_collection_memberships": collection_memberships(ln, obs),
            "exact_obs_key_count": int(ln.Artifact.filter(key=OBS_KEY).count()),
            "exact_x_key_count": int(ln.Artifact.filter(key=X_KEY).count()),
            "exact_var_key_count": int(ln.Artifact.filter(key=VAR_KEY).count()),
            **registries,
        },
    }


def source_rows_between(
    database: sqlite3.Connection, first: int, last: int
) -> list[tuple[int, str, str, str, str, str]]:
    rows = database.execute(
        "SELECT source_index, row_id, profile_id, lfc, lfc_cb, pass_value "
        "FROM source_rows WHERE source_index BETWEEN ? AND ? ORDER BY source_index",
        (first, last),
    ).fetchall()
    if len(rows) != last - first + 1:
        raise RuntimeError("source index range is incomplete")
    return rows


def source_rows_for_indices(
    database: sqlite3.Connection, indices: list[int]
) -> dict[int, tuple[int, str, str, str, str, str]]:
    """Load bounded contiguous spans, including the control→field index wrap."""
    unique_indices = list(dict.fromkeys(indices))
    spans: list[tuple[int, int]] = []
    first = previous = unique_indices[0]
    for value in unique_indices[1:]:
        if value != previous + 1:
            spans.append((first, previous))
            first = value
        previous = value
    spans.append((first, previous))
    rows: dict[int, tuple[int, str, str, str, str, str]] = {}
    for first, last in spans:
        for row in source_rows_between(database, first, last):
            rows[row[0]] = row
    if set(rows) != set(unique_indices):
        raise RuntimeError("source index lookup did not resolve exactly requested rows")
    return rows


def normalize_treatment_type(raw: str) -> tuple[str, str, bool]:
    value = raw.strip().lower()
    if value == "ctl_vehicle":
        return "drug", "vehicle", True
    if value == "trt_poscon":
        return "drug", "positive_control", False
    if value == "trt_cp":
        return "drug", "not_control", False
    raise RuntimeError(f"unsupported treatment type {raw!r}")


def parse_timepoint_minutes(profile_id: str) -> int | None:
    match = re.search(r"(?:^|_)(\d+)H(?:_|$)", profile_id)
    return int(match.group(1)) * 60 if match else None


def expected_source_index_and_role(position: int, source_rows: int) -> tuple[int, str]:
    if position < source_rows:
        return position, "legacy_synthetic_control"
    shifted = position - source_rows
    return shifted // len(SOURCE_FIELDS), SOURCE_FIELDS[shifted % len(SOURCE_FIELDS)]


def set_column(frame: pd.DataFrame, name: str, values: Any) -> None:
    frame.attrs.setdefault("_column_updates", {})[name] = values


def build_candidate(
    *,
    predecessor_path: Path,
    source_database_path: Path,
    treatments: dict[str, dict[str, str]],
    cell_lines: dict[str, dict[str, str]],
    output_path: Path,
) -> dict[str, Any]:
    if output_path.exists():
        raise RuntimeError(f"refusing to overwrite candidate {output_path}")
    parquet = pq.ParquetFile(predecessor_path)
    database = sqlite3.connect(source_database_path)
    writer: pq.ParquetWriter | None = None
    global_position = 0
    counts: Counter[str] = Counter()
    source_type_counts: Counter[str] = Counter()
    predecessor_identity_digest = hashlib.sha256()
    maximum_materialized_batch_rows = 0
    try:
        for batch_index, batch in enumerate(
            parquet.iter_batches(batch_size=MATERIALIZATION_BATCH_ROWS)
        ):
            require_available_memory()
            table = pa.Table.from_batches([batch])
            update_ordered_identity_digest(predecessor_identity_digest, table)
            frame = table.to_pandas()
            maximum_materialized_batch_rows = max(
                maximum_materialized_batch_rows, len(frame)
            )
            positions = list(range(global_position, global_position + len(frame)))
            source_indices_roles = [
                expected_source_index_and_role(position, EXPECTED_SOURCE_ROWS)
                for position in positions
            ]
            by_index = source_rows_for_indices(
                database, [index for index, _ in source_indices_roles]
            )
            source_rows = [by_index[index] for index, _ in source_indices_roles]
            roles = [role for _, role in source_indices_roles]
            expected_row_ids = [row[1] for row in source_rows]
            observed_row_ids = frame["depmap_id"].astype(str).tolist()
            mismatches = sum(a != b for a, b in zip(observed_row_ids, expected_row_ids))
            if mismatches:
                raise RuntimeError(
                    f"legacy row/source order mismatch in batch {batch_index}: {mismatches}"
                )
            observed_roles = [
                "legacy_synthetic_control"
                if role == "legacy_synthetic_control"
                else str(value)
                for role, value in zip(roles, frame["broad_id"].tolist())
            ]
            if observed_roles != roles:
                raise RuntimeError(
                    f"legacy field order mismatch in batch {batch_index}"
                )

            metadata_rows: list[dict[str, str]] = []
            cell_rows: list[dict[str, str] | None] = []
            ach_ids: list[str] = []
            for raw in source_rows:
                profile_id = raw[2]
                metadata = treatments.get(profile_id)
                if metadata is None:
                    raise RuntimeError(f"unmatched treatment profile_id {profile_id!r}")
                metadata_rows.append(metadata)
                ach_id = raw[1].split("::", maxsplit=1)[0]
                ach_ids.append(ach_id)
                cell_rows.append(cell_lines.get(ach_id))

            treatment_types = [row["perturbation_type"] for row in metadata_rows]
            normalized = [normalize_treatment_type(value) for value in treatment_types]
            direct_lfc = [role == "LFC" for role in roles]
            pass_flags = [raw[5].lower() == "true" for raw in source_rows]
            finite_flags = []
            for raw in source_rows:
                try:
                    finite_flags.append(math.isfinite(float(raw[3])))
                except ValueError:
                    finite_flags.append(False)
            response_present = [
                direct and finite for direct, finite in zip(direct_lfc, finite_flags)
            ]

            set_column(frame, "source_release", SOURCE_RELEASE)
            set_column(frame, "source_lfc_uri", LFC_URI)
            set_column(frame, "source_lfc_generation", LFC_GENERATION)
            set_column(frame, "source_lfc_sha256", LFC_SHA256)
            set_column(frame, "source_treatment_metadata_uri", TREATMENT_URI)
            set_column(
                frame, "source_treatment_metadata_generation", TREATMENT_GENERATION
            )
            set_column(frame, "source_treatment_metadata_sha256", TREATMENT_SHA256)
            set_column(frame, "source_row_id", expected_row_ids)
            set_column(frame, "source_profile_id", [raw[2] for raw in source_rows])
            set_column(
                frame,
                "source_row_identifier",
                [f"{raw[1]}|{raw[2]}" for raw in source_rows],
            )
            set_column(
                frame, "source_file_row_number", [raw[0] + 2 for raw in source_rows]
            )
            set_column(frame, "source_field_role", roles)
            set_column(frame, "source_pass", pass_flags)
            set_column(frame, "source_lfc", [raw[3] for raw in source_rows])
            set_column(frame, "source_lfc_cb", [raw[4] for raw in source_rows])
            set_column(frame, "source_treatment_type", treatment_types)
            set_column(
                frame,
                "source_prism_replicate",
                [row["prism_replicate"] for row in metadata_rows],
            )
            set_column(
                frame,
                "source_perturbation_well",
                [row["perturbation_well"] for row in metadata_rows],
            )
            set_column(
                frame, "source_culture", [row["culture"] for row in metadata_rows]
            )
            set_column(
                frame,
                "source_compound_plate",
                [row["compound_plate"] for row in metadata_rows],
            )
            set_column(frame, "source_rep", [row["rep"] for row in metadata_rows])
            set_column(frame, "source_screen", [row["screen"] for row in metadata_rows])
            set_column(frame, "source_paper_doi", PAPER_DOI)
            set_column(frame, "source_figshare_doi", FIGSHARE_DOI)

            set_column(frame, "dataset", DATASET_ID)
            set_column(frame, "dataset_state", "present")
            set_column(frame, "dataset_source", "curation.dataset_id")
            set_column(frame, "sample", expected_row_ids)
            set_column(frame, "sample_state", "present")
            set_column(frame, "sample_source", "source.row_id")
            set_column(
                frame, "batch", [row["prism_replicate"] for row in metadata_rows]
            )
            set_column(frame, "batch_state", "present")
            set_column(frame, "batch_source", "treatment_metadata.prism_replicate")
            set_column(frame, "depmap_id", ach_ids)
            set_column(frame, "cell_line", ach_ids)
            set_column(frame, "cell_line_state", "present")
            set_column(frame, "cell_line_source", "source.row_id ACH prefix")
            set_column(
                frame,
                "cell_line_name",
                [row["ccle_name"] if row else "unknown" for row in cell_rows],
            )
            tissues = [row["primary_tissue"] if row else "unknown" for row in cell_rows]
            set_column(frame, "tissue", tissues)
            set_column(
                frame,
                "tissue_state",
                ["present" if row else "missing" for row in cell_rows],
            )
            set_column(
                frame,
                "tissue_source",
                ["figshare.primary_tissue" if row else "contract" for row in cell_rows],
            )
            set_column(frame, "tissue_type", tissues)
            set_column(
                frame,
                "tissue_type_state",
                ["present" if row else "missing" for row in cell_rows],
            )
            set_column(
                frame,
                "tissue_type_source",
                ["figshare.primary_tissue" if row else "contract" for row in cell_rows],
            )
            set_column(frame, "disease", "cancer")
            set_column(frame, "disease_state", "present")
            set_column(frame, "disease_source", f"paper:{PAPER_DOI}")
            set_column(frame, "organism", "Homo sapiens")
            set_column(frame, "organism_state", "present")
            set_column(frame, "organism_source", f"paper:{PAPER_DOI}")
            set_column(frame, "assay", "PRISM multiplexed pooled-cell-line viability")
            set_column(frame, "assay_state", "present")
            set_column(frame, "assay_source", f"paper:{PAPER_DOI}")
            set_column(frame, "modality", "drug_response")
            set_column(frame, "modality_state", "present")
            set_column(frame, "modality_source", "curation")
            set_column(frame, "technology", "PRISM molecular barcoding viability assay")
            set_column(frame, "technology_state", "present")
            set_column(frame, "technology_source", f"paper:{PAPER_DOI}")
            set_column(frame, "is_bulk", "not_applicable")
            set_column(frame, "is_bulk_state", "not_applicable")
            set_column(frame, "is_bulk_source", "contract")
            set_column(frame, "is_pseudobulk", "not_applicable")
            set_column(frame, "is_pseudobulk_state", "not_applicable")
            set_column(frame, "is_pseudobulk_source", "contract")
            set_column(
                frame,
                "perturbation",
                [row["name"] or "unknown" for row in metadata_rows],
            )
            set_column(
                frame,
                "perturbation_state",
                ["present" if row["name"] else "missing" for row in metadata_rows],
            )
            set_column(frame, "perturbation_source", "treatment_metadata.name")
            set_column(
                frame,
                "perturbation_id",
                [row["broad_id"] or "unknown" for row in metadata_rows],
            )
            set_column(
                frame,
                "perturbation_id_state",
                ["present" if row["broad_id"] else "missing" for row in metadata_rows],
            )
            set_column(frame, "perturbation_id_source", "treatment_metadata.broad_id")
            set_column(frame, "perturbation_type", [value[0] for value in normalized])
            set_column(frame, "perturbation_type_state", "present")
            set_column(
                frame,
                "perturbation_type_source",
                "treatment_metadata.perturbation_type",
            )
            set_column(
                frame, "perturbation_technology", "PRISM pooled viability screen"
            )
            set_column(frame, "perturbation_technology_state", "present")
            set_column(frame, "perturbation_technology_source", f"paper:{PAPER_DOI}")
            set_column(frame, "perturbation_library", "Broad Drug Repurposing Hub")
            set_column(frame, "perturbation_library_state", "present")
            set_column(frame, "perturbation_library_source", f"paper:{PAPER_DOI}")
            set_column(
                frame, "dose", [row["dose"] or "unknown" for row in metadata_rows]
            )
            set_column(
                frame,
                "dose_state",
                ["present" if row["dose"] else "missing" for row in metadata_rows],
            )
            set_column(frame, "dose_source", "treatment_metadata.dose")
            set_column(frame, "dose_unit", "micromolar")
            set_column(frame, "dose_unit_state", "present")
            set_column(
                frame, "dose_unit_source", f"paper:{PAPER_DOI};figshare:{FIGSHARE_DOI}"
            )
            timepoints = [parse_timepoint_minutes(raw[2]) for raw in source_rows]
            set_column(
                frame,
                "timepoint",
                [
                    str(value) if value is not None else "unknown"
                    for value in timepoints
                ],
            )
            set_column(
                frame,
                "timepoint_state",
                ["present" if value is not None else "missing" for value in timepoints],
            )
            set_column(frame, "timepoint_source", "source.profile_id encoded hours")
            set_column(frame, "timepoint_unit", "minutes")
            set_column(frame, "timepoint_unit_state", "present")
            set_column(frame, "timepoint_unit_source", "curation conversion")
            set_column(frame, "is_control", [value[2] for value in normalized])
            set_column(frame, "is_control_state", "present")
            set_column(
                frame, "is_control_source", "treatment_metadata.perturbation_type"
            )
            set_column(frame, "control_type", [value[1] for value in normalized])
            set_column(frame, "control_type_state", "present")
            set_column(
                frame, "control_type_source", "treatment_metadata.perturbation_type"
            )
            set_column(frame, "control_availability", "dataset_control_available")
            set_column(frame, "control_availability_state", "present")
            set_column(frame, "control_availability_source", f"paper:{PAPER_DOI}")
            set_column(
                frame,
                "response_metric",
                [
                    "lfc" if present else "not_applicable"
                    for present in response_present
                ],
            )
            set_column(
                frame,
                "response_metric_state",
                [
                    "present" if present else "not_applicable"
                    for present in response_present
                ],
            )
            set_column(
                frame,
                "response_metric_source",
                [
                    "source.LFC" if present else "contract"
                    for present in response_present
                ],
            )
            response_values = [
                float(raw[3]) if present else None
                for raw, present in zip(source_rows, response_present)
            ]
            numeric_response_values = pd.Series(
                response_values, dtype="float64", index=frame.index
            )
            set_column(frame, "response_value", numeric_response_values)
            set_column(
                frame,
                "response_value_state",
                [
                    "present" if present else "not_applicable"
                    for present in response_present
                ],
            )
            set_column(
                frame,
                "response_value_source",
                [
                    "source.LFC" if present else "contract"
                    for present in response_present
                ],
            )
            set_column(frame, "sensitivity", numeric_response_values)
            set_column(
                frame,
                "sensitivity_state",
                [
                    "present" if present else "not_applicable"
                    for present in response_present
                ],
            )
            set_column(
                frame,
                "sensitivity_source",
                [
                    "source.LFC" if present else "contract"
                    for present in response_present
                ],
            )
            set_column(
                frame,
                "response_source",
                [
                    "Repurposing_Public_24Q2_LFC.csv:LFC joined to treatment metadata by profile_id"
                    if present
                    else "not_applicable"
                    for present in response_present
                ],
            )
            set_column(
                frame,
                "response_source_state",
                [
                    "present" if present else "not_applicable"
                    for present in response_present
                ],
            )
            set_column(
                frame,
                "response_source_source",
                ["curation" if present else "contract" for present in response_present],
            )
            set_column(
                frame,
                "response_transform",
                [
                    "log2_fold_change_vs_vehicle" if present else "not_applicable"
                    for present in response_present
                ],
            )
            set_column(
                frame,
                "response_direction",
                [
                    "lower_more_sensitive" if present else "not_applicable"
                    for present in response_present
                ],
            )
            set_column(frame, "target_is_direct", response_present)
            set_column(frame, "x_semantics", "empty")
            set_column(frame, "x_semantics_state", "present")
            set_column(frame, "x_semantics_source", "live X/VAR readback")
            set_column(frame, "has_expression", False)
            set_column(frame, "has_expression_X", False)
            loader_projectable = [
                present and passed
                for present, passed in zip(response_present, pass_flags)
            ]
            set_column(frame, "loader_projectable", loader_projectable)
            set_column(frame, "loader_projectable_state", "present")
            set_column(frame, "loader_projectable_source", "curation eligibility")
            set_column(frame, "model_ready", False)
            set_column(frame, "model_ready_state", "present")
            set_column(frame, "model_ready_source", "empty expression X")
            set_column(frame, "model_ready_status", "loader_projectable_only")
            quality = []
            for role, passed, finite, joined_cell in zip(
                roles, pass_flags, finite_flags, cell_rows
            ):
                if role == "legacy_synthetic_control":
                    quality.append("legacy_synthetic_padding")
                elif role != "LFC":
                    quality.append("legacy_source_field_padding")
                elif not finite:
                    quality.append("source_non_finite_lfc")
                elif not passed:
                    quality.append("source_qc_failed")
                elif joined_cell is None:
                    quality.append("accepted_lfc_cell_metadata_unknown")
                else:
                    quality.append("accepted_lfc")
            set_column(frame, "quality_flag", quality)
            set_column(frame, "quality_flag_state", "present")
            set_column(frame, "quality_flag_source", "curation")
            set_column(
                frame,
                "is_low_quality",
                [
                    flag in {"source_non_finite_lfc", "source_qc_failed"}
                    for flag in quality
                ],
            )
            set_column(frame, "is_low_quality_state", "present")
            set_column(frame, "is_low_quality_source", "source.PASS and finite LFC")

            updates = frame.attrs.pop("_column_updates")
            base = frame.drop(
                columns=[name for name in updates if name in frame.columns]
            )
            frame = pd.concat(
                [base, pd.DataFrame(updates, index=frame.index)], axis=1, copy=False
            )
            out_table = pa.Table.from_pandas(frame, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(
                    output_path,
                    out_table.schema,
                    compression="zstd",
                    use_dictionary=True,
                )
            writer.write_table(out_table, row_group_size=len(frame))
            global_position += len(frame)
            counts.update(roles)
            source_type_counts.update(treatment_types)
            counts["direct_lfc_rows"] += sum(response_present)
            counts["pass_true_direct_lfc_rows"] += sum(
                present and passed
                for present, passed in zip(response_present, pass_flags)
            )
            counts["cell_metadata_unmatched_rows"] += sum(
                row is None for row in cell_rows
            )
            emit_heartbeat(
                "materializing_candidate",
                batch_index + 1,
                global_position,
                EXPECTED_OBS_ROWS,
            )
    finally:
        database.close()
        if writer is not None:
            writer.close()
    if global_position != EXPECTED_OBS_ROWS:
        raise RuntimeError("candidate row count mismatch")
    candidate = pq.ParquetFile(output_path)
    if candidate.metadata.num_rows != EXPECTED_OBS_ROWS:
        raise RuntimeError("candidate Parquet metadata row count mismatch")
    identity_sha256 = predecessor_identity_digest.hexdigest()
    if ordered_identity_sha256(output_path) != identity_sha256:
        raise RuntimeError("ordered obs_uuid/original_obs_index identity drift")
    return {
        "rows": global_position,
        "row_groups": candidate.num_row_groups,
        "bytes": output_path.stat().st_size,
        "sha256": sha256_file(output_path),
        "ordered_identity_sha256": identity_sha256,
        "materialization_batch_rows": MATERIALIZATION_BATCH_ROWS,
        "maximum_materialized_batch_rows": maximum_materialized_batch_rows,
        "role_counts": dict(sorted(counts.items())),
        "source_treatment_type_counts_repeated_on_legacy_axis": dict(
            sorted(source_type_counts.items())
        ),
        "legacy_order_mismatch": 0,
        "treatment_join_mismatch": 0,
        "source_duplicate_count": 0,
    }


def source_search_log() -> list[dict[str, Any]]:
    return [
        {
            "source": "DepMap PRISM Repurposing portal",
            "url": "https://depmap.org/repurposing/",
            "result": "current release identity; portal automation is Cloudflare-gated",
        },
        {
            "source": "DepMap Public 24Q2 staged LFC",
            "uri": LFC_URI,
            "generation": LFC_GENERATION,
            "sha256": LFC_SHA256,
            "result": "complete long-form response source",
        },
        {
            "source": "DepMap Public 24Q2 staged treatment metadata",
            "uri": TREATMENT_URI,
            "generation": TREATMENT_GENERATION,
            "sha256": TREATMENT_SHA256,
            "result": "row-joinable profile, compound, dose, plate, replicate and screen metadata",
        },
        {
            "source": "Corsello et al. Nature Cancer",
            "doi": PAPER_DOI,
            "pmid": PAPER_PMID,
            "pmcid": PAPER_PMCID,
            "result": "human cancer cell-line PRISM viability assay, dose unit/protocol, controls and 5-day readout",
        },
        {
            "source": "PRISM Repurposing Figshare v4",
            "doi": FIGSHARE_DOI,
            "file_id": CELL_INFO_FIGSHARE_ID,
            "sha256": CELL_INFO_SHA256,
            "result": "row-joinable cell-line names and tissue metadata by depmap_id",
        },
        {
            "source": "card DOI correction",
            "incorrect_doi": INCORRECT_CARD_DOI,
            "incorrect_title": "The landscape of cancer cell line metabolism",
            "correct_doi": PAPER_DOI,
            "correct_title": "Discovering the anticancer potential of non-oncology drugs by systematic viability profiling",
            "result": "incorrect DOI rejected; not used as PRISM evidence",
        },
    ]


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def run_plan(run_root: Path) -> dict[str, Any]:
    require_eu_worker()
    run_root.mkdir(parents=True, exist_ok=True)
    plan_path = run_root / "plan.json"
    candidate_path = run_root / "broad_prism_obs_candidate.parquet"
    candidate_partial_path = candidate_path.with_suffix(".parquet.partial")
    if plan_path.exists() or candidate_path.exists():
        raise RuntimeError("plan destinations already exist; refuse replay/overwrite")
    candidate_partial_path.unlink(missing_ok=True)
    if run_checked(["git", "status", "--porcelain"]).strip():
        raise RuntimeError(
            "plan code worktree is dirty; commit exact code before execution"
        )
    emit_heartbeat("preflight", 1, 0, EXPECTED_OBS_ROWS)
    lfc_path = run_root / "Repurposing_Public_24Q2_LFC.csv"
    treatment_path = run_root / "Repurposing_Public_24Q2_Treatment_Meta_Data.csv"
    cell_path = run_root / "primary-screen-cell-line-info.csv"
    sources = {
        "lfc": materialize_gcs_source(
            LFC_URI,
            lfc_path,
            expected_generation=LFC_GENERATION,
            expected_size=LFC_BYTES,
            expected_sha256=LFC_SHA256,
        ),
        "treatment_metadata": materialize_gcs_source(
            TREATMENT_URI,
            treatment_path,
            expected_generation=TREATMENT_GENERATION,
            expected_size=TREATMENT_BYTES,
            expected_sha256=TREATMENT_SHA256,
        ),
        "cell_line_metadata": materialize_http_source(
            CELL_INFO_URL,
            cell_path,
            expected_sha256=CELL_INFO_SHA256,
            expected_md5=CELL_INFO_MD5,
        ),
    }
    treatments = load_unique_csv(
        treatment_path, key="profile_id", required=TREATMENT_REQUIRED_COLUMNS
    )
    cell_lines = load_unique_csv(
        cell_path, key="depmap_id", required=CELL_REQUIRED_COLUMNS
    )
    database_path = run_root / "source_rows.sqlite"
    source_summary = build_source_index(lfc_path, database_path)
    ln = connect_pertdata()
    live = inspect_lamin(ln)
    validate_expected_predecessor(live["snapshot"])
    candidate_summary = build_candidate(
        predecessor_path=live["obs_path"],
        source_database_path=database_path,
        treatments=treatments,
        cell_lines=cell_lines,
        output_path=candidate_partial_path,
    )
    candidate_partial_path.replace(candidate_path)
    commit = run_checked(["git", "rev-parse", "HEAD"]).strip()
    script_sha256 = sha256_file(Path(__file__).resolve())
    plan = {
        "format": "pert-gym.broad-prism-obs-plan.v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "created_at": utc_now(),
        "code_commit": commit,
        "code_script_sha256": script_sha256,
        "mode": "plan_no_lamin_write",
        "source_search_log": source_search_log(),
        "sources": sources,
        "source_summary": source_summary,
        "predecessor": live["snapshot"],
        "candidate": {**candidate_summary, "path": str(candidate_path)},
        "field_dispositions": FIELD_DISPOSITIONS,
        "structural_decision": {
            "recompact_or_rewrite_X": "no_op_for_this_card",
            "reason": "JIT packet forbids X rewrite/recompaction; preserve exact 22,316,860-row empty-X axis and expose legacy padding explicitly",
            "future_separate_lane": "replace malformed 5x legacy response axis with the 4,463,372 direct source rows only after a separately reviewed X/Collection migration",
        },
        "var_verdict": live["snapshot"]["var"],
        "allowed_mutation": "one append-only OBS revision linked to the exact predecessor X",
        "forbidden_mutations": [
            "X rewrite",
            "VAR rewrite",
            "Collection recreation or promotion",
            "Lamin main write",
            "deletion or cleanup",
        ],
    }
    plan["plan_sha256"] = canonical_json_sha256(plan)
    write_json_atomic(plan_path, plan)
    emit_heartbeat("planned", 2, EXPECTED_OBS_ROWS, EXPECTED_OBS_ROWS)
    print("PLAN_RESULT " + json.dumps(plan, sort_keys=True), flush=True)
    return plan


def validate_authorization(plan: dict[str, Any], authorization: dict[str, Any]) -> None:
    expected = {
        "format": "pert-gym.broad-prism-obs-authorization.v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "approved": True,
        "plan_sha256": plan["plan_sha256"],
        "candidate_sha256": plan["candidate"]["sha256"],
        "predecessor_obs_uid": plan["predecessor"]["obs"]["uid"],
        "predecessor_x_uid": plan["predecessor"]["X"]["uid"],
        "predecessor_var_uid": plan["predecessor"]["var"]["uid"],
    }
    mismatches = {
        key: {"expected": value, "actual": authorization.get(key)}
        for key, value in expected.items()
        if authorization.get(key) != value
    }
    if mismatches:
        raise RuntimeError(f"authorization mismatch: {mismatches}")


def fresh_remote_copy(artifact: Any, destination: Path) -> None:
    destination.unlink(missing_ok=True)
    with artifact.path.open("rb") as source, destination.open("wb") as target:
        shutil.copyfileobj(source, target, length=8 * 1024 * 1024)


def verify_candidate_readback(
    path: Path, expected_sha256: str, expected_identity_sha256: str
) -> dict[str, Any]:
    if sha256_file(path) != expected_sha256:
        raise RuntimeError("fresh remote OBS readback SHA-256 mismatch")
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows != EXPECTED_OBS_ROWS:
        raise RuntimeError("fresh remote OBS readback row mismatch")
    required = {
        "source_row_identifier",
        "source_field_role",
        "response_metric",
        "response_value",
        "quality_flag",
        "cell_line",
        "perturbation",
        "dose",
    }
    missing = sorted(required - set(parquet.schema_arrow.names))
    if missing:
        raise RuntimeError(f"fresh remote OBS readback lacks columns: {missing}")
    identity_sha256 = ordered_identity_sha256(path)
    if identity_sha256 != expected_identity_sha256:
        raise RuntimeError("fresh remote ordered row identity mismatch")
    return {
        "bytes": path.stat().st_size,
        "sha256": expected_sha256,
        "ordered_identity_sha256": identity_sha256,
        "rows": parquet.metadata.num_rows,
        "row_groups": parquet.num_row_groups,
        "required_columns_missing": [],
    }


def validate_registry_transition(
    before: dict[str, Any], after: dict[str, Any], candidate: Any
) -> None:
    expected_artifacts = sorted(
        [*before["artifacts"], artifact_identity(candidate)],
        key=lambda item: (str(item["uid"]), str(item["key"])),
    )
    if after["artifacts"] != expected_artifacts:
        raise RuntimeError(
            "artifact registry drift outside the one authorized OBS revision"
        )
    if after["collections"] != before["collections"]:
        raise RuntimeError("Collection registry or membership drift")


def validate_prewrite_journal(plan: dict[str, Any], journal: dict[str, Any]) -> None:
    validate_sealed_evidence(
        journal, digest_field="journal_sha256", label="write journal"
    )
    expected = {
        "format": "pert-gym.broad-prism-obs-write-journal.v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "plan_sha256": plan["plan_sha256"],
        "candidate_sha256": plan["candidate"]["sha256"],
        "predecessor_obs_identity": plan["predecessor"]["obs"],
        "preserved_x_identity": plan["predecessor"]["X"],
        "preserved_var_identity": plan["predecessor"]["var"],
        "pre_registry": {
            "artifacts": plan["predecessor"]["artifacts"],
            "collections": plan["predecessor"]["collections"],
        },
    }
    mismatches = {
        field: {"expected": value, "actual": journal.get(field)}
        for field, value in expected.items()
        if journal.get(field) != value
    }
    if mismatches:
        raise RuntimeError(f"write journal authorization mismatch: {mismatches!r}")


def validate_receipt_against_plan(
    plan: dict[str, Any], receipt: dict[str, Any]
) -> None:
    predecessor = plan["predecessor"]
    expected = {
        "format": "pert-gym.broad-prism-obs-write-receipt.v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "plan_sha256": plan["plan_sha256"],
        "candidate_sha256": plan["candidate"]["sha256"],
        "predecessor_obs_identity": predecessor["obs"],
        "rollback_obs_uid": predecessor["obs"]["uid"],
        "preserved_x_uid": predecessor["X"]["uid"],
        "preserved_x_identity": predecessor["X"],
        "preserved_var_uid": predecessor["var"]["uid"],
        "preserved_var_identity": predecessor["var"],
        "predecessor_collection_memberships": predecessor[
            "predecessor_collection_memberships"
        ],
        "candidate_collection_memberships": [],
        "var_verdict": plan["var_verdict"],
    }
    mismatches = {
        field: {"expected": value, "actual": receipt.get(field)}
        for field, value in expected.items()
        if receipt.get(field) != value
    }
    if mismatches:
        if "preserved_x_identity" in mismatches:
            raise RuntimeError(
                f"authorized X artifact identity mismatch: {mismatches['preserved_x_identity']!r}"
            )
        raise RuntimeError(f"write receipt authorization mismatch: {mismatches!r}")
    candidate_identity = receipt.get("candidate_obs_identity")
    if not isinstance(candidate_identity, dict):
        raise RuntimeError("write receipt lacks candidate OBS identity")
    candidate_registry_identity = {
        field: candidate_identity.get(field)
        for field in ("uid", "key", "hash", "size", "path")
    }
    expected_post_registry = {
        "artifacts": sorted(
            [*predecessor["artifacts"], candidate_registry_identity],
            key=lambda item: (str(item["uid"]), str(item["key"])),
        ),
        "collections": predecessor["collections"],
    }
    if receipt.get("post_registry") != expected_post_registry:
        raise RuntimeError(
            "write receipt registry transition is not the one authorized by the plan"
        )


def validate_terminal_journal(
    *, plan: dict[str, Any], receipt: dict[str, Any], journal_path: Path
) -> dict[str, Any]:
    if not journal_path.exists():
        raise RuntimeError("terminal write journal is missing")
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    validate_prewrite_journal(plan, journal)
    expected = {
        "state": "receipt_sealed",
        "candidate_obs_uid": receipt.get("candidate_obs_uid"),
        "receipt_sha256": receipt.get("receipt_sha256"),
    }
    mismatches = {
        field: {"expected": value, "actual": journal.get(field)}
        for field, value in expected.items()
        if journal.get(field) != value
    }
    if mismatches:
        raise RuntimeError(f"terminal write journal mismatch: {mismatches!r}")
    return journal


def validate_live_receipt(
    *,
    ln: Any,
    plan: dict[str, Any],
    receipt: dict[str, Any],
    run_root: Path,
    readback_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_sealed_evidence(
        receipt, digest_field="receipt_sha256", label="write receipt"
    )
    required = {
        "candidate_obs_identity",
        "preserved_x_identity",
        "preserved_var_identity",
        "post_registry",
        "predecessor_collection_memberships",
        "candidate_collection_memberships",
    }
    missing = sorted(required - set(receipt))
    if missing:
        raise RuntimeError(f"write receipt is incomplete: {missing}")
    validate_receipt_against_plan(plan, receipt)
    validate_terminal_journal(
        plan=plan,
        receipt=receipt,
        journal_path=run_root / "write_journal.json",
    )
    live = inspect_lamin(ln)
    snapshot = live["snapshot"]
    require_artifact_identity(
        receipt["candidate_obs_identity"], snapshot["obs"], label="candidate OBS"
    )
    require_artifact_identity(
        receipt["preserved_x_identity"], snapshot["X"], label="preserved X"
    )
    require_artifact_identity(
        receipt["preserved_var_identity"], snapshot["var"], label="preserved VAR"
    )
    post_registry = receipt["post_registry"]
    if snapshot["artifacts"] != post_registry.get("artifacts"):
        raise RuntimeError("artifact registry drift after sealed write")
    if snapshot["collections"] != post_registry.get("collections"):
        raise RuntimeError("Collection registry or membership drift after sealed write")
    predecessor = ln.Artifact.get(uid=receipt["predecessor_obs_identity"]["uid"])
    if (
        collection_memberships(ln, predecessor)
        != receipt["predecessor_collection_memberships"]
    ):
        raise RuntimeError("predecessor Collection memberships drifted")
    if (
        collection_memberships(ln, live["obs_artifact"])
        != receipt["candidate_collection_memberships"]
    ):
        raise RuntimeError("candidate Collection memberships drifted")
    fresh_path = run_root / readback_name
    fresh_remote_copy(live["obs_artifact"], fresh_path)
    readback = verify_candidate_readback(
        fresh_path,
        plan["candidate"]["sha256"],
        plan["candidate"]["ordered_identity_sha256"],
    )
    return live, readback


def run_write(run_root: Path, authorization_path: Path) -> dict[str, Any]:
    require_eu_worker()
    plan = json.loads((run_root / "plan.json").read_text(encoding="utf-8"))
    validate_plan_integrity(plan)
    authorization = json.loads(authorization_path.read_text(encoding="utf-8"))
    validate_authorization(plan, authorization)
    candidate_path = Path(plan["candidate"]["path"])
    if sha256_file(candidate_path) != plan["candidate"]["sha256"]:
        raise RuntimeError("candidate bytes drifted after planning")
    receipt_path = run_root / "write_receipt.json"
    journal_path = run_root / "write_journal.json"
    ln = connect_pertdata()
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        validate_live_receipt(
            ln=ln,
            plan=plan,
            receipt=receipt,
            run_root=run_root,
            readback_name="replay_fresh_remote_obs.parquet",
        )
        replay = dict(receipt)
        replay["replay"] = {
            "writes": 0,
            "status": "verified_no_op",
            "verified_at": utc_now(),
        }
        print("WRITE_RESULT " + json.dumps(replay, sort_keys=True), flush=True)
        return replay

    predecessor_identity = plan["predecessor"]["obs"]
    x_identity = plan["predecessor"]["X"]
    var_identity = plan["predecessor"]["var"]
    predecessor = ln.Artifact.get(uid=predecessor_identity["uid"])
    x_artifact = ln.Artifact.get(uid=x_identity["uid"])
    require_artifact_identity(
        predecessor_identity,
        artifact_identity(predecessor),
        label="predecessor OBS",
    )
    require_artifact_identity(
        x_identity, artifact_identity(x_artifact), label="authorized X"
    )
    require_artifact_identity(
        var_identity,
        artifact_identity(ln.Artifact.get(uid=var_identity["uid"])),
        label="authorized VAR",
    )
    if journal_path.exists():
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        validate_prewrite_journal(plan, journal)
        if journal.get("state") != "authorized_pre_write":
            raise RuntimeError(
                "write journal is terminal or incomplete while receipt is missing"
            )
    else:
        current = ln.Artifact.get(key=OBS_KEY)
        if str(current.uid) != predecessor_identity["uid"]:
            raise RuntimeError(
                "live OBS advanced before crash-recovery journal existed"
            )
        live_before = inspect_lamin(ln)
        require_artifact_identity(
            predecessor_identity,
            live_before["snapshot"]["obs"],
            label="predecessor OBS",
        )
        require_artifact_identity(
            x_identity, live_before["snapshot"]["X"], label="authorized X"
        )
        require_artifact_identity(
            var_identity, live_before["snapshot"]["var"], label="authorized VAR"
        )
        if live_before["snapshot"]["artifacts"] != plan["predecessor"]["artifacts"]:
            raise RuntimeError("artifact registry drift before write")
        if live_before["snapshot"]["collections"] != plan["predecessor"]["collections"]:
            raise RuntimeError("Collection registry or membership drift before write")
        journal = seal_evidence(
            {
                "format": "pert-gym.broad-prism-obs-write-journal.v1",
                "task_id": TASK_ID,
                "dataset_id": DATASET_ID,
                "created_at": utc_now(),
                "plan_sha256": plan["plan_sha256"],
                "candidate_sha256": plan["candidate"]["sha256"],
                "predecessor_obs_identity": predecessor_identity,
                "preserved_x_identity": x_identity,
                "preserved_var_identity": var_identity,
                "pre_registry": {
                    "artifacts": plan["predecessor"]["artifacts"],
                    "collections": plan["predecessor"]["collections"],
                },
                "state": "authorized_pre_write",
            },
            "journal_sha256",
        )
        write_json_atomic(journal_path, journal)

    emit_heartbeat("writing", 1, 0, EXPECTED_OBS_ROWS)
    current = ln.Artifact.get(key=OBS_KEY)
    candidate_was_adopted = str(current.uid) != predecessor_identity["uid"]
    if candidate_was_adopted:
        candidate = current
        if str(candidate.key) != OBS_KEY:
            raise RuntimeError("crash-recovery candidate key mismatch")
        recovery_path = run_root / "recovery_candidate_readback.parquet"
        fresh_remote_copy(candidate, recovery_path)
        verify_candidate_readback(
            recovery_path,
            plan["candidate"]["sha256"],
            plan["candidate"]["ordered_identity_sha256"],
        )
    else:
        candidate = ln.Artifact.from_dataframe(
            candidate_path,
            key=OBS_KEY,
            revises=predecessor,
            description=(
                f"Broad PRISM 24Q2 source-exhaustive OBS curation; task {TASK_ID}; "
                f"plan {plan['plan_sha256']}; preserves legacy 22,316,860-row order"
            ),
        ).save()
    feature_values = candidate.features.get_values()
    if "X" not in feature_values:
        candidate.features.set_values({"X": x_artifact})
        feature_values = candidate.features.get_values()
    linked_x = resolve_artifact(ln, feature_values["X"])
    require_artifact_identity(
        x_identity,
        artifact_identity(linked_x),
        label="candidate OBS authorized X link",
    )
    fresh_path = run_root / "fresh_remote_obs_readback.parquet"
    fresh_remote_copy(candidate, fresh_path)
    readback = verify_candidate_readback(
        fresh_path,
        plan["candidate"]["sha256"],
        plan["candidate"]["ordered_identity_sha256"],
    )
    live_after = inspect_lamin(ln)
    if (
        live_after["snapshot"]["exact_obs_key_count"]
        != plan["predecessor"]["exact_obs_key_count"] + 1
    ):
        raise RuntimeError("append-only OBS revision count delta is not exactly one")
    if live_after["snapshot"]["obs"]["uid"] != str(candidate.uid):
        raise RuntimeError("candidate is not current OBS after write")
    require_artifact_identity(
        x_identity, live_after["snapshot"]["X"], label="preserved X"
    )
    require_artifact_identity(
        var_identity, live_after["snapshot"]["var"], label="preserved VAR"
    )
    validate_registry_transition(
        journal["pre_registry"], live_after["snapshot"], candidate
    )
    predecessor_memberships_after = collection_memberships(ln, predecessor)
    if (
        predecessor_memberships_after
        != plan["predecessor"]["predecessor_collection_memberships"]
    ):
        raise RuntimeError("predecessor Collection memberships drifted")
    candidate_memberships = collection_memberships(ln, candidate)
    if candidate_memberships:
        raise RuntimeError(
            "OBS-only card unexpectedly promoted candidate to a Collection"
        )
    receipt = seal_evidence(
        {
            "format": "pert-gym.broad-prism-obs-write-receipt.v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "created_at": utc_now(),
            "plan_sha256": plan["plan_sha256"],
            "candidate_sha256": plan["candidate"]["sha256"],
            "predecessor_obs_identity": predecessor_identity,
            "rollback_obs_uid": predecessor_identity["uid"],
            "candidate_obs_uid": str(candidate.uid),
            "candidate_obs_identity": live_after["snapshot"]["obs"],
            "preserved_x_uid": x_identity["uid"],
            "preserved_x_identity": live_after["snapshot"]["X"],
            "preserved_var_uid": var_identity["uid"],
            "preserved_var_identity": live_after["snapshot"]["var"],
            "append_only_obs_revision_delta": 1,
            "X_revision_delta": 0,
            "VAR_revision_delta": 0,
            "Collection_count_delta": 0,
            "predecessor_collection_memberships": predecessor_memberships_after,
            "candidate_collection_memberships": candidate_memberships,
            "fresh_remote_readback": readback,
            "post_registry": {
                "artifacts": live_after["snapshot"]["artifacts"],
                "collections": live_after["snapshot"]["collections"],
            },
            "row_order_mismatch": 0,
            "source_join_mismatch": 0,
            "unrelated_drift": 0,
            "var_verdict": plan["var_verdict"],
            "recovery": {"candidate_was_adopted": candidate_was_adopted},
            "replay": {"writes": 1, "status": "initial_write"},
        },
        "receipt_sha256",
    )
    write_json_atomic(receipt_path, receipt)
    terminal_journal = seal_evidence(
        {
            **{key: value for key, value in journal.items() if key != "journal_sha256"},
            "state": "receipt_sealed",
            "candidate_obs_uid": str(candidate.uid),
            "receipt_sha256": receipt["receipt_sha256"],
            "terminal_at": utc_now(),
        },
        "journal_sha256",
    )
    write_json_atomic(journal_path, terminal_journal)
    emit_heartbeat("terminal", 2, EXPECTED_OBS_ROWS, EXPECTED_OBS_ROWS)
    print("WRITE_RESULT " + json.dumps(receipt, sort_keys=True), flush=True)
    return receipt


def run_verify(run_root: Path) -> dict[str, Any]:
    require_eu_worker()
    plan = json.loads((run_root / "plan.json").read_text(encoding="utf-8"))
    validate_plan_integrity(plan)
    receipt_path = run_root / "write_receipt.json"
    if not receipt_path.exists():
        raise RuntimeError("write receipt is missing")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    ln = connect_pertdata()
    _live, readback = validate_live_receipt(
        ln=ln,
        plan=plan,
        receipt=receipt,
        run_root=run_root,
        readback_name=f"verify_fresh_{int(time.time())}.parquet",
    )
    report = seal_evidence(
        {
            "format": "pert-gym.broad-prism-obs-verify.v1",
            "task_id": TASK_ID,
            "verified_at": utc_now(),
            "candidate_obs_identity": receipt["candidate_obs_identity"],
            "preserved_x_uid": receipt["preserved_x_uid"],
            "preserved_var_uid": receipt["preserved_var_uid"],
            "fresh_remote_readback": readback,
            "row_order_mismatch": 0,
            "source_join_mismatch": 0,
            "collection_count_delta": 0,
            "unrelated_drift": 0,
            "var_verdict": plan["var_verdict"],
            "collection_registry_sha256": canonical_json_sha256(
                {"collections": receipt["post_registry"]["collections"]}
            ),
            "artifact_registry_sha256": canonical_json_sha256(
                {"artifacts": receipt["post_registry"]["artifacts"]}
            ),
        },
        "verify_sha256",
    )
    write_json_atomic(run_root / "verify_report.json", report)
    print("VERIFY_RESULT " + json.dumps(report, sort_keys=True), flush=True)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("plan", "write", "verify"), required=True)
    parser.add_argument("--run-root", type=Path, default=RUN_ROOT_DEFAULT)
    parser.add_argument("--authorization", type=Path)
    args = parser.parse_args()
    if args.mode == "write" and args.authorization is None:
        parser.error("--authorization is required for write mode")
    return args


def main() -> int:
    args = parse_args()
    if args.mode == "plan":
        run_plan(args.run_root)
    elif args.mode == "write":
        run_write(args.run_root, args.authorization)
    else:
        run_verify(args.run_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
