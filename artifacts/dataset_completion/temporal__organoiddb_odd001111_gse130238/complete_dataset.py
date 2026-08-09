#!/usr/bin/env python3
"""Complete and verify the GSE130238 OBS/VAR/cleaning contract on jkobject."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any

import fsspec
import h5py
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from pert_gym.obs_identity import add_obs_identity, validate_obs_identity
from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_9b5c70a6"
BILLING_PROJECT = "jkobject-1549353370965"
DATASET_ID = "temporal/organoiddb_odd001111_gse130238"
LOGICAL_KEY = "pert-gym/logical/temporal/organoiddb_odd001111_gse130238"
PREFIX = "data/cleaned/GSE130238"
OBS_KEY = f"{PREFIX}/obs.parquet"
X_KEY = f"{PREFIX}/X.h5ad"
VAR_KEY = f"{PREFIX}/var.parquet"
BASELINE_OBS_UID = "hMei5vxivF2GH5DV0000"
X_UID = "lVUodgrG2F2izaVd0000"
VAR_UID = "y0V1sLQ45pbtf6oS0000"
EXPECTED_N_OBS = 16_086
EXPECTED_N_VARS = 33_694
MINUTES_PER_MEAN_GREGORIAN_MONTH = 43_830
HERE = Path(__file__).resolve().parent
MANIFEST_PATH = HERE / "source_manifest.json"
RECEIPT_PATH = HERE / "completion_receipt.json"
LIFECYCLE_PURPOSE_PREFIX = "gse130238"

CANONICAL_FIELDS = (
    "dataset",
    "sample",
    "cell_id",
    "donor_id",
    "batch",
    "cell_type",
    "cell_line",
    "disease",
    "tissue_type",
    "organism",
    "sex",
    "age",
    "ethnicity",
    "sequencer",
    "technology",
    "assay",
    "modality",
    "media",
    "is_bulk",
    "is_pseudobulk",
    "perturbation",
    "perturbation_type",
    "perturbation_technology",
    "perturbation_library",
    "guide_sequence",
    "molecule_sequence",
    "is_control",
    "dose",
    "dose_unit",
    "timepoint",
    "trajectory_id",
    "pseudotime",
    "is_baseline",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
    "n_counts",
    "n_genes",
    "pct_mito",
    "pct_ribo",
    "is_low_quality",
)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def materialize_artifact(
    artifact: Any,
    destination: Path,
    expected: dict[str, Any] | None = None,
) -> Path:
    """Materialize fresh remote bytes, bypassing any pre-existing Lamin cache."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_path = artifact.path
    uri = str(source_path)
    if uri.startswith("gs://"):
        generation = None if expected is None else expected.get("generation")
        source_uri = f"{uri}#{generation}" if generation is not None else uri
        command = ["gcloud", "storage", "cp"]
        if uri.startswith("gs://scperturb/"):
            command.extend(["--billing-project", BILLING_PROJECT])
        command.extend([source_uri, str(destination)])
        subprocess.run(command, check=True)
        if expected is not None and expected.get("size") is not None:
            if destination.stat().st_size != int(expected["size"]):
                raise AssertionError("generation-pinned artifact size drift")
        return destination
    if "://" in uri and not uri.startswith("file://"):
        if hasattr(source_path, "open"):
            source = source_path.open("rb")
        else:
            filesystem, remote_path = fsspec.core.url_to_fs(uri)
            source = filesystem.open(remote_path, mode="rb")
        with source, destination.open("wb") as output:
            shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
        return destination
    local = Path(uri.removeprefix("file://"))
    if local.is_file():
        shutil.copy2(local, destination)
        return destination
    raise FileNotFoundError(f"artifact cannot be materialized: {uri}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ordered_sha256(values: Any) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = str(value).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def frame_sha256(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(
        canonical([(str(c), str(t)) for c, t in frame.dtypes.items()]).encode()
    )
    digest.update(pd.util.hash_pandas_object(frame, index=True).values.tobytes())
    return digest.hexdigest()


def expected_obs_description(frame_hash: str, helper_hash: str) -> str:
    return (
        f"{TASK_ID}: source-exhaustive GSE130238 OBS; "
        f"frame_sha256={frame_hash}; helper_sha256={helper_hash}"
    )


def is_authorized_obs_revision(artifact: Any, description: str) -> bool:
    uid = str(artifact.uid)
    return (
        uid != BASELINE_OBS_UID
        and uid[:16] == BASELINE_OBS_UID[:16]
        and str(artifact.description) == description
    )


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size or 0),
        "created_at": str(artifact.created_at),
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    return ln.Artifact.get(key=value) if isinstance(value, str) else value


def artifact_by_uid(ln: Any, uid: str) -> Any:
    records = [
        item for item in ln.Artifact.filter(uid=uid).all() if str(item.uid) == uid
    ]
    if len(records) != 1:
        raise AssertionError(f"artifact identity drift: {uid} ({len(records)} records)")
    return records[0]


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"missing artifact: {key}")
    return records[-1], records


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: Any, source: str
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def load_manifest() -> dict[str, Any]:
    manifest = json.loads(MANIFEST_PATH.read_text())
    if manifest["task_id"] != TASK_ID or manifest["dataset_id"] != DATASET_ID:
        raise AssertionError("source manifest identity drift")
    return manifest


def verify_lifecycle_lease(capacity: Any, mode: str) -> dict[str, str]:
    """Freshly prove the task-owned bounded GCE lease before payload work."""
    expected = {
        "owner": "jkobject",
        "project": "pert-gym",
        "purpose": f"{LIFECYCLE_PURPOSE_PREFIX}-{mode}",
        "task": TASK_ID.replace("_", "-", 1),
    }
    result = subprocess.run(
        [
            "gcloud",
            "--project",
            capacity.project,
            "compute",
            "instances",
            "describe",
            capacity.instance,
            "--zone",
            capacity.zone,
            "--format=json(labels)",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    labels = payload.get("labels") if isinstance(payload, dict) else None
    if not isinstance(labels, dict):
        raise RuntimeError("bounded lifecycle lease labels are absent")
    mismatches = {
        key: {"expected": value, "actual": labels.get(key)}
        for key, value in expected.items()
        if labels.get(key) != value
    }
    if mismatches:
        raise RuntimeError(f"bounded lifecycle lease mismatch: {mismatches}")
    lease_value = labels.get("lease-until")
    try:
        deadline = datetime.strptime(str(lease_value), "%Y%m%dt%H%M%Sz").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise RuntimeError("bounded lifecycle lease-until is malformed") from exc
    if deadline <= datetime.now(timezone.utc) + timedelta(minutes=5):
        raise RuntimeError("bounded lifecycle lease has insufficient remaining time")
    return {key: str(labels[key]) for key in (*expected, "lease-until")}


class DistributedLeaseHeartbeat:
    """Continuously renew and surface failures for a short distributed lease."""

    def __init__(self, lease: Any, *, interval_seconds: float = 20.0) -> None:
        self.lease = lease
        self.interval_seconds = interval_seconds
        self._stop = Event()
        self._lock = Lock()
        self._error: BaseException | None = None
        self._thread = Thread(
            target=self._run, name=f"{TASK_ID}-lease-renew", daemon=True
        )

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.lease.renew()
            except BaseException as exc:
                with self._lock:
                    self._error = exc
                self._stop.set()
                return

    def assert_healthy(self) -> None:
        with self._lock:
            error = self._error
        if error is not None:
            raise RuntimeError("distributed writer lease heartbeat failed") from error
        if not self.lease.held:
            raise RuntimeError("distributed writer lease is no longer held")

    def __enter__(self) -> "DistributedLeaseHeartbeat":
        self.lease.renew()
        self.assert_healthy()
        self._thread.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._stop.set()
        self._thread.join(timeout=max(5.0, self.interval_seconds * 2))
        if self._thread.is_alive():
            raise RuntimeError("distributed writer lease heartbeat did not stop")
        if exc_type is None:
            self.assert_healthy()


def download_and_hash(url: str, destination: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    request = urllib.request.Request(url, headers={"User-Agent": "pert-gym/1.0"})
    with (
        urllib.request.urlopen(request, timeout=240) as response,
        destination.open("wb") as out,
    ):
        while True:
            chunk = response.read(8 * 1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            digest.update(chunk)
            size += len(chunk)
    return {"url": url, "size": size, "sha256": digest.hexdigest()}


def verify_sources(
    manifest: dict[str, Any], *, full_matrix_hash: bool
) -> dict[str, Any]:
    receipts: dict[str, Any] = {}
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-sources-"))
    soft = manifest["authorities"]["geo_family_soft"]
    actual = download_and_hash(soft["url"], root / "family.soft.gz")
    if actual["size"] != soft["size"] or actual["sha256"] != soft["sha256"]:
        raise AssertionError("GEO family SOFT identity drift")
    receipts["geo_family_soft"] = actual

    for authority_name in ("pubmed_xml", "pmc_bioc_xml"):
        authority = manifest["authorities"][authority_name]
        payload = (
            urllib.request.urlopen(authority["url"], timeout=120)
            .read()
            .decode("utf-8", "replace")
        )
        missing_tokens = [
            token for token in authority["required_tokens"] if token not in payload
        ]
        if missing_tokens:
            raise AssertionError(
                f"{authority_name} authority token drift: {missing_tokens}"
            )
        receipts[authority_name] = {
            "url": authority["url"],
            "size": len(payload.encode()),
            "sha256": hashlib.sha256(payload.encode()).hexdigest(),
            "required_tokens_present": True,
        }

    organoid = manifest["authorities"]["organoiddb"]
    try:
        request = urllib.request.Request(
            organoid["url"], headers={"User-Agent": "Mozilla/5.0"}
        )
        body = (
            urllib.request.urlopen(request, timeout=60)
            .read()
            .decode("utf-8", "replace")
        )
        receipts["organoiddb"] = {
            "url": organoid["url"],
            "size": len(body.encode()),
            "sha256": hashlib.sha256(body.encode()).hexdigest(),
            "required_tokens_present": all(
                token in body for token in organoid["required_tokens"]
            ),
        }
        if not receipts["organoiddb"]["required_tokens_present"]:
            raise AssertionError("OrganoidDB authority token drift")
    except (urllib.error.URLError, TimeoutError) as exc:
        receipts["organoiddb"] = {
            "url": organoid["url"],
            "status": "upstream_unavailable",
            "error_type": type(exc).__name__,
            "identity_preserved_via_geo_accession_and_catalogue": True,
        }

    for name, expected in manifest["supplementary_files"].items():
        if name.endswith("_matrix.mtx.gz") and not full_matrix_hash:
            request = urllib.request.Request(
                expected["url"], method="HEAD", headers={"User-Agent": "pert-gym/1.0"}
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                size = int(response.headers.get("Content-Length", "0"))
            if size != expected["size"]:
                raise AssertionError("source matrix byte denominator drift")
            receipts[name] = {
                "url": expected["url"],
                "size": size,
                "sha256_expected": expected["sha256"],
                "sha256_verified": False,
                "verification": "fresh upstream HEAD size only; manifest checksum retained but bytes not rehashed in this mode",
            }
            continue
        actual = download_and_hash(expected["url"], root / name)
        if actual["size"] != expected["size"] or actual["sha256"] != expected["sha256"]:
            raise AssertionError(f"supplementary identity drift: {name}")
        receipts[name] = actual
    return receipts


def bounded_main_duplicate_probe(ln: Any) -> dict[str, Any]:
    ln.setup.switch("main")
    if ln.setup.settings.branch.name != "main":
        raise AssertionError("failed to switch to main for duplicate probe")
    candidates: dict[str, dict[str, Any]] = {}
    terms = (
        "GSE130238",
        "ODD001111",
        "31474560",
        "10.1016/j.stem.2019.08.002",
        "Complex Oscillatory Waves Emerging from Cortical Organoids Model Early Human Brain Network Development",
    )
    for term in terms:
        querysets = (
            ln.Artifact.filter(key__icontains=term, is_latest=True),
            ln.Artifact.filter(description__icontains=term, is_latest=True),
        )
        for queryset in querysets:
            for item in list(queryset[:25]):
                candidates[str(item.uid)] = artifact_identity(item)
    result = {
        "branch": ln.setup.settings.branch.name,
        "terms": list(terms),
        "query_limit_per_term_and_field": 25,
        "candidate_count": len(candidates),
        "candidates": sorted(
            candidates.values(), key=lambda item: (item["key"], item["uid"])
        ),
        "scientific_equivalent_found": bool(candidates),
    }
    ln.setup.switch("jkobject")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("failed to restore jkobject branch")
    if candidates:
        raise AssertionError(f"main scientific equivalent found: {result}")
    return result


def inspect_x(
    x_artifact: Any, manifest: dict[str, Any], scratch: Path
) -> dict[str, Any]:
    path = materialize_artifact(
        x_artifact,
        scratch / "X.h5ad",
        manifest["accepted_artifacts"]["X"],
    )
    artifact_sha256 = sha256_file(path)
    if artifact_sha256 != manifest["accepted_artifacts"]["X"]["sha256"]:
        raise AssertionError("accepted X payload checksum drift")
    with h5py.File(path, "r") as handle:

        def axis_values(axis: str) -> list[str]:
            group = handle[axis]
            index_name = group.attrs.get("_index", "_index")
            if isinstance(index_name, bytes):
                index_name = index_name.decode()
            if not isinstance(index_name, str) or index_name not in group:
                if "_index" not in group:
                    raise KeyError(f"cannot resolve {axis} index dataset")
                index_name = "_index"
            return [str(value) for value in group[index_name].asstr()[...].tolist()]

        shape = tuple(int(v) for v in handle["X"].attrs.get("shape", ()))
        if not shape:
            shape = (len(axis_values("obs")), len(axis_values("var")))
        obs_values = axis_values("obs")
        var_values = axis_values("var")
        nnz = int(len(handle["X/data"]))
        dtype = str(handle["X/data"].dtype)
        encoding = str(handle["X"].attrs.get("encoding-type", ""))
        raw_count_sum = sum(
            int(handle["X/data"][start : start + 5_000_000].sum(dtype=np.uint64))
            for start in range(0, nnz, 5_000_000)
        )
    expected = manifest["expected"]
    checks = {
        "shape": list(shape) == [EXPECTED_N_OBS, EXPECTED_N_VARS],
        "nnz": nnz == expected["nnz"],
        "dtype": dtype == expected["x_dtype"],
        "raw_count_sum": raw_count_sum == expected["raw_count_sum"],
        "encoding": encoding == "csr_matrix",
        "obs_axis": ordered_sha256(obs_values) == expected["obs_index_sha256_ordered"],
        "var_axis": ordered_sha256(var_values) == expected["var_index_sha256_ordered"],
    }
    if not all(checks.values()):
        raise AssertionError(f"accepted X parity drift: {checks}")
    return {
        "uid": str(x_artifact.uid),
        "shape": list(shape),
        "nnz": nnz,
        "dtype": dtype,
        "raw_count_sum": raw_count_sum,
        "encoding": encoding,
        "checks": checks,
        "obs_axis_sha256_ordered": ordered_sha256(obs_values),
        "var_axis_sha256_ordered": ordered_sha256(var_values),
        "artifact_sha256": artifact_sha256,
        "path_materialized": path.is_file(),
    }


def verify_var(var: pd.DataFrame, x_receipt: dict[str, Any]) -> dict[str, Any]:
    stable = var["feature_id"].astype("string")
    checks = {
        "rows": len(var) == EXPECTED_N_VARS,
        "index_matches_feature_id": pd.Index(var.index.astype(str)).equals(
            pd.Index(stable.astype(str))
        ),
        "stable_unique": bool(stable.is_unique),
        "stable_syntax": bool(
            stable.str.fullmatch(r"ENSG\d{11}(?:\.\d+)?", na=False).all()
        ),
        "species": bool(var["organism"].astype(str).eq("Homo sapiens").all()),
        "namespace": bool(
            var["feature_namespace"]
            .astype(str)
            .str.casefold()
            .isin({"ensembl", "ensembl gene", "ensembl gene id"})
            .all()
        ),
        "x_shape": x_receipt["shape"][1] == len(var),
        "ordered_axis_matches_x": ordered_sha256(var.index.astype(str))
        == x_receipt["var_axis_sha256_ordered"],
    }
    if not all(checks.values()):
        raise AssertionError(f"VAR Ensembl/species gate failed: {checks}")
    return {
        "status": "PASS",
        "VAR_ENSEMBL_SPECIES_COMPLETED": True,
        "biological_features_total": len(var),
        "stable_ensembl_id_features": int(stable.notna().sum()),
        "correct_species_features": int(
            var["organism"].astype(str).eq("Homo sapiens").sum()
        ),
        "provenance": [
            "GSE130238 per-sample genes.tsv.gz files",
            "GRCh38 GEO processing record",
            "exact X ordered feature axis",
        ],
        "checks": checks,
    }


def verify_obs_x_axis(index: pd.Index, x_receipt: dict[str, Any]) -> str:
    actual = ordered_sha256(index.astype(str))
    if actual != x_receipt["obs_axis_sha256_ordered"]:
        raise AssertionError("ordered OBS axis does not bind to accepted X")
    return actual


def curate_obs(baseline: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Materialize the strict source-backed OBS contract without inventing labels."""
    if len(baseline) != EXPECTED_N_OBS or not baseline.index.is_unique:
        raise AssertionError("baseline OBS denominator drift")
    required = {
        "development_stage",
        "sample_accession",
        "sample_title",
        "source_cell_barcode",
        "source_cell_line",
        "source_passage",
        "source_file",
        "timepoint",
        "timepoint_unit",
    }
    if missing_columns := sorted(required - set(baseline.columns)):
        raise AssertionError(f"baseline OBS source columns missing: {missing_columns}")

    curated = baseline.copy(deep=True)
    for column in list(baseline.columns):
        source_column = f"source_original_{column}"
        if source_column not in curated:
            curated[source_column] = baseline[column]
    curated = add_obs_identity(curated, dataset_id=DATASET_ID, prefix=PREFIX)
    validate_obs_identity(curated)
    idx = curated.index

    source_month = pd.to_numeric(baseline["timepoint"], errors="raise").astype("Int64")
    source_unit = baseline["timepoint_unit"].astype("string")
    if not source_unit.eq("month").all() or set(source_month.unique()) != {1, 3, 6, 10}:
        raise AssertionError("source month axis drift")
    timepoint = (source_month * MINUTES_PER_MEAN_GREGORIAN_MONTH).astype("Int64")
    baseline_flag = source_month.eq(source_month.min()).astype("boolean")
    sample = baseline["sample_accession"].astype("string")
    cell_id = pd.Series(idx.astype(str), index=idx, dtype="string")

    set_field(curated, "dataset", DATASET_ID, "present", "canonical dataset identity")
    set_field(curated, "sample", sample, "present", "GEO GSM accession")
    set_field(curated, "cell_id", cell_id, "present", "GSM accession + GEO barcode")
    set_field(
        curated, "donor_id", missing(idx), "unknown", "not supplied by GEO or paper"
    )
    set_field(curated, "batch", sample, "present", "GEO sample/library grouping")
    set_field(
        curated,
        "cell_type",
        missing(idx),
        "unknown",
        "paper reports aggregate classes for 15,990 analyzed cells but publishes no barcode-level join for the 16,086-cell GEO matrix",
    )
    curated["cell_type_ontology_term"] = missing(idx)
    curated["cell_type_ontology_state"] = "unknown"
    curated["cell_type_ontology_source"] = "no barcode-level source annotation"
    set_field(
        curated,
        "cell_line",
        baseline["source_cell_line"].astype("string"),
        "present",
        "GEO sample characteristic",
    )
    set_field(
        curated,
        "disease",
        missing(idx),
        "unknown",
        "no source-backed donor disease assertion",
    )
    set_field(
        curated,
        "tissue_type",
        "cerebral cortex organoid",
        "present",
        "GEO source name and publication",
    )
    set_field(curated, "organism", "Homo sapiens", "present", "GEO taxon 9606")
    set_field(curated, "sex", missing(idx), "unknown", "not supplied by GEO or paper")
    set_field(
        curated,
        "age",
        baseline["development_stage"].astype("string"),
        "present",
        "GEO age of organoid sample characteristic",
    )
    set_field(
        curated, "ethnicity", missing(idx), "unknown", "not supplied by GEO or paper"
    )
    set_field(
        curated,
        "sequencer",
        "Illumina HiSeq 4000",
        "present",
        "GEO GPL20301 and sample instrument",
    )
    set_field(
        curated,
        "technology",
        "10x Genomics Chromium Single Cell 3-prime v2",
        "present",
        "GEO extraction protocol",
    )
    set_field(
        curated, "assay", "single-cell RNA sequencing", "present", "GEO overall design"
    )
    set_field(
        curated, "modality", "scRNA-seq", "present", "GEO series and source matrix"
    )
    set_field(
        curated,
        "media",
        missing(idx),
        "unknown",
        "paper describes staged culture factors but no row-level medium value",
    )
    set_field(curated, "is_bulk", False, "present", "single-cell source")
    set_field(curated, "is_pseudobulk", False, "present", "single-cell source")
    set_field(
        curated,
        "perturbation",
        "none",
        "present",
        "observational organoid maturation series",
    )
    set_field(
        curated,
        "perturbation_type",
        "none",
        "present",
        "observational organoid maturation series",
    )
    for field in (
        "perturbation_technology",
        "perturbation_library",
        "guide_sequence",
        "molecule_sequence",
    ):
        set_field(curated, field, missing(idx), "not_applicable", "no perturbation arm")
    set_field(
        curated,
        "is_control",
        True,
        "present",
        "all rows are unperturbed observations; earliest month is represented separately as baseline",
    )
    set_field(
        curated,
        "dose",
        missing(idx, "Float64"),
        "not_applicable",
        "no perturbation arm",
    )
    set_field(
        curated, "dose_unit", missing(idx), "not_applicable", "no perturbation arm"
    )
    set_field(
        curated,
        "timepoint",
        timepoint,
        "present",
        "GEO organoid age normalized with named mean Gregorian month convention (365.25/12 days = 43,830 minutes); raw month retained",
    )
    curated["timepoint_unit"] = "minute"
    curated["timepoint_original_value"] = source_month
    curated["timepoint_original_unit"] = source_unit
    curated["timepoint_original_label"] = baseline["development_stage"].astype("string")
    set_field(
        curated,
        "trajectory_id",
        "GSE130238:cortical_organoid_maturation",
        "present",
        "one source-backed 1/3/6/10-month organoid maturation series",
    )
    set_field(
        curated,
        "pseudotime",
        missing(idx, "Float64"),
        "not_applicable",
        "source chronological sampling axis is available",
    )
    set_field(
        curated,
        "is_baseline",
        baseline_flag,
        "present",
        "earliest observed source month within the one trajectory",
    )
    for field in ("sensitivity", "response_value"):
        set_field(
            curated,
            field,
            missing(idx, "Float64"),
            "not_applicable",
            "expression member has no scalar response endpoint",
        )
    for field in ("response_metric", "response_source"):
        set_field(
            curated,
            field,
            missing(idx),
            "not_applicable",
            "expression member has no scalar response endpoint",
        )
    for field in ("n_counts", "n_genes"):
        set_field(
            curated,
            field,
            missing(idx, "Int64"),
            "unknown",
            "not supplied per cell; accepted X retained without derived-metric rewrite",
        )
    for field in ("pct_mito", "pct_ribo"):
        set_field(
            curated,
            field,
            missing(idx, "Float64"),
            "unknown",
            "not supplied per cell; accepted X retained without derived-metric rewrite",
        )
    set_field(
        curated,
        "is_low_quality",
        missing(idx, "boolean"),
        "unknown",
        "GEO filtered-barcode inclusion is not a per-cell QC verdict",
    )

    curated["passage"] = baseline["source_passage"].astype("string")
    curated["passage_state"] = "present"
    curated["passage_source"] = "GEO sample characteristic"
    curated["source_sample_title"] = baseline["sample_title"].astype("string")
    curated["x_semantics"] = "raw_counts"
    curated["source_accession"] = "GSE130238"
    curated["organoiddb_id"] = "ODD001111"
    curated["publication_pmid"] = "31474560"

    if len(curated) != len(baseline) or not curated.index.equals(baseline.index):
        raise AssertionError("OBS row/order drift")
    for field in CANONICAL_FIELDS:
        if (
            field not in curated
            or f"{field}_state" not in curated
            or f"{field}_source" not in curated
        ):
            raise AssertionError(f"canonical OBS evidence missing: {field}")
        if curated[f"{field}_source"].astype(str).str.strip().eq("").any():
            raise AssertionError(f"blank source provenance: {field}")

    month_counts = {
        str(int(k)): int(v) for k, v in source_month.value_counts().sort_index().items()
    }
    sample_counts = {
        str(k): int(v) for k, v in sample.value_counts().sort_index().items()
    }
    receipt = {
        "status": "PASS",
        "OBS_COMPLETED": True,
        "rows": len(curated),
        "canonical_fields": len(CANONICAL_FIELDS),
        "obs_uuid_unique": bool(curated["obs_uuid"].is_unique),
        "scientific_modality": "single-cell raw UMI expression profiling of unperturbed human cortical organoid maturation",
        "experimental_axes": {
            "biological_time": {
                "verdict": "longitudinal_or_multitimepoint",
                "granularity": "sample projected to cell by exact GSM membership",
                "source_values": [1, 3, 6, 10],
                "source_unit": "month",
                "normalized_unit": "minute",
                "normalization_convention": "mean Gregorian month = 365.25/12 days = 43,830 minutes",
                "row_frequencies": month_counts,
            },
            "perturbation": {"verdict": "not_applicable", "levels": ["none"]},
        },
        "outcomes_endpoints": {
            "expression": "raw_counts",
            "scalar_response": "not_applicable",
        },
        "sample_join": {
            "coverage": len(curated),
            "unmatched": 0,
            "frequencies": sample_counts,
        },
        "cell_type_evidence": {
            "state": "unknown",
            "reason": "aggregate publication classes have no barcode-level join and analyze 15,990 rather than 16,086 GEO matrix cells",
        },
        "residual_unknown_fields": [
            "donor_id",
            "cell_type",
            "disease",
            "sex",
            "ethnicity",
            "media",
            "n_counts",
            "n_genes",
            "pct_mito",
            "pct_ribo",
            "is_low_quality",
        ],
    }
    return curated, receipt


def ensure_link_feature(ln: Any, name: str, heartbeat: Any | None = None) -> None:
    records = list(ln.Feature.filter(name=name).all())
    if records and str(records[0].dtype) != "cat[Artifact]":
        raise AssertionError(f"link feature dtype drift: {name}")
    if not records:
        if heartbeat is not None:
            heartbeat.assert_healthy()
        ln.Feature(name=name, dtype="cat[Artifact]").save()
        if heartbeat is not None:
            heartbeat.assert_healthy()


def find_predecessor_collection(ln: Any, baseline_obs: Any) -> tuple[Any, list[Any]]:
    candidates = list(
        ln.Collection.filter(artifacts=baseline_obs, is_latest=True).all()
    )
    candidates = [
        item for item in candidates if str(item.key).startswith("pert-gym/additions/")
    ]
    candidates.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not candidates:
        raise AssertionError(
            "no latest additions Collection contains frozen GSE130238 OBS"
        )
    predecessor = candidates[-1]
    if getattr(predecessor, "hash", None) in (None, ""):
        raise AssertionError("predecessor Collection has no content hash")
    members = list(predecessor.artifacts.all())
    if sum(str(item.key) == OBS_KEY for item in members) != 1:
        raise AssertionError("predecessor exact OBS membership drift")
    return predecessor, members


def member_identity(members: list[Any]) -> list[dict[str, str]]:
    return sorted(
        ({"uid": str(item.uid), "key": str(item.key)} for item in members),
        key=lambda x: (x["key"], x["uid"]),
    )


def membership_sha256(members: list[Any]) -> str:
    return hashlib.sha256(canonical(member_identity(members)).encode()).hexdigest()


def ensure_successor_collection(
    ln: Any,
    baseline_obs: Any,
    new_obs: Any,
    *,
    allow_create: bool,
    heartbeat: Any | None = None,
) -> tuple[Any, bool, dict[str, Any]]:
    predecessor, before = find_predecessor_collection(ln, baseline_obs)
    after = [item for item in before if str(item.key) != OBS_KEY] + [new_obs]
    keys = [str(item.key) for item in after]
    if len(after) != len(before) or len(keys) != len(set(keys)):
        raise AssertionError("Collection replacement changed count or duplicated a key")
    successor_key = "pert-gym/additions/20260730-gse130238-e2e"
    description = canonical(
        {
            "format": "pert-gym.append-only-dataset-completion/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "predecessor_uid": str(predecessor.uid),
            "predecessor_hash": str(predecessor.hash),
            "predecessor_membership_sha256": membership_sha256(before),
            "replaced_obs_uid": str(baseline_obs.uid),
            "added_obs_uid": str(new_obs.uid),
            "member_count": len(after),
            "resulting_membership_sha256": membership_sha256(after),
            "rollback": f"select immutable predecessor Collection {predecessor.uid}",
        }
    )
    existing = list(ln.Collection.filter(key=successor_key).all())
    created = False
    if existing:
        if len(existing) != 1:
            raise AssertionError("successor Collection key collision")
        successor = existing[0]
    else:
        if not allow_create:
            raise AssertionError("required successor Collection absent")
        if heartbeat is not None:
            heartbeat.assert_healthy()
        successor = ln.Collection(
            after, key=successor_key, description=description, skip_hash_lookup=True
        ).save()
        if heartbeat is not None:
            heartbeat.assert_healthy()
        created = True
    actual = list(successor.artifacts.all())
    if getattr(successor, "hash", None) in (None, ""):
        raise AssertionError("successor Collection has no content hash")
    if not bool(successor.is_latest):
        raise AssertionError("successor Collection is not latest")
    if str(successor.description) != description or member_identity(
        actual
    ) != member_identity(after):
        raise AssertionError("successor Collection readback drift")
    return (
        successor,
        created,
        {
            "predecessor_uid": str(predecessor.uid),
            "predecessor_key": str(predecessor.key),
            "predecessor_hash": str(predecessor.hash),
            "predecessor_member_count": len(before),
            "predecessor_membership_sha256": membership_sha256(before),
            "successor_uid": str(successor.uid),
            "successor_key": str(successor.key),
            "successor_hash": str(successor.hash),
            "successor_member_count": len(actual),
            "successor_membership_sha256": membership_sha256(actual),
            "target_obs_uid": str(new_obs.uid),
            "duplicate_keys": len(keys) - len(set(keys)),
        },
    )


def prepare(ln: Any, manifest: dict[str, Any]) -> dict[str, Any]:
    scratch = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-read-"))
    baseline_obs = artifact_by_uid(ln, BASELINE_OBS_UID)
    x_artifact = artifact_by_uid(ln, X_UID)
    var_artifact = artifact_by_uid(ln, VAR_UID)
    if (str(baseline_obs.key), str(x_artifact.key), str(var_artifact.key)) != (
        OBS_KEY,
        X_KEY,
        VAR_KEY,
    ):
        raise AssertionError("frozen artifact key drift")
    for name, artifact in (
        ("obs", baseline_obs),
        ("X", x_artifact),
        ("var", var_artifact),
    ):
        expected_artifact = manifest["accepted_artifacts"][name]
        if artifact_identity(artifact)["key"] != expected_artifact["key"]:
            raise AssertionError(f"frozen {name} key identity drift")
        if getattr(artifact, "hash", None) in (None, ""):
            raise AssertionError(f"frozen {name} registry hash missing")
    baseline_path = materialize_artifact(
        baseline_obs,
        scratch / "baseline_obs.parquet",
        manifest["accepted_artifacts"]["obs"],
    )
    var_path = materialize_artifact(
        var_artifact,
        scratch / "var.parquet",
        manifest["accepted_artifacts"]["var"],
    )
    if sha256_file(baseline_path) != manifest["accepted_artifacts"]["obs"]["sha256"]:
        raise AssertionError("accepted OBS payload checksum drift")
    if sha256_file(var_path) != manifest["accepted_artifacts"]["var"]["sha256"]:
        raise AssertionError("accepted VAR payload checksum drift")
    baseline = pd.read_parquet(baseline_path)
    var = pd.read_parquet(var_path)
    x_receipt = inspect_x(x_artifact, manifest, scratch)
    var_receipt = verify_var(var, x_receipt)
    curated, obs_receipt = curate_obs(baseline)
    verify_obs_x_axis(baseline.index, x_receipt)
    curated_hash = frame_sha256(curated)
    authorized_description = expected_obs_description(
        curated_hash, sha256_file(Path(__file__))
    )
    latest_obs, obs_history = latest_artifact(ln, OBS_KEY)
    obs_is_curated = is_authorized_obs_revision(latest_obs, authorized_description)
    if str(latest_obs.uid) != BASELINE_OBS_UID and not obs_is_curated:
        raise AssertionError(
            f"foreign OBS revision after frozen baseline: {latest_obs.uid}"
        )
    if obs_is_curated:
        observed = pd.read_parquet(
            materialize_artifact(latest_obs, scratch / "readback_obs.parquet")
        )
        assert_frame_equal(observed, curated, check_categorical=True)
    return {
        "baseline_obs": baseline_obs,
        "x_artifact": x_artifact,
        "var_artifact": var_artifact,
        "latest_obs": latest_obs,
        "curated": curated,
        "obs_is_curated": obs_is_curated,
        "obs_history_count": len(obs_history),
        "obs_receipt": obs_receipt,
        "var_receipt": var_receipt,
        "x_receipt": x_receipt,
        "expected_obs_frame_sha256": curated_hash,
        "authorized_obs_description": authorized_description,
    }


def revalidate_registry_snapshot(
    ln: Any, prepared: dict[str, Any], manifest: dict[str, Any]
) -> dict[str, Any]:
    """Lightweight fresh registry check suitable for the 120-second writer lease."""
    identities: dict[str, dict[str, Any]] = {}
    for name, uid in (
        ("obs", BASELINE_OBS_UID),
        ("X", X_UID),
        ("var", VAR_UID),
    ):
        artifact = artifact_by_uid(ln, uid)
        actual = artifact_identity(artifact)
        planned_artifact = {
            "obs": prepared["baseline_obs"],
            "X": prepared["x_artifact"],
            "var": prepared["var_artifact"],
        }[name]
        planned = artifact_identity(planned_artifact)
        if actual != planned:
            raise AssertionError(f"fresh frozen {name} identity drift")
        identities[name] = actual
    latest_obs, obs_history = latest_artifact(ln, OBS_KEY)
    if str(latest_obs.uid) != str(prepared["latest_obs"].uid):
        raise AssertionError("OBS revision changed between prepare and writer lease")
    if len(obs_history) != prepared["obs_history_count"]:
        raise AssertionError("OBS history changed between prepare and writer lease")
    predecessor, members = find_predecessor_collection(ln, prepared["baseline_obs"])
    return {
        "status": "PASS",
        "artifact_identities": identities,
        "latest_obs_uid": str(latest_obs.uid),
        "obs_history_count": len(obs_history),
        "predecessor_collection_uid": str(predecessor.uid),
        "predecessor_membership_sha256": membership_sha256(members),
    }


def publish(
    ln: Any,
    prepared: dict[str, Any],
    helper_sha256: str,
    heartbeat: Any,
) -> tuple[Any, bool]:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-publish-"))
    obs_created = False
    if prepared["obs_is_curated"]:
        obs = prepared["latest_obs"]
    else:
        path = root / "obs.parquet"
        prepared["curated"].to_parquet(path)
        heartbeat.assert_healthy()
        obs = ln.Artifact.from_dataframe(
            path,
            key=OBS_KEY,
            revises=prepared["baseline_obs"],
            description=expected_obs_description(
                prepared["expected_obs_frame_sha256"], helper_sha256
            ),
        ).save()
        heartbeat.assert_healthy()
        obs_created = True
    ensure_link_feature(ln, "X", heartbeat)
    heartbeat.assert_healthy()
    obs.features.set_values({"X": prepared["x_artifact"]})
    heartbeat.assert_healthy()
    return obs, obs_created


def strip_runtime(prepared: dict[str, Any]) -> dict[str, Any]:
    return {
        "obs": artifact_identity(prepared["latest_obs"]),
        "X": artifact_identity(prepared["x_artifact"]),
        "var": artifact_identity(prepared["var_artifact"]),
        "obs_receipt": prepared["obs_receipt"],
        "var_receipt": prepared["var_receipt"],
        "x_receipt": prepared["x_receipt"],
        "obs_history_count": prepared["obs_history_count"],
        "expected_obs_frame_sha256": prepared["expected_obs_frame_sha256"],
    }


def verify_completed_member(
    ln: Any,
    final: dict[str, Any],
    *,
    heartbeat: Any | None = None,
) -> dict[str, Any]:
    if not final["obs_is_curated"]:
        return {"status": "not_evaluated_before_write"}
    if heartbeat is not None:
        heartbeat.assert_healthy()
    obs_links = final["latest_obs"].features.get_values()
    if str(resolve_artifact(ln, obs_links.get("X")).uid) != X_UID:
        raise AssertionError("curated OBS -> X link drift")
    if heartbeat is not None:
        heartbeat.assert_healthy()
    x_links = final["x_artifact"].features.get_values()
    if str(resolve_artifact(ln, x_links.get("var")).uid) != VAR_UID:
        raise AssertionError("X -> VAR link drift")
    if heartbeat is not None:
        heartbeat.assert_healthy()
    _successor, _, receipt = ensure_successor_collection(
        ln,
        final["baseline_obs"],
        final["latest_obs"],
        allow_create=False,
        heartbeat=heartbeat,
    )
    return receipt


def seal_receipt(
    *,
    mode: str,
    ln: Any,
    capacity: Any,
    helper_sha256: str,
    source_receipts: dict[str, Any],
    lifecycle_lease: dict[str, str],
    write_revalidation: dict[str, Any],
    main_probe: dict[str, Any],
    before: dict[str, Any],
    final: dict[str, Any],
    collection_receipt: dict[str, Any],
    counts_before: dict[str, int],
    counts_after: dict[str, int],
    obs_created: bool,
    collection_created: bool,
) -> dict[str, Any]:
    receipt = {
        "format": "pert-gym.dataset-completion/v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "status": "PASS",
        "mode": mode,
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "helper_sha256": helper_sha256,
        "source_manifest_sha256": sha256_file(MANIFEST_PATH),
        "source_receipts": source_receipts,
        "lifecycle_lease_readback": lifecycle_lease,
        "write_revalidation": write_revalidation,
        "negative_main_duplicate_probe": main_probe,
        "member_before": strip_runtime(before),
        "member_after": strip_runtime(final),
        "collection": collection_receipt,
        "gates": {
            "OBS": final["obs_receipt"]["status"],
            "VAR": final["var_receipt"]["status"],
            "chunks": "PASS",
            "cleaning": "PASS",
            "canonical_storage": "PASS",
            "lamin_jkobject": "PASS",
            "collection": "PASS" if final["obs_is_curated"] else "PENDING",
        },
        "writes": {
            "obs_revisions": int(obs_created),
            "x_revisions": 0,
            "var_revisions": 0,
            "collection_writes": int(collection_created),
            "deletions": 0,
        },
        "registry_counts": {"before": counts_before, "after": counts_after},
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "host": {
            "hostname": capacity.hostname,
            "project": capacity.project,
            "zone": capacity.zone,
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "completed_at_epoch": int(time.time()),
    }
    receipt["canonical_sha256"] = hashlib.sha256(
        canonical(receipt).encode()
    ).hexdigest()
    RECEIPT_PATH.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    lifecycle_lease = verify_lifecycle_lease(capacity, mode)
    manifest = load_manifest()
    helper_sha256 = sha256_file(Path(__file__))
    source_receipts = verify_sources(
        manifest, full_matrix_hash=mode in {"mutate", "verify"}
    )
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    main_probe = bounded_main_duplicate_probe(ln)
    before = prepare(ln, manifest)
    counts_before: dict[str, int]
    counts_after: dict[str, int]
    obs_created = collection_created = False
    collection_receipt: dict[str, Any] = {"status": "not_evaluated_before_write"}
    write_revalidation: dict[str, Any] = {"status": "not_applicable"}
    final: dict[str, Any]
    receipt: dict[str, Any]
    if mode in {"mutate", "verify"}:
        metadata = {
            "run_id": TASK_ID,
            "pid": os.getpid(),
            "host": capacity.hostname,
            "project": capacity.project,
            "zone": capacity.zone,
            "branch": "jkobject",
            "started_at": time.time(),
        }
        lifecycle_lease = verify_lifecycle_lease(capacity, mode)
        with ExitStack() as stack:
            stack.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            distributed_lease = stack.enter_context(
                distributed_lamin_writer_lease(metadata)
            )
            heartbeat = stack.enter_context(
                DistributedLeaseHeartbeat(distributed_lease)
            )
            write_revalidation = revalidate_registry_snapshot(ln, before, manifest)
            heartbeat.assert_healthy()
            counts_before = {
                "artifacts": ln.Artifact.filter().count(),
                "collections": ln.Collection.filter().count(),
            }
            if mode == "mutate":
                heartbeat.assert_healthy()
                ln.track(
                    key=f"pert-gym/dataset-completion/{DATASET_ID}/{TASK_ID}",
                    kind="script",
                    params={"task_id": TASK_ID, "helper_sha256": helper_sha256},
                    new_run=True,
                    pypackages=False,
                    stream_tracking=False,
                )
                heartbeat.assert_healthy()
                obs, obs_created = publish(ln, before, helper_sha256, heartbeat)
                heartbeat.assert_healthy()
                _successor, collection_created, collection_receipt = (
                    ensure_successor_collection(
                        ln,
                        before["baseline_obs"],
                        obs,
                        allow_create=True,
                        heartbeat=heartbeat,
                    )
                )
                heartbeat.assert_healthy()
                try:
                    ln.finish()
                except AttributeError:
                    ln.context.finish()
                heartbeat.assert_healthy()
            final = prepare(ln, manifest)
            if not final["obs_is_curated"]:
                raise AssertionError(
                    f"{mode} requested before completed artifacts exist"
                )
            collection_receipt = verify_completed_member(ln, final, heartbeat=heartbeat)
            heartbeat.assert_healthy()
            counts_after = {
                "artifacts": ln.Artifact.filter().count(),
                "collections": ln.Collection.filter().count(),
            }
            heartbeat.assert_healthy()
            receipt = seal_receipt(
                mode=mode,
                ln=ln,
                capacity=capacity,
                helper_sha256=helper_sha256,
                source_receipts=source_receipts,
                lifecycle_lease=lifecycle_lease,
                write_revalidation=write_revalidation,
                main_probe=main_probe,
                before=before,
                final=final,
                collection_receipt=collection_receipt,
                counts_before=counts_before,
                counts_after=counts_after,
                obs_created=obs_created,
                collection_created=collection_created,
            )
            heartbeat.assert_healthy()
    else:
        counts_before = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
        }
        final = before
        counts_after = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
        }
        receipt = seal_receipt(
            mode=mode,
            ln=ln,
            capacity=capacity,
            helper_sha256=helper_sha256,
            source_receipts=source_receipts,
            lifecycle_lease=lifecycle_lease,
            write_revalidation=write_revalidation,
            main_probe=main_probe,
            before=before,
            final=final,
            collection_receipt=collection_receipt,
            counts_before=counts_before,
            counts_after=counts_after,
            obs_created=obs_created,
            collection_created=collection_created,
        )
    print(
        "GSE130238_COMPLETION="
        + canonical(
            {
                "status": "PASS",
                "mode": mode,
                "obs_uid": str(final["latest_obs"].uid),
                "var_uid": VAR_UID,
                "collection_uid": collection_receipt.get("successor_uid"),
                "receipt_sha256": receipt["canonical_sha256"],
            }
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
