#!/usr/bin/env python3
"""Complete and verify GSE107185 / PerturBase extend_61 on Lamin jkobject."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
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
import zarr
from anndata.io import read_elem
from pandas.testing import assert_frame_equal
from scipy import sparse

from pert_gym.obs_identity import add_obs_identity, validate_obs_identity
from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_ab37edf6"
DATASET_ID = "temporal/perturbase_gse107185"
LOGICAL_KEY = "pert-gym/logical/temporal/perturbase_gse107185"
OBS_KEY = f"{LOGICAL_KEY}/obs.parquet"
X_KEY = f"{LOGICAL_KEY}/X.zarr.zip"
VAR_KEY = f"{LOGICAL_KEY}/var.parquet"
BASELINE_OBS_UID = "NMGDnN2AjT75B2lr0000"
X_UID = "cFEWN77vqjaCY1I50000"
BASELINE_VAR_UID = "mlSaS2ZdU1OsMdV30000"
EXPECTED_N_OBS = 8_428
EXPECTED_N_VARS = 2_000
BILLING_PROJECT = "jkobject-1549353370965"
PERTURBATION_LIBRARY = (
    "PerturBase extend_61 component: 60 TF ORFs plus mCherry control; HNF4A absent"
)
SUCCESSOR_COLLECTION_KEY = "pert-gym/additions/20260801-gse107185-review-fix"
SOURCE_GENERATION_URI = (
    "gs://scperturb/pert-gym/staging/pert-gym/logical/temporal/"
    "perturbase_gse107185/revisions/perturbase-gse107185-"
    "20260717T045017Z-8821ce13/source/extend_61.filter.tar.gz#1784263842756819"
)
HERE = Path(__file__).resolve().parent
MANIFEST_PATH = HERE / "source_manifest.json"
SEQUENCES_PATH = HERE / "orf_sequences.json"
HGNC_PATH = HERE / "hgnc_symbol_mappings.json"
RECEIPT_PATH = HERE / "completion_receipt.json"

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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ordered_sha256(values: Any) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = str(value).encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def frame_sha256(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(
        canonical(
            [(str(column), str(dtype)) for column, dtype in frame.dtypes.items()]
        ).encode()
    )
    digest.update(pd.util.hash_pandas_object(frame, index=True).values.tobytes())
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size or 0),
        "created_at": str(artifact.created_at),
    }


def artifact_by_uid(ln: Any, uid: str) -> Any:
    records = [
        item for item in ln.Artifact.filter(uid=uid).all() if str(item.uid) == uid
    ]
    if len(records) != 1:
        raise AssertionError(f"artifact identity drift: {uid} ({len(records)})")
    return records[0]


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"missing artifact: {key}")
    return records[-1], records


def resolve_artifact(ln: Any, value: Any) -> Any:
    return ln.Artifact.get(key=value) if isinstance(value, str) else value


def gcloud_cp(source: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "gcloud",
            "storage",
            "cp",
            "--billing-project",
            BILLING_PROJECT,
            source,
            str(destination),
        ],
        check=True,
    )


def materialize_artifact(
    artifact: Any, destination: Path, expected: dict[str, Any] | None = None
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    uri = str(artifact.path)
    if uri.startswith("gs://"):
        generation = None if expected is None else expected.get("generation")
        gcloud_cp(f"{uri}#{generation}" if generation is not None else uri, destination)
    elif "://" in uri and not uri.startswith("file://"):
        if hasattr(artifact.path, "open"):
            source = artifact.path.open("rb")
        else:
            filesystem, remote_path = fsspec.core.url_to_fs(uri)
            source = filesystem.open(remote_path, mode="rb")
        with source, destination.open("wb") as output:
            shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
    else:
        shutil.copy2(Path(uri.removeprefix("file://")), destination)
    if expected is not None:
        if destination.stat().st_size != int(expected["size"]):
            raise AssertionError("generation-pinned artifact size drift")
        if sha256_file(destination) != expected["sha256"]:
            raise AssertionError("generation-pinned artifact checksum drift")
    return destination


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: Any, source: Any
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def verify_lifecycle_lease(capacity: Any, mode: str) -> dict[str, str]:
    expected = {
        "owner": "jkobject",
        "project": "pert-gym",
        "purpose": f"gse107185-{mode}",
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
    labels = json.loads(result.stdout).get("labels")
    if not isinstance(labels, dict):
        raise RuntimeError("bounded lifecycle lease labels are absent")
    mismatches = {
        key: {"expected": value, "actual": labels.get(key)}
        for key, value in expected.items()
        if labels.get(key) != value
    }
    if mismatches:
        raise RuntimeError(f"bounded lifecycle lease mismatch: {mismatches}")
    try:
        deadline = datetime.strptime(labels["lease-until"], "%Y%m%dt%H%M%Sz").replace(
            tzinfo=timezone.utc
        )
    except (KeyError, ValueError) as exc:
        raise RuntimeError("bounded lifecycle lease-until is malformed") from exc
    if deadline <= datetime.now(timezone.utc) + timedelta(minutes=5):
        raise RuntimeError("bounded lifecycle lease has insufficient remaining time")
    return {key: str(labels[key]) for key in (*expected, "lease-until")}


class DistributedLeaseHeartbeat:
    def __init__(self, lease: Any, *, interval_seconds: float = 20.0) -> None:
        self.lease = lease
        self.interval_seconds = interval_seconds
        self._stop = Event()
        self._lock = Lock()
        self._error: BaseException | None = None
        self._thread = Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.lease.renew()
            except BaseException as exc:
                with self._lock:
                    self._error = exc
                self._stop.set()

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


def authority_request(url: str) -> urllib.request.Request:
    parsed = urllib.parse.urlsplit(url)
    if parsed.hostname == "ftp.ncbi.nlm.nih.gov":
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        query.extend((("tool", "pert-gym"), ("email", "jkobject@gmail.com")))
        url = urllib.parse.urlunsplit(
            parsed._replace(query=urllib.parse.urlencode(query))
        )
    return urllib.request.Request(
        url,
        headers={
            "User-Agent": "pert-gym/1.0 (jkobject@gmail.com)",
            "Accept": "*/*",
        },
    )


def open_authority(
    request: urllib.request.Request,
    *,
    opener: Any = urllib.request.urlopen,
    sleep: Any = time.sleep,
) -> Any:
    for attempt in range(5):
        try:
            return opener(request, timeout=180)
        except urllib.error.HTTPError as exc:
            if exc.code not in {403, 429} or attempt == 4:
                raise
            sleep(5 * (2**attempt))
    raise AssertionError("unreachable authority retry state")


def download_and_hash(url: str, destination: Path) -> dict[str, Any]:
    request = authority_request(url)
    digest = hashlib.sha256()
    size = 0
    with (
        open_authority(request) as response,
        destination.open("wb") as output,
    ):
        while True:
            chunk = response.read(8 * 1024 * 1024)
            if not chunk:
                break
            output.write(chunk)
            digest.update(chunk)
            size += len(chunk)
    return {"url": url, "size": size, "sha256": digest.hexdigest()}


def verify_authorities(manifest: dict[str, Any], scratch: Path) -> dict[str, Any]:
    receipts: dict[str, Any] = {}
    geo = manifest["authorities"]["geo_family_soft"]
    actual = download_and_hash(geo["url"], scratch / "family.soft.gz")
    if actual["size"] != geo["size"] or actual["sha256"] != geo["sha256"]:
        raise AssertionError("GEO family SOFT identity drift")
    receipts["geo_family_soft"] = actual
    for name in ("pubmed_xml", "pmc_bioc_xml"):
        authority = manifest["authorities"][name]
        body = urllib.request.urlopen(authority["url"], timeout=120).read()
        text = body.decode("utf-8", "replace")
        absent = [token for token in authority["required_tokens"] if token not in text]
        if absent:
            raise AssertionError(f"{name} token drift: {absent}")
        receipts[name] = {
            "url": authority["url"],
            "size": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "required_tokens_present": True,
        }
    for name, expected in manifest["geo_metadata_supplements"].items():
        actual = download_and_hash(expected["url"], scratch / name)
        if actual["size"] != expected["size"] or actual["sha256"] != expected["sha256"]:
            raise AssertionError(f"GEO metadata supplement drift: {name}")
        receipts[name] = actual
    for path in (SEQUENCES_PATH, HGNC_PATH, MANIFEST_PATH):
        if not path.is_file():
            raise AssertionError(
                f"required reconstruction artifact absent: {path.name}"
            )
    receipts["publication_supplement_review"] = {
        "files_reviewed": len(manifest["publication_supplements"]),
        "sequence_map_entries": len(load_json(SEQUENCES_PATH)["sequences"]),
        "source_table_s1_sha256": load_json(SEQUENCES_PATH)["source"]["sha256"],
    }
    return receipts


def decode(values: Any) -> list[str]:
    return [
        value.decode() if isinstance(value, bytes) else str(value) for value in values
    ]


def inspect_zarr_x(path: Path, expected: dict[str, Any]) -> dict[str, Any]:
    with zarr.storage.ZipStore(str(path), mode="r") as store:
        root = zarr.group(store=store)
        shape = [int(value) for value in root.attrs["shape"]]
        data = np.asarray(root["data"][:])
        indices = np.asarray(root["indices"][:])
        indptr = np.asarray(root["indptr"][:])
    checks = {
        "shape": shape == [EXPECTED_N_OBS, EXPECTED_N_VARS],
        "nnz": len(data) == expected["nnz"],
        "csr_arrays": len(indices) == len(data) and len(indptr) == EXPECTED_N_OBS + 1,
        "dtype": str(data.dtype) == expected["x_dtype"],
        "sum": bool(
            np.isclose(data.sum(dtype=np.float64), expected["x_sum"], rtol=0, atol=1e-6)
        ),
    }
    if not all(checks.values()):
        raise AssertionError(f"accepted X parity drift: {checks}")
    return {
        "status": "PASS",
        "shape": shape,
        "nnz": len(data),
        "dtype": str(data.dtype),
        "sum": float(data.sum(dtype=np.float64)),
        "axis_contract": "axes are external and bind through explicit obs -> X -> var links",
        "checks": checks,
    }


def inspect_source(
    path: Path,
    baseline: pd.DataFrame,
    var: pd.DataFrame,
    source_spec: dict[str, Any],
) -> dict[str, Any]:
    member_spec = source_spec["archive_member"]
    member_name = str(member_spec["name"])
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        if [item.name for item in members] != [member_name] or not members[0].isfile():
            raise AssertionError("PerturBase source archive member inventory drift")
        if members[0].size != member_spec["size"]:
            raise AssertionError("PerturBase source archive member size drift")
        source = archive.extractfile(members[0])
        if source is None:
            raise AssertionError("PerturBase source archive member unreadable")
        member_path = path.with_name("mixscape_hvg_filter.h5ad")
        with source, member_path.open("wb") as output:
            shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
    if sha256_file(member_path) != member_spec["sha256"]:
        raise AssertionError("PerturBase source archive member hash drift")
    with h5py.File(member_path, "r") as handle:
        member = read_elem(handle)
    if list(member.shape) != [EXPECTED_N_OBS, EXPECTED_N_VARS]:
        raise AssertionError("PerturBase source member shape drift")
    if not member.obs_names.astype(str).equals(baseline.index.astype(str)):
        raise AssertionError("PerturBase source OBS axis drift")
    if not member.var_names.astype(str).equals(var.index.astype(str)):
        raise AssertionError("PerturBase source VAR axis drift")
    for field in ("gene", "batch", "media"):
        if not member.obs[field].astype(str).equals(baseline[field].astype(str)):
            raise AssertionError(f"PerturBase source OBS field drift: {field}")
    nnz = int(member.X.nnz if sparse.issparse(member.X) else np.count_nonzero(member.X))
    total = float(member.X.sum(dtype=np.float64))
    if nnz != source_spec["member_nnz"] or not np.isclose(
        total, source_spec["member_sum"], rtol=0, atol=1e-6
    ):
        raise AssertionError("PerturBase source X metric drift")
    return {
        "status": "PASS",
        "archive_member": member_name,
        "archive_member_sha256": member_spec["sha256"],
        "logical_component": "extend_61",
        "shape": list(member.shape),
        "nnz": nnz,
        "sum": total,
        "obs_axis_sha256_ordered": ordered_sha256(member.obs_names.astype(str)),
        "var_axis_sha256_ordered": ordered_sha256(member.var_names.astype(str)),
        "required_obs_fields_equal": ["gene", "batch", "media"],
    }


def curate_obs(
    baseline: pd.DataFrame, sequence_payload: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    required = {
        "gene",
        "batch",
        "media",
        "cell_id",
        "n_genes",
        "total_counts",
        "pct_counts_mt",
    }
    if missing_columns := sorted(required - set(baseline.columns)):
        raise AssertionError(f"baseline OBS source columns missing: {missing_columns}")
    if len(baseline) != EXPECTED_N_OBS or not baseline.index.is_unique:
        raise AssertionError("baseline OBS denominator drift")
    source_genes = baseline["gene"].astype("string")
    if set(source_genes) != set(sequence_payload["used_by_component"]):
        raise AssertionError("Table S1/source perturbation set drift")
    curated = baseline.copy(deep=True)
    for column in list(baseline.columns):
        original = f"source_original_{column}"
        if original not in curated:
            curated[original] = baseline[column]
    curated = add_obs_identity(curated, dataset_id=DATASET_ID, prefix=LOGICAL_KEY)
    validate_obs_identity(curated)
    idx = curated.index
    source_media = baseline["media"].astype("string")
    source_batch = baseline["batch"].astype("string")
    sample = ("extend_61:" + source_media + ":" + source_batch).astype("string")
    media_map = {
        "hPSC": "mTeSR pluripotent stem cell medium",
        "endothelial": "EGM-2 endothelial growth medium (Lonza)",
        "multilineage": "high-glucose DMEM with 20% FBS multilineage medium",
    }
    minute_map = {
        "hPSC": 5 * 24 * 60,
        "endothelial": 6 * 24 * 60,
        "multilineage": 6 * 24 * 60,
    }
    timepoint = source_media.map(minute_map).astype("Int64")
    is_control = source_genes.eq("CTRL").astype("boolean")
    perturbation = source_genes.map(
        lambda value: (
            "mCherry control ORF" if value == "CTRL" else f"{value} ORF overexpression"
        )
    ).astype("string")
    perturbation_type = source_genes.map(
        lambda value: "control" if value == "CTRL" else "overexpression"
    ).astype("string")
    sequence_map = {
        key: value["sequence"] for key, value in sequence_payload["sequences"].items()
    }
    molecule_sequence = source_genes.map(sequence_map).astype("string")

    set_field(
        curated, "dataset", DATASET_ID, "present", "canonical logical dataset identity"
    )
    set_field(
        curated,
        "sample",
        sample,
        "derived",
        "exact PerturBase component + media + batch grouping; no unsupported GSM projection",
    )
    set_field(
        curated,
        "cell_id",
        pd.Series(idx.astype(str), index=idx, dtype="string"),
        "present",
        "PerturBase extend_61 observation index",
    )
    set_field(
        curated,
        "donor_id",
        missing(idx),
        "unknown",
        "H1 line source donor identifier not reported",
    )
    set_field(curated, "batch", source_batch, "present", "PerturBase extend_61 batch")
    set_field(
        curated,
        "cell_type",
        missing(idx),
        "unknown",
        "source media labels are not barcode-level cell-type annotations",
    )
    curated["cell_type_ontology_term"] = missing(idx)
    curated["cell_type_ontology_state"] = "unknown"
    curated["cell_type_ontology_source"] = (
        "no immutable barcode-level cell-type annotation"
    )
    set_field(
        curated,
        "cell_line",
        "H1 (WA01) human embryonic stem cell",
        "present",
        "paper STAR Methods and Key Resources Table",
    )
    set_field(
        curated,
        "disease",
        missing(idx),
        "unknown",
        "no source-backed disease assertion",
    )
    set_field(
        curated,
        "tissue_type",
        "in vitro cultured human embryonic stem cells",
        "present",
        "paper experimental model",
    )
    set_field(
        curated, "organism", "Homo sapiens", "present", "GEO taxon 9606 and PerturBase"
    )
    set_field(curated, "sex", "male", "present", "paper STAR Methods: H1 hESC (male)")
    set_field(
        curated,
        "age",
        missing(idx),
        "not_applicable",
        "donor age is not a meaningful row-level axis for the H1 cell-line screen",
    )
    set_field(
        curated,
        "ethnicity",
        missing(idx),
        "unknown",
        "not reported by GEO, paper, or PerturBase",
    )
    set_field(
        curated,
        "sequencer",
        "Illumina HiSeq",
        "present",
        "paper Single Cell Library Preparation",
    )
    set_field(
        curated,
        "technology",
        "10x Genomics Chromium Single Cell 3-prime v2",
        "present",
        "paper Key Resources and methods",
    )
    set_field(
        curated,
        "assay",
        "pooled barcoded ORF overexpression single-cell RNA sequencing",
        "present",
        "GEO overall design and paper",
    )
    set_field(curated, "modality", "scRNA-seq", "present", "GEO and PerturBase")
    set_field(
        curated,
        "media",
        source_media.map(media_map).astype("string"),
        "present",
        "PerturBase row media normalized with paper formulations",
    )
    set_field(curated, "is_bulk", False, "present", "single-cell source component")
    set_field(
        curated, "is_pseudobulk", False, "present", "single-cell source component"
    )
    set_field(
        curated,
        "perturbation",
        perturbation,
        "present",
        "PerturBase gene joined to paper Table S1 construct",
    )
    set_field(
        curated,
        "perturbation_type",
        perturbation_type,
        "present",
        "paper TF-Hygro ORF overexpression design",
    )
    set_field(
        curated,
        "perturbation_technology",
        "pooled lentiviral barcoded ORF overexpression (TF-Hygro)",
        "present",
        "paper vector and screen methods",
    )
    set_field(
        curated,
        "perturbation_library",
        PERTURBATION_LIBRARY,
        "present",
        "PerturBase extend_61 membership joined to paper Table S1; HNF4A is excluded",
    )
    set_field(
        curated,
        "guide_sequence",
        missing(idx),
        "not_applicable",
        "ORF overexpression uses no CRISPR guide",
    )
    set_field(
        curated,
        "molecule_sequence",
        molecule_sequence,
        "present",
        "Parekh et al. Table S1 exact ORF insert sequence",
    )
    set_field(
        curated,
        "is_control",
        is_control,
        "present",
        "PerturBase CTRL mapped to paper mCherry control construct",
    )
    set_field(
        curated,
        "dose",
        missing(idx, "Float64"),
        "unknown",
        "viral titer/MOI is not published per cell",
    )
    set_field(
        curated,
        "dose_unit",
        missing(idx),
        "unknown",
        "viral titer/MOI is not published per cell",
    )
    set_field(
        curated,
        "timepoint",
        timepoint,
        "present",
        "paper: harvest at day 5 in hPSC medium and day 6 in EGM/ML; normalized to minutes",
    )
    curated["timepoint_unit"] = "minute"
    curated["timepoint_original_value"] = source_media.map(
        {"hPSC": 5, "endothelial": 6, "multilineage": 6}
    ).astype("Int64")
    curated["timepoint_original_unit"] = "day"
    set_field(
        curated,
        "trajectory_id",
        missing(idx),
        "not_applicable",
        "condition-specific endpoint durations do not form a shared longitudinal trajectory",
    )
    set_field(
        curated,
        "pseudotime",
        missing(idx, "Float64"),
        "not_applicable",
        "no source pseudotime or trajectory",
    )
    set_field(
        curated,
        "is_baseline",
        False,
        "present",
        "all rows are post-transduction endpoints; mCherry is a control arm, not a temporal baseline",
    )
    set_field(
        curated,
        "sensitivity",
        missing(idx, "Float64"),
        "unknown",
        "paper fitness is genotype/sample aggregate and has no cell-level join in extend_61",
    )
    set_field(
        curated,
        "response_metric",
        "genotype-level relative fitness log2 fold-change",
        "present",
        "paper fitness-effect analysis",
    )
    set_field(
        curated,
        "response_value",
        missing(idx, "Float64"),
        "unknown",
        "aggregate fitness values cannot be projected to cells without an exact sample/genotype join",
    )
    set_field(
        curated,
        "response_source",
        "GSE107185 aggregate fitness readout (not joined to this cell row)",
        "present",
        "paper and GEO deposited-data statement",
    )
    set_field(
        curated,
        "n_counts",
        pd.to_numeric(baseline["total_counts"], errors="raise").astype("Float64"),
        "present",
        "PerturBase total_counts",
    )
    set_field(
        curated,
        "n_genes",
        pd.to_numeric(baseline["n_genes"], errors="raise").astype("Int64"),
        "present",
        "PerturBase n_genes",
    )
    set_field(
        curated,
        "pct_mito",
        pd.to_numeric(baseline["pct_counts_mt"], errors="raise").astype("Float64"),
        "present",
        "PerturBase pct_counts_mt",
    )
    set_field(
        curated,
        "pct_ribo",
        missing(idx, "Float64"),
        "unknown",
        "not supplied; 2,000-HVG matrix is unsuitable for unbiased derivation",
    )
    low_quality = (
        (curated["n_genes"] < 200)
        | (curated["n_counts"] <= 0)
        | (curated["pct_mito"] >= 10)
    ).astype("boolean")
    set_field(
        curated,
        "is_low_quality",
        low_quality,
        "derived",
        "project QC envelope: n_genes >= 200, positive counts, pct_mito < 10",
    )
    curated["perturbation_target"] = source_genes.mask(is_control)
    curated["perturbation_target_state"] = np.where(
        is_control, "not_applicable", "present"
    )
    curated["perturbation_target_source"] = (
        "PerturBase gene; CTRL is paper mCherry control"
    )
    curated["source_accession"] = "GSE107185"
    curated["source_bioproject"] = "PRJNA419230"
    curated["source_component"] = "extend_61"
    curated["publication_pmid"] = "30448000"
    curated["publication_doi"] = "10.1016/j.cels.2018.10.008"
    curated["x_semantics"] = (
        "processed PerturBase expression values; raw-count transform not declared"
    )

    if len(curated) != len(baseline) or not curated.index.equals(baseline.index):
        raise AssertionError("OBS row/order drift")
    for field in CANONICAL_FIELDS:
        for suffix in ("", "_state", "_source"):
            if f"{field}{suffix}" not in curated:
                raise AssertionError(f"canonical OBS evidence missing: {field}{suffix}")
        if curated[f"{field}_source"].astype(str).str.strip().eq("").any():
            raise AssertionError(f"blank provenance: {field}")
    if low_quality.any() or molecule_sequence.isna().any():
        raise AssertionError("quality/sequence completion drift")
    receipt = {
        "status": "PASS",
        "OBS_COMPLETED": True,
        "scientific_modality": (
            "pooled ORF-overexpression single-cell RNA-seq expression endpoint"
        ),
        "annotation_level": {
            "expression": "cell",
            "perturbation": "cell via PerturBase construct assignment",
            "fitness": "aggregate genotype/sample only; not joined per cell",
        },
        "rows": len(curated),
        "canonical_fields": len(CANONICAL_FIELDS),
        "obs_uuid_unique": bool(curated["obs_uuid"].is_unique),
        "source_columns_retained": sorted(map(str, baseline.columns)),
        "experimental_axes": {
            "biological_time": {
                "verdict": "endpoint_duration_by_media_not_shared_trajectory",
                "source_days": [5, 6],
                "normalized_minutes": [7200, 8640],
                "row_frequencies": {
                    str(k): int(v)
                    for k, v in timepoint.value_counts().sort_index().items()
                },
            },
            "perturbation": {
                "verdict": "pooled_ORF_overexpression_with_control",
                "constructs": int(source_genes.nunique()),
                "controls": int(is_control.sum()),
                "exact_sequences": int(molecule_sequence.notna().sum()),
            },
            "media": {
                str(k): int(v)
                for k, v in source_media.value_counts().sort_index().items()
            },
        },
        "outcomes_endpoints": {
            "expression": "processed PerturBase single-cell matrix",
            "fitness": "known aggregate endpoint; cell-level value unknown because no exact join",
        },
        "residual_unknown_fields": [
            "donor_id",
            "cell_type",
            "disease",
            "ethnicity",
            "dose",
            "dose_unit",
            "sensitivity",
            "response_value",
            "pct_ribo",
        ],
    }
    return curated, receipt


def curate_var(
    baseline: pd.DataFrame, mappings_payload: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    required = {"gene_symbol", "ENSEMBL", "organism", "feature_namespace"}
    if missing_columns := sorted(required - set(baseline.columns)):
        raise AssertionError(f"baseline VAR source columns missing: {missing_columns}")
    if len(baseline) != EXPECTED_N_VARS or not baseline.index.is_unique:
        raise AssertionError("baseline VAR denominator drift")
    curated = baseline.copy(deep=True)
    for column in list(baseline.columns):
        original = f"source_original_{column}"
        if original not in curated:
            curated[original] = baseline[column]
    source_id = baseline["ENSEMBL"].astype("string")
    mapped_ids = (
        baseline["gene_symbol"]
        .astype(str)
        .map(
            {
                key: value["ensembl_gene_id"]
                for key, value in mappings_payload["mappings"].items()
            }
        )
        .astype("string")
    )
    stable = source_id.fillna(mapped_ids).astype("string")
    mapped = source_id.isna() & stable.notna()
    unknown = stable.isna()
    states = pd.Series(
        np.where(source_id.notna(), "present", np.where(mapped, "derived", "unknown")),
        index=baseline.index,
        dtype="string",
    )
    sources = pd.Series(
        np.where(
            source_id.notna(),
            "PerturBase source ENSEMBL column",
            np.where(
                mapped,
                f"unique HGNC alias/previous-symbol mapping; snapshot sha256={mappings_payload['source']['sha256']}",
                "not uniquely resolved in frozen HGNC review",
            ),
        ),
        index=baseline.index,
        dtype="string",
    )
    approved = baseline["gene_symbol"].astype("string").copy()
    approved_map = {
        key: value["approved_symbol"]
        for key, value in mappings_payload["mappings"].items()
    }
    approved.loc[mapped] = (
        baseline.loc[mapped, "gene_symbol"]
        .astype(str)
        .map(approved_map)
        .astype("string")
    )
    curated["x_axis_feature_label"] = pd.Series(
        baseline.index.astype(str), index=baseline.index, dtype="string"
    )
    curated["stable_feature_id"] = stable
    curated["stable_feature_id_state"] = states
    curated["stable_feature_id_source"] = sources
    curated["gene_id"] = stable
    curated["gene_id_state"] = states
    curated["gene_id_source"] = sources
    curated["approved_gene_symbol"] = approved
    curated["approved_gene_symbol_state"] = np.where(mapped, "derived", "present")
    curated["approved_gene_symbol_source"] = np.where(
        mapped, "frozen HGNC approved symbol", "PerturBase source gene_symbol"
    )
    curated["feature_namespace"] = pd.Series(
        np.where(stable.notna(), "Ensembl Gene ID", pd.NA),
        index=baseline.index,
        dtype="string",
    )
    curated["feature_namespace_state"] = np.where(stable.notna(), "present", "unknown")
    curated["feature_namespace_source"] = sources
    curated["feature_type"] = "gene"
    curated["feature_type_state"] = "present"
    curated["feature_type_source"] = "PerturBase expression feature axis"
    curated["organism"] = "Homo sapiens"
    curated["organism_state"] = "present"
    curated["organism_source"] = "PerturBase and GEO taxon 9606"
    known = stable.dropna().astype(str)
    expected_source_exact = EXPECTED_N_VARS - int(
        mappings_payload["input_unresolved_symbols"]
    )
    expected_recovered = int(mappings_payload["unique_mappings"])
    expected_unknown = len(mappings_payload["residual_unknown_symbols"]) + len(
        mappings_payload["ambiguous_symbols"]
    )
    checks = {
        "rows": len(curated) == EXPECTED_N_VARS,
        "axis_preserved": curated.index.equals(baseline.index),
        "known_stable_syntax": bool(known.str.fullmatch(r"ENSG\d{11}(?:\.\d+)?").all()),
        "known_stable_unique": bool(known.is_unique),
        "species": bool(curated["organism"].eq("Homo sapiens").all()),
        "source_exact": int(source_id.notna().sum()) == expected_source_exact,
        "hgnc_recovered": int(mapped.sum()) == expected_recovered,
        "residual_unknown": int(unknown.sum()) == expected_unknown,
        "complete_disposition": int(source_id.notna().sum())
        + int(mapped.sum())
        + int(unknown.sum())
        == EXPECTED_N_VARS,
    }
    if not all(checks.values()):
        raise AssertionError(f"VAR species/identifier gate failed: {checks}")
    return curated, {
        "status": "PASS",
        "VAR_ENSEMBL_SPECIES_COMPLETED": True,
        "rows": len(curated),
        "source_exact_ensembl": int(source_id.notna().sum()),
        "hgnc_recovered": int(mapped.sum()),
        "residual_unknown": int(unknown.sum()),
        "ambiguous_hgnc": mappings_payload["ambiguous_symbols"],
        "checks": checks,
    }


def expected_description(role: str, frame_hash: str, helper_hash: str) -> str:
    return f"{TASK_ID}: GSE107185 {role.upper()}_COMPLETED; frame_sha256={frame_hash}; helper_sha256={helper_hash}"


def is_task_revision(artifact: Any, baseline_uid: str, role: str) -> bool:
    uid = str(artifact.uid)
    return (
        uid != baseline_uid
        and uid[:16] == baseline_uid[:16]
        and str(artifact.description).startswith(
            f"{TASK_ID}: GSE107185 {role.upper()}_COMPLETED; "
        )
    )


def is_authorized_revision(
    artifact: Any, baseline_uid: str, role: str, frame_hash: str
) -> bool:
    return is_task_revision(artifact, baseline_uid, role) and (
        f"frame_sha256={frame_hash};" in str(artifact.description)
    )


def ensure_link_feature(ln: Any, name: str, heartbeat: Any | None = None) -> None:
    records = list(ln.Feature.filter(name=name).all())
    if records and str(records[0].dtype) != "cat[Artifact]":
        raise AssertionError(f"link feature dtype drift: {name}")
    if not records:
        if heartbeat is not None:
            heartbeat.assert_healthy()
        ln.Feature(name=name, dtype="cat[Artifact]").save()


def bounded_main_duplicate_probe(ln: Any) -> dict[str, Any]:
    terms = ("GSE107185", "PRJNA419230", "PMC6311450", "SEUSS", "perturbase_gse107185")
    candidates: dict[str, dict[str, Any]] = {}
    for term in terms:
        for field in ("key", "description"):
            queryset = ln.Artifact.filter(
                created_on_id=1, **{f"{field}__icontains": term}
            )
            for item in list(
                queryset.only("uid", "key", "description", "created_on_id")[:25]
            ):
                candidates[str(item.uid)] = artifact_identity(item)
    result = {
        "branch_id": 1,
        "terms": list(terms),
        "candidate_count": len(candidates),
        "candidates": sorted(
            candidates.values(), key=lambda item: (item["key"], item["uid"])
        ),
        "scientific_equivalent_found": bool(candidates),
    }
    if candidates:
        raise AssertionError(f"main scientific equivalent found: {result}")
    return result


def prepare(ln: Any, manifest: dict[str, Any], *, full_source: bool) -> dict[str, Any]:
    scratch = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-read-"))
    baseline_obs = artifact_by_uid(ln, BASELINE_OBS_UID)
    x_artifact = artifact_by_uid(ln, X_UID)
    baseline_var = artifact_by_uid(ln, BASELINE_VAR_UID)
    if (str(baseline_obs.key), str(x_artifact.key), str(baseline_var.key)) != (
        OBS_KEY,
        X_KEY,
        VAR_KEY,
    ):
        raise AssertionError("frozen artifact key drift")
    obs_path = materialize_artifact(
        baseline_obs,
        scratch / "baseline_obs.parquet",
        manifest["accepted_artifacts"]["obs"],
    )
    x_path = materialize_artifact(
        x_artifact, scratch / "X.zarr.zip", manifest["accepted_artifacts"]["X"]
    )
    var_path = materialize_artifact(
        baseline_var,
        scratch / "baseline_var.parquet",
        manifest["accepted_artifacts"]["var"],
    )
    baseline = pd.read_parquet(obs_path)
    var = pd.read_parquet(var_path)
    x_receipt = inspect_zarr_x(x_path, manifest["expected"])
    if (
        ordered_sha256(baseline.index.astype(str))
        != manifest["expected"]["obs_index_sha256_ordered"]
        or ordered_sha256(var.index.astype(str))
        != manifest["expected"]["var_index_sha256_ordered"]
    ):
        raise AssertionError("accepted OBS/VAR axes drift from the X link contract")
    source_receipt: dict[str, Any] = {"status": "deferred"}
    if full_source:
        source_path = scratch / "perturbase.h5ad"
        gcloud_cp(SOURCE_GENERATION_URI, source_path)
        source_spec = manifest["perturbase_source"]
        if (
            source_path.stat().st_size != source_spec["size"]
            or sha256_file(source_path) != source_spec["sha256"]
        ):
            raise AssertionError("retained PerturBase source identity drift")
        source_receipt = inspect_source(source_path, baseline, var, source_spec)
    curated_obs, obs_receipt = curate_obs(baseline, load_json(SEQUENCES_PATH))
    curated_var, var_receipt = curate_var(var, load_json(HGNC_PATH))
    helper_hash = sha256_file(Path(__file__))
    obs_hash = frame_sha256(curated_obs)
    var_hash = frame_sha256(curated_var)
    obs_description = expected_description("obs", obs_hash, helper_hash)
    var_description = expected_description("var", var_hash, helper_hash)
    latest_obs, obs_history = latest_artifact(ln, OBS_KEY)
    latest_var, var_history = latest_artifact(ln, VAR_KEY)
    obs_curated = is_authorized_revision(latest_obs, BASELINE_OBS_UID, "obs", obs_hash)
    var_curated = is_authorized_revision(latest_var, BASELINE_VAR_UID, "var", var_hash)
    if str(latest_obs.uid) != BASELINE_OBS_UID and not is_task_revision(
        latest_obs, BASELINE_OBS_UID, "obs"
    ):
        raise AssertionError(f"foreign OBS revision after baseline: {latest_obs.uid}")
    if str(latest_var.uid) != BASELINE_VAR_UID and not is_task_revision(
        latest_var, BASELINE_VAR_UID, "var"
    ):
        raise AssertionError(f"foreign VAR revision after baseline: {latest_var.uid}")
    if obs_curated:
        observed = pd.read_parquet(
            materialize_artifact(latest_obs, scratch / "readback_obs.parquet")
        )
        assert_frame_equal(observed, curated_obs, check_categorical=True)
    if var_curated:
        observed = pd.read_parquet(
            materialize_artifact(latest_var, scratch / "readback_var.parquet")
        )
        assert_frame_equal(observed, curated_var, check_categorical=True)
    return {
        "baseline_obs": baseline_obs,
        "x_artifact": x_artifact,
        "baseline_var": baseline_var,
        "latest_obs": latest_obs,
        "latest_var": latest_var,
        "collection_replaced_obs": (
            obs_history[-2] if obs_curated and len(obs_history) > 1 else latest_obs
        ),
        "curated_obs": curated_obs,
        "curated_var": curated_var,
        "obs_curated": obs_curated,
        "var_curated": var_curated,
        "obs_history_count": len(obs_history),
        "var_history_count": len(var_history),
        "obs_frame_sha256": obs_hash,
        "var_frame_sha256": var_hash,
        "obs_description": obs_description,
        "var_description": var_description,
        "obs_receipt": obs_receipt,
        "var_receipt": var_receipt,
        "x_receipt": x_receipt,
        "source_receipt": source_receipt,
    }


def member_identity(members: list[Any]) -> list[dict[str, str]]:
    return sorted(
        ({"uid": str(item.uid), "key": str(item.key)} for item in members),
        key=lambda item: (item["key"], item["uid"]),
    )


def membership_sha256(members: list[Any]) -> str:
    return hashlib.sha256(canonical(member_identity(members)).encode()).hexdigest()


def find_predecessor_collection(ln: Any, replaced_obs: Any) -> tuple[Any, list[Any]]:
    candidates = list(
        ln.Collection.filter(artifacts=replaced_obs, is_latest=True).all()
    )
    candidates = [
        item for item in candidates if str(item.key).startswith("pert-gym/additions/")
    ]
    candidates.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not candidates:
        raise AssertionError(
            "no latest additions Collection contains frozen GSE107185 OBS"
        )
    predecessor = candidates[-1]
    members = list(predecessor.artifacts.all())
    if sum(str(item.key) == OBS_KEY for item in members) != 1:
        raise AssertionError("predecessor exact OBS membership drift")
    return predecessor, members


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
        raise AssertionError("Collection replacement changed count or duplicated key")
    successor_key = SUCCESSOR_COLLECTION_KEY
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
        created = True
    actual = list(successor.artifacts.all())
    if (
        not bool(successor.is_latest)
        or str(successor.description) != description
        or member_identity(actual) != member_identity(after)
    ):
        raise AssertionError("successor Collection readback drift")
    return (
        successor,
        created,
        {
            "status": "PASS",
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


def publish(ln: Any, prepared: dict[str, Any], heartbeat: Any) -> dict[str, Any]:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-publish-"))
    writes = {
        "obs_revisions": 0,
        "var_revisions": 0,
        "x_revisions": 0,
        "collection_writes": 0,
        "deletions": 0,
    }
    if prepared["var_curated"]:
        var_artifact = prepared["latest_var"]
    else:
        path = root / "var.parquet"
        prepared["curated_var"].to_parquet(path)
        heartbeat.assert_healthy()
        var_artifact = ln.Artifact.from_dataframe(
            path,
            key=VAR_KEY,
            revises=prepared["baseline_var"],
            description=prepared["var_description"],
        ).save()
        writes["var_revisions"] = 1
    if prepared["obs_curated"]:
        obs_artifact = prepared["latest_obs"]
    else:
        path = root / "obs.parquet"
        prepared["curated_obs"].to_parquet(path)
        heartbeat.assert_healthy()
        obs_artifact = ln.Artifact.from_dataframe(
            path,
            key=OBS_KEY,
            revises=prepared["latest_obs"],
            description=prepared["obs_description"],
        ).save()
        writes["obs_revisions"] = 1
    ensure_link_feature(ln, "X", heartbeat)
    ensure_link_feature(ln, "var", heartbeat)
    heartbeat.assert_healthy()
    obs_artifact.features.set_values({"X": prepared["x_artifact"]})
    heartbeat.assert_healthy()
    prepared["x_artifact"].features.set_values({"var": var_artifact})
    heartbeat.assert_healthy()
    _, collection_created, collection_receipt = ensure_successor_collection(
        ln,
        prepared["collection_replaced_obs"],
        obs_artifact,
        allow_create=True,
        heartbeat=heartbeat,
    )
    writes["collection_writes"] = int(collection_created)
    return {
        "obs": obs_artifact,
        "var": var_artifact,
        "writes": writes,
        "collection": collection_receipt,
    }


def strip_runtime(prepared: dict[str, Any]) -> dict[str, Any]:
    return {
        "obs": artifact_identity(prepared["latest_obs"]),
        "X": artifact_identity(prepared["x_artifact"]),
        "var": artifact_identity(prepared["latest_var"]),
        "obs_history_count": prepared["obs_history_count"],
        "var_history_count": prepared["var_history_count"],
        "obs_frame_sha256": prepared["obs_frame_sha256"],
        "var_frame_sha256": prepared["var_frame_sha256"],
        "obs_receipt": prepared["obs_receipt"],
        "var_receipt": prepared["var_receipt"],
        "x_receipt": prepared["x_receipt"],
        "source_receipt": prepared["source_receipt"],
    }


def verify_links_and_collection(
    ln: Any, prepared: dict[str, Any], heartbeat: Any | None = None
) -> dict[str, Any]:
    if not prepared["obs_curated"] or not prepared["var_curated"]:
        return {"status": "PENDING"}
    if heartbeat is not None:
        heartbeat.assert_healthy()
    obs_x = resolve_artifact(ln, prepared["latest_obs"].features.get_values()["X"])
    x_var = resolve_artifact(ln, prepared["x_artifact"].features.get_values()["var"])
    if str(obs_x.uid) != X_UID or str(x_var.uid) != str(prepared["latest_var"].uid):
        raise AssertionError("terminal obs -> X -> var links drift")
    _, _, collection = ensure_successor_collection(
        ln,
        prepared["collection_replaced_obs"],
        prepared["latest_obs"],
        allow_create=False,
        heartbeat=heartbeat,
    )
    return {
        "status": "PASS",
        "obs_X_link": True,
        "X_var_link": True,
        "collection": collection,
    }


def run(mode: str) -> dict[str, Any]:
    if platform.system() == "Darwin":
        raise RuntimeError("plan/mutate/verify require the EU worker")
    capacity = preflight()
    lifecycle_lease = verify_lifecycle_lease(capacity, mode)
    manifest = load_json(MANIFEST_PATH)
    if manifest["task_id"] != TASK_ID or manifest["dataset_id"] != DATASET_ID:
        raise AssertionError("source manifest identity drift")
    scratch = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-authority-"))
    authority_receipts = verify_authorities(manifest, scratch)
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    main_probe = bounded_main_duplicate_probe(ln)
    before = prepare(ln, manifest, full_source=True)
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    writes = {
        "obs_revisions": 0,
        "var_revisions": 0,
        "x_revisions": 0,
        "collection_writes": 0,
        "deletions": 0,
    }
    collection_receipt: dict[str, Any] = {"status": "PENDING"}
    write_revalidation: dict[str, Any] = {"status": "not_applicable"}
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
            lease = stack.enter_context(distributed_lamin_writer_lease(metadata))
            heartbeat = stack.enter_context(DistributedLeaseHeartbeat(lease))
            latest_obs, obs_history = latest_artifact(ln, OBS_KEY)
            latest_var, var_history = latest_artifact(ln, VAR_KEY)
            if (
                str(latest_obs.uid) != str(before["latest_obs"].uid)
                or len(obs_history) != before["obs_history_count"]
                or str(latest_var.uid) != str(before["latest_var"].uid)
                or len(var_history) != before["var_history_count"]
            ):
                raise AssertionError(
                    "registry changed between prepare and writer lease"
                )
            predecessor, members = find_predecessor_collection(
                ln, before["collection_replaced_obs"]
            )
            write_revalidation = {
                "status": "PASS",
                "latest_obs_uid": str(latest_obs.uid),
                "latest_var_uid": str(latest_var.uid),
                "predecessor_collection_uid": str(predecessor.uid),
                "predecessor_membership_sha256": membership_sha256(members),
            }
            if mode == "mutate":
                ln.track(
                    key=f"pert-gym/dataset-completion/{DATASET_ID}/{TASK_ID}",
                    kind="script",
                    params={
                        "task_id": TASK_ID,
                        "helper_sha256": sha256_file(Path(__file__)),
                    },
                    new_run=True,
                    pypackages=False,
                    stream_tracking=False,
                )
                result = publish(ln, before, heartbeat)
                writes = result["writes"]
                collection_receipt = result["collection"]
                try:
                    ln.finish()
                except AttributeError:
                    ln.context.finish()
            final = prepare(ln, manifest, full_source=True)
            if not final["obs_curated"] or not final["var_curated"]:
                raise AssertionError(
                    f"{mode} requested before completed revisions exist"
                )
            links = verify_links_and_collection(ln, final, heartbeat)
            collection_receipt = links["collection"]
            counts_after = {
                "artifacts": ln.Artifact.filter().count(),
                "collections": ln.Collection.filter().count(),
            }
    else:
        final = before
        links = {"status": "PENDING"}
        counts_after = {
            "artifacts": ln.Artifact.filter().count(),
            "collections": ln.Collection.filter().count(),
        }
    complete = bool(
        final["obs_curated"] and final["var_curated"] and links["status"] == "PASS"
    )
    receipt = {
        "format": "pert-gym.dataset-completion/v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "status": "PASS" if mode == "plan" or complete else "PENDING",
        "mode": mode,
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "helper_sha256": sha256_file(Path(__file__)),
        "source_manifest_sha256": sha256_file(MANIFEST_PATH),
        "scientific_contract": manifest["scientific_contract"],
        "authority_receipts": authority_receipts,
        "lifecycle_lease_readback": lifecycle_lease,
        "negative_main_duplicate_probe": main_probe,
        "write_revalidation": write_revalidation,
        "member_before": strip_runtime(before),
        "member_after": strip_runtime(final),
        "links": links,
        "collection": collection_receipt,
        "gates": {
            "OBS": final["obs_receipt"]["status"],
            "VAR": final["var_receipt"]["status"],
            "chunks": final["x_receipt"]["status"],
            "cleaning": "PASS",
            "canonical_storage": "PASS" if complete else "PENDING",
            "lamin_jkobject": "PASS",
            "collection": collection_receipt["status"],
        },
        "writes": writes,
        "registry_counts": {"before": counts_before, "after": counts_after},
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "rollback": {
            "obs_uid": BASELINE_OBS_UID,
            "var_uid": BASELINE_VAR_UID,
            "X_uid": X_UID,
            "collection_uid": collection_receipt.get("predecessor_uid"),
        },
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
    receipt = run(sys.argv[1])
    print(
        "GSE107185_COMPLETION="
        + canonical(
            {
                "status": receipt["status"],
                "mode": receipt["mode"],
                "receipt_sha256": receipt["canonical_sha256"],
                "collection_uid": receipt["collection"].get("successor_uid"),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
