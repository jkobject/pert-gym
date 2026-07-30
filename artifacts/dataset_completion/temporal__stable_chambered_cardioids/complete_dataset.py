#!/usr/bin/env python3
"""Complete GSE269572 OBS/VAR cleaning and verify the stable Lamin identity."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
import time
import urllib.request
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import h5py
import pandas as pd
from pandas.testing import assert_frame_equal

from pert_gym.obs_identity import add_obs_identity, validate_obs_identity
from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_363b9754"
DATASET_ID = (
    "temporal/stable_chambered_cardioids_from_human_pluripotent_stem_cells_scrna_seq"
)
GEO_ACCESSION = "GSE269572"
BIOPROJECT = "PRJNA1122543"
PREFIX = "data/cleaned/GSE269572"
OBS_KEY = f"{PREFIX}/obs.parquet"
X_KEY = f"{PREFIX}/X.h5ad"
VAR_KEY = f"{PREFIX}/var.parquet"
BASELINE_OBS_UID = "wTvFyR9fchIwao8l0000"
X_UID = "vNprtc3z84zMcEUj0000"
BASELINE_VAR_UID = "umZYkLN9bq9nfl5m0000"
EXPECTED_N_OBS = 59_373
EXPECTED_N_VARS = 36_601
SUCCESSOR_COLLECTION_KEY = "pert-gym/additions/20260730-gse269572-e2e"
BILLING_PROJECT = "jkobject-1549353370965"
HERE = Path(__file__).resolve().parent
MANIFEST_PATH = HERE / "source_manifest.json"
RECEIPT_PATH = HERE / "completion_receipt.json"

OBS_FIELDS = (
    "dataset",
    "sample",
    "cell_id",
    "source_accession",
    "sample_accession",
    "source_cell_barcode",
    "source_file",
    "sample_title",
    "cell_line",
    "condition",
    "perturbation",
    "perturbation_type",
    "organism",
    "tissue_type",
    "assay",
    "modality",
    "is_control",
    "donor_id",
    "age",
    "sex",
    "ethnicity",
    "disease",
    "cell_type",
)
SOURCE_OBS_COLUMNS = (
    "source_accession",
    "sample_accession",
    "sample_title",
    "source_file",
    "source_cell_barcode",
    "source_name",
    "source_cell_line",
    "treatment",
    "condition",
    "timepoint",
    "timepoint_unit",
    "development_stage",
    "donor_age",
    "donor_sex",
    "donor_ethnicity",
    "organism",
    "assay",
    "tissue",
    "is_control",
    "trajectory_id",
    "source_matrix_semantics",
)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_sha256(values: Any) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = str(value).encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def newline_sha256(values: Any) -> str:
    return sha256_bytes("\n".join(map(str, values)).encode())


def frame_sha256(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(
        canonical(
            {
                "columns": list(map(str, frame.columns)),
                "dtypes": list(map(str, frame.dtypes)),
            }
        ).encode()
    )
    digest.update(pd.util.hash_pandas_object(frame, index=True).to_numpy().tobytes())
    return digest.hexdigest()


def load_manifest() -> dict[str, Any]:
    manifest = json.loads(MANIFEST_PATH.read_text())
    if manifest["task_id"] != TASK_ID or manifest["dataset_id"] != DATASET_ID:
        raise AssertionError("source manifest identity drift")
    return manifest


def download(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "pert-gym/1.0"})
    with urllib.request.urlopen(request, timeout=180) as response:
        return response.read()


def verify_sources(manifest: dict[str, Any]) -> dict[str, Any]:
    soft_spec = manifest["authorities"]["geo_soft"]
    soft = download(soft_spec["url"])
    if len(soft) != soft_spec["size"] or sha256_bytes(soft) != soft_spec["sha256"]:
        raise AssertionError("GEO SOFT identity drift")
    text = gzip.decompress(soft).decode("utf-8")
    required = (
        "^SERIES = GSE269572",
        "!Series_relation = BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA1122543",
        "All samples were collected for single-cell sequencing on day 42.5.",
    )
    if not all(token in text for token in required):
        raise AssertionError("GEO scientific source tokens absent")
    sample_count = text.count("^SAMPLE = ")
    if sample_count != manifest["expected"]["samples"]:
        raise AssertionError("GEO sample denominator drift")

    filelist_spec = manifest["authorities"]["geo_filelist"]
    filelist = download(filelist_spec["url"])
    if (
        len(filelist) != filelist_spec["size"]
        or sha256_bytes(filelist) != filelist_spec["sha256"]
    ):
        raise AssertionError("GEO supplementary filelist identity drift")

    raw_spec = manifest["authorities"]["geo_raw_tar"]
    request = urllib.request.Request(
        raw_spec["url"], method="HEAD", headers={"User-Agent": "pert-gym/1.0"}
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        raw_size = int(response.headers["Content-Length"])
    if raw_size != raw_spec["size"]:
        raise AssertionError("GEO RAW tar byte denominator drift")
    return {
        "status": "PASS",
        "geo_soft": {
            "url": soft_spec["url"],
            "size": len(soft),
            "sha256": sha256_bytes(soft),
            "sample_count": sample_count,
        },
        "geo_filelist": {
            "url": filelist_spec["url"],
            "size": len(filelist),
            "sha256": sha256_bytes(filelist),
        },
        "geo_raw_tar": {
            "url": raw_spec["url"],
            "size": raw_size,
            "sha256": raw_spec["sha256"],
            "verification": raw_spec["verification"],
        },
        "publication": {
            "status": "unknown",
            "reason": "GEO supplies no PubMed or DOI relation",
        },
    }


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size or 0),
        "branch_id": int(artifact.branch_id),
        "is_latest": bool(artifact.is_latest),
        "description": str(artifact.description),
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
        raise AssertionError(f"artifact absent: {key}")
    return records[-1], records


def resolve_artifact(ln: Any, value: Any) -> Any:
    if hasattr(value, "uid"):
        return value
    identity = str(value)
    try:
        return ln.Artifact.get(uid=identity)
    except ln.Artifact.DoesNotExist:
        return ln.Artifact.get(key=identity)


def materialize_artifact(artifact: Any, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    uri = str(artifact.path)
    if uri.startswith("gs://"):
        subprocess.run(
            [
                "gcloud",
                "storage",
                "cp",
                f"--billing-project={BILLING_PROJECT}",
                uri,
                str(destination),
            ],
            check=True,
        )
        return destination
    if uri.startswith("s3://"):
        cached = Path(artifact.cache())
        if not cached.is_file():
            raise FileNotFoundError(f"failed to cache {uri}")
        shutil.copy2(cached, destination)
        return destination
    cached = Path(artifact.cache())
    if cached.resolve() != destination.resolve():
        shutil.copy2(cached, destination)
    return destination


def missing(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: Any, source: str
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def curate_obs(baseline: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if len(baseline) != EXPECTED_N_OBS or not baseline.index.is_unique:
        raise AssertionError("accepted OBS denominator drift")
    absent = sorted(set(SOURCE_OBS_COLUMNS) - set(baseline.columns))
    if absent:
        raise AssertionError(f"accepted OBS source fields absent: {absent}")
    if set(baseline["source_accession"].astype(str)) != {GEO_ACCESSION}:
        raise AssertionError("source accession drift")
    if set(pd.to_numeric(baseline["timepoint"])) != {42.5}:
        raise AssertionError("single-snapshot source time drift")
    if set(baseline["timepoint_unit"].astype(str)) != {"day in vitro"}:
        raise AssertionError("source time unit drift")

    result = pd.DataFrame(index=baseline.index.copy())
    result = add_obs_identity(result, dataset_id=DATASET_ID, prefix=PREFIX)
    validate_obs_identity(result)
    index = result.index
    sample = baseline["sample_accession"].astype("string")
    barcode = baseline["source_cell_barcode"].astype("string")
    cell_id = sample + ":" + barcode
    if not cell_id.is_unique:
        raise AssertionError("derived cell identity is not unique")
    treatment = baseline["treatment"].astype("string")
    perturbation = pd.Series("none", index=index, dtype="string")
    perturbation.loc[treatment.eq("with PD173074")] = "PD173074"
    perturbation_type = pd.Series("none", index=index, dtype="string")
    perturbation_type.loc[treatment.eq("with PD173074")] = "drug"

    set_field(
        result, "dataset", DATASET_ID, "present", "stable logical dataset identity"
    )
    set_field(result, "sample", sample, "present", "GEO sample accession")
    set_field(
        result,
        "cell_id",
        cell_id,
        "present",
        "GEO sample accession plus source barcode",
    )
    set_field(
        result, "source_accession", GEO_ACCESSION, "present", "GEO series accession"
    )
    set_field(result, "sample_accession", sample, "present", "GEO sample accession")
    set_field(
        result, "source_cell_barcode", barcode, "present", "GEO author matrix barcode"
    )
    set_field(
        result,
        "source_file",
        baseline["source_file"].astype("string"),
        "present",
        "GEO supplementary matrix member",
    )
    set_field(
        result,
        "sample_title",
        baseline["sample_title"].astype("string"),
        "present",
        "GEO sample title",
    )
    set_field(
        result,
        "cell_line",
        baseline["source_cell_line"].astype("string"),
        "present",
        "GEO sample characteristic: cell line",
    )
    set_field(
        result,
        "condition",
        baseline["condition"].astype("string"),
        "present",
        "GEO three-arm culture/treatment design",
    )
    set_field(
        result,
        "perturbation",
        perturbation,
        "present",
        "source treatment; only with-PD arm is drug treated",
    )
    set_field(
        result,
        "perturbation_type",
        perturbation_type,
        "present",
        "source treatment mapped to drug or none",
    )
    set_field(result, "organism", "Homo sapiens", "present", "GEO taxon 9606")
    set_field(
        result,
        "tissue_type",
        baseline["tissue"].astype("string"),
        "present",
        "source culture context",
    )
    set_field(
        result,
        "assay",
        baseline["assay"].astype("string"),
        "present",
        "GEO extraction protocol",
    )
    set_field(result, "modality", "scRNA-seq", "present", "GEO series type")
    set_field(
        result,
        "is_control",
        missing(index, "boolean"),
        "unknown",
        "three source arms do not define one globally canonical binary control",
    )
    for field, source in (
        ("donor_id", "donor identity absent from GEO"),
        ("age", "donor age absent; day 42.5 is culture time, not donor age"),
        ("sex", "donor sex absent from GEO"),
        ("ethnicity", "donor ethnicity absent from GEO"),
        ("disease", "disease state absent from GEO"),
        ("cell_type", "no immutable barcode-level cell annotation source"),
    ):
        set_field(result, field, missing(index), "unknown", source)

    for source_column in SOURCE_OBS_COLUMNS:
        if source_column in {
            "timepoint",
            "timepoint_unit",
            "development_stage",
            "trajectory_id",
            "source_name",
            "source_matrix_semantics",
        }:
            continue
        result[f"source_original_{source_column}"] = baseline[source_column]
    result["source_treatment"] = treatment
    result["x_semantics"] = "raw_counts"

    if not result.index.equals(baseline.index) or len(result) != len(baseline):
        raise AssertionError("OBS row/order drift")
    forbidden = {"timepoint", "timepoint_unit", "development_stage", "trajectory_id"}
    if forbidden & set(result.columns):
        raise AssertionError("single-snapshot time leaked into cell-level OBS")
    for field in OBS_FIELDS:
        for required in (field, f"{field}_state", f"{field}_source"):
            if required not in result:
                raise AssertionError(f"canonical OBS evidence absent: {required}")
        if result[f"{field}_source"].astype(str).str.strip().eq("").any():
            raise AssertionError(f"blank OBS provenance: {field}")

    conditions = {
        str(value): int(count)
        for value, count in treatment.value_counts().sort_index().items()
    }
    samples = {
        str(value): int(count)
        for value, count in sample.value_counts().sort_index().items()
    }
    return result, {
        "status": "PASS",
        "OBS_COMPLETED": True,
        "rows": len(result),
        "canonical_field_count": len(OBS_FIELDS),
        "obs_uuid_unique": bool(result["obs_uuid"].is_unique),
        "scientific_modality": "single-cell expression with one drug/culture design axis",
        "experimental_unit": {
            "cell": "one author-supplied Cell Ranger barcode within one GEO sample",
            "sample": "one biological/culture replicate",
            "dataset": "three-arm H9-derived cardioid/2D comparison at day 42.5",
        },
        "experimental_axes": {
            "condition": {
                "cardinality": int(treatment.nunique()),
                "frequencies": conditions,
                "level": "cell joined to GEO sample",
            },
            "sample": {"cardinality": int(sample.nunique()), "frequencies": samples},
        },
        "temporal_verdict": {
            "status": "non_temporal_single_snapshot",
            "source_day_in_vitro": 42.5,
            "source_frequency": {"42.5": len(result)},
            "decision": "retain as dataset metadata only; no cell-level time or trajectory axis",
        },
        "outcomes_endpoints": {
            "expression_matrix": "raw_counts",
            "drug_toxicity_endpoint": "not present in this scRNA-seq payload",
            "scalar_response_endpoint": "not_applicable",
        },
        "residual_unknowns": [
            "donor_id",
            "age",
            "sex",
            "ethnicity",
            "disease",
            "cell_type",
            "is_control",
        ],
    }


def curate_var(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if len(raw) != EXPECTED_N_VARS or not raw.index.is_unique:
        raise AssertionError("accepted VAR denominator drift")
    required = {
        "feature_id",
        "gene_symbol",
        "feature_type",
        "feature_namespace",
        "organism",
        "genome_build",
    }
    absent = sorted(required - set(raw.columns))
    if absent:
        raise AssertionError(f"accepted VAR fields absent: {absent}")
    stable = raw["feature_id"].astype("string")
    checks = {
        "index_matches_feature_id": raw.index.astype(str).tolist()
        == stable.astype(str).tolist(),
        "stable_unique": bool(stable.is_unique),
        "stable_syntax": bool(
            stable.str.fullmatch(r"ENSG\d{11}(?:\.\d+)?", na=False).all()
        ),
        "human": bool(raw["organism"].astype(str).eq("Homo sapiens").all()),
        "grch38": bool(raw["genome_build"].astype(str).eq("GRCh38").all()),
        "namespace": bool(
            raw["feature_namespace"].astype(str).eq("Ensembl gene ID").all()
        ),
    }
    if not all(checks.values()):
        raise AssertionError(f"species-correct stable VAR gate failed: {checks}")
    result = raw.copy(deep=True)
    for column in raw.columns:
        result[f"source_original_{column}"] = raw[column]
    result["original_var_index"] = raw.index.astype(str)
    result["ensembl_id"] = stable
    result["author_gene_id"] = stable
    result["author_gene_symbol"] = raw["gene_symbol"].astype("string")
    result["stable_feature_id_state"] = "present"
    result["stable_feature_id_source"] = "source-native GEO Cell Ranger feature_id"
    result["species_validation_state"] = "present"
    result["species_validation_source"] = "GEO taxon 9606, hg38, and ENSG namespace"
    if not result.index.equals(raw.index):
        raise AssertionError("VAR row/order drift")
    return result, {
        "status": "PASS",
        "VAR_ENSEMBL_SPECIES_COMPLETED": True,
        "biological_features_total": len(result),
        "stable_ensembl_id_features": int(stable.notna().sum()),
        "correct_species_features": int(result["organism"].eq("Homo sapiens").sum()),
        "ordered_feature_id_sha256": ordered_sha256(stable),
        "checks": checks,
    }


def inspect_x(
    artifact: Any,
    obs_index: pd.Index,
    var_index: pd.Index,
    manifest: dict[str, Any],
    scratch: Path,
) -> dict[str, Any]:
    path = materialize_artifact(artifact, scratch / "X.h5ad")
    with h5py.File(path, "r") as handle:
        shape = tuple(map(int, handle["X"].attrs["shape"]))
        obs_key = handle["obs"].attrs["_index"]
        var_key = handle["var"].attrs["_index"]
        if isinstance(obs_key, bytes):
            obs_key = obs_key.decode()
        if isinstance(var_key, bytes):
            var_key = var_key.decode()
        obs = [str(value) for value in handle[f"obs/{obs_key}"].asstr()[...]]
        var = [str(value) for value in handle[f"var/{var_key}"].asstr()[...]]
        nnz = int(len(handle["X/data"]))
        dtype = str(handle["X/data"].dtype)
        count_sum = int(handle["X/data"][...].sum(dtype="uint64"))
        max_count = int(handle["X/data"][...].max())
        encoding = str(handle["X"].attrs["encoding-type"])
    expected = manifest["expected"]
    checks = {
        "shape": list(shape) == [EXPECTED_N_OBS, EXPECTED_N_VARS],
        "obs_axis": obs == list(map(str, obs_index)),
        "var_axis": var == list(map(str, var_index)),
        "nnz": nnz == expected["nnz"],
        "dtype": dtype == expected["x_dtype"],
        "count_sum": count_sum == expected["count_sum"],
        "max_count": max_count == expected["max_count"],
        "encoding": encoding == "csr_matrix",
    }
    if not all(checks.values()):
        raise AssertionError(f"accepted X parity drift: {checks}")
    return {
        "status": "PASS",
        "identity": artifact_identity(artifact),
        "shape": list(shape),
        "nnz": nnz,
        "dtype": dtype,
        "count_sum": count_sum,
        "max_count": max_count,
        "checks": checks,
    }


def bounded_main_duplicate_probe(ln: Any) -> dict[str, Any]:
    terms = (
        GEO_ACCESSION,
        BIOPROJECT,
        "stable chambered cardioids",
        "Generation of Stable Chambered Cardioids",
    )
    candidates: dict[str, dict[str, Any]] = {}
    ln.setup.switch("main")
    try:
        if ln.setup.settings.branch.name != "main":
            raise AssertionError("failed to switch to main")
        for term in terms:
            for field in ("key", "description"):
                query = ln.Artifact.filter(
                    **{f"{field}__icontains": term}, is_latest=True
                )
                for artifact in list(query[:25]):
                    candidates[str(artifact.uid)] = artifact_identity(artifact)
    finally:
        ln.setup.switch("jkobject")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("failed to restore jkobject")
    if candidates:
        raise AssertionError(f"main scientific equivalent found: {candidates}")
    return {
        "status": "PASS",
        "branch": "main",
        "terms": list(terms),
        "query_limit_per_term_and_field": 25,
        "candidate_count": 0,
        "candidates": [],
        "scientific_equivalent_found": False,
    }


def ensure_link_feature(ln: Any, name: str) -> None:
    records = list(ln.Feature.filter(name=name).all())
    if records and str(records[0].dtype) != "cat[Artifact]":
        raise AssertionError(f"link feature dtype drift: {name}")
    if not records:
        ln.Feature(name=name, dtype="cat[Artifact]").save()


def prepare(ln: Any, manifest: dict[str, Any]) -> dict[str, Any]:
    scratch = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-read-"))
    accepted_obs = artifact_by_uid(ln, BASELINE_OBS_UID)
    x_artifact = artifact_by_uid(ln, X_UID)
    accepted_var = artifact_by_uid(ln, BASELINE_VAR_UID)
    expected_keys = {"obs": OBS_KEY, "X": X_KEY, "var": VAR_KEY}
    actual_keys = {
        "obs": str(accepted_obs.key),
        "X": str(x_artifact.key),
        "var": str(accepted_var.key),
    }
    if actual_keys != expected_keys:
        raise AssertionError(f"accepted artifact key drift: {actual_keys}")
    baseline = pd.read_parquet(
        materialize_artifact(accepted_obs, scratch / "accepted_obs.parquet")
    )
    raw_var = pd.read_parquet(
        materialize_artifact(accepted_var, scratch / "accepted_var.parquet")
    )
    if (
        newline_sha256(baseline.index)
        != manifest["expected"]["obs_index_sha256_newline"]
    ):
        raise AssertionError("accepted OBS ordered index drift")
    if (
        newline_sha256(raw_var.index)
        != manifest["expected"]["var_index_sha256_newline"]
    ):
        raise AssertionError("accepted VAR ordered index drift")
    if (
        baseline["condition"].astype(str).value_counts().sort_index().to_dict()
        != manifest["expected"]["condition_counts"]
    ):
        raise AssertionError("accepted condition frequencies drift")
    if (
        baseline["sample_accession"].astype(str).value_counts().sort_index().to_dict()
        != manifest["expected"]["sample_counts"]
    ):
        raise AssertionError("accepted sample frequencies drift")

    curated_obs, obs_receipt = curate_obs(baseline)
    curated_var, var_receipt = curate_var(raw_var)
    x_receipt = inspect_x(x_artifact, baseline.index, raw_var.index, manifest, scratch)
    latest_obs, obs_history = latest_artifact(ln, OBS_KEY)
    latest_var, var_history = latest_artifact(ln, VAR_KEY)
    obs_prefix = f"{TASK_ID}: source-exhaustive GSE269572 OBS"
    var_prefix = f"{TASK_ID}: species-correct canonical GSE269572 VAR"
    obs_is_curated = str(latest_obs.uid) != BASELINE_OBS_UID and str(
        latest_obs.description
    ).startswith(obs_prefix)
    var_is_curated = str(latest_var.uid) != BASELINE_VAR_UID and str(
        latest_var.description
    ).startswith(var_prefix)
    if str(latest_obs.uid) != BASELINE_OBS_UID and not obs_is_curated:
        raise AssertionError(
            f"foreign OBS revision after accepted baseline: {latest_obs.uid}"
        )
    if str(latest_var.uid) != BASELINE_VAR_UID and not var_is_curated:
        raise AssertionError(
            f"foreign VAR revision after accepted baseline: {latest_var.uid}"
        )
    if obs_is_curated:
        observed = pd.read_parquet(
            materialize_artifact(latest_obs, scratch / "readback_obs.parquet")
        )
        assert_frame_equal(observed, curated_obs, check_categorical=True)
    if var_is_curated:
        observed = pd.read_parquet(
            materialize_artifact(latest_var, scratch / "readback_var.parquet")
        )
        assert_frame_equal(observed, curated_var, check_categorical=True)
    return {
        "accepted_obs": accepted_obs,
        "x_artifact": x_artifact,
        "accepted_var": accepted_var,
        "latest_obs": latest_obs,
        "latest_var": latest_var,
        "curated_obs": curated_obs,
        "curated_var": curated_var,
        "obs_is_curated": obs_is_curated,
        "var_is_curated": var_is_curated,
        "obs_receipt": obs_receipt,
        "var_receipt": var_receipt,
        "x_receipt": x_receipt,
        "obs_history_count": len(obs_history),
        "var_history_count": len(var_history),
        "expected_obs_frame_sha256": frame_sha256(curated_obs),
        "expected_var_frame_sha256": frame_sha256(curated_var),
    }


def publish(ln: Any, prepared: dict[str, Any], helper_sha256: str) -> dict[str, int]:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-publish-"))
    writes = {"obs_revisions": 0, "var_revisions": 0, "x_revisions": 0, "deletions": 0}
    if not prepared["obs_is_curated"]:
        path = root / "obs.parquet"
        prepared["curated_obs"].to_parquet(path)
        prepared["latest_obs"] = ln.Artifact.from_dataframe(
            path,
            key=OBS_KEY,
            revises=prepared["accepted_obs"],
            description=f"{TASK_ID}: source-exhaustive GSE269572 OBS; frame_sha256={prepared['expected_obs_frame_sha256']}; helper_sha256={helper_sha256}",
        ).save()
        prepared["obs_is_curated"] = True
        writes["obs_revisions"] = 1
    if not prepared["var_is_curated"]:
        path = root / "var.parquet"
        prepared["curated_var"].to_parquet(path)
        prepared["latest_var"] = ln.Artifact.from_dataframe(
            path,
            key=VAR_KEY,
            revises=prepared["accepted_var"],
            description=f"{TASK_ID}: species-correct canonical GSE269572 VAR; frame_sha256={prepared['expected_var_frame_sha256']}; helper_sha256={helper_sha256}",
        ).save()
        prepared["var_is_curated"] = True
        writes["var_revisions"] = 1
    ensure_link_feature(ln, "X")
    ensure_link_feature(ln, "var")
    prepared["latest_obs"].features.set_values({"X": prepared["x_artifact"]})
    prepared["x_artifact"].features.set_values({"var": prepared["latest_var"]})
    return writes


def member_identity(members: list[Any]) -> list[dict[str, str]]:
    return sorted(
        ({"uid": str(item.uid), "key": str(item.key)} for item in members),
        key=lambda item: (item["key"], item["uid"]),
    )


def membership_sha256(members: list[Any]) -> str:
    return sha256_bytes(canonical(member_identity(members)).encode())


def predecessor_collection(ln: Any, accepted_obs: Any) -> tuple[Any, list[Any]]:
    candidates = [
        item
        for item in ln.Collection.filter(artifacts=accepted_obs, is_latest=True).all()
        if str(item.key).startswith("pert-gym/additions/")
    ]
    candidates.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not candidates:
        raise AssertionError(
            "no latest additions Collection contains accepted GSE269572 OBS"
        )
    predecessor = candidates[-1]
    members = list(predecessor.artifacts.all())
    if sum(str(item.uid) == BASELINE_OBS_UID for item in members) != 1:
        raise AssertionError("predecessor exact accepted OBS membership drift")
    return predecessor, members


def ensure_successor_collection(
    ln: Any, prepared: dict[str, Any], *, allow_create: bool
) -> tuple[Any, bool, dict[str, Any]]:
    predecessor, before = predecessor_collection(ln, prepared["accepted_obs"])
    after = [
        prepared["latest_obs"] if str(item.uid) == BASELINE_OBS_UID else item
        for item in before
    ]
    if not prepared["obs_is_curated"]:
        raise AssertionError("cannot create Collection before OBS revision exists")
    keys = [str(item.key) for item in after]
    if len(after) != len(before) or len(keys) != len(set(keys)):
        raise AssertionError("Collection replacement changed count or duplicated a key")
    description = canonical(
        {
            "format": "pert-gym.append-only-dataset-completion/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "predecessor_uid": str(predecessor.uid),
            "predecessor_membership_sha256": membership_sha256(before),
            "replaced_obs_uid": BASELINE_OBS_UID,
            "added_obs_uid": str(prepared["latest_obs"].uid),
            "member_count": len(after),
            "resulting_membership_sha256": membership_sha256(after),
            "rollback": f"select immutable predecessor Collection {predecessor.uid}",
        }
    )
    existing = list(ln.Collection.filter(key=SUCCESSOR_COLLECTION_KEY).all())
    created = False
    if existing:
        if len(existing) != 1:
            raise AssertionError("successor Collection key collision")
        successor = existing[0]
    else:
        if not allow_create:
            raise AssertionError("successor Collection absent")
        successor = ln.Collection(
            after,
            key=SUCCESSOR_COLLECTION_KEY,
            description=description,
            skip_hash_lookup=True,
        ).save()
        created = True
    actual = list(successor.artifacts.all())
    if str(successor.description) != description or member_identity(
        actual
    ) != member_identity(after):
        raise AssertionError("successor Collection readback drift")
    return (
        successor,
        created,
        {
            "status": "PASS",
            "predecessor_uid": str(predecessor.uid),
            "predecessor_key": str(predecessor.key),
            "predecessor_member_count": len(before),
            "predecessor_membership_sha256": membership_sha256(before),
            "successor_uid": str(successor.uid),
            "successor_key": str(successor.key),
            "successor_member_count": len(actual),
            "successor_membership_sha256": membership_sha256(actual),
            "target_obs_uid": str(prepared["latest_obs"].uid),
            "duplicate_keys": len(keys) - len(set(keys)),
        },
    )


def verify_links(ln: Any, prepared: dict[str, Any]) -> dict[str, Any]:
    obs_links = prepared["latest_obs"].features.get_values()
    x_links = prepared["x_artifact"].features.get_values()
    obs_x = resolve_artifact(ln, obs_links["X"])
    x_var = resolve_artifact(ln, x_links["var"])
    if str(obs_x.uid) != X_UID or str(x_var.uid) != str(prepared["latest_var"].uid):
        raise AssertionError("revised OBS -> accepted X -> revised VAR link drift")
    return {
        "status": "PASS",
        "obs": artifact_identity(prepared["latest_obs"]),
        "X": artifact_identity(obs_x),
        "var": artifact_identity(x_var),
    }


def run(mode: str) -> dict[str, Any]:
    if mode in {"mutate", "verify"} and platform.system() == "Darwin":
        raise RuntimeError(
            "live completion and full payload verification require EU worker"
        )
    manifest = load_manifest()
    sources = verify_sources(manifest)
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    main_probe = bounded_main_duplicate_probe(ln)
    before_counts = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    prepare(ln, manifest)
    helper_sha256 = sha256_file(Path(__file__))
    writes = {
        "obs_revisions": 0,
        "var_revisions": 0,
        "x_revisions": 0,
        "collection_writes": 0,
        "deletions": 0,
    }
    collection_receipt: dict[str, Any] = {"status": "PENDING"}
    if mode == "mutate":
        capacity = preflight()
        metadata = {
            "run_id": TASK_ID,
            "pid": os.getpid(),
            "host": capacity.hostname,
            "project": capacity.project,
            "zone": capacity.zone,
            "branch": "jkobject",
            "started_at": time.time(),
        }
        with ExitStack() as stack:
            stack.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            stack.enter_context(distributed_lamin_writer_lease(metadata))
            prepared = prepare(ln, manifest)
            ln.track(
                key=f"pert-gym/dataset-completion/{DATASET_ID}/{TASK_ID}",
                kind="script",
                params={"task_id": TASK_ID, "helper_sha256": helper_sha256},
                new_run=True,
                pypackages=False,
                stream_tracking=False,
            )
            writes.update(publish(ln, prepared, helper_sha256))
            _collection, created, collection_receipt = ensure_successor_collection(
                ln, prepared, allow_create=True
            )
            writes["collection_writes"] = int(created)
            try:
                ln.finish()
            except AttributeError:
                ln.context.finish()
    final = prepare(ln, manifest)
    completed = final["obs_is_curated"] and final["var_is_curated"]
    if mode == "verify" and not completed:
        raise AssertionError("verify requested before completed revisions exist")
    links: dict[str, Any] = {"status": "PENDING"}
    if completed:
        links = verify_links(ln, final)
        _collection, _created, collection_receipt = ensure_successor_collection(
            ln, final, allow_create=False
        )
    after_counts = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    receipt = {
        "schema_version": "pert-gym.dataset-completion-receipt/v2",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "status": "PASS" if (mode == "plan" or completed) else "PENDING",
        "mode": mode,
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "helper_sha256": helper_sha256,
        "source_manifest_sha256": sha256_file(MANIFEST_PATH),
        "sources": sources,
        "accepted_component_receipt": manifest["accepted_component_receipt"],
        "negative_main_duplicate_probe": main_probe,
        "counts": {
            "biological_datasets": 1,
            "logical_families": 1,
            "physical_members": 1,
            "observations": EXPECTED_N_OBS,
            "variables": EXPECTED_N_VARS,
            "matrix_nonzeros": manifest["expected"]["nnz"],
        },
        "obs": final["obs_receipt"],
        "var": final["var_receipt"],
        "chunks": {
            "status": final["x_receipt"]["status"],
            "verdict": "unchunked_appropriate",
            "physical_members": 1,
            "X": final["x_receipt"],
        },
        "links": links,
        "collection": collection_receipt,
        "dataset_metadata": {
            "source_snapshot_day_in_vitro": 42.5,
            "temporal_status": "non_temporal_single_snapshot",
            "source_name": "H9",
            "x_semantics": "raw_counts",
            "publication": "unknown",
        },
        "gates": {
            "OBS": final["obs_receipt"]["status"],
            "VAR": final["var_receipt"]["status"],
            "chunks": final["x_receipt"]["status"],
            "cleaning": "PASS",
            "canonical_storage": links["status"] if completed else "PENDING",
            "lamin_jkobject": "PASS",
            "collection": collection_receipt["status"],
        },
        "writes": writes,
        "registry_counts": {"before": before_counts, "after": after_counts},
        "replay_noop": mode == "verify" and before_counts == after_counts,
        "rollback": {
            "obs_uid": BASELINE_OBS_UID,
            "var_uid": BASELINE_VAR_UID,
            "X_uid": X_UID,
            "collection_uid": collection_receipt.get("predecessor_uid"),
        },
        "histories": {
            "obs_revisions": final["obs_history_count"],
            "var_revisions": final["var_history_count"],
        },
    }
    receipt["canonical_sha256"] = sha256_bytes(canonical(receipt).encode())
    RECEIPT_PATH.write_text(
        json.dumps(receipt, indent=2, sort_keys=True, default=str) + "\n"
    )
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("plan", "mutate", "verify"))
    args = parser.parse_args()
    receipt = run(args.mode)
    print(
        "GSE269572_COMPLETION="
        + canonical(
            {
                "status": receipt["status"],
                "mode": args.mode,
                "receipt_sha256": receipt["canonical_sha256"],
                "obs_uid": receipt["links"].get("obs", {}).get("uid"),
                "var_uid": receipt["links"].get("var", {}).get("uid"),
                "collection_uid": receipt["collection"].get("successor_uid"),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
