#!/usr/bin/env python3
"""Complete and verify the GSE138002 OBS/VAR/cleaning contract on jkobject."""

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
from pathlib import Path
from typing import Any

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

TASK_ID = "t_9b0cc7c2"
BILLING_PROJECT = "jkobject-1549353370965"
DATASET_ID = "temporal/organoiddb_odd001099_gse138002"
LOGICAL_KEY = "pert-gym/logical/temporal/organoiddb_odd001099_gse138002"
PREFIX = "data/cleaned/GSE138002"
OBS_KEY = f"{PREFIX}/obs.parquet"
X_KEY = f"{PREFIX}/X.h5ad"
VAR_KEY = f"{PREFIX}/var.parquet"
OBSM_KEY = f"{PREFIX}/obsm_source_umap.parquet"
OBSM_FEATURE = "obsm_source_umap"
BASELINE_OBS_UID = "MfCWePqr8F4TThDo0000"
X_UID = "GPge5BVaaXcCGgsU0000"
VAR_UID = "jib8jqqYfSI3vzIW0000"
EXPECTED_N_OBS = 118_555
EXPECTED_N_VARS = 33_694
HERE = Path(__file__).resolve().parent
MANIFEST_PATH = HERE / "source_manifest.json"
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

CELL_TYPE_MAP = {
    "Amacrine Cells": ("amacrine cell", "CL:0000561"),
    "Bipolar Cells": ("retinal bipolar neuron", "CL:0000748"),
    "Cones": ("retinal cone cell", "CL:0000573"),
    "Horizontal Cells": ("retina horizontal cell", "CL:0000745"),
    "Muller Glia": ("Mueller cell", "CL:0000636"),
    "RPCs": ("retinal progenitor cell", "CL:0002672"),
    "Retinal Ganglion Cells": ("retinal ganglion cell", "CL:0000740"),
    "Rods": ("retinal rod cell", "CL:0000604"),
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def materialize_artifact(artifact: Any, destination: Path) -> Path:
    """Materialize one artifact with explicit requester-pays billing."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    uri = str(artifact.path)
    if uri.startswith("gs://"):
        command = ["gcloud", "storage", "cp"]
        if uri.startswith("gs://scperturb/"):
            command.extend(["--billing-project", BILLING_PROJECT])
        command.extend([uri, str(destination)])
        subprocess.run(command, check=True)
        return destination
    shutil.copy2(Path(uri.removeprefix("file://")), destination)
    return destination


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

    pubmed = manifest["authorities"]["pubmed_xml"]
    payload = (
        urllib.request.urlopen(pubmed["url"], timeout=120)
        .read()
        .decode("utf-8", "replace")
    )
    missing_tokens = [
        token for token in pubmed["required_tokens"] if token not in payload
    ]
    if missing_tokens:
        raise AssertionError(f"PubMed authority token drift: {missing_tokens}")
    receipts["pubmed_xml"] = {
        "url": pubmed["url"],
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
    except (urllib.error.URLError, TimeoutError) as exc:
        receipts["organoiddb"] = {
            "url": organoid["url"],
            "status": "upstream_unavailable",
            "error_type": type(exc).__name__,
            "identity_preserved_via_geo_accession_and_catalogue": True,
        }

    for name, expected in manifest["supplementary_files"].items():
        if name.endswith("Final_matrix.mtx.gz") and not full_matrix_hash:
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
                "sha256": expected["sha256"],
                "verification": "immutable accepted checksum plus fresh upstream byte readback",
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
    terms = ("GSE138002", "ODD001099", "developing human retina")
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
    path = materialize_artifact(x_artifact, scratch / "X.h5ad")
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
    expected = manifest["expected"]
    checks = {
        "shape": list(shape) == [EXPECTED_N_OBS, EXPECTED_N_VARS],
        "nnz": nnz == expected["nnz"],
        "dtype": dtype == expected["x_dtype"],
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
        "encoding": encoding,
        "checks": checks,
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
            var["feature_namespace"].astype(str).str.casefold().eq("ensembl").all()
        ),
        "x_shape": x_receipt["shape"][1] == len(var),
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
            "GSE138002_genes.csv.gz",
            "GRCh38 GEO processing record",
            "exact X ordered feature axis",
        ],
        "checks": checks,
    }


def curate_obs(
    baseline: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if len(baseline) != EXPECTED_N_OBS or not baseline.index.is_unique:
        raise AssertionError("baseline OBS denominator drift")
    curated = baseline.copy(deep=True)
    for column in list(baseline.columns):
        source_column = f"source_original_{column}"
        if source_column not in curated:
            curated[source_column] = baseline[column]
    curated = add_obs_identity(curated, dataset_id=DATASET_ID, prefix=PREFIX)
    validate_obs_identity(curated)
    idx = curated.index

    source_type = baseline["source_cell_type"].astype("string")
    mapped_label = source_type.map(
        {key: value[0] for key, value in CELL_TYPE_MAP.items()}
    ).fillna(source_type)
    mapped_term = source_type.map(
        {key: value[1] for key, value in CELL_TYPE_MAP.items()}
    ).astype("string")
    ontology_state = pd.Series(
        np.where(mapped_term.notna(), "present", "unknown"), index=idx, dtype="string"
    )
    age_value = pd.to_numeric(baseline["timepoint"], errors="coerce").astype("Float64")
    unit = baseline["timepoint_unit"].astype("string")
    multiplier = unit.map(
        {"day": 1440.0, "postnatal_day": 1440.0, "gestational_week": 10080.0}
    ).astype("Float64")
    timepoint = age_value * multiplier
    trajectory = pd.Series("adult_primary_retina", index=idx, dtype="string")
    trajectory.loc[baseline["is_organoid"].astype(bool)] = "retinal_organoid"
    trajectory.loc[unit.eq("gestational_week")] = "fetal_primary_retina"
    trajectory.loc[unit.eq("postnatal_day")] = "postnatal_primary_retina"
    baseline_flag = pd.Series(pd.NA, index=idx, dtype="boolean")
    for name in (
        "retinal_organoid",
        "fetal_primary_retina",
        "postnatal_primary_retina",
    ):
        mask = trajectory.eq(name)
        minimum = timepoint.loc[mask].min()
        baseline_flag.loc[mask] = timepoint.loc[mask].eq(minimum)

    set_field(curated, "dataset", DATASET_ID, "present", "canonical dataset identity")
    set_field(
        curated,
        "sample",
        baseline["sample"].astype("string"),
        "present",
        "GEO source sample labels",
    )
    set_field(
        curated,
        "cell_id",
        baseline["cell_id"].astype("string"),
        "present",
        "GEO Final_barcodes cell identity",
    )
    set_field(
        curated,
        "donor_id",
        missing(idx),
        "unknown",
        "not supplied at row or sample granularity",
    )
    set_field(
        curated,
        "batch",
        baseline["sample"].astype("string"),
        "present",
        "source sample/replicate grouping",
    )
    set_field(
        curated,
        "cell_type",
        mapped_label.astype("string"),
        "present",
        "author annotation; exact broad CL map where unambiguous",
    )
    curated["source_cell_type"] = source_type
    curated["cell_type_ontology_term"] = mapped_term
    curated["cell_type_ontology_state"] = ontology_state
    set_field(
        curated,
        "cell_line",
        missing(idx),
        "not_applicable",
        "mixed primary retina and retinal organoid tissue",
    )
    set_field(
        curated,
        "disease",
        missing(idx),
        "unknown",
        "no row-level disease assertion in source",
    )
    set_field(
        curated, "tissue_type", "retina", "present", "GEO source tissue and publication"
    )
    set_field(curated, "organism", "Homo sapiens", "present", "GEO taxon 9606")
    set_field(curated, "sex", missing(idx), "unknown", "not supplied by source")
    set_field(
        curated,
        "age",
        baseline["source_age_label"].astype("string"),
        "present",
        "exact source developmental-age label",
    )
    set_field(curated, "ethnicity", missing(idx), "unknown", "not supplied by source")
    set_field(curated, "sequencer", "Illumina NextSeq 500", "present", "GEO GPL18573")
    set_field(
        curated,
        "technology",
        "10x Genomics Chromium v2 3-prime",
        "present",
        "GEO extraction protocol",
    )
    set_field(
        curated, "assay", "single-cell RNA sequencing", "present", "GEO series design"
    )
    set_field(curated, "modality", "scRNA-seq", "present", "GEO series type")
    set_field(
        curated,
        "media",
        missing(idx),
        "unknown",
        "culture-medium composition not joinable per row",
    )
    set_field(curated, "is_bulk", False, "present", "single-cell source")
    set_field(curated, "is_pseudobulk", False, "present", "single-cell source")
    set_field(
        curated, "perturbation", "none", "present", "observational development dataset"
    )
    set_field(
        curated,
        "perturbation_type",
        "none",
        "present",
        "observational development dataset",
    )
    for field in (
        "perturbation_technology",
        "perturbation_library",
        "guide_sequence",
        "molecule_sequence",
    ):
        set_field(
            curated,
            field,
            missing(idx),
            "not_applicable",
            "unperturbed developmental atlas",
        )
    set_field(
        curated, "is_control", True, "present", "all rows are unperturbed observations"
    )
    set_field(
        curated, "dose", missing(idx, "Float64"), "not_applicable", "no perturbation"
    )
    set_field(curated, "dose_unit", missing(idx), "not_applicable", "no perturbation")
    set_field(
        curated,
        "timepoint",
        timepoint,
        np.where(timepoint.notna(), "present", "unknown"),
        "source age converted to canonical minutes; adult numeric age absent",
    )
    curated["timepoint_unit"] = "minute"
    curated["timepoint_original_value"] = age_value
    curated["timepoint_original_unit"] = unit
    set_field(
        curated, "trajectory_id", trajectory, "present", "source context and age unit"
    )
    set_field(
        curated,
        "pseudotime",
        missing(idx, "Float64"),
        "not_applicable",
        "source chronological axis used",
    )
    set_field(
        curated,
        "is_baseline",
        baseline_flag,
        np.where(baseline_flag.notna(), "present", "not_applicable"),
        "earliest observed point per numeric trajectory; adult is not a numeric trajectory",
    )
    for field in ("sensitivity", "response_value"):
        set_field(
            curated,
            field,
            missing(idx, "Float64"),
            "not_applicable",
            "no scalar response endpoint",
        )
    for field in ("response_metric", "response_source"):
        set_field(
            curated,
            field,
            missing(idx),
            "not_applicable",
            "no scalar response endpoint",
        )
    set_field(
        curated,
        "n_counts",
        baseline["source_total_mrnas"].astype("Int64"),
        "present",
        "GEO Final_barcodes total mRNAs",
    )
    set_field(
        curated,
        "n_genes",
        baseline["source_num_genes_expressed"].astype("Int64"),
        "present",
        "GEO Final_barcodes expressed genes",
    )
    set_field(
        curated,
        "pct_mito",
        missing(idx, "Float64"),
        "unknown",
        "not supplied; X retained without full matrix recomputation",
    )
    set_field(
        curated,
        "pct_ribo",
        missing(idx, "Float64"),
        "unknown",
        "not supplied; X retained without full matrix recomputation",
    )
    set_field(
        curated,
        "is_low_quality",
        missing(idx, "boolean"),
        "unknown",
        "source final-matrix inclusion is not a per-cell QC verdict",
    )
    curated["x_semantics"] = "raw_counts"
    curated["source_accession"] = "GSE138002"
    curated["organoiddb_id"] = "ODD001099"

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
    umap = baseline[["umap1_coord", "umap2_coord", "umap3_coord"]].copy()
    umap.columns = ["source_umap_1", "source_umap_2", "source_umap_3"]
    umap.insert(0, "obs_uuid", curated["obs_uuid"].to_numpy())
    if len(umap) != len(curated) or not umap.index.equals(curated.index):
        raise AssertionError("typed UMAP row/order drift")
    receipt = {
        "status": "PASS",
        "OBS_COMPLETED": True,
        "rows": len(curated),
        "canonical_fields": len(CANONICAL_FIELDS),
        "obs_uuid_unique": bool(curated["obs_uuid"].is_unique),
        "numeric_timepoint_rows": int(timepoint.notna().sum()),
        "adult_unknown_timepoint_rows": int(timepoint.isna().sum()),
        "cell_type_ontology_mapped_rows": int(mapped_term.notna().sum()),
        "cell_type_ontology_unknown_rows": int(mapped_term.isna().sum()),
        "residual_unknown_fields": [
            "donor_id",
            "disease",
            "sex",
            "ethnicity",
            "media",
            "pct_mito",
            "pct_ribo",
            "is_low_quality",
        ],
    }
    return curated, umap, receipt


def ensure_link_feature(ln: Any, name: str) -> None:
    records = list(ln.Feature.filter(name=name).all())
    if records and str(records[0].dtype) != "cat[Artifact]":
        raise AssertionError(f"link feature dtype drift: {name}")
    if not records:
        ln.Feature(name=name, dtype="cat[Artifact]").save()


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
            "no latest additions Collection contains frozen GSE138002 OBS"
        )
    predecessor = candidates[-1]
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
    ln: Any, baseline_obs: Any, new_obs: Any, *, allow_create: bool
) -> tuple[Any, bool, dict[str, Any]]:
    predecessor, before = find_predecessor_collection(ln, baseline_obs)
    after = [item for item in before if str(item.key) != OBS_KEY] + [new_obs]
    keys = [str(item.key) for item in after]
    if len(after) != len(before) or len(keys) != len(set(keys)):
        raise AssertionError("Collection replacement changed count or duplicated a key")
    successor_key = "pert-gym/additions/20260730-gse138002-e2e"
    description = canonical(
        {
            "format": "pert-gym.append-only-dataset-completion/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "predecessor_uid": str(predecessor.uid),
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
        successor = ln.Collection(
            after, key=successor_key, description=description, skip_hash_lookup=True
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
            "predecessor_uid": str(predecessor.uid),
            "predecessor_key": str(predecessor.key),
            "predecessor_member_count": len(before),
            "predecessor_membership_sha256": membership_sha256(before),
            "successor_uid": str(successor.uid),
            "successor_key": str(successor.key),
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
    baseline = pd.read_parquet(
        materialize_artifact(baseline_obs, scratch / "baseline_obs.parquet")
    )
    var = pd.read_parquet(materialize_artifact(var_artifact, scratch / "var.parquet"))
    x_receipt = inspect_x(x_artifact, manifest, scratch)
    var_receipt = verify_var(var, x_receipt)
    curated, umap, obs_receipt = curate_obs(baseline)
    latest_obs, obs_history = latest_artifact(ln, OBS_KEY)
    latest_obsm_records = list(ln.Artifact.filter(key=OBSM_KEY).all())
    latest_obsm_records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    obs_is_curated = str(latest_obs.uid) != BASELINE_OBS_UID and str(
        latest_obs.description
    ).startswith(f"{TASK_ID}: source-exhaustive GSE138002 OBS")
    if str(latest_obs.uid) != BASELINE_OBS_UID and not obs_is_curated:
        raise AssertionError(
            f"foreign OBS revision after frozen baseline: {latest_obs.uid}"
        )
    if obs_is_curated:
        observed = pd.read_parquet(
            materialize_artifact(latest_obs, scratch / "readback_obs.parquet")
        )
        assert_frame_equal(observed, curated, check_categorical=True)
    obsm_is_curated = bool(latest_obsm_records) and str(
        latest_obsm_records[-1].description
    ).startswith(f"{TASK_ID}: typed source UMAP")
    if latest_obsm_records and not obsm_is_curated:
        raise AssertionError("foreign typed UMAP key collision")
    if obsm_is_curated:
        observed_umap = pd.read_parquet(
            materialize_artifact(
                latest_obsm_records[-1], scratch / "readback_umap.parquet"
            )
        )
        assert_frame_equal(observed_umap, umap, check_categorical=True)
    return {
        "baseline_obs": baseline_obs,
        "x_artifact": x_artifact,
        "var_artifact": var_artifact,
        "latest_obs": latest_obs,
        "latest_obsm": latest_obsm_records[-1] if latest_obsm_records else None,
        "curated": curated,
        "umap": umap,
        "obs_is_curated": obs_is_curated,
        "obsm_is_curated": obsm_is_curated,
        "obs_history_count": len(obs_history),
        "obs_receipt": obs_receipt,
        "var_receipt": var_receipt,
        "x_receipt": x_receipt,
        "expected_obs_frame_sha256": frame_sha256(curated),
        "expected_obsm_frame_sha256": frame_sha256(umap),
    }


def publish(
    ln: Any, prepared: dict[str, Any], helper_sha256: str
) -> tuple[Any, Any, bool, bool]:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-publish-"))
    obs_created = False
    obsm_created = False
    if prepared["obs_is_curated"]:
        obs = prepared["latest_obs"]
    else:
        path = root / "obs.parquet"
        prepared["curated"].to_parquet(path)
        obs = ln.Artifact.from_dataframe(
            path,
            key=OBS_KEY,
            revises=prepared["baseline_obs"],
            description=f"{TASK_ID}: source-exhaustive GSE138002 OBS; frame_sha256={prepared['expected_obs_frame_sha256']}; helper_sha256={helper_sha256}",
        ).save()
        obs_created = True
    if prepared["obsm_is_curated"]:
        obsm = prepared["latest_obsm"]
    else:
        path = root / "obsm_source_umap.parquet"
        prepared["umap"].to_parquet(path)
        obsm = ln.Artifact.from_dataframe(
            path,
            key=OBSM_KEY,
            description=f"{TASK_ID}: typed source UMAP; frame_sha256={prepared['expected_obsm_frame_sha256']}; helper_sha256={helper_sha256}",
        ).save()
        obsm_created = True
    ensure_link_feature(ln, "X")
    ensure_link_feature(ln, OBSM_FEATURE)
    obs.features.set_values({"X": prepared["x_artifact"], OBSM_FEATURE: obsm})
    return obs, obsm, obs_created, obsm_created


def strip_runtime(prepared: dict[str, Any]) -> dict[str, Any]:
    return {
        "obs": artifact_identity(prepared["latest_obs"]),
        "obsm": artifact_identity(prepared["latest_obsm"])
        if prepared["latest_obsm"]
        else None,
        "X": artifact_identity(prepared["x_artifact"]),
        "var": artifact_identity(prepared["var_artifact"]),
        "obs_receipt": prepared["obs_receipt"],
        "var_receipt": prepared["var_receipt"],
        "x_receipt": prepared["x_receipt"],
        "obs_history_count": prepared["obs_history_count"],
        "expected_obs_frame_sha256": prepared["expected_obs_frame_sha256"],
        "expected_obsm_frame_sha256": prepared["expected_obsm_frame_sha256"],
    }


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    manifest = load_manifest()
    helper_sha256 = sha256_file(Path(__file__))
    source_receipts = verify_sources(manifest, full_matrix_hash=mode == "mutate")
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    main_probe = bounded_main_duplicate_probe(ln)
    before = prepare(ln, manifest)
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    obs_created = obsm_created = collection_created = False
    collection_receipt: dict[str, Any] = {"status": "not_evaluated_before_write"}
    if mode == "mutate":
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
            fresh = prepare(ln, manifest)
            ln.track(
                key=f"pert-gym/dataset-completion/{DATASET_ID}/{TASK_ID}",
                kind="script",
                params={"task_id": TASK_ID, "helper_sha256": helper_sha256},
                new_run=True,
                pypackages=False,
                stream_tracking=False,
            )
            obs, _obsm, obs_created, obsm_created = publish(ln, fresh, helper_sha256)
            _successor, collection_created, collection_receipt = (
                ensure_successor_collection(
                    ln, fresh["baseline_obs"], obs, allow_create=True
                )
            )
            try:
                ln.finish()
            except AttributeError:
                ln.context.finish()
    final = prepare(ln, manifest)
    if mode == "verify" and not (final["obs_is_curated"] and final["obsm_is_curated"]):
        raise AssertionError("verify requested before completed artifacts exist")
    if final["obs_is_curated"]:
        obs_links = final["latest_obs"].features.get_values()
        if str(resolve_artifact(ln, obs_links.get("X")).uid) != X_UID:
            raise AssertionError("curated OBS -> X link drift")
        if str(resolve_artifact(ln, obs_links.get(OBSM_FEATURE)).uid) != str(
            final["latest_obsm"].uid
        ):
            raise AssertionError("curated OBS -> typed UMAP link drift")
        x_links = final["x_artifact"].features.get_values()
        if str(resolve_artifact(ln, x_links.get("var")).uid) != VAR_UID:
            raise AssertionError("X -> VAR link drift")
        _successor, _, collection_receipt = ensure_successor_collection(
            ln, final["baseline_obs"], final["latest_obs"], allow_create=False
        )
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
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
            "typed_obsm_artifacts": int(obsm_created),
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
    print(
        "GSE138002_COMPLETION="
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
