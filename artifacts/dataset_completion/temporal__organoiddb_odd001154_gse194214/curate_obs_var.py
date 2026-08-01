#!/usr/bin/env python3
"""Append-only, source-exhaustive ODD001154/GSE194214 OBS curation."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import platform
import re
import sys
import tempfile
import time
import urllib.request
import warnings
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import anndata as ad
import fsspec
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from scipy import sparse

import tools.pert_gym_vm_runner as vm_runner
from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

# Transferred immutable helpers execute from /tmp on the EU VM; point the
# runner's legacy-lock discovery at the actual remote repository explicitly.
PROJECT_ROOT = Path(
    os.environ.get("PERT_GYM_REPO_ROOT", Path(__file__).resolve().parents[3])
).resolve()
if os.environ.get("PERT_GYM_REPO_ROOT"):
    vm_runner.ROOT = PROJECT_ROOT

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

TASK_ID = "t_56a7b7cf"
DATASET_ID = "temporal/organoiddb_odd001154_gse194214"
LOGICAL_KEY = "pert-gym/logical/temporal/organoiddb_odd001154_gse194214"
PREFIX = "data/cleaned/GSE194214"
OBS_KEY = f"{PREFIX}/obs.parquet"
X_KEY = f"{PREFIX}/X.h5ad"
VAR_KEY = f"{PREFIX}/var.parquet"
EXPECTED_OBS_UID = "GtDvEO1BsANR8VKR0000"
EXPECTED_X_UID = "FIfScz6bImLLe9cD0000"
EXPECTED_VAR_UID = "KFzdzY1k7TreexTs0000"
EXPECTED_X_HASH = "M1zaZQKPlogEaSdoxnLn6A"
EXPECTED_SHAPE = (18_716, 33_694)
EXPECTED_NNZ = 69_489_485
EXPECTED_SUM = 310_355_119
EXPECTED_MAX = 2_436
PREDECESSOR_UID = "whLgwg8opPvW9qdN0000"
PREDECESSOR_KEY = "pert-gym/additions/20260730-gse130238-e2e"
PREDECESSOR_MEMBER_COUNT = 1_018
SUCCESSOR_KEY = "pert-gym/additions/20260730-odd001154-gse194214-e2e"
BILLING_PROJECT = "jkobject-1549353370965"
HERE = Path(__file__).resolve().parent
MANIFEST_PATH = HERE / "source_manifest.json"
OBS_CONTRACT_PATH = PROJECT_ROOT / "config/obs_completed_contract_v1.json"
JOURNAL_PATH = Path.home() / ".cache/pert-gym/curation_journal" / f"{TASK_ID}.json"
ALIASES = (
    "GSE194214",
    "ODD001154",
    "Odd001154",
    "organoiddb_odd001154",
    "10.7554/eLife.68925",
    "Paraxial mesoderm organoids",
    "Somitoid",
)

CANONICAL_OBS_FIELDS = (
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
PRIOR_TASK_OBS_UID = "GtDvEO1BsANR8VKR0001"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def ordered_sha256(values: pd.Index) -> str:
    return sha256_bytes(("\n".join(values.astype(str)) + "\n").encode())


def frame_sha256(frame: pd.DataFrame) -> str:
    schema = canonical([(str(c), str(d)) for c, d in frame.dtypes.items()])
    values = pd.util.hash_pandas_object(frame, index=True).values.tobytes()
    return sha256_bytes(schema.encode() + values)


def write_journal(phase: str, **extra: Any) -> None:
    JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "format": "pert-gym.odd001154-curation-journal/v1",
        "task_id": TASK_ID,
        "phase": phase,
        "updated_at": int(time.time()),
        **extra,
    }
    temp = JOURNAL_PATH.with_suffix(".tmp")
    temp.write_text(canonical(entry) + "\n", encoding="utf-8")
    temp.replace(JOURNAL_PATH)


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: Any, source: str
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def artifact_by_uid(ln: Any, uid: str) -> Any:
    records = [
        item for item in ln.Artifact.filter(uid=uid).all() if str(item.uid) == uid
    ]
    if len(records) != 1:
        raise AssertionError(f"artifact identity drift: {uid}")
    return records[0]


def collection_by_uid(ln: Any, uid: str) -> Any:
    records = [
        item for item in ln.Collection.filter(uid=uid).all() if str(item.uid) == uid
    ]
    if len(records) != 1:
        raise AssertionError(f"Collection identity drift: {uid}")
    return records[0]


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records or not bool(records[-1].is_latest):
        raise AssertionError(f"latest artifact absent/drifted: {key}")
    return records[-1], records


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size) if artifact.size is not None else None,
        "created_at": str(artifact.created_at),
    }


def member_identity(members: list[Any]) -> list[dict[str, str]]:
    return sorted(
        ({"uid": str(x.uid), "key": str(x.key)} for x in members),
        key=lambda item: (item["key"], item["uid"]),
    )


def membership_sha256(members: list[Any]) -> str:
    return sha256_bytes(canonical(member_identity(members)).encode())


def verify_manifest() -> dict[str, Any]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    contract = json.loads(OBS_CONTRACT_PATH.read_text(encoding="utf-8"))
    if tuple(contract["canonical_obs_columns"]) != CANONICAL_OBS_FIELDS:
        raise AssertionError("binding OBS contract field drift")
    if (
        manifest["task_id"] != TASK_ID
        or manifest["dataset_id"] != DATASET_ID
        or manifest["logical_key"] != LOGICAL_KEY
    ):
        raise AssertionError("source manifest identity drift")
    frozen = manifest["frozen_lamin_inputs"]
    expected = {
        "obs": EXPECTED_OBS_UID,
        "X": EXPECTED_X_UID,
        "var": EXPECTED_VAR_UID,
        "additions_predecessor": PREDECESSOR_UID,
    }
    if any(frozen[role]["uid"] != uid for role, uid in expected.items()):
        raise AssertionError("frozen Lamin identity drift")
    return manifest


def verify_authorities(manifest: dict[str, Any]) -> dict[str, Any]:
    receipts: dict[str, Any] = {}
    for name, expected in manifest["source_authorities"].items():
        payload = urllib.request.urlopen(expected["url"], timeout=120).read()
        actual = {
            "url": expected["url"],
            "size": len(payload),
            "sha256": sha256_bytes(payload),
        }
        if actual["size"] != expected["size"] or actual["sha256"] != expected["sha256"]:
            raise AssertionError(f"source authority drift: {name}")
        receipts[name] = actual
        if name == "geo_family_soft":
            text = gzip.decompress(payload).decode("utf-8", "replace")
            titles: dict[str, str] = {}
            for accession, block in re.findall(
                r"\^SAMPLE = (GSM\d+)(.*?)(?=\n\^SAMPLE|\Z)", text, re.S
            ):
                title = re.search(r"!Sample_title = ([^\r\n]+)", block)
                if title is None:
                    raise AssertionError(f"GEO title absent: {accession}")
                titles[accession] = title.group(1)
            if titles != expected["sample_titles"]:
                raise AssertionError("GEO sample title drift")
        elif name == "elife_supplementary_marker_table":
            table = pd.read_csv(io.BytesIO(payload))
            labels = sorted(table["cluster"].dropna().astype(str).unique())
            if len(table) != expected["rows"] or labels != expected["cluster_labels"]:
                raise AssertionError("supplementary marker table drift")
    return receipts


def materialize(
    artifact: Any, root: Path, role: str, expected_sha256: str | None = None
) -> Path:
    source = str(artifact.path)
    if source.startswith("gs://"):
        target = root / f"{role}-{Path(str(artifact.key)).name}"
        fs = fsspec.filesystem(
            "gcs", project=BILLING_PROJECT, requester_pays=True, version_aware=True
        )
        fs.get_file(source.removeprefix("gs://"), str(target))
    else:
        target = Path(artifact.cache())
    if expected_sha256 is not None and sha256_file(target) != expected_sha256:
        raise AssertionError(f"{role} payload sha256 drift")
    return target


def normalized_csr(matrix: Any) -> sparse.csr_matrix:
    result = sparse.csr_matrix(matrix, copy=True)
    result.sum_duplicates()
    result.eliminate_zeros()
    result.sort_indices()
    return result


def matrix_qc(
    x_artifact: Any,
    baseline: pd.DataFrame,
    var: pd.DataFrame,
    root: Path,
    manifest: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    x_path = materialize(
        x_artifact, root, "X", manifest["accepted_component"]["objects"]["X"]["sha256"]
    )
    adata = ad.read_h5ad(x_path)
    try:
        matrix = normalized_csr(adata.X)
        obs_axis = pd.Index(adata.obs_names.astype(str))
        var_axis = pd.Index(adata.var_names.astype(str))
    finally:
        del adata
    if matrix.shape != EXPECTED_SHAPE or matrix.nnz != EXPECTED_NNZ:
        raise AssertionError("accepted X shape/nnz drift")
    if int(matrix.sum()) != EXPECTED_SUM or int(matrix.max()) != EXPECTED_MAX:
        raise AssertionError("accepted X values drift")
    if not obs_axis.equals(pd.Index(baseline.index.astype(str))) or not var_axis.equals(
        pd.Index(var.index.astype(str))
    ):
        raise AssertionError("OBS/X/VAR axis drift")
    names = var["gene_symbol"].astype("string")
    counts = np.asarray(matrix.sum(axis=1)).ravel().astype(np.int64)
    genes = matrix.getnnz(axis=1).astype(np.int64)
    mito_mask = names.str.startswith("MT-").fillna(False).to_numpy(dtype=bool)
    ribo_mask = names.str.match(r"^RP[SL]").fillna(False).to_numpy(dtype=bool)
    mito = np.asarray(matrix[:, mito_mask].sum(axis=1)).ravel()
    ribo = np.asarray(matrix[:, ribo_mask].sum(axis=1)).ravel()
    if (counts <= 0).any():
        raise AssertionError("zero-count accepted cell")
    qc = pd.DataFrame(
        {
            "n_counts": counts,
            "n_genes": genes,
            "pct_mito": mito / counts * 100.0,
            "pct_ribo": ribo / counts * 100.0,
        },
        index=baseline.index,
    )
    complexity = np.log10(qc["n_genes"].clip(lower=1)) / np.log10(
        qc["n_counts"].clip(lower=2)
    )
    qc["source_qc_complexity"] = complexity
    qc["source_initial_qc_failure"] = (
        (qc["n_counts"] < 500)
        | (qc["n_genes"] < 200)
        | (qc["pct_mito"] > 20.0)
        | (complexity < 0.8)
    )
    return qc, {
        "shape": list(matrix.shape),
        "nnz": int(matrix.nnz),
        "finite_value_sum": int(matrix.sum()),
        "max_count": int(matrix.max()),
        "obs_axis_sha256": ordered_sha256(obs_axis),
        "var_axis_sha256": ordered_sha256(var_axis),
        "x_semantics": "raw_counts",
        "X_rewrite_required": False,
    }


def verify_var(var: pd.DataFrame) -> dict[str, Any]:
    required = {
        "feature_id",
        "gene_symbol",
        "feature_type",
        "feature_namespace",
        "organism",
        "genome_build",
    }
    stable = (
        var["feature_id"].astype("string")
        if "feature_id" in var
        else pd.Series([], dtype="string")
    )
    checks = {
        "rows": len(var) == EXPECTED_SHAPE[1],
        "columns": required.issubset(var.columns),
        "index_unique": bool(var.index.is_unique),
        "stable_unique": bool(stable.is_unique),
        "stable_syntax": bool(stable.str.fullmatch(r"ENSG\d{11}", na=False).all()),
        "stable_axis": pd.Index(stable.astype(str)).equals(
            pd.Index(var.index.astype(str))
        ),
        "feature_type": var.get("feature_type", pd.Series())
        .astype(str)
        .eq("Gene Expression")
        .all(),
        "namespace": var.get("feature_namespace", pd.Series())
        .astype(str)
        .eq("Ensembl gene ID")
        .all(),
        "organism": var.get("organism", pd.Series())
        .astype(str)
        .eq("Homo sapiens")
        .all(),
        "genome_build": var.get("genome_build", pd.Series())
        .astype(str)
        .eq("GRCh38")
        .all(),
    }
    if not all(checks.values()):
        raise AssertionError(f"accepted VAR drift: {checks}")
    return {
        **checks,
        "rows": len(var),
        "needs_revision": False,
        "rewrite_reason": "none; exact source-backed Ensembl GRCh38 axis is complete",
    }


def curate_obs(
    baseline: pd.DataFrame, qc: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if len(baseline) != EXPECTED_SHAPE[0] or not baseline.index.is_unique:
        raise AssertionError("baseline OBS identity drift")
    curated = baseline.copy(deep=True)
    for field in ("timepoint", "timepoint_unit", "organism", "assay", "is_control"):
        if field in baseline and f"source_original_{field}" not in curated:
            curated[f"source_original_{field}"] = baseline[field]
    index = curated.index
    set_field(curated, "dataset", DATASET_ID, "present", "canonical logical dataset")
    set_field(
        curated,
        "sample",
        baseline["sample_accession"].astype("string"),
        "present",
        "GEO sample accession",
    )
    set_field(
        curated,
        "cell_id",
        pd.Series(index.astype(str), index=index, dtype="string"),
        "present",
        "accepted unique OBS axis",
    )
    set_field(
        curated,
        "batch",
        missing(index),
        "missing",
        "multiple organoids were pooled without a cell-level batch map",
    )
    set_field(
        curated,
        "donor_id",
        missing(index),
        "missing",
        "NCRM1 donor identity absent from source authorities",
    )
    set_field(
        curated,
        "replicate",
        missing(index),
        "missing",
        "multiple organoids were pooled without a cell-level replicate map",
    )
    for field in ("plate_id", "well_id"):
        set_field(
            curated,
            field,
            missing(index),
            "not_applicable",
            "droplet single-cell assay",
        )
    set_field(
        curated,
        "cell_type",
        missing(index),
        "missing",
        "publication cluster labels lack a barcode-to-cluster map",
    )
    set_field(
        curated,
        "cell_line",
        "NCRM1",
        "present",
        "GEO source name and publication methods",
    )
    set_field(
        curated,
        "tissue_type",
        "paraxial mesoderm organoid (Somitoid)",
        "present",
        "GEO characteristics and publication",
    )
    set_field(curated, "organism", "Homo sapiens", "present", "GEO organism")
    set_field(
        curated,
        "disease",
        missing(index),
        "not_applicable",
        "normal developmental organoid model",
    )
    set_field(curated, "age", missing(index, "Float64"), "missing", "donor age absent")
    for field in ("sex", "ethnicity"):
        set_field(curated, field, missing(index), "missing", f"donor {field} absent")
    set_field(
        curated,
        "sequencer",
        "Illumina NovaSeq 6000",
        "present",
        "GEO platform GPL24676",
    )
    set_field(
        curated,
        "technology",
        "10x Genomics Chromium Single Cell 3' v3",
        "present",
        "GEO library strategy and publication methods",
    )
    set_field(curated, "assay", "scRNA-seq", "present", "GEO experiment type")
    set_field(curated, "modality", "scRNA-seq", "present", "GEO experiment type")
    set_field(
        curated,
        "media",
        missing(index),
        "missing",
        "protocol varies by developmental day and collection boundary is not mapped per cell",
    )
    set_field(
        curated,
        "treatment",
        "optimized Somitoid differentiation protocol",
        "present",
        "publication protocol used for sequenced trajectory",
    )
    for field in (
        "perturbation",
        "perturbation_type",
        "perturbation_technology",
        "perturbation_library",
        "guide_id",
        "guide_sequence",
        "perturbation_target",
        "dose",
        "dose_unit",
    ):
        dtype = "Float64" if field == "dose" else "string"
        set_field(
            curated,
            field,
            missing(index, dtype),
            "not_applicable",
            "developmental trajectory, not a perturbation-response arm",
        )
    set_field(
        curated,
        "timepoint",
        baseline["timepoint"].astype("Float64"),
        "present",
        "GEO developmental stage Day 1/2/3/5",
    )
    set_field(curated, "timepoint_unit", "day", "present", "GEO developmental stage")
    set_field(
        curated,
        "condition",
        baseline["sample_title"].astype("string"),
        "present",
        "GEO sample title",
    )
    set_field(
        curated,
        "is_control",
        False,
        "present",
        "no GEO sample is designated a control; Day 1 is an early timepoint",
    )
    set_field(
        curated,
        "control_availability",
        "no_explicit_control; developmental_timecourse",
        "present",
        "GEO sample design",
    )
    for field in ("n_counts", "n_genes", "pct_mito", "pct_ribo"):
        set_field(
            curated, field, qc[field], "present", "exact accepted raw-count matrix"
        )
    low_quality = pd.Series(pd.NA, index=index, dtype="boolean")
    failures = qc["source_initial_qc_failure"].astype(bool)
    low_quality.loc[failures] = True
    set_field(
        curated,
        "is_low_quality",
        low_quality,
        np.where(failures, "present", "missing"),
        "publication initial QC thresholds; remaining cells unresolved because excluded cluster barcodes are unpublished",
    )
    set_field(
        curated,
        "pseudotime",
        missing(index, "Float64"),
        "missing",
        "publication supplies chronological day but no per-cell pseudotime",
    )
    for field in ("response_value", "response_type"):
        dtype = "Float64" if field == "response_value" else "string"
        set_field(
            curated,
            field,
            missing(index, dtype),
            "not_applicable",
            "no scalar response endpoint",
        )
    set_field(curated, "is_bulk", False, "present", "GEO single-cell experiment type")
    set_field(
        curated,
        "is_pseudobulk",
        False,
        "present",
        "accepted OBS axis contains individual cell barcodes",
    )
    set_field(
        curated,
        "molecule_sequence",
        missing(index),
        "not_applicable",
        "developmental expression atlas without a perturbation molecule",
    )
    set_field(
        curated,
        "trajectory_id",
        baseline["trajectory_id"].astype("string"),
        "present",
        "GSE194214 Somitoid developmental series identity",
    )
    set_field(
        curated,
        "is_baseline",
        baseline["timepoint"].eq(baseline["timepoint"].min()).astype("boolean"),
        "present",
        "derived earliest measured developmental timepoint (Day 1); not an untreated control",
    )
    for field in ("sensitivity", "response_metric", "response_source"):
        dtype = "Float64" if field == "sensitivity" else "string"
        set_field(
            curated,
            field,
            missing(index, dtype),
            "not_applicable",
            "developmental expression atlas without a scalar response endpoint",
        )
    curated["source_qc_complexity"] = qc["source_qc_complexity"]
    curated["source_initial_qc_failure"] = qc["source_initial_qc_failure"].astype(
        "boolean"
    )
    curated["development_stage_state"] = "present"
    curated["development_stage_source"] = "GEO sample characteristics"
    curated["source"] = "GEO"
    curated["x_semantics"] = "raw_counts"
    if len(curated) != len(baseline) or not curated.index.equals(baseline.index):
        raise AssertionError("OBS row/order drift")
    return curated, {
        "rows": len(curated),
        "sample_counts": baseline["sample_accession"]
        .value_counts()
        .sort_index()
        .to_dict(),
        "timepoint_counts": baseline["timepoint"].value_counts().sort_index().to_dict(),
        "initial_qc_failure_rows": int(failures.sum()),
        "unresolved_low_quality_rows": int((~failures).sum()),
        "source_day1_control_rows_corrected": int(
            baseline["is_control"].astype(bool).sum()
        ),
    }


def field_dispositions(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in CANONICAL_OBS_FIELDS:
        if (
            field not in frame
            or f"{field}_state" not in frame
            or f"{field}_source" not in frame
        ):
            raise AssertionError(f"canonical OBS evidence absent: {field}")
        state = frame[f"{field}_state"].astype("string")
        if not state.isin({"present", "missing", "not_applicable"}).all():
            raise AssertionError(f"invalid canonical OBS state: {field}")
        result[field] = {
            "state_counts": state.value_counts(dropna=False).sort_index().to_dict(),
            "present_rows": int(state.eq("present").sum()),
            "missing_rows": int(state.eq("missing").sum()),
            "not_applicable_rows": int(state.eq("not_applicable").sum()),
            "non_null_rows": int(frame[field].notna().sum()),
            "total_rows": len(frame),
            "sources": sorted(
                frame[f"{field}_source"].dropna().astype(str).unique().tolist()
            ),
        }
    if tuple(result) != CANONICAL_OBS_FIELDS:
        raise AssertionError("canonical OBS field order/coverage drift")
    return result


def scientific_context(obs_receipt: dict[str, Any]) -> dict[str, Any]:
    frequencies = {
        str(key): int(value) for key, value in obs_receipt["timepoint_counts"].items()
    }
    return {
        "scientific_modality": {
            "value": "single-cell RNA-seq developmental expression atlas",
            "assay": "10x Genomics Chromium Single Cell 3' v3",
            "x_semantics": "raw UMI counts",
            "perturbation_status": "not_applicable",
            "assertion_level": "dataset",
            "experimental_unit": "single cell from pooled NCRM1 iPSC-derived Somitoids",
            "source": "GEO GSE194214 and eLife 68925",
        },
        "experimental_axes": [
            {
                "name": "developmental_time",
                "raw_field": "development_stage",
                "raw_values": ["Day 1", "Day 2", "Day 3", "Day 5"],
                "canonical_field": "timepoint",
                "values": [1, 2, 3, 5],
                "unit": "day",
                "cardinality": 4,
                "frequencies_by_observation": frequencies,
                "assertion_level": "observation",
                "source": "GEO sample titles and characteristics",
                "classification": "chronological developmental time; not pseudotime, batch, or endpoint",
            }
        ],
        "outcomes_endpoints": {
            "status": "not_applicable",
            "items": [],
            "assertion_level": "dataset",
            "source": "GEO design and eLife study describe developmental expression without a scalar response endpoint",
        },
    }


def duplicate_probe(ln: Any) -> dict[str, Any]:
    active = int(ln.setup.settings.branch.id)
    found: dict[str, Any] = {}
    for alias in ALIASES:
        for field in ("key", "description"):
            for artifact in ln.Artifact.filter(
                **{f"{field}__icontains": alias}
            ).order_by("-created_at")[:50]:
                found[str(artifact.uid)] = artifact
    inherited = [
        artifact_identity(item)
        for item in found.values()
        if int(item.created_on_id) != active
    ]
    if inherited:
        raise AssertionError(
            f"latest-main/public scientific-equivalence duplicate found: {inherited}"
        )
    return {
        "queries": list(ALIASES),
        "match_count": len(found),
        "public_or_inherited_match_count": 0,
        "legacy_parallel_triplet_uids": sorted(
            uid
            for uid, item in found.items()
            if str(item.key).startswith("temporal_pretraining/")
        ),
    }


def prepare(ln: Any, manifest: dict[str, Any], root: Path) -> dict[str, Any]:
    baseline_artifact = artifact_by_uid(ln, EXPECTED_OBS_UID)
    x_artifact = artifact_by_uid(ln, EXPECTED_X_UID)
    var_artifact = artifact_by_uid(ln, EXPECTED_VAR_UID)
    if (
        str(baseline_artifact.key) != OBS_KEY
        or str(x_artifact.key) != X_KEY
        or str(x_artifact.hash) != EXPECTED_X_HASH
        or str(var_artifact.key) != VAR_KEY
    ):
        raise AssertionError("frozen artifact drift")
    baseline_path = materialize(
        baseline_artifact,
        root,
        "baseline-obs",
        manifest["accepted_component"]["objects"]["obs"]["sha256"],
    )
    var_path = materialize(
        var_artifact,
        root,
        "var",
        manifest["accepted_component"]["objects"]["var"]["sha256"],
    )
    baseline = pd.read_parquet(baseline_path)
    var = pd.read_parquet(var_path)
    var_receipt = verify_var(var)
    qc, x_receipt = matrix_qc(x_artifact, baseline, var, root, manifest)
    expected_obs, obs_receipt = curate_obs(baseline, qc)
    expected_hash = frame_sha256(expected_obs)
    latest, history = latest_artifact(ln, OBS_KEY)
    if str(latest.uid) == EXPECTED_OBS_UID:
        curated = False
    elif str(latest.description).startswith(
        f"{TASK_ID}: source-exhaustive ODD001154 OBS"
    ):
        actual = latest.load()
        if (
            str(latest.uid) == PRIOR_TASK_OBS_UID
            and frame_sha256(actual) != expected_hash
        ):
            curated = False
        else:
            assert_frame_equal(actual, expected_obs, check_categorical=True)
            if frame_sha256(actual) != expected_hash:
                raise AssertionError("curated OBS frame hash drift")
            curated = True
    else:
        raise AssertionError(
            f"foreign OBS revision after frozen baseline: {latest.uid}"
        )
    return {
        "baseline_artifact": baseline_artifact,
        "x_artifact": x_artifact,
        "var_artifact": var_artifact,
        "latest_obs_artifact": latest,
        "expected_obs": expected_obs,
        "expected_obs_sha256": expected_hash,
        "obs_curated": curated,
        "obs_history_count": len(history),
        "obs_receipt": obs_receipt,
        "x_receipt": x_receipt,
        "var_receipt": var_receipt,
        "field_dispositions": field_dispositions(expected_obs),
    }


def successor_description(
    new_obs: Any, old_obs: Any, predecessor: Any, before: list[Any], after: list[Any]
) -> str:
    return canonical(
        {
            "format": "pert-gym.append-only-dataset-e2e-successor/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "predecessor_uid": str(predecessor.uid),
            "predecessor_key": str(predecessor.key),
            "predecessor_membership_sha256": membership_sha256(before),
            "replaced_obs_uid": str(old_obs.uid),
            "added_obs_uid": str(new_obs.uid),
            "member_count_before": len(before),
            "member_count_after": len(after),
            "resulting_membership_sha256": membership_sha256(after),
            "membership_rule": "immutable predecessor with same-key OBS replaced by exact source-curated OBS",
            "rollback": f"select immutable predecessor Collection {predecessor.uid}",
        }
    )


def ensure_successor(
    ln: Any, new_obs: Any, allow_create: bool = False
) -> tuple[Any, bool, dict[str, Any]]:
    predecessor = collection_by_uid(ln, PREDECESSOR_UID)
    if str(predecessor.key) != PREDECESSOR_KEY:
        raise AssertionError("predecessor key drift")
    before = list(predecessor.artifacts.all())
    if len(before) != PREDECESSOR_MEMBER_COUNT:
        raise AssertionError("predecessor member count drift")
    matches = [item for item in before if str(item.key) == OBS_KEY]
    if len(matches) != 1 or str(matches[0].uid) != EXPECTED_OBS_UID:
        raise AssertionError("predecessor target OBS drift")
    after = [item for item in before if str(item.key) != OBS_KEY] + [new_obs]
    keys = [str(item.key) for item in after]
    if len(after) != len(before) or len(keys) != len(set(keys)):
        raise AssertionError("successor key uniqueness/count drift")
    description = successor_description(new_obs, matches[0], predecessor, before, after)
    existing = list(ln.Collection.filter(key=SUCCESSOR_KEY).all())
    existing.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    created = False
    if existing:
        successor = existing[-1]
        current = list(successor.artifacts.all())
        if str(successor.description) != description or member_identity(
            current
        ) != member_identity(after):
            current_target = [item for item in current if str(item.key) == OBS_KEY]
            if (
                not allow_create
                or len(current_target) != 1
                or str(current_target[0].uid) != PRIOR_TASK_OBS_UID
            ):
                raise AssertionError("successor readback drift")
            successor = ln.Collection(
                after,
                key=SUCCESSOR_KEY,
                description=description,
                revises=successor,
                skip_hash_lookup=True,
            ).save()
            created = True
    else:
        if not allow_create:
            raise AssertionError("required successor absent")
        successor = ln.Collection(
            after, key=SUCCESSOR_KEY, description=description, skip_hash_lookup=True
        ).save()
        created = True
    actual = list(successor.artifacts.all())
    if str(successor.description) != description or member_identity(
        actual
    ) != member_identity(after):
        raise AssertionError("successor readback drift")
    receipt = {
        "predecessor": {
            "uid": str(predecessor.uid),
            "key": str(predecessor.key),
            "member_count": len(before),
            "membership_sha256": membership_sha256(before),
        },
        "successor": {
            "uid": str(successor.uid),
            "key": str(successor.key),
            "member_count": len(actual),
            "membership_sha256": membership_sha256(actual),
            "target_obs_uid": str(new_obs.uid),
        },
    }
    return successor, created, receipt


def publish(
    ln: Any, prepared: dict[str, Any], helper_sha256: str, helper_commit: str
) -> tuple[Any, bool]:
    if prepared["obs_curated"]:
        return prepared["latest_obs_artifact"], False
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-publish-"))
    path = root / "obs.parquet"
    prepared["expected_obs"].to_parquet(path)
    description = (
        f"{TASK_ID}: source-exhaustive ODD001154 OBS; exact 18716-cell and 33694-feature raw-count parity; "
        f"Day 1 control semantics corrected; frame_sha256={prepared['expected_obs_sha256']}; "
        f"helper_sha256={helper_sha256}; helper_commit={helper_commit}"
    )
    ln.track(
        key=f"pert-gym/dataset-completion/{DATASET_ID}/{TASK_ID}",
        kind="script",
        params={
            "task_id": TASK_ID,
            "helper_sha256": helper_sha256,
            "helper_commit": helper_commit,
        },
        new_run=True,
        pypackages=False,
        stream_tracking=False,
    )
    obs = ln.Artifact.from_dataframe(
        path,
        key=OBS_KEY,
        revises=prepared["latest_obs_artifact"],
        description=description,
    ).save()
    obs.features.set_values({"X": prepared["x_artifact"]})
    write_journal("obs_saved", obs_uid=str(obs.uid))
    return obs, True


def strip_runtime(prepared: dict[str, Any]) -> dict[str, Any]:
    hidden = {
        "baseline_artifact",
        "x_artifact",
        "var_artifact",
        "latest_obs_artifact",
        "expected_obs",
    }
    result = {key: value for key, value in prepared.items() if key not in hidden}
    result["latest_obs"] = artifact_identity(prepared["latest_obs_artifact"])
    result["X"] = artifact_identity(prepared["x_artifact"])
    result["var"] = artifact_identity(prepared["var_artifact"])
    return result


def emit_product(phase: str, current: int) -> None:
    print(
        "PRODUCT_EXECUTION="
        + canonical(
            {
                "product_execution": {
                    "host": os.uname().nodename,
                    "pid": os.getpid(),
                    "phase": phase,
                    "payload_heartbeat_at": int(time.time()),
                    "metric": "dataset_end_to_end",
                    "current": current,
                    "denominator": 1,
                    "unit": "biological_dataset",
                }
            }
        ),
        flush=True,
    )


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    helper_commit = os.environ.get("PERT_GYM_HELPER_COMMIT", "")
    if not re.fullmatch(r"[0-9a-f]{40}", helper_commit):
        raise AssertionError("immutable PERT_GYM_HELPER_COMMIT required")
    helper_sha256 = sha256_file(Path(__file__))
    manifest = verify_manifest()
    authorities = verify_authorities(manifest)
    capacity = preflight()
    emit_product("preflight", 0)
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    duplicates = duplicate_probe(ln)
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-prepare-"))
    prepared = prepare(ln, manifest, root)
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    obs_created = False
    collection_created = False
    collection_receipt: dict[str, Any] = {"status": "not_evaluated_before_write"}
    if mode == "mutate":
        metadata = {
            "run_id": TASK_ID,
            "pid": os.getpid(),
            "host": capacity.hostname,
            "project": capacity.project,
            "zone": capacity.zone,
            "branch": ln.setup.settings.branch.name,
            "started_at": time.time(),
        }
        with ExitStack() as stack:
            stack.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            stack.enter_context(distributed_lamin_writer_lease(metadata))
            duplicate_probe(ln)
            fresh = prepare(ln, manifest, root)
            obs, obs_created = publish(ln, fresh, helper_sha256, helper_commit)
            successor, collection_created, collection_receipt = ensure_successor(
                ln, obs, allow_create=True
            )
            try:
                ln.finish()
            except AttributeError:
                ln.context.finish()
            write_journal(
                "published", obs_uid=str(obs.uid), collection_uid=str(successor.uid)
            )
    elif prepared["obs_curated"]:
        _, _, collection_receipt = ensure_successor(ln, prepared["latest_obs_artifact"])
    elif mode == "verify":
        raise AssertionError("verify requested before curated OBS exists")
    final = prepare(ln, manifest, root)
    if mode in {"mutate", "verify"} and not final["obs_curated"]:
        raise AssertionError("terminal curated OBS readback failed")
    if final["obs_curated"]:
        _, _, collection_receipt = ensure_successor(ln, final["latest_obs_artifact"])
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    if mode in {"plan", "verify"} and counts_before != counts_after:
        raise AssertionError("zero-write mode changed Lamin counts")
    receipt = {
        "format": "pert-gym.dataset-e2e-curation/v1",
        "task_id": TASK_ID,
        "dataset_id": DATASET_ID,
        "logical_key": LOGICAL_KEY,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "helper_commit": helper_commit,
        "source_manifest_sha256": sha256_file(MANIFEST_PATH),
        "source_authorities": authorities,
        "duplicate_probe": duplicates,
        "prepared": strip_runtime(final),
        **scientific_context(final["obs_receipt"]),
        "canonical_obs_contract": {
            "contract_id": "pert-gym/OBS_COMPLETED/v1",
            "field_count": len(CANONICAL_OBS_FIELDS),
            "fields": list(CANONICAL_OBS_FIELDS),
            "row_state_vocabulary": ["present", "missing", "not_applicable"],
        },
        "collection": collection_receipt,
        "writes": {
            "obs_created": obs_created,
            "collection_created": collection_created,
            "counts_before": counts_before,
            "counts_after": counts_after,
            "zero_write_replay": mode in {"plan", "verify"},
        },
        "invariants": {
            "main_branch_writes": 0,
            "X_rewritten": False,
            "var_rewritten": False,
            "single_physical_member": True,
            "OBS_COMPLETED": bool(final["obs_curated"]),
            "VAR_COMPLETED": bool(final["var_receipt"]["needs_revision"] is False),
            "accepted_component_status": "include",
        },
    }
    emit_product("terminal_readback", 1 if final["obs_curated"] else 0)
    print("ODD001154_CURATION=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
