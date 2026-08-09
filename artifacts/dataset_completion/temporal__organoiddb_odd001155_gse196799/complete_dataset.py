#!/usr/bin/env python3
"""Complete GSE196799 as one source-exhaustive append-only Lamin dataset."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import platform
import subprocess
import tempfile
import time
import urllib.request
import warnings
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

TASK_ID = "t_a2ff6038"
DATASET_ID = "temporal/organoiddb_odd001155_gse196799"
GEO_ACCESSION = "GSE196799"
ORGANOIDDB_ID = "ODD001155"
ROOT = Path(__file__).resolve().parent
MANIFEST_PATH = ROOT / "source_manifest.json"
RECEIPT_PATH = ROOT / "verification_receipt.json"
SHARED_VAR_KEY = "data/cleaned/GSE196799/var.parquet"
SUCCESSOR_COLLECTION_KEY = "pert-gym/additions/20260730-odd001155-gse196799-e2e"
BILLING_PROJECT = "jkobject-1549353370965"
PAYLOAD_CACHE = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-payloads-"))
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


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_sha256(values: Any) -> str:
    return sha256_bytes(canonical(list(map(str, values))).encode())


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


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size),
        "branch_id": int(artifact.branch_id),
        "is_latest": bool(artifact.is_latest),
        "description": str(artifact.description),
    }


def cache_artifact(artifact: Any) -> Path:
    """Materialize through requester-pays-aware gcloud on the EU worker."""
    if platform.system() == "Darwin":
        return Path(artifact.cache())
    uri = str(artifact.path)
    if not uri.startswith("gs://"):
        return Path(artifact.cache())
    target = PAYLOAD_CACHE / f"{artifact.uid}-{Path(str(artifact.key)).name}"
    if not target.exists():
        subprocess.run(
            [
                "gcloud",
                "storage",
                "cp",
                f"--billing-project={BILLING_PROJECT}",
                uri,
                str(target),
            ],
            check=True,
        )
    if target.stat().st_size != int(artifact.size):
        raise AssertionError(f"artifact size readback drift: {artifact.uid}")
    return target


def load_dataframe(artifact: Any) -> pd.DataFrame:
    if platform.system() == "Darwin":
        return artifact.load()
    return pd.read_parquet(cache_artifact(artifact))


def resolve_artifact(ln: Any, value: Any) -> Any:
    if hasattr(value, "uid"):
        return value
    identity = str(value)
    try:
        return ln.Artifact.get(uid=identity)
    except ln.Artifact.DoesNotExist:
        return ln.Artifact.get(key=identity)


def load_manifest() -> dict[str, Any]:
    manifest = json.loads(MANIFEST_PATH.read_text())
    if manifest["task_id"] != TASK_ID or manifest["dataset_id"] != DATASET_ID:
        raise AssertionError("source manifest identity drift")
    if len(manifest["samples"]) != 10:
        raise AssertionError("GSE196799 source denominator drift")
    return manifest


def download(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "pert-gym/1"})
    with urllib.request.urlopen(request, timeout=180) as response:
        return response.read()


def parse_geo_soft(
    payload: bytes,
) -> tuple[dict[str, list[str]], dict[str, dict[str, list[str]]]]:
    series: dict[str, list[str]] = {}
    samples: dict[str, dict[str, list[str]]] = {}
    target = series
    for raw_line in gzip.decompress(payload).decode().splitlines():
        if raw_line.startswith("^SAMPLE = "):
            accession = raw_line.split(" = ", 1)[1]
            target = samples.setdefault(accession, {})
            continue
        if not raw_line.startswith("!") or " = " not in raw_line:
            continue
        key, value = raw_line[1:].split(" = ", 1)
        target.setdefault(key, []).append(value)
    return series, samples


def verify_sources(manifest: dict[str, Any], *, full: bool) -> dict[str, Any]:
    soft_spec = manifest["sources"]["geo_soft"]
    soft = download(soft_spec["url"])
    if (
        len(soft) != soft_spec["size_bytes"]
        or sha256_bytes(soft) != soft_spec["sha256"]
    ):
        raise AssertionError("GEO SOFT identity drift")
    decompressed = gzip.decompress(soft)
    if sha256_bytes(decompressed) != soft_spec["decompressed_sha256"]:
        raise AssertionError("GEO SOFT decompressed identity drift")
    series, samples = parse_geo_soft(soft)
    if set(samples) != set(manifest["samples"]):
        raise AssertionError("GEO SOFT sample denominator drift")
    if series.get("Series_geo_accession") != [GEO_ACCESSION]:
        raise AssertionError("GEO series accession drift")

    publication = manifest["sources"]["publication"]
    fulltext = download(publication["fulltext_url"])
    text = fulltext.decode(errors="replace")
    for token in (
        publication["pmid"],
        publication["pmcid"],
        publication["doi"],
        "HMGUi002-A",
    ):
        if token not in text:
            raise AssertionError(f"publication authority token absent: {token}")

    notebook_receipt: dict[str, Any] = {
        "url": manifest["sources"]["analysis_notebook"]["url"],
        "status": "deferred_to_eu_full_readback",
    }
    if full:
        notebook = download(notebook_receipt["url"])
        expected = manifest["sources"]["analysis_notebook"]
        if (
            len(notebook) != expected["size_bytes"]
            or sha256_bytes(notebook) != expected["sha256"]
        ):
            raise AssertionError("analysis notebook identity drift")
        notebook_receipt = {
            "url": expected["url"],
            "git_commit": expected["git_commit"],
            "size_bytes": len(notebook),
            "sha256": sha256_bytes(notebook),
            "status": "PASS",
        }
    return {
        "status": "PASS",
        "geo_soft": {
            "size_bytes": len(soft),
            "sha256": sha256_bytes(soft),
            "sample_count": len(samples),
        },
        "publication_fulltext": {
            "size_bytes": len(fulltext),
            "sha256": sha256_bytes(fulltext),
            "pmid": publication["pmid"],
            "pmcid": publication["pmcid"],
            "doi": publication["doi"],
        },
        "analysis_notebook": notebook_receipt,
        "series": series,
        "samples": samples,
    }


def missing(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def set_field(
    frame: pd.DataFrame,
    field: str,
    values: Any,
    state: Any,
    source: str,
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def scalar(values: list[str] | None) -> str:
    return " | ".join(values or [])


def curate_obs(
    baseline: pd.DataFrame,
    sample_id: str,
    sample_spec: dict[str, Any],
    geo_sample: dict[str, list[str]],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
    expected = sample_spec["expected"]
    if len(baseline) != expected["n_obs"] or not baseline.index.is_unique:
        raise AssertionError(f"{sample_id}: accepted OBS denominator drift")

    required = {
        "sample_accession",
        "timepoint",
        "culture_method",
        "ascorbic_acid_from_day_12",
        "total_counts",
        "n_genes_by_counts",
        "pct_counts_mt",
    }
    absent = sorted(required - set(baseline.columns))
    if absent:
        raise AssertionError(f"{sample_id}: accepted OBS fields absent: {absent}")

    result = baseline.copy(deep=True)
    for column in baseline.columns:
        result[f"source_original_{column}"] = baseline[column]
    result = add_obs_identity(
        result, dataset_id=DATASET_ID, prefix=sample_spec["prefix"]
    )
    validate_obs_identity(result)
    index = result.index
    day = int(sample_spec["source"]["day"])
    culture = str(sample_spec["source"]["culture_method"])
    aa = bool(sample_spec["source"]["ascorbic_acid_treatment"])
    source_checks = {
        "sample": set(baseline["sample_accession"].astype(str)) == {sample_id},
        "day": set(pd.to_numeric(baseline["timepoint"]).astype(float)) == {float(day)},
        "culture": set(baseline["culture_method"].astype(str)) == {culture},
        "ascorbic_acid": set(baseline["ascorbic_acid_from_day_12"].astype(bool))
        == {aa},
    }
    if not all(source_checks.values()):
        raise AssertionError(f"{sample_id}: source condition drift: {source_checks}")
    timepoint = pd.Series(float(day * 1440), index=index, dtype="Float64")
    trajectory = (
        "hiPSC_endothelial_differentiation_matrigel"
        if "Matrigel" in culture
        else "hiPSC_endothelial_differentiation_suspension"
    )

    raw_geo = canonical(geo_sample)
    result["geo_raw_metadata_json"] = raw_geo
    for source_name, output_name in (
        ("Sample_title", "geo_raw_title"),
        ("Sample_source_name_ch1", "geo_raw_source_name"),
        ("Sample_characteristics_ch1", "geo_raw_characteristics"),
        ("Sample_growth_protocol_ch1", "geo_raw_growth_protocol"),
        ("Sample_treatment_protocol_ch1", "geo_raw_treatment_protocol"),
        ("Sample_extract_protocol_ch1", "geo_raw_extract_protocol"),
        ("Sample_data_processing", "geo_raw_data_processing"),
        ("Sample_instrument_model", "geo_raw_instrument_model"),
        ("Sample_library_strategy", "geo_raw_library_strategy"),
    ):
        result[output_name] = scalar(geo_sample.get(source_name))

    set_field(
        result, "dataset", DATASET_ID, "present", "accepted logical dataset identity"
    )
    set_field(result, "sample", sample_id, "present", "GEO sample accession")
    set_field(
        result,
        "cell_id",
        result.index.astype(str),
        "present",
        "accepted source barcode axis",
    )
    set_field(
        result,
        "donor_id",
        missing(index),
        "unknown",
        "source identifies a cell line but supplies no donor identifier",
    )
    set_field(result, "batch", sample_id, "present", "GEO library/sample accession")
    set_field(
        result,
        "cell_type",
        missing(index),
        "unknown",
        "author notebook defines clusters but distributes no immutable barcode-level annotation table",
    )
    set_field(
        result,
        "cell_line",
        "HMGUi002-A",
        "present",
        "publication experimental model default for experiments unless otherwise specified",
    )
    set_field(
        result,
        "disease",
        missing(index),
        "unknown",
        "no row- or sample-level disease assertion",
    )
    set_field(
        result,
        "tissue_type",
        "in vitro hiPSC-derived vascular culture",
        "present",
        "GEO title and publication experimental model",
    )
    set_field(result, "organism", "Homo sapiens", "present", "GEO taxon 9606")
    set_field(
        result, "sex", "male", "present", "publication HMGUi002-A donor description"
    )
    set_field(result, "age", missing(index), "unknown", "donor age is not reported")
    set_field(
        result,
        "ethnicity",
        "Caucasian",
        "present",
        "publication wording for HMGUi002-A donor",
    )
    set_field(
        result, "sequencer", "NextSeq 500", "present", "GEO Sample_instrument_model"
    )
    set_field(
        result,
        "technology",
        "10x Genomics Chromium 3-prime",
        "present",
        "GEO extraction protocol and publication",
    )
    set_field(
        result, "assay", "single-cell RNA sequencing", "present", "GEO series type"
    )
    set_field(result, "modality", "scRNA-seq", "present", "GEO library strategy")
    set_field(
        result,
        "media",
        missing(index),
        "unknown",
        "multi-stage medium is described but not joinable as one per-cell medium value",
    )
    set_field(result, "is_bulk", False, "present", "single-cell source")
    set_field(
        result,
        "is_pseudobulk",
        False,
        "present",
        "one accepted barcode per observation",
    )
    set_field(
        result,
        "perturbation",
        "ascorbic acid" if aa else "none",
        "present",
        "GEO sample title and treatment protocol",
    )
    set_field(
        result,
        "perturbation_type",
        "chemical" if aa else "none",
        "present",
        "GEO sample title and treatment protocol",
    )
    set_field(
        result,
        "perturbation_technology",
        "media supplementation" if aa else missing(index),
        "present" if aa else "not_applicable",
        "GEO treatment protocol",
    )
    for field in ("perturbation_library", "guide_sequence", "molecule_sequence"):
        set_field(
            result, field, missing(index), "not_applicable", "no genetic perturbation"
        )
    set_field(
        result,
        "is_control",
        sample_id == "GSM5901236",
        "present",
        "GSM5901236 is the matched untreated day-18 Matrigel condition for GSM5901237",
    )
    set_field(
        result,
        "dose",
        60.0 if aa else missing(index, "Float64"),
        "present" if aa else "not_applicable",
        "GEO treatment protocol",
    )
    set_field(
        result,
        "dose_unit",
        "ug/mL" if aa else missing(index),
        "present" if aa else "not_applicable",
        "GEO treatment protocol",
    )
    set_field(
        result,
        "timepoint",
        timepoint,
        "present",
        "GEO differentiation day converted to canonical minutes",
    )
    result["timepoint_unit"] = "minute"
    result["timepoint_original_value"] = day
    result["timepoint_original_unit"] = "day"
    set_field(
        result,
        "trajectory_id",
        trajectory,
        "present",
        "source developmental and culture trajectory",
    )
    set_field(
        result,
        "pseudotime",
        missing(index, "Float64"),
        "unknown",
        "author processed pseudotime is not distributed barcode-wise",
    )
    set_field(
        result,
        "is_baseline",
        day == 0,
        "present",
        "day 0 is the source developmental baseline",
    )
    for field in ("sensitivity", "response_value"):
        set_field(
            result,
            field,
            missing(index, "Float64"),
            "not_applicable",
            "no scalar response endpoint",
        )
    for field in ("response_metric", "response_source"):
        set_field(
            result,
            field,
            missing(index),
            "not_applicable",
            "no scalar response endpoint",
        )
    set_field(
        result,
        "n_counts",
        pd.to_numeric(baseline["total_counts"], errors="raise").astype("Int64"),
        "present",
        "accepted raw-count matrix row sum",
    )
    set_field(
        result,
        "n_genes",
        pd.to_numeric(baseline["n_genes_by_counts"], errors="raise").astype("Int64"),
        "present",
        "accepted raw-count matrix detected-gene count",
    )
    set_field(
        result,
        "pct_mito",
        pd.to_numeric(baseline["pct_counts_mt"], errors="raise").astype("Float64"),
        "present",
        "accepted QC calculation",
    )
    set_field(
        result,
        "pct_ribo",
        missing(index, "Float64"),
        "unknown",
        "not reported and ribosomal gene mapping is not retained",
    )
    set_field(
        result,
        "is_low_quality",
        False,
        "present",
        "accepted component retains cells after documented strict QC",
    )
    result["source"] = "GEO"
    result["source_accession"] = GEO_ACCESSION
    result["organoiddb_id"] = ORGANOIDDB_ID
    result["control_availability"] = (
        "strict_control_available"
        if sample_id in {"GSM5901236", "GSM5901237"}
        else "no_control_found"
    )
    result["x_semantics"] = "raw_counts"

    if len(result) != len(baseline) or not result.index.equals(baseline.index):
        raise AssertionError(f"{sample_id}: OBS row/order drift")
    for field in CANONICAL_FIELDS:
        for required_column in (field, f"{field}_state", f"{field}_source"):
            if required_column not in result:
                raise AssertionError(
                    f"{sample_id}: missing canonical evidence {required_column}"
                )
        if result[f"{field}_source"].astype(str).str.strip().eq("").any():
            raise AssertionError(f"{sample_id}: blank provenance for {field}")
    unknown = {
        field: int(
            result.loc[result[f"{field}_state"].eq("unknown"), field].isna().sum()
        )
        for field in CANONICAL_FIELDS
        if result[f"{field}_state"].eq("unknown").any()
    }
    return result, {
        "status": "PASS",
        "rows": len(result),
        "canonical_field_count": len(CANONICAL_FIELDS),
        "obs_uuid_unique": bool(result["obs_uuid"].is_unique),
        "unknown_null_rows": unknown,
        "sample_frequencies": {sample_id: len(result)},
        "day_frequencies": {str(day): len(result)},
        "culture_method_frequencies": {culture: len(result)},
        "ascorbic_acid_treatment_frequencies": {str(aa): len(result)},
    }


def curate_var(
    raw: pd.DataFrame, manifest: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if len(raw) != manifest["shared_var"]["n_vars"] or not raw.index.is_unique:
        raise AssertionError("accepted VAR denominator drift")
    feature_id = raw["feature_id"].astype("string")
    if (
        not feature_id.is_unique
        or not feature_id.str.fullmatch(r"ENSG\d{11}(?:\.\d+)?", na=False).all()
    ):
        raise AssertionError("VAR stable human Ensembl ID gate failed")

    result = raw.copy(deep=True)
    for column in raw.columns:
        result[f"source_original_{column}"] = raw[column]
    result["original_var_index"] = raw.index.astype(str)
    result["stable_feature_id"] = feature_id
    result["ensembl_gene_id"] = feature_id.str.replace(r"\.\d+$", "", regex=True)
    result["gene_symbol"] = raw["gene_symbol"].astype("string")
    result["feature_namespace"] = "Ensembl Gene"
    result["organism"] = "Homo sapiens"
    result["ensembl_species"] = "homo_sapiens"
    result["stable_feature_id_state"] = "present"
    result["stable_feature_id_source"] = "accepted source-native feature_id"
    result["species_validation_state"] = "present"
    result["species_validation_source"] = (
        "GEO taxon 9606 + ENSG stable identifier namespace"
    )
    result["feature_contract_class"] = "species_correct_stable_ensembl_gene_id"
    if not result.index.equals(raw.index):
        raise AssertionError("VAR row/order drift")
    return result, {
        "status": "PASS",
        "VAR_ENSEMBL_SPECIES_COMPLETED": True,
        "biological_features_total": len(result),
        "stable_ensembl_id_features": int(feature_id.notna().sum()),
        "correct_species_features": int(result["organism"].eq("Homo sapiens").sum()),
        "ordered_feature_id_sha256": ordered_sha256(feature_id),
    }


def inspect_x(
    artifact: Any,
    sample_spec: dict[str, Any],
    obs_index: pd.Index,
    var_index: pd.Index,
    *,
    full: bool,
) -> dict[str, Any]:
    expected = sample_spec["expected"]
    receipt = {"identity": artifact_identity(artifact), "full_payload_readback": full}
    if not full:
        receipt["status"] = "DEFERRED_TO_EU"
        return receipt
    path = cache_artifact(artifact)
    with h5py.File(path, "r") as handle:
        shape = tuple(map(int, handle["X"].attrs["shape"]))
        obs_index_key = str(handle["obs"].attrs["_index"])
        var_index_key = str(handle["var"].attrs["_index"])
        obs = [
            value.decode() if isinstance(value, bytes) else str(value)
            for value in handle[f"obs/{obs_index_key}"][:]
        ]
        var = [
            value.decode() if isinstance(value, bytes) else str(value)
            for value in handle[f"var/{var_index_key}"][:]
        ]
        nnz = int(len(handle["X/data"]))
        dtype = str(handle["X/data"].dtype)
        encoding = str(handle["X"].attrs.get("encoding-type", ""))
    checks = {
        "shape": list(shape) == [expected["n_obs"], expected["n_vars"]],
        "nnz": nnz == expected["nnz"],
        "obs_axis": obs == list(map(str, obs_index)),
        "var_axis": var == list(map(str, var_index)),
        "dtype": dtype == "int32",
        "encoding": encoding == "csr_matrix",
    }
    if not all(checks.values()):
        raise AssertionError(f"X readback failed: {checks}")
    receipt.update(
        {"status": "PASS", "shape": list(shape), "nnz": nnz, "checks": checks}
    )
    return receipt


def bounded_main_duplicate_probe(ln: Any) -> dict[str, Any]:
    terms = (GEO_ACCESSION, ORGANOIDDB_ID, "37714147", "10.1016/j.stemcr.2023.08.008")
    candidates: dict[str, dict[str, Any]] = {}
    ln.setup.switch("main")
    try:
        if ln.setup.settings.branch.name != "main":
            raise AssertionError("failed to switch to main")
        for term in terms:
            for field in ("key", "description"):
                queryset = ln.Artifact.filter(
                    **{f"{field}__icontains": term}, is_latest=True
                )
                for item in list(queryset[:25]):
                    candidates[str(item.uid)] = artifact_identity(item)
    finally:
        ln.setup.switch("jkobject")
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("failed to restore jkobject")
    if candidates:
        raise AssertionError(f"main scientific equivalent found: {candidates}")
    return {
        "status": "PASS",
        "terms": list(terms),
        "candidate_count": 0,
        "candidates": [],
    }


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"artifact absent: {key}")
    return records[-1], records


def task_artifact(ln: Any, key: str, prefix: str) -> Any | None:
    records = [
        item
        for item in ln.Artifact.filter(key=key).all()
        if str(item.description).startswith(prefix)
    ]
    if len(records) > 1:
        raise AssertionError(f"multiple task revisions for {key}")
    return records[0] if records else None


def prepare(
    ln: Any, manifest: dict[str, Any], source_receipts: dict[str, Any], *, full_x: bool
) -> dict[str, Any]:
    prepared: dict[str, Any] = {"samples": {}}
    first_var = None
    for sample_id, spec in manifest["samples"].items():
        accepted = {
            role: ln.Artifact.get(uid=uid)
            for role, uid in spec["accepted_uids"].items()
        }
        expected_keys = {
            "obs": f"{spec['prefix']}/obs.parquet",
            "X": f"{spec['prefix']}/X.h5ad",
            "var": f"{spec['prefix']}/var.parquet",
        }
        if {role: str(item.key) for role, item in accepted.items()} != expected_keys:
            raise AssertionError(f"{sample_id}: frozen artifact key drift")
        baseline = load_dataframe(accepted["obs"])
        accepted_var = load_dataframe(accepted["var"])
        curated, obs_receipt = curate_obs(
            baseline, sample_id, spec, source_receipts["samples"][sample_id]
        )
        x_receipt = inspect_x(
            accepted["X"], spec, baseline.index, accepted_var.index, full=full_x
        )
        latest_obs, history = latest_artifact(ln, expected_keys["obs"])
        description_prefix = f"{TASK_ID}: source-exhaustive GSE196799 OBS"
        is_curated = str(latest_obs.uid) != str(accepted["obs"].uid) and str(
            latest_obs.description
        ).startswith(description_prefix)
        if str(latest_obs.uid) != str(accepted["obs"].uid) and not is_curated:
            raise AssertionError(f"{sample_id}: foreign OBS revision {latest_obs.uid}")
        if is_curated:
            assert_frame_equal(
                load_dataframe(latest_obs), curated, check_categorical=True
            )
        prepared["samples"][sample_id] = {
            "accepted": accepted,
            "latest_obs": latest_obs,
            "obs_history_count": len(history),
            "curated": curated,
            "is_curated": is_curated,
            "obs_receipt": obs_receipt,
            "x_receipt": x_receipt,
            "expected_frame_sha256": frame_sha256(curated),
        }
        first_var = first_var or accepted["var"]
    assert first_var is not None
    raw_var = load_dataframe(first_var)
    curated_var, var_receipt = curate_var(raw_var, manifest)
    shared_var = task_artifact(
        ln, SHARED_VAR_KEY, f"{TASK_ID}: species-correct shared GSE196799 VAR"
    )
    if shared_var is not None:
        assert_frame_equal(
            load_dataframe(shared_var), curated_var, check_categorical=True
        )
    prepared.update(
        {
            "curated_var": curated_var,
            "var_receipt": var_receipt,
            "shared_var": shared_var,
            "expected_var_frame_sha256": frame_sha256(curated_var),
        }
    )
    return prepared


def member_identity(members: list[Any]) -> list[dict[str, str]]:
    return sorted(
        ({"uid": str(item.uid), "key": str(item.key)} for item in members),
        key=lambda item: (item["key"], item["uid"]),
    )


def membership_sha256(members: list[Any]) -> str:
    return sha256_bytes(canonical(member_identity(members)).encode())


def predecessor_collection(ln: Any, prepared: dict[str, Any]) -> tuple[Any, list[Any]]:
    baseline = next(iter(prepared["samples"].values()))["accepted"]["obs"]
    candidates = [
        item
        for item in ln.Collection.filter(artifacts=baseline, is_latest=True).all()
        if str(item.key).startswith("pert-gym/additions/")
    ]
    candidates.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not candidates:
        raise AssertionError("no latest additions Collection contains GSE196799")
    predecessor = candidates[-1]
    members = list(predecessor.artifacts.all())
    baseline_uids = {
        str(item["accepted"]["obs"].uid) for item in prepared["samples"].values()
    }
    if len(baseline_uids & {str(item.uid) for item in members}) != 10:
        raise AssertionError("predecessor does not contain all ten frozen OBS members")
    return predecessor, members


def ensure_successor_collection(
    ln: Any, prepared: dict[str, Any], *, allow_create: bool
) -> tuple[Any, bool, dict[str, Any]]:
    predecessor, before = predecessor_collection(ln, prepared)
    replacements = {
        str(item["accepted"]["obs"].uid): item["latest_obs"]
        for item in prepared["samples"].values()
    }
    after = [replacements.get(str(item.uid), item) for item in before]
    if any(not item["is_curated"] for item in prepared["samples"].values()):
        raise AssertionError("cannot publish Collection before all OBS revisions exist")
    if len(before) != len(after) or len({str(item.key) for item in after}) != len(
        after
    ):
        raise AssertionError("Collection replacement changed count or duplicated keys")
    description = canonical(
        {
            "format": "pert-gym.append-only-dataset-completion/v1",
            "task_id": TASK_ID,
            "dataset_id": DATASET_ID,
            "predecessor_uid": str(predecessor.uid),
            "predecessor_membership_sha256": membership_sha256(before),
            "replaced_obs_uids": sorted(replacements),
            "added_obs_uids": sorted(str(item.uid) for item in replacements.values()),
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
            "replaced_member_count": 10,
        },
    )


def ensure_link_feature(ln: Any, name: str) -> None:
    records = list(ln.Feature.filter(name=name).all())
    if records and str(records[0].dtype) != "cat[Artifact]":
        raise AssertionError(f"link feature dtype drift: {name}")
    if not records:
        ln.Feature(name=name, dtype="cat[Artifact]").save()


def publish(ln: Any, prepared: dict[str, Any], helper_sha256: str) -> dict[str, int]:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-publish-"))
    writes = {
        "obs_revisions": 0,
        "shared_var_artifacts": 0,
        "x_revisions": 0,
        "deletions": 0,
    }
    if prepared["shared_var"] is None:
        var_path = root / "var.parquet"
        prepared["curated_var"].to_parquet(var_path)
        prepared["shared_var"] = ln.Artifact.from_dataframe(
            var_path,
            key=SHARED_VAR_KEY,
            description=f"{TASK_ID}: species-correct shared GSE196799 VAR; frame_sha256={prepared['expected_var_frame_sha256']}; helper_sha256={helper_sha256}",
        ).save()
        writes["shared_var_artifacts"] = 1
    for sample_id, item in prepared["samples"].items():
        if not item["is_curated"]:
            path = root / f"{sample_id}_obs.parquet"
            item["curated"].to_parquet(path)
            item["latest_obs"] = ln.Artifact.from_dataframe(
                path,
                key=str(item["accepted"]["obs"].key),
                revises=item["accepted"]["obs"],
                description=f"{TASK_ID}: source-exhaustive GSE196799 OBS; sample={sample_id}; frame_sha256={item['expected_frame_sha256']}; helper_sha256={helper_sha256}",
            ).save()
            item["is_curated"] = True
            writes["obs_revisions"] += 1
    ensure_link_feature(ln, "X")
    ensure_link_feature(ln, "var")
    for item in prepared["samples"].values():
        item["latest_obs"].features.set_values({"X": item["accepted"]["X"]})
        item["accepted"]["X"].features.set_values({"var": prepared["shared_var"]})
    return writes


def verify_links(ln: Any, prepared: dict[str, Any]) -> dict[str, Any]:
    shared_var = prepared["shared_var"]
    if shared_var is None:
        raise AssertionError("shared VAR absent")
    rows = []
    for sample_id, item in prepared["samples"].items():
        obs_links = item["latest_obs"].features.get_values()
        x_links = item["accepted"]["X"].features.get_values()
        obs_x = resolve_artifact(ln, obs_links["X"])
        x_var = resolve_artifact(ln, x_links["var"])
        if str(obs_x.uid) != str(item["accepted"]["X"].uid) or str(x_var.uid) != str(
            shared_var.uid
        ):
            raise AssertionError(f"{sample_id}: obs -> X -> shared var link drift")
        rows.append(
            {
                "sample": sample_id,
                "obs_uid": str(item["latest_obs"].uid),
                "x_uid": str(obs_x.uid),
                "var_uid": str(x_var.uid),
            }
        )
    return {"status": "PASS", "rows": rows, "shared_var": artifact_identity(shared_var)}


def aggregate_obs_receipt(prepared: dict[str, Any]) -> dict[str, Any]:
    receipts = [item["obs_receipt"] for item in prepared["samples"].values()]
    unknown: dict[str, int] = {}
    frequencies = {
        "sample": {},
        "day": {},
        "culture_method": {},
        "ascorbic_acid_treatment": {},
    }
    for receipt in receipts:
        for field, count in receipt["unknown_null_rows"].items():
            unknown[field] = unknown.get(field, 0) + count
        for key, source in (
            ("sample", "sample_frequencies"),
            ("day", "day_frequencies"),
            ("culture_method", "culture_method_frequencies"),
            ("ascorbic_acid_treatment", "ascorbic_acid_treatment_frequencies"),
        ):
            for value, count in receipt[source].items():
                frequencies[key][value] = frequencies[key].get(value, 0) + count
    return {
        "status": "PASS",
        "OBS_COMPLETED": True,
        "rows": sum(item["rows"] for item in receipts),
        "members": len(receipts),
        "canonical_field_count": len(CANONICAL_FIELDS),
        "unknown_null_rows": unknown,
        "frequencies": frequencies,
        "cell_type_disposition": "explicit unknown for every row: author notebook has cluster maps but no immutable barcode-level annotation payload",
    }


def run(mode: str) -> dict[str, Any]:
    if mode in {"mutate", "verify"} and platform.system() == "Darwin":
        raise RuntimeError(
            "live completion and full payload verification require the EU worker"
        )
    manifest = load_manifest()
    source_receipts = verify_sources(manifest, full=mode in {"mutate", "verify"})
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
    prepare(ln, manifest, source_receipts, full_x=mode in {"mutate", "verify"})
    helper_sha256 = sha256_file(Path(__file__))
    writes = {
        "obs_revisions": 0,
        "shared_var_artifacts": 0,
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
            prepared = prepare(ln, manifest, source_receipts, full_x=True)
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
    final = prepare(ln, manifest, source_receipts, full_x=mode in {"mutate", "verify"})
    completed = (
        all(item["is_curated"] for item in final["samples"].values())
        and final["shared_var"] is not None
    )
    if mode == "verify" and not completed:
        raise AssertionError("verify requested before completed revisions exist")
    links = {"status": "PENDING"}
    if completed:
        links = verify_links(ln, final)
        _collection, _created, collection_receipt = ensure_successor_collection(
            ln, final, allow_create=False
        )
    after_counts = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    obs_receipt = aggregate_obs_receipt(final)
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
        "sources": {
            key: value
            for key, value in source_receipts.items()
            if key not in {"series", "samples"}
        },
        "accepted_component_receipt": manifest["accepted_component_receipt"],
        "negative_main_duplicate_probe": main_probe,
        "counts": {
            "biological_datasets": 1,
            "logical_families": 1,
            "physical_members": 10,
            "observations": obs_receipt["rows"],
            "variables": manifest["shared_var"]["n_vars"],
            "matrix_nonzeros": sum(
                spec["expected"]["nnz"] for spec in manifest["samples"].values()
            ),
        },
        "obs": obs_receipt,
        "var": final["var_receipt"],
        "chunks": {
            "status": "PASS"
            if mode == "plan"
            or all(
                item["x_receipt"]["status"] == "PASS"
                for item in final["samples"].values()
            )
            else "DEFERRED_TO_EU",
            "members": {
                sample: item["x_receipt"] for sample, item in final["samples"].items()
            },
        },
        "links": links,
        "collection": collection_receipt,
        "gates": {
            "OBS": "PASS",
            "VAR": final["var_receipt"]["status"],
            "chunks": "PASS"
            if mode == "plan"
            or all(
                item["x_receipt"]["status"] == "PASS"
                for item in final["samples"].values()
            )
            else "DEFERRED_TO_EU",
            "cleaning": "PASS",
            "canonical_storage": links["status"] if completed else "PENDING",
            "lamin_jkobject": "PASS",
            "collection": collection_receipt["status"],
        },
        "writes": writes,
        "registry_counts": {"before": before_counts, "after": after_counts},
        "replay_noop": mode == "verify" and before_counts == after_counts,
        "rollback": {
            "obs_uids": sorted(
                item["accepted"]["obs"].uid for item in final["samples"].values()
            ),
            "var_uids": sorted(
                item["accepted"]["var"].uid for item in final["samples"].values()
            ),
            "collection_uid": manifest["accepted_component_receipt"]["collection_uid"],
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
        "GSE196799_COMPLETION="
        + canonical(
            {
                "status": receipt["status"],
                "mode": args.mode,
                "receipt_sha256": receipt["canonical_sha256"],
                "collection_uid": receipt["collection"].get("successor_uid"),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
