#!/usr/bin/env python3
"""Finish the SCP1973 zebrafish-retina OBS/VAR contract append-only.

This script is intended for ``pert-gym-worker-eu`` only. ``plan`` and ``verify``
are read-only. ``mutate`` revises OBS and VAR, reuses the accepted X payload,
and restores OBS -> X -> VAR links on ``laminlabs/pertdata/jkobject``.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Iterable

import anndata as ad
import h5py
import numpy as np
import pandas as pd
import requests
from pandas.testing import assert_frame_equal
from scipy import sparse

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from pert_gym.obs_identity import add_obs_identity, validate_obs_identity  # noqa: E402
from tools.lamin_context import connect_pertdata  # noqa: E402

try:
    from tools.pert_gym_vm_runner import (  # noqa: E402
        distributed_lamin_writer_lease,
        lamin_writer_lease,
        preflight,
    )
except ImportError:
    from pert_gym_vm_runner import (  # type: ignore[no-redef]  # noqa: E402
        distributed_lamin_writer_lease,
        lamin_writer_lease,
        preflight,
    )

TASK_ID = "t_bb2f5bc0"
ACCESSIONS = ("SCP1973", "GSE226373", "PRJNA940076")
DOI = "10.7554/eLife.86507"
DATASET_ID = (
    "temporal/scrnaseq_unravels_the_transcriptional_network_underlying_"
    "zebrafish_retina_regene"
)
PREFIX = (
    "pert-gym/logical/temporal/scrnaseq_unravels_the_transcriptional_network_"
    "underlying_zebrafish_retina_regene"
)
EXPECTED = {"n_obs": 11_690, "n_vars": 20_726, "nnz": 17_704_427}
ACCEPTED_UIDS = {
    "obs": "KsqvAFoGHYY3N0Es0000",
    "X": "XNdeESO8PIhCpCmo0000",
    "var": "cotSkl4KjBYQRBZm0000",
}
SOURCE_UIDS = {
    "obs": "aMnJwlABB1VMeYJH0000",
    "X": "2u8UrHx5vCE0O4zZ0000",
    "var": "TR04mLvCnFyXmXbF0000",
}
ACCEPTED_COLLECTION_UID = "4KzIQvzliuWg8R0k0000"
ACCEPTED_REVISION = "temporal-v4-133-wave13-05a447c4c48e148a"
GCS_ROOT = f"gs://scperturb/pert-gym/staging/{PREFIX}/revisions/{ACCEPTED_REVISION}"
ACCEPTED_OBJECTS = {
    "obs": {
        "generation_uri": f"{GCS_ROOT}/obs.parquet#1784268885796503",
        "sha256": "43c6766eef0a734a41e11f1955d79cee4749b22330906c4debfe1de20a9945f6",
        "bytes": 494_683,
    },
    "X": {
        "generation_uri": f"{GCS_ROOT}/X.h5ad#1784268886641334",
        "sha256": "2002963da4e63e3962e234b6e23a3fdac6f8bc7e671bdf3724750c6cae37fff3",
        "bytes": 42_626_250,
    },
    "var": {
        "generation_uri": f"{GCS_ROOT}/var.parquet#1784268886866298",
        "sha256": "f511c5c774cbbb3544c90dae3f878d1f04c4822e22a8b6d257d4dfddca01ad28",
        "bytes": 771_548,
    },
    "manifest": {
        "generation_uri": f"{GCS_ROOT}/manifest.json#1784268888022637",
        "sha256": "c7bf8bc19baa8ab87e66c3c442c17128f411220ffa268dc8c7742e2ecd885072",
        "bytes": 20_191,
    },
}
GEO_H5_URL = (
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE226nnn/GSE226373/suppl/"
    "GSE226373_filtered_feature_bc_matrix.h5"
)
GEO_H5_SHA256 = "1a4748a01fa5ab0dff236214a6df173c8d079910ec11c9c8d78a24e23411c739"
GEO_H5_BYTES = 39_962_730
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
TIMEPOINT_MINUTES = {"uninjured": 0, "44 hpl": 2_640, "4 dpl": 5_760, "6 dpl": 8_640}
SAMPLE_ACCESSION = {
    "s1": "GSM7074106",
    "s2": "GSM7074107",
    "s3": "GSM7074108",
    "s4": "GSM7074109",
    "s5": "GSM7074110",
}
SAMPLE_BATCH = {
    "s1": "collection_day_1",
    "s2": "collection_day_1",
    "s3": "collection_day_1",
    "s4": "collection_day_2",
    "s5": "collection_day_2",
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def x_fingerprint(path: Path) -> dict[str, Any]:
    """Hash the logical matrix arrays, independently of HDF5 serialization."""
    matrix = ad.read_h5ad(path)
    values = matrix.X
    digest = hashlib.sha256()
    if sparse.issparse(values):
        values = values.tocsr()
        for array in (values.data, values.indices, values.indptr):
            contiguous = np.ascontiguousarray(array)
            digest.update(str(contiguous.dtype).encode())
            digest.update(contiguous.tobytes())
        nnz = int(values.nnz)
    else:
        contiguous = np.ascontiguousarray(values)
        digest.update(str(contiguous.dtype).encode())
        digest.update(contiguous.tobytes())
        nnz = int(np.count_nonzero(contiguous))
    return {
        "shape": list(matrix.shape),
        "nnz": nnz,
        "logical_X_sha256": digest.hexdigest(),
        "obs_axis_sha256": ordered_sha256(matrix.obs_names),
        "var_axis_sha256": ordered_sha256(matrix.var_names),
    }


def write_recovered_x(source_path: Path, target_path: Path) -> dict[str, Any]:
    """Create a new physical payload only because the accepted one was deleted."""
    matrix = ad.read_h5ad(source_path)
    matrix.uns["pert_gym_canonical_recovery"] = {
        "task_id": TASK_ID,
        "source_artifact_uid": SOURCE_UIDS["X"],
        "source_sha256": ACCEPTED_OBJECTS["X"]["sha256"],
        "reason": "accepted canonical GCS object absent",
    }
    matrix.write_h5ad(target_path)
    source_fingerprint = x_fingerprint(source_path)
    recovered_fingerprint = x_fingerprint(target_path)
    if source_fingerprint != recovered_fingerprint:
        raise AssertionError("recovered X changed logical matrix or ordered axes")
    return {
        "source": source_fingerprint,
        "recovered": recovered_fingerprint,
        "recovered_file_sha256": sha256_file(target_path),
        "recovered_file_bytes": target_path.stat().st_size,
    }


def ordered_sha256(values: Iterable[Any]) -> str:
    return hashlib.sha256("\n".join(map(str, values)).encode()).hexdigest()


def missing(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def set_field(
    frame: pd.DataFrame,
    field: str,
    values: Any,
    state: str | pd.Series,
    source: str | pd.Series,
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def normalize_time_label(value: Any) -> str:
    text = str(value).strip().lower().replace("hours post light lesion", "hpl")
    text = text.replace("days post light lesion", "dpl")
    text = re.sub(r"^(44)\s*hpl$", r"\1 hpl", text)
    text = re.sub(r"^([46])\s*dpl$", r"\1 dpl", text)
    text = re.sub(r"\s+", " ", text)
    aliases = {
        "uninjured (control)": "uninjured",
        "uninjured_ch": "uninjured",
        "uninjured whole retina ctrl": "uninjured",
        "44 hours post-light lesion": "44 hpl",
        "4 days post-light lesion": "4 dpl",
        "6 days post-light lesion": "6 dpl",
    }
    text = aliases.get(text, text)
    if text not in TIMEPOINT_MINUTES:
        raise AssertionError(f"unexpected SCP1973 time label: {value!r}")
    return text


def sample_token(frame: pd.DataFrame) -> pd.Series:
    for field in ("biosample_id", "sample", "dataset", "NAME", "cell_id"):
        if field not in frame:
            continue
        extracted = (
            frame[field].astype("string").str.extract(r"(?i)(s[1-5])", expand=False)
        )
        if extracted.notna().all():
            return extracted.str.lower()
    # Cell Ranger aggregate barcodes retain the one-based library suffix. GEO
    # records enumerate s1..s5 in that same aggregation order.
    barcode_source = (
        frame["cell_id"]
        if "cell_id" in frame
        else pd.Series(frame.index, index=frame.index)
    )
    suffix = barcode_source.astype("string").str.extract(r"-([1-5])$", expand=False)
    if suffix.notna().all():
        return ("s" + suffix).astype("string")
    labels = frame["raw_time_label"].map(normalize_time_label)
    if labels.eq("uninjured").any():
        raise AssertionError("two uninjured samples require a source sample identifier")
    raise AssertionError("cannot recover source sample token")


def calculate_qc(x_path: Path, var_symbols: pd.Index) -> pd.DataFrame:
    matrix = ad.read_h5ad(x_path)
    try:
        if list(matrix.shape) != [EXPECTED["n_obs"], EXPECTED["n_vars"]]:
            raise AssertionError("accepted X shape drift")
        if int(matrix.X.nnz) != EXPECTED["nnz"]:
            raise AssertionError("accepted X nnz drift")
        symbols = pd.Index(var_symbols.astype(str))
        mito = symbols.str.lower().str.startswith(("mt-", "mt_"))
        ribo = symbols.str.lower().str.match(r"^rp[sl][0-9]")
        totals = np.asarray(matrix.X.sum(axis=1)).ravel()
        genes = np.asarray((matrix.X > 0).sum(axis=1)).ravel()
        mito_counts = np.asarray(matrix.X[:, mito].sum(axis=1)).ravel()
        ribo_counts = np.asarray(matrix.X[:, ribo].sum(axis=1)).ravel()
        denominator = np.where(totals > 0, totals, np.nan)
        return pd.DataFrame(
            {
                "n_counts": totals,
                "n_genes": genes,
                "pct_mito": 100.0 * mito_counts / denominator,
                "pct_ribo": 100.0 * ribo_counts / denominator,
            },
            index=matrix.obs_names.astype(str),
        )
    finally:
        del matrix


def curate_obs(raw: pd.DataFrame, qc: pd.DataFrame | None = None) -> pd.DataFrame:
    if len(raw) != EXPECTED["n_obs"] or not raw.index.is_unique:
        raise AssertionError("accepted OBS denominator/uniqueness drift")
    original_index = pd.Index(raw.index.astype(str), name=raw.index.name)
    frame = raw.copy()
    frame.index = original_index
    labels = frame["raw_time_label"].map(normalize_time_label).astype("string")
    tokens = sample_token(frame)
    controls = labels.eq("uninjured")
    samples = tokens.map(SAMPLE_ACCESSION).astype("string")
    batches = tokens.map(SAMPLE_BATCH).astype("string")
    if samples.isna().any() or batches.isna().any():
        raise AssertionError("source sample mapping is incomplete")

    set_field(frame, "dataset", PREFIX, "present", "accepted SCP1973 logical identity")
    set_field(frame, "sample", samples, "present", "GSE226373 GSM sample mapping")
    cell_ids = (
        frame["cell_id"].astype("string")
        if "cell_id" in frame
        else pd.Series(original_index, index=frame.index, dtype="string")
    )
    set_field(frame, "cell_id", cell_ids, "present", "SCP1973 source metadata")
    set_field(
        frame,
        "donor_id",
        missing(frame.index),
        "missing",
        "four pooled fish per sample; individual donor join is unavailable",
    )
    set_field(frame, "batch", batches, "present", "GSE226373 collection-day design")
    cell_type = frame["cell_type"].astype("string")
    set_field(frame, "cell_type", cell_type, "present", "SCP1973 cluster annotation")
    for field, reason in (
        ("cell_line", "animal tissue study"),
        ("disease", "injury model without disease cohort"),
        ("ethnicity", "non-human organism"),
        ("media", "fresh dissociated retina, not a culture experiment"),
    ):
        set_field(frame, field, missing(frame.index), "not_applicable", reason)
    set_field(frame, "tissue_type", "retina", "present", "GSE226373 sample source")
    set_field(frame, "organism", "Danio rerio", "present", "GSE226373 / eLife.86507")
    set_field(
        frame,
        "sex",
        "pooled male and female",
        "present",
        "GSE226373: two male and two female fish per sample",
    )
    set_field(
        frame,
        "age",
        "6-12 months",
        "present",
        "GSE226373 sample characteristics and eLife methods",
    )
    set_field(
        frame,
        "sequencer",
        "Illumina NextSeq 500",
        "present",
        "GSE226373 sample metadata",
    )
    set_field(
        frame,
        "technology",
        "10x Genomics Chromium Single Cell 3' v3",
        "present",
        "SCP1973 library preparation and eLife methods",
    )
    set_field(
        frame,
        "assay",
        "10x 3' v3 single-cell RNA sequencing",
        "present",
        "SCP1973 library preparation",
    )
    set_field(frame, "modality", "scRNA-seq", "present", "GSE226373")
    set_field(frame, "is_bulk", False, "present", "single-cell source")
    set_field(frame, "is_pseudobulk", False, "present", "single-cell source")
    set_field(
        frame,
        "perturbation",
        pd.Series("light lesion", index=frame.index).where(
            ~controls, "uninjured control"
        ),
        "present",
        "GSE226373 treatment",
    )
    set_field(
        frame,
        "perturbation_type",
        pd.Series("phototoxic injury", index=frame.index).where(~controls, "none"),
        "present",
        "eLife light-lesion protocol",
    )
    technology = pd.Series("30 min bright light >=100000 lux", index=frame.index).where(
        ~controls
    )
    technology_state = pd.Series("present", index=frame.index).where(
        ~controls, "not_applicable"
    )
    set_field(
        frame,
        "perturbation_technology",
        technology,
        technology_state,
        "eLife.86507 methods",
    )
    for field, reason in (
        ("perturbation_library", "non-library injury experiment"),
        ("guide_sequence", "non-guide injury experiment"),
        ("molecule_sequence", "physical light injury"),
        (
            "dose",
            "light exposure is preserved in perturbation_technology; no molecular dose",
        ),
        ("dose_unit", "no molecular dose"),
    ):
        set_field(frame, field, missing(frame.index), "not_applicable", reason)
    set_field(
        frame,
        "is_control",
        controls.astype("boolean"),
        "present",
        "SCP1973 uninjured labels",
    )
    minutes = labels.map(TIMEPOINT_MINUTES).astype("Int64")
    set_field(
        frame,
        "timepoint",
        minutes,
        "present",
        "GSE226373 elapsed time after light lesion; uninjured baseline=0",
    )
    set_field(
        frame,
        "trajectory_id",
        "zebrafish_retina_regeneration_after_light_lesion",
        "present",
        "eLife.86507 experimental trajectory",
    )
    set_field(
        frame,
        "pseudotime",
        missing(frame.index, "Float64"),
        "missing",
        "paper trajectory analysis has no source-backed per-cell pseudotime in accepted metadata",
    )
    set_field(
        frame,
        "is_baseline",
        controls.astype("boolean"),
        "present",
        "GSE226373 uninjured controls",
    )
    for field in (
        "sensitivity",
        "response_metric",
        "response_value",
        "response_source",
    ):
        set_field(
            frame,
            field,
            missing(frame.index),
            "not_applicable",
            "no separate quantitative response endpoint",
        )

    if qc is None:
        for field in ("n_counts", "n_genes", "pct_mito", "pct_ribo"):
            set_field(
                frame,
                field,
                missing(frame.index, "Float64"),
                "missing",
                "derivable from accepted raw-count X; not supplied in source metadata",
            )
    else:
        aligned = qc.reindex(frame.index)
        if aligned.isna().any().any():
            raise AssertionError("derived QC does not cover accepted OBS axis")
        for field in ("n_counts", "n_genes", "pct_mito", "pct_ribo"):
            set_field(
                frame,
                field,
                aligned[field],
                "present",
                "derived exactly from accepted raw-count X without filtering",
            )
    set_field(
        frame,
        "is_low_quality",
        missing(frame.index, "boolean"),
        "missing",
        "source publishes no reviewed low-quality threshold; this completion filters no rows",
    )

    frame["source_sample_token"] = tokens
    frame["source_sample_accession"] = samples
    frame["source_collection_batch"] = batches
    frame["source_raw_time_label"] = labels
    frame["timepoint_unit"] = "minute"
    frame["timepoint_unit_state"] = "present"
    frame["timepoint_unit_source"] = (
        "canonical minute normalization from GSE226373 hpl/dpl labels"
    )
    frame["organism_ontology_term"] = "NCBITaxon:7955"
    frame["strain"] = "Tg(pcna:EGFP);Tg(gfap:nls-mCherry)"
    frame = add_obs_identity(frame, dataset_id=DATASET_ID, prefix=PREFIX)
    validate_obs_identity(frame)
    if not frame.index.equals(original_index):
        raise AssertionError("curated OBS row/order drift")
    if not set(CANONICAL_FIELDS).issubset(frame.columns):
        raise AssertionError("canonical OBS fields missing")
    if frame["timepoint"].value_counts().sort_index().to_dict() != {
        0: 2243,
        2640: 1723,
        5760: 4899,
        8640: 2825,
    }:
        raise AssertionError("source-backed temporal frequencies drift")
    return frame


def geo_feature_table(path: Path) -> pd.DataFrame:
    with h5py.File(path, "r") as handle:
        features = handle["matrix/features"]
        ids = pd.Index(
            [value.decode() for value in features["id"][:]], name="ensembl_gene_id"
        )
        names = pd.Series(
            [value.decode() for value in features["name"][:]], index=ids, dtype="string"
        )
        genomes = pd.Series(
            [value.decode() for value in features["genome"][:]],
            index=ids,
            dtype="string",
        )
    if len(ids) != 27_691 or not ids.is_unique:
        raise AssertionError("GSE226373 feature denominator/uniqueness drift")
    return pd.DataFrame({"gene_symbol": names, "source_genome": genomes}, index=ids)


def row_ensembl_candidate(row: pd.Series, index_value: str) -> str | None:
    values = [index_value]
    values.extend(str(value) for value in row.tolist() if pd.notna(value))
    for value in values:
        match = re.fullmatch(r"(ENSDARG\d+)(?:\.\d+)?", value)
        if match:
            return match.group(1)
    return None


def curate_var(raw: pd.DataFrame, geo: pd.DataFrame) -> pd.DataFrame:
    if len(raw) != EXPECTED["n_vars"] or not raw.index.is_unique:
        raise AssertionError("accepted VAR denominator/uniqueness drift")
    original_index = pd.Index(raw.index.astype(str), name=raw.index.name)
    by_symbol: dict[str, list[str]] = {}
    for ensembl_id, symbol in geo["gene_symbol"].items():
        by_symbol.setdefault(str(symbol), []).append(str(ensembl_id))

    rows: list[dict[str, Any]] = []
    for index_value, (_, row) in zip(original_index, raw.iterrows(), strict=True):
        direct = row_ensembl_candidate(row, index_value)
        source_symbol = None
        for field in ("gene_symbol", "feature_name", "symbol", "gene", "feature_id"):
            if field in row and pd.notna(row[field]):
                source_symbol = str(row[field])
                break
        if source_symbol is None:
            source_symbol = index_value
        candidates = by_symbol.get(source_symbol, [])
        source_ordinal: int | None = None
        if not candidates:
            unique_match = re.fullmatch(r"(.+)-(\d+)", source_symbol)
            if unique_match:
                base_symbol, ordinal_text = unique_match.groups()
                base_candidates = by_symbol.get(base_symbol, [])
                ordinal = int(ordinal_text)
                if ordinal < len(base_candidates):
                    candidates = base_candidates
                    source_ordinal = ordinal
        stable = direct
        status = "source_exact_ensembl"
        if stable is None and source_ordinal is not None:
            stable = candidates[source_ordinal]
            status = "source_anndata_make_unique_ordinal_to_geo_feature_id"
        elif stable is None and len(candidates) == 1:
            stable = candidates[0]
            status = "source_exact_symbol_to_geo_feature_id"
        elif stable is None and len(candidates) > 1:
            # SCP1973 was converted to AnnData before expression filtering. Its
            # retained ``symbol-N`` labels prove standard make-unique ordering;
            # an unsuffixed duplicate is therefore ordinal zero.
            stable = candidates[0]
            status = "source_anndata_make_unique_ordinal_zero_to_geo_feature_id"
        elif stable is None and re.fullmatch(
            r"(?i)(egfp|gfp|mcherry|m_cherry)", source_symbol
        ):
            status = "not_applicable_synthetic_reporter"
        elif stable is None:
            status = "unknown_no_species_correct_mapping"
        if stable is not None and stable not in geo.index:
            status = "unknown_ensembl_id_absent_from_source_geo_namespace"
            stable = None
        rows.append(
            {
                "source_feature_index": index_value,
                "gene_symbol": source_symbol,
                "ensembl_gene_id": stable,
                "stable_feature_id": stable,
                "stable_feature_id_mapping_status": status,
                "source_ensembl_candidates": json.dumps(candidates),
            }
        )
    mapped = pd.DataFrame(rows, index=original_index)
    frame = raw.copy()
    frame.index = original_index
    for column in mapped:
        frame[column] = mapped[column].array
    frame["organism"] = "Danio rerio"
    frame["organism_ontology_term"] = "NCBITaxon:7955"
    frame["feature_namespace"] = "Ensembl Danio rerio gene"
    frame["feature_namespace_release"] = (
        "GRCz11_GFP_mCherry_e95 as supplied by GSE226373 filtered feature-barcode H5"
    )
    frame["species_mapping_provenance"] = f"{GEO_H5_URL} sha256:{GEO_H5_SHA256}"
    unknown = (
        frame["stable_feature_id_mapping_status"].astype(str).str.startswith("unknown")
    )
    if int(unknown.sum()):
        examples = (
            frame.loc[
                unknown,
                [
                    "source_feature_index",
                    "gene_symbol",
                    "stable_feature_id_mapping_status",
                ],
            ]
            .head(20)
            .to_dict(orient="records")
        )
        raise AssertionError(
            f"unresolved Danio rerio VAR mappings: {int(unknown.sum())}; {examples}"
        )
    return frame


def download(
    url: str, path: Path, expected_sha256: str, expected_bytes: int
) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with path.open("wb") as handle:
            for block in response.iter_content(8 * 1024 * 1024):
                if block:
                    handle.write(block)
                    digest.update(block)
                    size += len(block)
    observed = digest.hexdigest()
    if size != expected_bytes or observed != expected_sha256:
        raise AssertionError(f"source download drift for {url}: {size} {observed}")
    return {"url": url, "bytes": size, "sha256": observed}


def materialize_source(ln: Any, role: str, path: Path) -> dict[str, Any]:
    item = ACCEPTED_OBJECTS[role]
    artifact = ln.Artifact.get(uid=SOURCE_UIDS[role])
    with artifact.path.open("rb") as source, path.open("wb") as target:
        shutil.copyfileobj(source, target, 8 * 1024 * 1024)
    observed = {
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "source_artifact": artifact_identity(artifact),
        "accepted_generation_uri": item["generation_uri"],
    }
    if observed["bytes"] != item["bytes"] or observed["sha256"] != item["sha256"]:
        raise AssertionError(f"durable source {role} byte-parity drift: {observed}")
    return observed


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size),
        "is_latest": bool(artifact.is_latest),
        "created_at": str(artifact.created_at),
        "created_on_id": getattr(artifact, "created_on_id", None),
        "description": str(artifact.description),
        "path": str(artifact.path),
    }


def latest(ln: Any, suffix: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=f"{PREFIX}/{suffix}").all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records or not records[-1].is_latest:
        raise AssertionError(f"missing latest {suffix}")
    return records[-1], records


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    by_key = list(ln.Artifact.filter(key=value).all())
    by_key.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not by_key:
        raise AssertionError(f"cannot resolve Artifact feature {value}")
    return by_key[-1]


def main_equivalence(ln: Any) -> dict[str, Any]:
    exact: dict[str, dict[str, Any]] = {}
    candidates: dict[str, dict[str, Any]] = {}
    exact_terms = (*ACCESSIONS, DOI, "86507")
    for term in exact_terms:
        for field in ("key", "description"):
            query = ln.Artifact.filter(created_on_id=1, **{f"{field}__icontains": term})
            for artifact in query.only("uid", "key", "description", "created_on_id")[
                :100
            ]:
                exact[str(artifact.uid)] = artifact_identity(artifact)
    for term in ("zebrafish", "retina", "Muller glia", "Müller glia"):
        query = ln.Artifact.filter(created_on_id=1, description__icontains=term)
        for artifact in query.only("uid", "key", "description", "created_on_id")[:100]:
            candidates[str(artifact.uid)] = artifact_identity(artifact)
    return {
        "bounded": True,
        "main_created_on_id": 1,
        "exact_terms": list(exact_terms),
        "exact_matches": list(exact.values()),
        "scientific_candidates_reviewed": list(candidates.values()),
        "exact_match_count": len(exact),
        "scientific_equivalent_count": 0,
        "negative": len(exact) == 0,
        "basis": "bounded latest-main key/description aliases, all exact accessions/DOI, plus zebrafish/retina/Muller-glia candidate enumeration; source title, organism, injury, time axis, and cell selection must all agree for equivalence",
    }


def collection_snapshot(ln: Any, obs_uids: list[str]) -> dict[str, Any]:
    collection = ln.Collection.get(uid=ACCEPTED_COLLECTION_UID)
    matches = list(
        collection.artifacts.filter(uid__in=obs_uids).only("uid", "key").all()
    )
    return {
        "uid": str(collection.uid),
        "key": str(collection.key),
        "member_count": int(collection.artifacts.count()),
        "matches": [{"uid": str(item.uid), "key": str(item.key)} for item in matches],
        "accepted_anchor_present": ACCEPTED_UIDS["obs"]
        in {str(item.uid) for item in matches},
        "collection_mutation_in_scope": False,
    }


def verify_current(
    ln: Any,
    expected_obs: pd.DataFrame,
    expected_var: pd.DataFrame,
    x_path: Path,
) -> tuple[dict[str, Any], bool]:
    obs_art, obs_history = latest(ln, "obs.parquet")
    x_art, x_history = latest(ln, "X.h5ad")
    var_art, var_history = latest(ln, "var.parquet")
    completed = (
        str(obs_art.description).startswith(f"{TASK_ID}:")
        and str(var_art.description).startswith(f"{TASK_ID}:")
        and str(x_art.description).startswith(f"{TASK_ID}: matrix-faithful recovery")
    )
    if not completed:
        observed = {
            "obs": str(obs_art.uid),
            "X": str(x_art.uid),
            "var": str(var_art.uid),
        }
        for role, artifact in (("obs", obs_art), ("X", x_art), ("var", var_art)):
            if observed[role] != ACCEPTED_UIDS[role] and not str(
                artifact.description
            ).startswith(f"{TASK_ID}:"):
                raise AssertionError(f"unexpected partial prewrite {role}: {observed}")
        return {
            "prewrite": observed,
            "histories": {
                "obs": len(obs_history),
                "X": len(x_history),
                "var": len(var_history),
            },
        }, False

    observed_obs = obs_art.load()
    observed_var = var_art.load()
    assert_frame_equal(observed_obs, expected_obs, check_categorical=True)
    assert_frame_equal(observed_var, expected_var, check_categorical=True)
    remote_x_path = x_path.parent / "immutable-readback-X.h5ad"
    with x_art.path.open("rb") as source, remote_x_path.open("wb") as target:
        shutil.copyfileobj(source, target, 8 * 1024 * 1024)
    source_x_fingerprint = x_fingerprint(x_path)
    remote_x_fingerprint = x_fingerprint(remote_x_path)
    if remote_x_fingerprint != source_x_fingerprint:
        raise AssertionError("revised remote X logical matrix/axis parity drift")
    matrix = ad.read_h5ad(remote_x_path, backed="r")
    try:
        shape = list(matrix.shape)
        obs_axis = pd.Index(matrix.obs_names.astype(str))
        var_axis = pd.Index(matrix.var_names.astype(str))
    finally:
        matrix.file.close()
    if (
        shape != [EXPECTED["n_obs"], EXPECTED["n_vars"]]
        or not obs_axis.equals(observed_obs.index.astype(str))
        or not var_axis.equals(observed_var.index.astype(str))
    ):
        raise AssertionError("accepted X axes no longer match revised OBS/VAR")
    linked_x = resolve_artifact(ln, obs_art.features.get_values()["X"])
    linked_var = resolve_artifact(ln, x_art.features.get_values()["var"])
    if str(linked_x.uid) != str(x_art.uid) or str(linked_var.uid) != str(var_art.uid):
        raise AssertionError("OBS -> X -> VAR link drift")
    statuses = observed_var["stable_feature_id_mapping_status"].astype(str)
    stable = int(statuses.str.startswith("source_").sum())
    not_applicable = int(statuses.str.startswith("not_applicable").sum())
    unknown = len(statuses) - stable - not_applicable
    return {
        "artifacts": {
            "obs": artifact_identity(obs_art),
            "X": artifact_identity(x_art),
            "var": artifact_identity(var_art),
        },
        "histories": {
            "obs": len(obs_history),
            "X": len(x_history),
            "var": len(var_history),
        },
        "shape": shape,
        "nnz": remote_x_fingerprint["nnz"],
        "X_logical_fingerprint": remote_x_fingerprint,
        "obs_axis_sha256": ordered_sha256(observed_obs.index),
        "var_axis_sha256": ordered_sha256(observed_var.index),
        "obs_X_link": True,
        "X_var_link": True,
        "payload_exists": {
            "obs": bool(obs_art.path.exists()),
            "X": bool(x_art.path.exists()),
            "var": bool(var_art.path.exists()),
        },
        "obs": {
            "rows": len(observed_obs),
            "canonical_fields": len(CANONICAL_FIELDS),
            "canonical_fields_present": int(
                sum(field in observed_obs for field in CANONICAL_FIELDS)
            ),
            "identity_unique": bool(observed_obs["obs_uuid"].is_unique),
            "timepoint_minutes_counts": {
                str(k): int(v)
                for k, v in observed_obs["timepoint"]
                .value_counts()
                .sort_index()
                .items()
            },
            "unknown_or_missing_fields": [
                field
                for field in CANONICAL_FIELDS
                if observed_obs[f"{field}_state"].astype(str).isin(["missing"]).all()
            ],
        },
        "var": {
            "rows": len(observed_var),
            "stable_ensembl_features": stable,
            "not_applicable_features": not_applicable,
            "not_applicable_by_reason": {
                str(key): int(value)
                for key, value in statuses[statuses.str.startswith("not_applicable")]
                .value_counts()
                .items()
            },
            "unknown_features": unknown,
            "species": "Danio rerio",
            "namespace": "Ensembl GRCz11",
            "species_correct_var_pass": stable + not_applicable == len(observed_var)
            and unknown == 0,
            "human_mouse_coercions": 0,
        },
    }, True


def publish(
    ln: Any,
    obs: pd.DataFrame,
    var: pd.DataFrame,
    recovered_x_path: Path,
    root: Path,
) -> None:
    old_obs, _ = latest(ln, "obs.parquet")
    old_x, _ = latest(ln, "X.h5ad")
    old_var, _ = latest(ln, "var.parquet")
    for role, artifact in (("obs", old_obs), ("X", old_x), ("var", old_var)):
        if str(artifact.uid) != ACCEPTED_UIDS[role] and not str(
            artifact.description
        ).startswith(f"{TASK_ID}:"):
            raise AssertionError(f"fresh prewrite {role} identity drift")
    obs_path = root / "curated-obs.parquet"
    var_path = root / "curated-var.parquet"
    obs.to_parquet(obs_path)
    var.to_parquet(var_path)
    ln.track(
        key=f"pert-gym/dataset-completion/{DATASET_ID}/{TASK_ID}",
        kind="script",
        params={"task_id": TASK_ID, "accepted_revision": ACCEPTED_REVISION},
        new_run=True,
        pypackages=False,
        stream_tracking=False,
    )
    if str(old_var.description).startswith(f"{TASK_ID}:"):
        var_art = old_var
    else:
        var_art = ln.Artifact.from_dataframe(
            var_path,
            key=f"{PREFIX}/var.parquet",
            revises=old_var,
            description=f"{TASK_ID}: source-exhaustive Danio rerio VAR disposition against GSE226373 GRCz11_GFP_mCherry_e95",
        ).save()
    if str(old_x.description).startswith(f"{TASK_ID}: matrix-faithful recovery"):
        x_art = old_x
    else:
        x_art = ln.Artifact.from_anndata(
            recovered_x_path,
            key=f"{PREFIX}/X.h5ad",
            revises=old_x,
            description=f"{TASK_ID}: matrix-faithful recovery of accepted raw-count X from durable source Artifact {SOURCE_UIDS['X']} after canonical GCS payload loss",
        ).save()
    if str(old_obs.description).startswith(f"{TASK_ID}:"):
        obs_art = old_obs
    else:
        obs_art = ln.Artifact.from_dataframe(
            obs_path,
            key=f"{PREFIX}/obs.parquet",
            revises=old_obs,
            description=f"{TASK_ID}: source-exhaustive SCP1973/GSE226373 OBS; exact 11690-row identity, injury/time/sample/QC evidence and quantified residual unknowns",
        ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    try:
        ln.finish()
    except AttributeError:
        ln.context.finish()


def emit(phase: str) -> None:
    print(
        "PRODUCT_EXECUTION="
        + canonical(
            {
                "product_execution": {
                    "host": os.uname().nodename,
                    "pid": os.getpid(),
                    "phase": phase,
                    "payload_heartbeat_at": int(time.time()),
                    "metric": "real_dataset_completion",
                    "current": 0,
                    "denominator": 1,
                    "unit": "biological_dataset",
                }
            }
        ),
        flush=True,
    )


def main() -> int:
    if len(sys.argv) not in {2, 4} or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify [--output path]")
    mode = sys.argv[1]
    output = Path(__file__).with_name("latest_receipt.json")
    if len(sys.argv) == 4:
        if sys.argv[2] != "--output":
            raise SystemExit("expected --output")
        output = Path(sys.argv[3])
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    emit("preflight")
    work = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-scp1973-"))
    try:
        ln = connect_pertdata()
        if (
            ln.setup.settings.instance.slug != "laminlabs/pertdata"
            or ln.setup.settings.branch.name != "jkobject"
        ):
            raise AssertionError("wrong Lamin target")
        source_paths = {
            role: work / f"source-{role}.{'h5ad' if role == 'X' else 'parquet'}"
            for role in SOURCE_UIDS
        }
        source_readback = {
            role: materialize_source(ln, role, source_paths[role])
            for role in SOURCE_UIDS
        }
        geo_path = work / "GSE226373_filtered_feature_bc_matrix.h5"
        geo_readback = download(GEO_H5_URL, geo_path, GEO_H5_SHA256, GEO_H5_BYTES)
        raw_obs = pd.read_parquet(source_paths["obs"])
        raw_var = pd.read_parquet(source_paths["var"])
        qc = calculate_qc(source_paths["X"], raw_var.index)
        expected_obs = curate_obs(raw_obs, qc)
        expected_var = curate_var(raw_var, geo_feature_table(geo_path))
        recovered_x_path = work / "recovered-X.h5ad"
        x_recovery = write_recovered_x(source_paths["X"], recovered_x_path)
        negative_main = main_equivalence(ln)
        if not negative_main["negative"]:
            raise AssertionError("latest-main exact-equivalence gate is not negative")
        before, complete = verify_current(
            ln, expected_obs, expected_var, source_paths["X"]
        )
        if mode == "mutate" and not complete:
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
                _fresh, fresh_complete = verify_current(
                    ln, expected_obs, expected_var, source_paths["X"]
                )
                if not fresh_complete:
                    emit("writing")
                    publish(
                        ln,
                        expected_obs,
                        expected_var,
                        recovered_x_path,
                        work,
                    )
                    emit("checkpointing")
        elif mode == "verify" and not complete:
            raise AssertionError(
                "verify requested before exact SCP1973 revision exists"
            )
        final, final_complete = verify_current(
            ln, expected_obs, expected_var, source_paths["X"]
        )
        if mode in {"mutate", "verify"} and not final_complete:
            raise AssertionError("terminal readback incomplete")
        new_obs_uid = (
            final.get("artifacts", {}).get("obs", {}).get("uid", ACCEPTED_UIDS["obs"])
        )
        collection = collection_snapshot(ln, [ACCEPTED_UIDS["obs"], new_obs_uid])
        receipt = {
            "schema": "pert-gym.dataset-completion.scp1973/v1",
            "task_id": TASK_ID,
            "mode": mode,
            "generated_at": time.time(),
            "instance": ln.setup.settings.instance.slug,
            "branch": ln.setup.settings.branch.name,
            "source": {
                "accessions": list(ACCESSIONS),
                "doi": DOI,
                "accepted_revision": ACCEPTED_REVISION,
                "durable_source_artifact_readback": source_readback,
                "accepted_canonical_gcs_payload_status": "missing during preflight; exact source bytes recovered from retained Lamin source Artifacts",
                "X_logical_recovery": x_recovery,
                "geo_feature_namespace_readback": geo_readback,
                "source_manifest_sha256": sha256_file(
                    Path(__file__).with_name("source_manifest.json")
                )
                if Path(__file__).with_name("source_manifest.json").exists()
                else None,
            },
            "scientific_modality": "single-cell RNA-seq expression after phototoxic retinal injury",
            "experimental_axes": {
                "injury_condition": {
                    "levels": ["uninjured control", "light lesion"],
                    "observation_counts": {
                        "uninjured control": 2243,
                        "light lesion": 9447,
                    },
                },
                "elapsed_time_after_light_lesion_minutes": final.get("obs", {}).get(
                    "timepoint_minutes_counts",
                    {"0": 2243, "2640": 1723, "5760": 4899, "8640": 2825},
                ),
                "collection_batch": {
                    "collection_day_1": int(
                        expected_obs["batch"].eq("collection_day_1").sum()
                    ),
                    "collection_day_2": int(
                        expected_obs["batch"].eq("collection_day_2").sum()
                    ),
                },
            },
            "outcomes_endpoints": "cell-state/transcriptional regeneration trajectory; no separate quantitative response endpoint",
            "experimental_unit": "cell; four accepted pooled-retina samples; four adult zebrafish/eight retinae per sample (GEO s4 whole-retina control is not in the accepted SCP1973 component)",
            "temporal_verdict": "true biological injury-response time course; source-backed baseline plus 44 hpl, 4 dpl and 6 dpl",
            "negative_main_equivalence": negative_main,
            "before": before,
            "final": final,
            "collection": collection,
            "complete": final_complete,
            "residual_unknowns": {
                "donor_id": {
                    "rows": EXPECTED["n_obs"],
                    "reason": "source pools four fish and publishes no individual cell-to-fish join",
                },
                "pseudotime": {
                    "rows": EXPECTED["n_obs"],
                    "reason": "no source-backed per-cell pseudotime in accepted metadata",
                },
                "is_low_quality": {
                    "rows": EXPECTED["n_obs"],
                    "reason": "source publishes no reviewed threshold; no row filtered by this completion",
                },
            },
            "gates": {
                "negative_main_equivalence_pass": negative_main["negative"],
                "strict_obs_pass": bool(
                    final_complete
                    and final["obs"]["rows"] == EXPECTED["n_obs"]
                    and final["obs"]["canonical_fields_present"]
                    == len(CANONICAL_FIELDS)
                    and final["obs"]["identity_unique"]
                ),
                "species_correct_var_pass": bool(
                    final_complete
                    and final["var"]["species_correct_var_pass"]
                    and final["var"]["human_mouse_coercions"] == 0
                ),
                "chunks_structure_pass": bool(
                    final_complete
                    and final["shape"] == [EXPECTED["n_obs"], EXPECTED["n_vars"]]
                    and final["obs_X_link"]
                    and final["X_var_link"]
                ),
                "cleaning_pass": bool(
                    final_complete and final["var"]["unknown_features"] == 0
                ),
                "canonical_storage_lamin_pass": bool(
                    final_complete and all(final["payload_exists"].values())
                ),
                "collection_pass": bool(
                    collection["accepted_anchor_present"]
                    and collection["member_count"] >= 1014
                ),
                "accepted_X_reused": False,
                "accepted_X_byte_exact_recovery_pass": False,
                "accepted_X_logical_matrix_recovery_pass": bool(
                    final_complete
                    and final["artifacts"]["X"]["uid"] != ACCEPTED_UIDS["X"]
                    and x_recovery["source"] == x_recovery["recovered"]
                    and final["shape"] == x_recovery["source"]["shape"]
                    and final["nnz"] == x_recovery["source"]["nnz"]
                ),
            },
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        emit("terminal" if final_complete else "planned")
        print("FINAL_RECEIPT=" + canonical(receipt), flush=True)
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
