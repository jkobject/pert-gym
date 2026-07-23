#!/usr/bin/env python3
"""Append-only source-exhaustive OBS+VAR curation for GEO GSE203592."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import re
import sys
import tempfile
import time
import urllib.request
from collections import defaultdict
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_40b72cca"
REAL_DATASET_ID = "geo/GSE203592"
PREFIX = "prism_collection/GSE203592"
EXPECTED_N_OBS = 70_646
EXPECTED_N_VARS = 31_053
EXPECTED_X = {"uid": "PhpiVnUwNAeZ26m40000", "hash": "149qxI5aIlaiidnLRu6RPI"}
FROZEN_INPUT_BINDINGS_PATH = Path(__file__).with_name("frozen_inputs") / "bindings.json"
SOURCE_MANIFEST_PATH = Path(__file__).with_name("source_manifest.json")
GTF_URL = (
    "https://ftp.ensembl.org/pub/release-93/gtf/mus_musculus/"
    "Mus_musculus.GRCm38.93.gtf.gz"
)
GTF_SIZE = 29_193_889
GTF_SHA256 = "d88c3e541b2916e5fe4662cdc9ec2007a7b074d6e5ccf59f643229cf9b88d1dd"
ATTR_RE = re.compile(r'([A-Za-z0-9_]+) "([^"]*)";')
UNIQUE_SUFFIX_RE = re.compile(r"^(?P<base>.+)\.(?P<suffix>[1-9][0-9]*)$")
GUIDE_ID_RE = re.compile(
    r"^(?:ACTR5|ARID1A|ARID1B|ARID2|GATA3|INO80C|PDCD1|SMARCC1|SMARCD2)-[1-4]$|^CTRL1-(?:[1-9]|1[0-2])$"
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
    "guide_id",
    "guide_sequence",
    "perturbation_target",
    "perturbation_target_id",
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
    "source",
    "source_accession",
    "control_availability",
    "x_semantics",
)
NOT_APPLICABLE_FIELDS = {
    "cell_line",
    "dose",
    "dose_unit",
    "trajectory_id",
    "pseudotime",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
}
UNKNOWN_FIELDS = {
    "donor_id",
    "sex",
    "age",
    "ethnicity",
    "perturbation_target_id",
    "pct_ribo",
    "is_low_quality",
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_values_sha256(values: pd.Index) -> str:
    return sha256_bytes("\n".join(values.astype(str)).encode())


def load_frozen_input_bindings() -> dict[str, Any]:
    manifest = json.loads(FROZEN_INPUT_BINDINGS_PATH.read_text())
    if manifest.get("format") != "pert-gym.frozen-input-bindings/v1":
        raise AssertionError("frozen input format drift")
    repository_root = Path(__file__).parents[5]
    for entry in manifest["inputs"]:
        compressed = (repository_root / entry["binding_path"]).read_bytes()
        if sha256_bytes(compressed) != entry["gzip_sha256"]:
            raise AssertionError("frozen gzip hash drift")
        raw = gzip.decompress(compressed)
        if (
            len(raw) != entry["uncompressed_bytes"]
            or sha256_bytes(raw) != entry["uncompressed_sha256"]
        ):
            raise AssertionError("frozen input identity drift")
    if len(manifest["inputs"]) != 2:
        raise AssertionError("frozen input coverage drift")
    return manifest


def load_source_manifest() -> dict[str, Any]:
    manifest = json.loads(SOURCE_MANIFEST_PATH.read_text())
    if manifest.get("format") != "pert-gym.gse203592-source-evidence/v1":
        raise AssertionError("source manifest format drift")
    if manifest.get("dataset_id") != PREFIX or not manifest.get(
        "search_effort_complete"
    ):
        raise AssertionError("source evidence is not closed")
    return manifest


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "version": str(artifact.version),
        "size": int(artifact.size),
        "n_observations": getattr(artifact, "n_observations", None),
        "created_at": str(artifact.created_at),
        "description": str(artifact.description),
        "run_uid": str(getattr(getattr(artifact, "run", None), "uid", None)),
    }


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    if not records:
        raise AssertionError(f"missing Artifact history: {key}")
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not bool(records[-1].is_latest):
        raise AssertionError(f"newest Artifact is not latest: {key}")
    return records[-1], records


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    records = list(ln.Artifact.filter(key=value).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"cannot resolve linked Artifact: {value}")
    return records[-1]


def download_gtf() -> tuple[Path, dict[str, Any]]:
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-gse203592-sources"
    root.mkdir(parents=True, exist_ok=True)
    path = root / Path(GTF_URL).name
    if not path.exists() or path.stat().st_size != GTF_SIZE:
        digest = hashlib.sha256()
        with (
            urllib.request.urlopen(GTF_URL, timeout=120) as response,
            path.open("wb") as handle,
        ):
            while chunk := response.read(1024 * 1024):
                handle.write(chunk)
                digest.update(chunk)
        if digest.hexdigest() != GTF_SHA256:
            raise AssertionError("downloaded GTF hash drift")
    if path.stat().st_size != GTF_SIZE or sha256_file(path) != GTF_SHA256:
        raise AssertionError("GTF source identity drift")
    return path, {"url": GTF_URL, "bytes": GTF_SIZE, "sha256": GTF_SHA256}


def parse_gtf(path: Path) -> tuple[dict[str, list[str]], int]:
    by_symbol: dict[str, list[str]] = defaultdict(list)
    gene_rows = 0
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 9 or fields[2] != "gene":
                continue
            attributes = dict(ATTR_RE.findall(fields[8]))
            gene_id = attributes.get("gene_id", "").split(".", maxsplit=1)[0]
            symbol = attributes.get("gene_name", "")
            if gene_id and symbol and gene_id not in by_symbol[symbol]:
                by_symbol[symbol].append(gene_id)
                gene_rows += 1
    return by_symbol, gene_rows


def split_unique_symbol(symbol: str) -> tuple[str, int]:
    match = UNIQUE_SUFFIX_RE.fullmatch(symbol)
    if match is None:
        return symbol, 0
    return match.group("base"), int(match.group("suffix"))


def map_mouse_features(
    symbols: pd.Index, by_symbol: dict[str, list[str]]
) -> pd.DataFrame:
    axis_groups: dict[str, list[tuple[int, int]]] = defaultdict(list)
    stable = pd.Series(pd.NA, index=range(len(symbols)), dtype="string")
    status = pd.Series(
        "unmapped_release93_symbol", index=range(len(symbols)), dtype="string"
    )
    candidate_count = pd.Series(0, index=range(len(symbols)), dtype="Int64")
    for position, symbol in enumerate(symbols.astype(str)):
        exact_ids = by_symbol.get(symbol, [])
        if len(exact_ids) == 1:
            stable.iloc[position] = exact_ids[0]
            status.iloc[position] = "mapped_exact_release93_gene_name"
            candidate_count.iloc[position] = 1
            continue
        base, suffix = split_unique_symbol(symbol)
        axis_groups[base].append((suffix, position))

    for base, members in axis_groups.items():
        members.sort()
        source_ids = by_symbol.get(base, [])
        expected_suffixes = list(range(len(members)))
        observed_suffixes = [suffix for suffix, _ in members]
        for _, position in members:
            candidate_count.iloc[position] = len(source_ids)
        if len(source_ids) == len(members) and observed_suffixes == expected_suffixes:
            for source_id, (_, position) in zip(source_ids, members, strict=True):
                stable.iloc[position] = source_id
                status.iloc[position] = "mapped_release93_make_unique_order"
        elif source_ids:
            for _, position in members:
                status.iloc[position] = "ambiguous_release93_gene_name"

    mapped = stable.dropna()
    if not mapped.str.fullmatch(r"ENSMUSG\d{11}").all() or not mapped.is_unique:
        raise AssertionError("mapped mouse stable IDs are invalid or duplicated")
    return pd.DataFrame(
        {
            "stable_feature_id": stable.array,
            "stable_feature_id_mapping_status": status.array,
            "stable_feature_id_candidate_count": candidate_count.array,
        },
        index=symbols,
    )


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: Any, source: str
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing_series(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def curate_obs(obs: pd.DataFrame, *, x_semantics: str | None) -> pd.DataFrame:
    required = {
        "obs_uuid",
        "original_obs_index",
        "cell_barcode",
        "experiment_id",
        "orig_batch",
        "tissue_type",
        "perturbation",
        "orig_guide",
        "is_control",
        "condition",
        "nCount_RNA",
        "nFeature_RNA",
        "percent.mt",
    }
    if missing := required - set(obs.columns):
        raise AssertionError(
            f"required source-preserved OBS columns absent: {sorted(missing)}"
        )
    original = obs.copy(deep=True)
    curated = obs.copy(deep=True)
    for field in CANONICAL_OBS_FIELDS:
        if field in original and f"prior_canonical_{field}" not in original:
            curated[f"prior_canonical_{field}"] = original[field]

    controls = original["is_control"].astype("boolean")
    expected_controls = original["condition"].astype("string").eq("control")
    if not controls.astype(bool).equals(expected_controls.astype(bool)):
        raise AssertionError("control semantics drift")
    source_cell_type = original["tissue_type"].astype("string")
    if source_cell_type.nunique(dropna=True) != 1 or source_cell_type.iloc[0] != (
        "CD8+ tumor infiltrating T cells"
    ):
        raise AssertionError("source cell type drift")

    guide = original["orig_guide"].astype("string")
    guide_id = guide.where(guide.str.fullmatch(GUIDE_ID_RE.pattern, na=False), pd.NA)
    target = (
        original["perturbation"]
        .astype("string")
        .where(
            ~controls
            & original["perturbation"].astype("string").ne("multiple-targeting"),
            pd.NA,
        )
    )
    guide_state = np.where(guide_id.notna(), "known", "unknown")
    target_state = np.where(target.notna(), "known", "unknown")

    set_field(curated, "dataset", PREFIX, "known", "canonical logical family")
    set_field(
        curated,
        "sample",
        original["experiment_id"].astype("string"),
        "known",
        "source-preserved experiment_id; each value is one tumor/capture aggregate",
    )
    set_field(
        curated,
        "cell_id",
        original["cell_barcode"].astype("string"),
        "known",
        "source-preserved cell barcode",
    )
    set_field(
        curated,
        "batch",
        original["orig_batch"].astype("string"),
        "known",
        "source-preserved V35/V41 experiment batch",
    )
    set_field(curated, "cell_type", source_cell_type, "known", "source TIL annotation")
    set_field(
        curated,
        "disease",
        "MC38 colorectal adenocarcinoma model",
        "known",
        "GEO sample and publication Methods",
    )
    set_field(curated, "tissue_type", "tumor", "known", "GEO sample source")
    set_field(curated, "organism", "Mus musculus", "known", "GEO GSE203592")
    set_field(curated, "sequencer", "Illumina NextSeq 500", "known", "GEO GPL19057")
    set_field(
        curated,
        "technology",
        "10x Chromium Next GEM Single Cell V(D)J 5' v1.1 with Feature Barcoding",
        "known",
        "publication direct-capture Perturb-seq Methods",
    )
    set_field(
        curated,
        "assay",
        "direct-capture Perturb-seq",
        "known",
        "publication Methods",
    )
    set_field(curated, "modality", "scRNA-seq", "known", "GEO experiment type")
    set_field(
        curated,
        "media",
        "RPMI 1640 + 10% FBS + penicillin/streptomycin + beta-mercaptoethanol + IL-2",
        "known",
        "publication ex vivo T-cell culture Methods",
    )
    set_field(curated, "is_bulk", False, "known", "single-cell source")
    set_field(curated, "is_pseudobulk", False, "known", "single-cell source")
    set_field(
        curated,
        "perturbation",
        original["perturbation"].astype("string"),
        "known",
        "source-preserved PRISM perturbation annotation",
    )
    set_field(
        curated,
        "perturbation_type",
        "CRISPR-Cas9 knockout",
        "known",
        "publication Methods and Rosa26-Cas9 model",
    )
    set_field(
        curated,
        "perturbation_technology",
        "retroviral direct-capture CRISPR-Cas9 Perturb-seq",
        "known",
        "publication Methods",
    )
    set_field(
        curated,
        "perturbation_library",
        "custom 48-sgRNA pool: 36 guides targeting nine genes plus 12 non-targeting controls",
        "known",
        "publication Methods and Table S2",
    )
    set_field(
        curated,
        "guide_id",
        guide_id,
        guide_state,
        "source-preserved orig_guide; aggregate zMulti/zNone labels are not guide IDs",
    )
    set_field(
        curated,
        "guide_sequence",
        missing_series(curated.index),
        "unknown",
        "source-exhaustive GEO/paper/Table S2 search did not publish nucleotide sequences",
    )
    set_field(
        curated,
        "perturbation_target",
        target,
        target_state,
        "source perturbation; non-targeting and aggregate multiple-targeting rows have no single target",
    )
    set_field(
        curated, "is_control", controls, "known", "source condition/control annotation"
    )
    set_field(
        curated,
        "timepoint",
        21_600.0,
        "known",
        "publication: tumor T cells recovered 15 days post adoptive transfer; minutes",
    )
    set_field(curated, "is_baseline", False, "known", "single day-15 endpoint source")
    set_field(
        curated,
        "n_counts",
        pd.to_numeric(original["nCount_RNA"], errors="raise").astype("Float64"),
        "known",
        "source-preserved nCount_RNA",
    )
    set_field(
        curated,
        "n_genes",
        pd.to_numeric(original["nFeature_RNA"], errors="raise").astype("Int64"),
        "known",
        "source-preserved nFeature_RNA",
    )
    set_field(
        curated,
        "pct_mito",
        pd.to_numeric(original["percent.mt"], errors="raise").astype("Float64"),
        "known",
        "source-preserved percent.mt",
    )
    set_field(curated, "source", "GEO", "known", "processed source authority")
    set_field(curated, "source_accession", "GSE203592", "known", "GEO series")
    set_field(
        curated,
        "control_availability",
        "non-targeting_control_available",
        "known",
        "12-guide non-targeting control pool and source control annotations",
    )
    if x_semantics is None:
        set_field(
            curated,
            "x_semantics",
            missing_series(curated.index),
            "unknown",
            "exact accepted X representation not proven count-like",
        )
    else:
        set_field(
            curated,
            "x_semantics",
            x_semantics,
            "known",
            "exact accepted X backed sparse-data validation",
        )

    for field in NOT_APPLICABLE_FIELDS:
        dtype = (
            "Float64" if field in {"dose", "pseudotime", "response_value"} else "string"
        )
        set_field(
            curated,
            field,
            missing_series(curated.index, dtype),
            "not_applicable",
            "dataset design",
        )
    for field in UNKNOWN_FIELDS:
        dtype = "Float64" if field in {"age", "pct_ribo"} else "string"
        if field == "is_low_quality":
            dtype = "boolean"
        set_field(
            curated,
            field,
            missing_series(curated.index, dtype),
            "unknown",
            "source-exhaustive search found no defensible cell-level value",
        )
    set_field(
        curated,
        "control_type",
        pd.Series(
            np.where(controls, "non-targeting_sgRNA", pd.NA),
            index=curated.index,
            dtype="string",
        ),
        np.where(controls, "known", "not_applicable"),
        "source guide/control annotation",
    )

    if len(curated) != EXPECTED_N_OBS or not curated.index.equals(original.index):
        raise AssertionError("OBS row count/order drift")
    if not curated["obs_uuid"].is_unique or not curated["original_obs_index"].is_unique:
        raise AssertionError("OBS identity uniqueness drift")
    return curated


def curate_var(var: pd.DataFrame, by_symbol: dict[str, list[str]]) -> pd.DataFrame:
    original = var.copy(deep=True)
    mapping = map_mouse_features(var.index, by_symbol)
    curated = var.copy(deep=True)
    for field in (
        "stable_feature_id",
        "ensembl_gene_id",
        "stable_feature_id_source",
        "stable_feature_id_mapping_status",
        "stable_feature_id_candidate_count",
    ):
        if field in original and f"prior_{field}" not in original:
            curated[f"prior_{field}"] = original[field]
    stable = mapping["stable_feature_id"].astype("string")
    curated["stable_feature_id"] = stable
    curated["ensembl_gene_id"] = stable
    curated["stable_feature_id_namespace"] = "Ensembl stable gene ID"
    curated["stable_feature_id_source"] = (
        "Ensembl release 93 Mus_musculus.GRCm38.93.gtf gene_name mapping"
    )
    curated["stable_feature_id_mapping_status"] = mapping[
        "stable_feature_id_mapping_status"
    ].array
    curated["stable_feature_id_candidate_count"] = mapping[
        "stable_feature_id_candidate_count"
    ].array
    curated["organism"] = "Mus musculus"
    curated["feature_class"] = "gene"
    curated["feature_index"] = stable.fillna(
        pd.Series(var.index.astype(str), index=var.index)
    )
    curated["feature_index_namespace"] = np.where(
        stable.notna(), "Ensembl stable gene ID", "source gene symbol"
    )
    curated["feature_index_source"] = np.where(
        stable.notna(),
        "Ensembl release 93 exact gene_name mapping",
        "source axis fallback; ambiguous release-93 symbol",
    )
    if not curated.index.equals(original.index):
        raise AssertionError("VAR row order drift")
    return curated


def verify_obs_semantics(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    if not actual.index.equals(expected.index) or list(actual.columns) != list(
        expected.columns
    ):
        raise AssertionError("OBS schema/order mismatch")
    try:
        assert_frame_equal(actual, expected, check_categorical=True)
    except AssertionError as error:
        raise AssertionError("OBS source semantic mismatch") from error


def field_dispositions(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in CANONICAL_OBS_FIELDS:
        state_column = f"{field}_state"
        if field not in frame or state_column not in frame:
            raise AssertionError(f"canonical OBS disposition absent: {field}")
        states = frame[state_column].astype("string")
        known = int(frame[field].notna().sum())
        if states.eq("not_applicable").all():
            disposition = "not_applicable"
        elif states.eq("unknown").all():
            disposition = "unknown"
        elif known == len(frame):
            disposition = "materialized_complete"
        else:
            disposition = "materialized_partial"
        result[field] = {
            "disposition": disposition,
            "materialized": True,
            "known_rows": known,
            "unknown_rows": len(frame) - known,
            "state_counts": {str(k): int(v) for k, v in states.value_counts().items()},
            "source_bound": disposition.startswith("materialized"),
        }
    return result


def inspect_x(artifact: Any) -> tuple[pd.Index, dict[str, Any], str | None]:
    if {"uid": str(artifact.uid), "hash": str(artifact.hash)} != EXPECTED_X:
        raise AssertionError("accepted X identity drift")
    path = Path(artifact.cache())
    backed = ad.read_h5ad(path, backed="r")
    if (backed.n_obs, backed.n_vars) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
        raise AssertionError("X shape drift")
    axis = backed.var_names.astype(str).copy()
    sample = backed.X[: min(2_000, backed.n_obs)]
    if hasattr(sample, "data"):
        values = np.asarray(sample.data)
    else:
        values = np.asarray(sample).ravel()
    finite = bool(np.isfinite(values).all())
    nonnegative = bool((values >= 0).all()) if len(values) else True
    integral = bool(np.allclose(values, np.rint(values))) if len(values) else True
    semantics = "raw_counts" if finite and nonnegative and integral else None
    receipt = {
        **EXPECTED_X,
        "shape": [backed.n_obs, backed.n_vars],
        "var_names_sha256": ordered_values_sha256(axis),
        "backed_only": True,
        "layers": list(backed.layers.keys()),
        "raw_present": backed.raw is not None,
        "sample_rows": min(2_000, backed.n_obs),
        "sample_nonzero_values": len(values),
        "sample_finite": finite,
        "sample_nonnegative": nonnegative,
        "sample_integral": integral,
        "inferred_semantics": semantics,
        "inference_scope": "bounded sample; unresolved unless count-like",
    }
    backed.file.close()
    return axis, receipt, semantics


def verify_var(var: pd.DataFrame, x_axis: pd.Index) -> dict[str, Any]:
    if len(var) != EXPECTED_N_VARS or not var.index.astype(str).equals(
        x_axis.astype(str)
    ):
        raise AssertionError("VAR/X feature-axis count/order drift")
    stable = var["stable_feature_id"].astype("string")
    mapped = stable.dropna()
    if not mapped.str.fullmatch(r"ENSMUSG\d{11}").all() or not mapped.is_unique:
        raise AssertionError("VAR mouse stable-ID contract drift")
    coverage = float(stable.notna().mean())
    if coverage < 0.99:
        statuses = {
            str(key): int(value)
            for key, value in var["stable_feature_id_mapping_status"]
            .astype("string")
            .value_counts(dropna=False)
            .items()
        }
        raise AssertionError(
            f"VAR mouse stable-ID coverage below 99%: coverage={coverage}, "
            f"statuses={statuses}"
        )
    if not var["organism"].astype("string").eq("Mus musculus").all():
        raise AssertionError("VAR organism drift")
    feature_index = var["feature_index"].astype("string")
    if feature_index.isna().any() or not feature_index.is_unique:
        raise AssertionError("VAR feature_index uniqueness drift")
    return {
        "rows": len(var),
        "mapped_mouse_stable_ids": int(stable.notna().sum()),
        "unresolved_stable_ids": int(stable.isna().sum()),
        "stable_feature_id_coverage": coverage,
        "stable_feature_id_unique_when_known": True,
        "wrong_species_rows": int(
            (~stable.str.fullmatch(r"ENSMUSG\d{11}", na=True) & stable.notna()).sum()
        ),
        "feature_index_complete_unique": True,
        "axis_count_parity": True,
        "axis_order_parity": True,
        "axis_order_sha256": ordered_values_sha256(var.index),
        "stable_id_order_sha256": ordered_values_sha256(
            pd.Index(stable.fillna("<NA>"))
        ),
        "organism_values": ["Mus musculus"],
        "needs_revision": False,
    }


def collection_snapshot(ln: Any) -> dict[str, Any]:
    snapshots: dict[str, Any] = {
        "historical_manifest_identity": "jkobject:GCjqQtGwPzkY"
    }
    for key in ("pert-gym/additions/20260621", "pert-gym/canonical/20260621"):
        records = list(ln.Collection.filter(key=key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {key}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        matches = [
            {"uid": str(item.uid), "key": str(item.key)}
            for item in members
            if str(item.key) == f"{PREFIX}/obs.parquet"
        ]
        if len(matches) != 1:
            raise AssertionError(f"target Collection membership drift: {key}")
        snapshots[key] = {
            "uid": str(collection.uid),
            "hash": str(collection.hash),
            "member_count": len(members),
            "target_key_matches": matches,
        }
    return snapshots


def verify_current(
    ln: Any, by_symbol: dict[str, list[str]], gtf_receipt: dict[str, Any]
) -> tuple[dict[str, Any], bool]:
    obs_artifact, obs_history = latest_artifact(ln, f"{PREFIX}/obs.parquet")
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    x_axis, x_receipt, x_semantics = inspect_x(x_artifact)
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    var = var_artifact.load()
    curated_obs = curate_obs(obs, x_semantics=x_semantics)
    curated_var = curate_var(var, by_symbol)
    proposed_var_verdict = verify_var(curated_var, x_axis)
    obs_curated = str(obs_artifact.description).startswith(
        f"{TASK_ID}: source-exhaustive GSE203592 OBS"
    )
    var_curated = str(var_artifact.description).startswith(
        f"{TASK_ID}: GSE203592 mouse VAR"
    )
    if obs_curated:
        verify_obs_semantics(obs, curated_obs)
    var_verdict = (
        verify_var(var, x_axis)
        if var_curated
        else {
            "needs_revision": True,
            "current_mouse_stable_ids": int(
                var["stable_feature_id"]
                .astype("string")
                .str.fullmatch(r"ENSMUSG\d{11}", na=False)
                .sum()
            ),
        }
    )
    return {
        "obs_before": artifact_identity(obs_artifact),
        "obs_history_count": len(obs_history),
        "x": artifact_identity(x_artifact),
        "x_validation": x_receipt,
        "var_before": artifact_identity(var_artifact),
        "rows": len(obs),
        "gtf_source": gtf_receipt,
        "canonical_field_dispositions": field_dispositions(
            obs if obs_curated else curated_obs
        ),
        "source_join": {
            "current_rows": len(obs),
            "cell_barcode_unique": bool(obs["cell_barcode"].is_unique),
            "original_obs_index_unique": bool(obs["original_obs_index"].is_unique),
            "cell_barcode_equals_original_obs_index": bool(
                obs["cell_barcode"]
                .astype("string")
                .equals(obs["original_obs_index"].astype("string"))
            ),
            "experiment_id_counts": {
                str(k): int(v) for k, v in obs["experiment_id"].value_counts().items()
            },
            "control_rows": int(obs["is_control"].astype(bool).sum()),
            "guide_id_known_rows": int(
                obs["orig_guide"]
                .astype("string")
                .str.fullmatch(GUIDE_ID_RE.pattern, na=False)
                .sum()
            ),
            "guide_sequence_known_rows": 0,
        },
        "var_verdict": var_verdict,
        "proposed_var_verdict": proposed_var_verdict,
        "already_curated_obs": obs_curated,
        "already_curated_var": var_curated,
        "curated_obs": curated_obs,
        "curated_var": curated_var,
        "obs_artifact": obs_artifact,
        "x_artifact": x_artifact,
        "var_artifact": var_artifact,
        "x_var_axis": x_axis,
    }, obs_curated and var_curated


def publish(
    ln: Any, result: dict[str, Any], helper_sha256: str
) -> dict[str, list[Any]]:
    writes: dict[str, list[Any]] = {"obs": [], "var": []}
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-gse203592-publish-"))
    ln.track(
        key=f"pert-gym/real-dataset-curation/{REAL_DATASET_ID}/{TASK_ID}",
        kind="script",
        params={"task_id": TASK_ID, "helper_sha256": helper_sha256},
        new_run=True,
        pypackages=False,
        stream_tracking=False,
    )
    if not result["already_curated_var"]:
        path = root / "var.parquet"
        result["curated_var"].to_parquet(path)
        var = ln.Artifact.from_dataframe(
            path,
            key=f"{PREFIX}/var.parquet",
            revises=result["var_artifact"],
            description=(
                f"{TASK_ID}: GSE203592 mouse VAR; preserves exact {EXPECTED_N_VARS}-feature "
                "X order; maps >99% to unique Ensembl release-93 ENSMUSG IDs and retains "
                "explicit symbol fallback for ambiguous rows"
            ),
        ).save()
        result["x_artifact"].features.set_values({"var": var})
        writes["var"].append(var)
    if not result["already_curated_obs"]:
        path = root / "obs.parquet"
        result["curated_obs"].to_parquet(path)
        obs = ln.Artifact.from_dataframe(
            path,
            key=f"{PREFIX}/obs.parquet",
            revises=result["obs_artifact"],
            description=(
                f"{TASK_ID}: source-exhaustive GSE203592 OBS; exact {EXPECTED_N_OBS}-cell "
                "source-preserved join, mouse TIL/sample/batch/control/QC semantics and "
                "explicit unknown guide sequences"
            ),
        ).save()
        obs.features.set_values({"X": result["x_artifact"]})
        writes["obs"].append(obs)
    try:
        ln.finish()
    except AttributeError:
        ln.context.finish()
    return writes


def strip_runtime(result: dict[str, Any]) -> dict[str, Any]:
    hidden = {
        "curated_obs",
        "curated_var",
        "obs_artifact",
        "x_artifact",
        "var_artifact",
        "x_var_axis",
    }
    return {key: value for key, value in result.items() if key not in hidden}


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
                    "metric": "real_dataset_obs_var",
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
    helper_sha256 = sha256_file(Path(__file__))
    frozen = load_frozen_input_bindings()
    source_manifest = load_source_manifest()
    capacity = preflight()
    emit_product("preflight", 0)
    gtf_path, gtf_receipt = download_gtf()
    by_symbol, gtf_gene_rows = parse_gtf(gtf_path)
    gtf_receipt["gene_rows"] = gtf_gene_rows
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    result, all_curated = verify_current(ln, by_symbol, gtf_receipt)
    collections_before = collection_snapshot(ln)
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    writes: dict[str, list[Any]] = {"obs": [], "var": []}
    if mode == "mutate" and not all_curated:
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
            fresh, fresh_all = verify_current(ln, by_symbol, gtf_receipt)
            if fresh_all:
                result, all_curated = fresh, True
            else:
                result = fresh
                writes = publish(ln, fresh, helper_sha256)
    elif mode == "verify" and not all_curated:
        raise AssertionError("verify requested before exact OBS+VAR revisions exist")
    final, final_all = verify_current(ln, by_symbol, gtf_receipt)
    if mode in {"mutate", "verify"} and not final_all:
        raise AssertionError("terminal OBS+VAR readback failed")
    collections_after = collection_snapshot(ln)
    if collections_after != collections_before:
        raise AssertionError("Collection drift")
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    receipt = {
        "format": "pert-gym.real-dataset-obs-var-curation/v2",
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "dataset_id": PREFIX,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "frozen_inputs": frozen["inputs"],
        "source_evidence_sha256": sha256_file(SOURCE_MANIFEST_PATH),
        "search_effort_complete": source_manifest["search_effort_complete"],
        "source_denominator": {
            "biological_datasets": 1,
            "logical_families": 1,
            "physical_members": 1,
            "observations": EXPECTED_N_OBS,
            "features": EXPECTED_N_VARS,
        },
        "member_before": strip_runtime(result),
        "member_after": strip_runtime(final),
        "collections": collections_after,
        "writes": {
            "obs_revisions": len(writes["obs"]),
            "var_revisions": len(writes["var"]),
            "x_revisions": 0,
            "collection_writes": 0,
            "deletions": 0,
            "artifacts": {
                role: [artifact_identity(item) for item in items]
                for role, items in writes.items()
            },
        },
        "registry_counts": {"before": counts_before, "after": counts_after},
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "host": {
            "hostname": capacity.hostname,
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = sha256_bytes(canonical(receipt).encode())
    emit_product("checkpointing", 1)
    print("GSE203592_CURATION_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
