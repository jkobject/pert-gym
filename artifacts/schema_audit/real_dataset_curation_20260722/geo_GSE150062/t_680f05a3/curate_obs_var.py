#!/usr/bin/env python3
"""Append-only, source-exhaustive OBS/VAR curation for GEO GSE150062."""

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

TASK_ID = "t_680f05a3"
REAL_DATASET_ID = "geo/GSE150062"
PREFIX = "prism_collection/GSE150062"
EXPECTED_N_OBS = 78_393
EXPECTED_N_VARS = 60_497
EXPECTED_STABLE_ENSG_COUNT = 44_025
EXPECTED_SOURCE_LH_COUNT = 16_401
EXPECTED_PREWRITE_OBS_UID = "CkcQf1IYkOkbxKed0002"
EXPECTED_PREWRITE_VAR_UIDS = {"rRlvtvSEpbFnek7K0001", "rRlvtvSEpbFnek7K0002"}
EXPECTED_STRUCTURAL_COLLECTION_OBS_UID = "CkcQf1IYkOkbxKed0000"
EXPECTED_X = {"uid": "11Cz4UCdFjK8eYy30000", "hash": "GuWO74YMmAykleWtRSS8LY"}
EXPECTED_AXIS_SHA256 = {
    "obs": "70f0732cfa9b73479d02a0927475c0cb755e99477db36bb3ea8b7b359e44ef88",
    "var": "27ee86db7e6b15617c6183a9e69036dd1be2057ca576684bdfa5b72e6c5f91aa",
}
SOURCE_MANIFEST_PATH = Path(__file__).with_name("source_manifest.json")
GEO_ROOT = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE150nnn/GSE150062/suppl"
GEO_SPECS = {
    "GSE150062_perturbseq_barcodes.tsv.gz": (
        338_738,
        "d86f132cc056002f55f67ce7658b6dcb1930e0bb577dc173cc958226d7fe4e37",
    ),
    "GSE150062_perturbseq_features.tsv.gz": (
        222_231,
        "90e1a3254c97215191bf9dbae0a603e6fb1b908da731e5505395a805fb790b28",
    ),
    "GSE150062_perturbseq_genes.tsv.gz": (
        222_165,
        "b6d13a64dca80162b49414a0967522326e77b58b40b08da93a672ffe0b6afa57",
    ),
    "GSE150062_sgrna_barcodes.tsv.gz": (
        338_738,
        "d86f132cc056002f55f67ce7658b6dcb1930e0bb577dc173cc958226d7fe4e37",
    ),
    "GSE150062_sgrna_features.tsv.gz": (
        2_763,
        "e0b66b2ba30c18d07c33ba861793857b2984c6aa2251e6c4ccd4cb1695b6bae3",
    ),
    "GSE150062_sgrna_genes.tsv.gz": (
        1_428,
        "fc7f4c16c63f5238cf57fbe0e56c670c05f3fb31d1f278a2068979743fb896d4",
    ),
}
PMC_TABLE_URL = "https://pmc.ncbi.nlm.nih.gov/articles/instance/9903861/bin/mmc6.xlsx"
PMC_TABLE_SPEC = (
    5_338_747,
    "fd307e6aec5a0044e0ec135594ed1d3071d3efb609809faa1f0ef91d111b465c",
)
AUTHOR_COMMIT = "4285c7f5e81eaada87cf668b7fa4039f4ff3b1c9"
AUTHOR_ROOT = f"https://raw.githubusercontent.com/davidwumdphd/dualgenomewide/{AUTHOR_COMMIT}/analysis/reference/output/02_unified_reference"
AUTHOR_SPECS = {
    "unified_metadata.tsv.gz": (
        9_302_370,
        "c1ac69e9aa3f6bc3ed43df116ae125ab9cf7a9612456db579576a191aea8b68b",
    ),
    "unified_display_names.tsv.gz": (
        439_619,
        "7bccfafa7ac9f6e01e8db7bdecb25e3f4afc52cabd343c9db5c4ab367ad605c5",
    ),
}

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
NOT_APPLICABLE_FIELDS = {
    "molecule_sequence",
    "dose",
    "dose_unit",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
}
UNKNOWN_FIELDS = {"donor_id", "age", "ethnicity", "pseudotime"}
TABLE_COLUMN_MAP = {
    "Guide identity": "guide",
    "Guide target": "perturbation",
    "Library": "library",
    "RNA velocity trajectory": "trajectory",
    "Gene expression UMI": "nCount_RNA",
    "Gene expression complexity": "nFeature_RNA",
    "CRISPRi sgRNA UMI": "UMI_count",
}


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


def ordered_sha256(values: pd.Index) -> str:
    return sha256_bytes("\n".join(values.astype(str)).encode())


def download(url: str, path: Path, spec: tuple[int, str]) -> None:
    size, sha256 = spec
    if not path.exists() or path.stat().st_size != size or sha256_file(path) != sha256:
        request = urllib.request.Request(
            url, headers={"User-Agent": "pert-gym-curation/1"}
        )
        with (
            urllib.request.urlopen(request, timeout=180) as source,
            path.open("wb") as target,
        ):
            while block := source.read(8 * 1024 * 1024):
                target.write(block)
    if path.stat().st_size != size or sha256_file(path) != sha256:
        raise AssertionError(f"source identity drift: {url}")


def download_pmc_table(path: Path) -> None:
    if path.exists() and (path.stat().st_size, sha256_file(path)) == PMC_TABLE_SPEC:
        return
    request = urllib.request.Request(
        PMC_TABLE_URL, headers={"User-Agent": "Mozilla/5.0"}
    )
    html = urllib.request.urlopen(request, timeout=120).read().decode()
    challenge_match = re.search(r'const POW_CHALLENGE = "([^"]+)"', html)
    difficulty_match = re.search(r'const POW_DIFFICULTY = "(\d+)"', html)
    if not challenge_match or not difficulty_match:
        raise AssertionError("PMC proof-of-work challenge absent")
    challenge = challenge_match.group(1)
    prefix = "0" * int(difficulty_match.group(1))
    nonce = 0
    while not sha256_bytes((challenge + str(nonce)).encode()).startswith(prefix):
        nonce += 1
    opener = urllib.request.build_opener()
    opener.addheaders = [
        ("User-Agent", "Mozilla/5.0"),
        ("Cookie", f"cloudpmc-viewer-pow={challenge},{nonce}"),
        ("Referer", PMC_TABLE_URL),
    ]
    path.write_bytes(opener.open(PMC_TABLE_URL, timeout=180).read())
    if (path.stat().st_size, sha256_file(path)) != PMC_TABLE_SPEC:
        raise AssertionError("PMC Table S5 identity drift")


def read_lines(path: Path) -> pd.Index:
    with gzip.open(path, "rt") as handle:
        return pd.Index([line.rstrip("\n") for line in handle], dtype="string")


def cellranger_feature_ids(values: pd.Series) -> pd.Series:
    """Return Cell Ranger's underscore-sanitized feature identifiers."""
    return values.astype("string").str.replace("_", "-", regex=False)


def load_sources() -> dict[str, Any]:
    manifest = json.loads(SOURCE_MANIFEST_PATH.read_text())
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-gse150062-sources"
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for name, spec in GEO_SPECS.items():
        path = root / name
        download(f"{GEO_ROOT}/{name}", path, spec)
        paths[name] = path
    table_path = root / "mmc6.xlsx"
    download_pmc_table(table_path)
    for name, spec in AUTHOR_SPECS.items():
        path = root / name
        download(f"{AUTHOR_ROOT}/{name}", path, spec)
        paths[name] = path

    barcodes = read_lines(paths["GSE150062_perturbseq_barcodes.tsv.gz"])
    sgrna_barcodes = read_lines(paths["GSE150062_sgrna_barcodes.tsv.gz"])
    genes = read_lines(paths["GSE150062_perturbseq_genes.tsv.gz"])
    features = read_lines(paths["GSE150062_perturbseq_features.tsv.gz"])
    table = pd.read_excel(table_path, sheet_name="TableS5", header=2, dtype="string")
    table = table.set_index("Cell barcode", drop=False)
    display = pd.read_csv(
        paths["unified_display_names.tsv.gz"], sep="\t", dtype="string"
    )
    metadata = pd.read_csv(paths["unified_metadata.tsv.gz"], sep="\t", dtype="string")
    gene_rows = metadata.loc[metadata["type"].eq("gene")].copy()
    # Cell Ranger sanitizes underscores in feature IDs to hyphens. This exact
    # source transformation accounts for the 29 author-reference/GEO spellings.
    display["feature_id"] = cellranger_feature_ids(display["feature_id"])
    gene_rows["feature_id"] = cellranger_feature_ids(gene_rows["feature_id"])
    if len(table) != EXPECTED_N_OBS or not table.index.is_unique:
        raise AssertionError("Table S5 denominator/uniqueness drift")
    if not barcodes.equals(sgrna_barcodes) or not barcodes.equals(table.index):
        raise AssertionError("accepted cell-axis source mismatch")
    if ordered_sha256(barcodes) != EXPECTED_AXIS_SHA256["obs"]:
        raise AssertionError("accepted cell-axis hash drift")
    if (
        len(genes) != EXPECTED_N_VARS
        or not genes.is_unique
        or features[1:].tolist() != genes.tolist()
    ):
        raise AssertionError("GEO expression feature-axis drift")
    if not display["feature_id"].is_unique or set(
        display["feature_id"].astype(str)
    ) != set(genes.astype(str)):
        raise AssertionError("author display-name feature coverage drift")
    if set(gene_rows["feature_id"].astype(str)) != set(genes.astype(str)):
        raise AssertionError("author reference gene coverage drift")
    if set(table.columns) != {
        "Cell barcode",
        "Guide identity",
        "Guide target",
        "Library",
        "RNA velocity trajectory",
        "Gene expression UMI",
        "Gene expression complexity",
        "CRISPRi sgRNA UMI",
        "Protospacer",
        "Batch",
    }:
        raise AssertionError("Table S5 schema drift")
    if not table["Protospacer"].str.fullmatch(r"[ACGT]{20}", na=False).all():
        raise AssertionError("invalid Table S5 protospacer")
    return {
        "manifest": manifest,
        "barcodes": barcodes,
        "genes": genes,
        "table": table,
        "display": display.set_index("feature_id").reindex(genes),
        "gene_rows": gene_rows,
    }


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
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records or not bool(records[-1].is_latest):
        raise AssertionError(f"latest Artifact drift: {key}")
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
        raise AssertionError(f"cannot resolve feature Artifact: {value}")
    return records[-1]


def exact_source_join(
    obs: pd.DataFrame, table: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, Any]]:
    keys = pd.Index(obs["original_obs_index"].astype(str))
    if not keys.is_unique or not keys.equals(table.index.astype(str)):
        raise AssertionError("source/current OBS identity or order drift")
    joined = table.reindex(keys).copy()
    joined.index = obs.index
    mismatches: dict[str, int] = {}
    for source_column, current_column in TABLE_COLUMN_MAP.items():
        left = joined[source_column].astype("string")
        if source_column == "Guide target":
            left = left.replace({"Non-Targeting": "non-targeting"})
        right = obs[current_column].astype("string")
        mismatches[current_column] = int((~left.eq(right)).sum())
    expected_sample = "diff_" + joined["Batch"].astype("string")
    mismatches["sample"] = int(
        (~expected_sample.eq(obs["sample"].astype("string"))).sum()
    )
    source_control = joined["Guide target"].eq("Non-Targeting") & joined["Library"].eq(
        "Control"
    )
    mismatches["is_control"] = int(
        (~source_control.astype(bool).eq(obs["is_control"].astype(bool))).sum()
    )
    if any(mismatches.values()):
        raise AssertionError(f"source/current OBS semantic mismatch: {mismatches}")
    return joined, {
        "source_rows": len(table),
        "current_rows": len(obs),
        "identity_order_match": True,
        "column_mismatches": mismatches,
        "join_mismatch_count": sum(mismatches.values()),
        "joined_order_sha256": ordered_sha256(keys),
    }


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: Any, source: str
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing_series(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def curate_obs(
    obs: pd.DataFrame, source: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    original = obs.copy(deep=True)
    joined, join_receipt = exact_source_join(obs, source["table"])
    curated = obs.copy(deep=True)
    if "dataset_state" not in original:
        for field in CANONICAL_OBS_FIELDS:
            if field in original and f"source_original_{field}" not in curated:
                curated[f"source_original_{field}"] = original[field]
    controls = joined["Guide target"].eq("Non-Targeting") & joined["Library"].eq(
        "Control"
    )
    sample = "diff_" + joined["Batch"].astype("string")
    set_field(curated, "dataset", PREFIX, "known", "canonical logical family")
    set_field(
        curated,
        "sample",
        sample,
        "known",
        "Table S5 batch mapped to source diff_N sample",
    )
    set_field(
        curated,
        "cell_id",
        joined["Cell barcode"].astype("string"),
        "known",
        "Table S5 Cell barcode",
    )
    set_field(curated, "batch", sample, "known", "Table S5 Batch")
    set_field(
        curated,
        "cell_type",
        "day 8 neural induction cell",
        "known",
        "GEO D8 and publication heterogeneous neural induction endpoint",
    )
    set_field(
        curated,
        "cell_line",
        "CRISPRi WTC11 iPSC (Gen1C)",
        "known",
        "publication key resources and GEO iPSC WTC",
    )
    set_field(
        curated, "disease", "non-diseased", "known", "publication experimental model"
    )
    set_field(
        curated,
        "tissue_type",
        "in vitro cell culture",
        "known",
        "GEO experimental model",
    )
    set_field(curated, "organism", "Homo sapiens", "known", "GEO GSE150062")
    set_field(curated, "sex", "male", "known", "publication experimental model details")
    set_field(
        curated,
        "sequencer",
        "Illumina NovaSeq 6000",
        "known",
        "GEO Perturb-seq samples",
    )
    set_field(
        curated,
        "technology",
        "10x Genomics Chromium 3' Assay v3 with direct sgRNA capture",
        "known",
        "GEO extraction protocol and publication",
    )
    set_field(curated, "assay", "Perturb-seq", "known", "GEO and publication")
    set_field(curated, "modality", "scRNA-seq", "known", "GEO gene-expression matrix")
    set_field(
        curated,
        "media",
        "dual SMAD-inhibition neural induction medium",
        "known",
        "GEO treatment protocol",
    )
    set_field(curated, "is_bulk", False, "known", "single-cell source")
    set_field(curated, "is_pseudobulk", False, "known", "single-cell source")
    perturbation = (
        joined["Guide target"]
        .astype("string")
        .replace({"Non-Targeting": "non-targeting"})
    )
    set_field(
        curated,
        "perturbation",
        perturbation,
        "known",
        "Table S5 Guide target; canonical non-targeting control spelling",
    )
    set_field(
        curated,
        "perturbation_type",
        "CRISPRi",
        "known",
        "publication experimental design",
    )
    set_field(
        curated,
        "perturbation_technology",
        "CRISPR interference",
        "known",
        "publication experimental design",
    )
    set_field(
        curated,
        "perturbation_library",
        "Wu et al. 492-sgRNA coding/lncRNA Perturb-seq library",
        "known",
        "publication and GEO sgRNA features",
    )
    set_field(
        curated,
        "guide_sequence",
        joined["Protospacer"].astype("string"),
        "known",
        "Table S5 Protospacer",
    )
    set_field(
        curated,
        "is_control",
        controls.astype("boolean"),
        "known",
        "Table S5 Non-Targeting target and Control library",
    )
    set_field(
        curated,
        "timepoint",
        11_520.0,
        "known",
        "GEO D8; minutes from neural-induction initiation",
    )
    set_field(
        curated,
        "trajectory_id",
        joined["RNA velocity trajectory"].astype("string"),
        "known",
        "Table S5 RNA velocity trajectory",
    )
    set_field(
        curated, "is_baseline", False, "known", "all accepted cells harvested at day 8"
    )
    set_field(
        curated,
        "n_counts",
        pd.to_numeric(joined["Gene expression UMI"], errors="raise").astype("Int64"),
        "known",
        "Table S5 Gene expression UMI",
    )
    set_field(
        curated,
        "n_genes",
        pd.to_numeric(joined["Gene expression complexity"], errors="raise").astype(
            "Int64"
        ),
        "known",
        "Table S5 Gene expression complexity",
    )
    set_field(
        curated,
        "pct_mito",
        pd.to_numeric(original["percent_mito"], errors="raise").astype("Float64"),
        "known",
        "source Seurat percent_mito",
    )
    set_field(
        curated,
        "pct_ribo",
        pd.to_numeric(original["percent_ribo"], errors="raise").astype("Float64"),
        "known",
        "source Seurat percent_ribo",
    )
    set_field(
        curated,
        "is_low_quality",
        False,
        "known",
        "Table S5 filtered singlet barcode inclusion",
    )
    for field in NOT_APPLICABLE_FIELDS:
        dtype = "Float64" if field in {"dose", "response_value"} else "string"
        set_field(
            curated,
            field,
            missing_series(curated.index, dtype),
            "not_applicable",
            "dataset design",
        )
    for field in UNKNOWN_FIELDS:
        dtype = "Float64" if field in {"age", "pseudotime"} else "string"
        set_field(
            curated,
            field,
            missing_series(curated.index, dtype),
            "unknown",
            "source-exhaustive search found no defensible row value",
        )
    curated["guide_id"] = joined["Guide identity"].astype("string")
    curated["guide_id_source"] = "Table S5 Guide identity"
    curated["perturbation_target"] = joined["Guide target"].astype("string")
    curated["source_library_class"] = joined["Library"].astype("string")
    curated["source_sgrna_umi"] = pd.to_numeric(
        joined["CRISPRi sgRNA UMI"], errors="raise"
    ).astype("Int64")
    curated = curated.copy()
    if len(curated) != EXPECTED_N_OBS or not curated.index.equals(original.index):
        raise AssertionError("OBS row count/order drift")
    if not curated["obs_uuid"].is_unique or not curated["original_obs_index"].is_unique:
        raise AssertionError("OBS identity uniqueness drift")
    return curated, {
        **join_receipt,
        "control_rows": int(controls.sum()),
        "guide_sequence_known_rows": int(curated["guide_sequence"].notna().sum()),
        "trajectory_known_rows": int(curated["trajectory_id"].notna().sum()),
        "qc_complete_rows": int(
            curated[["n_counts", "n_genes", "pct_mito", "pct_ribo", "is_low_quality"]]
            .notna()
            .all(axis=1)
            .sum()
        ),
    }


def field_dispositions(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in CANONICAL_OBS_FIELDS:
        states = frame[f"{field}_state"].astype("string")
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
            "known_rows": known,
            "unknown_rows": len(frame) - known,
            "source_bound": disposition.startswith("materialized"),
        }
    return result


def verify_obs(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    if not actual.index.equals(expected.index) or list(actual.columns) != list(
        expected.columns
    ):
        raise AssertionError("OBS schema/order mismatch")
    assert_frame_equal(actual, expected, check_categorical=True)


def reference_by_feature(source: dict[str, Any]) -> pd.DataFrame:
    grouped = source["gene_rows"].groupby("feature_id", sort=False)["gene_id"].agg(list)
    grouped = grouped.reindex(source["genes"])
    if grouped.isna().any():
        raise AssertionError("source reference feature coverage drift")
    result = pd.DataFrame(index=source["genes"])
    result["source_reference_gene_ids"] = grouped.map(
        lambda values: "|".join(map(str, values))
    ).astype("string")
    result["source_reference_gene_id_count"] = grouped.map(len).astype("Int64")
    result["source_native_gene_id"] = grouped.map(
        lambda values: values[0] if len(values) == 1 else pd.NA
    ).astype("string")
    result["stable_feature_id"] = result["source_native_gene_id"].where(
        result["source_native_gene_id"].str.fullmatch(r"ENSG\d{11}", na=False)
    )
    result["source_native_lh_id"] = result["source_native_gene_id"].where(
        result["source_native_gene_id"].str.fullmatch(r"LH\d{5}", na=False)
    )
    result["source_gene_name"] = source["display"]["gene_name"].astype("string")
    result["source_display_name"] = source["display"]["display_name"].astype("string")
    return result


def curate_var(var: pd.DataFrame, source: dict[str, Any]) -> pd.DataFrame:
    if not var.index.astype(str).equals(source["genes"].astype(str)):
        raise AssertionError("VAR/source feature-axis drift")
    reference = reference_by_feature(source).set_axis(var.index)
    curated = var.copy(deep=True)
    for column in (
        "stable_feature_id",
        "ensembl_gene_id",
        "stable_feature_id_source",
        "stable_feature_id_mapping_status",
    ):
        if column in var and f"previous_{column}" not in curated:
            curated[f"previous_{column}"] = var[column]
    curated["pert_gym_source_gene_symbol"] = var.index.astype("string")
    for column in reference.columns:
        curated[column] = reference[column]
    curated["ensembl_gene_id"] = curated["stable_feature_id"]
    curated["stable_feature_id_namespace"] = "Ensembl stable gene ID"
    curated["stable_feature_id_source"] = (
        f"Wu et al. unified_metadata.tsv.gz at Git commit {AUTHOR_COMMIT}"
    )
    unique = curated["source_reference_gene_id_count"].eq(1)
    curated["stable_feature_id_mapping_status"] = np.select(
        [
            curated["stable_feature_id"].notna(),
            curated["source_native_lh_id"].notna(),
            ~unique,
        ],
        [
            "exact_source_ensembl_gene_id",
            "source_native_lh_id_no_ensembl",
            "ambiguous_source_gene_ids",
        ],
        default="noncanonical_source_gene_id",
    )
    curated["feature_contract_class"] = np.select(
        [curated["stable_feature_id"].notna(), curated["source_native_lh_id"].notna()],
        ["standard_ensembl_gene", "source_native_custom_lh_feature"],
        default="unresolved_source_gene_feature",
    )
    curated["ensembl_id_state"] = np.select(
        [curated["stable_feature_id"].notna(), curated["source_native_lh_id"].notna()],
        ["known", "not_applicable"],
        default="unknown",
    )
    curated["stable_feature_id_candidate_count"] = curated[
        "source_reference_gene_id_count"
    ]
    curated["organism"] = "Homo sapiens"
    curated["feature_index"] = var.index.astype("string")
    curated["feature_index_namespace"] = "Wu et al. unified reference feature_id"
    curated["feature_index_source"] = f"dualgenomewide Git commit {AUTHOR_COMMIT}"
    if len(curated) != EXPECTED_N_VARS or not curated.index.equals(var.index):
        raise AssertionError("VAR row count/order drift")
    return curated


def verify_var(
    var: pd.DataFrame, source: dict[str, Any], x_axis: pd.Index
) -> dict[str, Any]:
    if len(var) != EXPECTED_N_VARS or not var.index.astype(str).equals(
        x_axis.astype(str)
    ):
        raise AssertionError("VAR/X axis drift")
    if not var.index.astype(str).equals(source["genes"].astype(str)):
        raise AssertionError("VAR/source axis drift")
    if (
        not var["feature_index"]
        .astype("string")
        .equals(pd.Series(var.index.astype(str), index=var.index, dtype="string"))
    ):
        raise AssertionError("VAR feature_index drift")
    stable = var["stable_feature_id"].astype("string")
    lh = var["source_native_lh_id"].astype("string")
    stable_count = int(stable.str.fullmatch(r"ENSG\d{11}", na=False).sum())
    lh_count = int(lh.str.fullmatch(r"LH\d{5}", na=False).sum())
    if (
        stable_count != EXPECTED_STABLE_ENSG_COUNT
        or lh_count != EXPECTED_SOURCE_LH_COUNT
    ):
        raise AssertionError("VAR source identifier counts drift")
    if not stable.dropna().is_unique or not var["organism"].eq("Homo sapiens").all():
        raise AssertionError("VAR stable-ID uniqueness/species drift")
    expected_states = pd.Series(
        np.select(
            [stable.notna(), lh.notna()],
            ["known", "not_applicable"],
            default="unknown",
        ),
        index=var.index,
        dtype="string",
    )
    if not var["ensembl_id_state"].astype("string").equals(expected_states):
        raise AssertionError("VAR Ensembl applicability disposition drift")
    unresolved_count = len(var) - stable_count - lh_count
    feature_denominator = {
        "standard_ensembl_gene": stable_count,
        "source_native_custom_lh_not_applicable": lh_count,
        "unresolved_applicable_unknown": unresolved_count,
        "total": len(var),
    }
    disposition_complete = sum(
        feature_denominator[key]
        for key in (
            "standard_ensembl_gene",
            "source_native_custom_lh_not_applicable",
            "unresolved_applicable_unknown",
        )
    ) == len(var)
    return {
        "biological_features_total": len(var),
        "stable_ensembl_id_features": stable_count,
        "correct_species_features": int(var["organism"].eq("Homo sapiens").sum()),
        "source_native_lh_features": lh_count,
        "other_nonpassing_features": unresolved_count,
        "feature_denominator": feature_denominator,
        "ensembl_disposition_complete": disposition_complete,
        "feature_index_unique": bool(var["feature_index"].is_unique),
        "axis_order_parity": True,
        "axis_order_sha256": ordered_sha256(var.index),
        "full_feature_ensembl_coverage": stable_count == len(var),
        "var_ensembl_species_completed": disposition_complete,
        "verdict": "accepted_partial_boundary",
        "reason": "44025 standard genes have exact source-backed ENSG IDs; 16401 source-native custom LH features are explicitly not applicable to exact ENSG assignment; 71 unresolved applicable features remain unknown; no identifier was fabricated",
    }


def x_axis(artifact: Any) -> tuple[pd.Index, dict[str, Any]]:
    if {"uid": str(artifact.uid), "hash": str(artifact.hash)} != EXPECTED_X:
        raise AssertionError("accepted X identity drift")
    backed = ad.read_h5ad(Path(artifact.cache()), backed="r")
    try:
        if tuple(backed.shape) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
            raise AssertionError("X shape drift")
        axis = backed.var_names.astype(str).copy()
        obs_hash = ordered_sha256(backed.obs_names.astype(str))
        var_hash = ordered_sha256(axis)
    finally:
        backed.file.close()
    if {"obs": obs_hash, "var": var_hash} != EXPECTED_AXIS_SHA256:
        raise AssertionError("X axis hash drift")
    return axis, {
        **EXPECTED_X,
        "shape": [EXPECTED_N_OBS, EXPECTED_N_VARS],
        "obs_names_sha256": obs_hash,
        "var_names_sha256": var_hash,
        "backed_only": True,
    }


def collection_snapshot(ln: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    target_key = f"{PREFIX}/obs.parquet"
    for collection in ln.Collection.filter().all():
        key = str(collection.key)
        if not key.startswith("pert-gym/"):
            continue
        matches = list(
            collection.artifacts.filter(key=target_key).only("uid", "key").all()
        )
        if matches:
            result[key] = {
                "uid": str(collection.uid),
                "hash": str(collection.hash),
                "member_count": collection.artifacts.count(),
                "target_matches": [
                    {"uid": str(item.uid), "key": str(item.key)} for item in matches
                ],
            }
    return result


def verify_structural_collection_reuse(
    collections: dict[str, Any], curated_obs_uid: str
) -> dict[str, Any]:
    if not collections:
        raise AssertionError("structural Collection anchor absent")
    observed = [
        str(match["uid"])
        for collection in collections.values()
        for match in collection["target_matches"]
    ]
    if (
        len(observed) != len(collections)
        or set(observed) != {EXPECTED_STRUCTURAL_COLLECTION_OBS_UID}
        or curated_obs_uid in observed
    ):
        raise AssertionError(
            "structural Collection anchor drift: expected only accepted structural OBS "
            f"{EXPECTED_STRUCTURAL_COLLECTION_OBS_UID}, observed {sorted(set(observed))}"
        )
    return {
        "verdict": "accepted_structural_reuse",
        "matching_versioned_collections": len(collections),
        "accepted_structural_obs_uid": EXPECTED_STRUCTURAL_COLLECTION_OBS_UID,
        "curated_obs_uid": curated_obs_uid,
        "structural_anchor_reused": True,
        "curated_obs_is_collection_anchor": False,
        "collection_mutation_in_scope": False,
    }


def verify_current(ln: Any, source: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    obs_artifact, obs_history = latest_artifact(ln, f"{PREFIX}/obs.parquet")
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    x_var_axis, x_receipt = x_axis(x_artifact)
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    var = var_artifact.load()
    curated_obs, join_receipt = curate_obs(obs, source)
    curated_var = curate_var(var, source)
    obs_curated = str(obs_artifact.description).startswith(
        f"{TASK_ID}: source-exhaustive GSE150062 OBS"
    )
    var_curated = str(var_artifact.description).startswith(
        f"{TASK_ID}: source-exhaustive GSE150062 VAR"
    ) and {"feature_contract_class", "ensembl_id_state"}.issubset(var.columns)
    if not obs_curated and str(obs_artifact.uid) != EXPECTED_PREWRITE_OBS_UID:
        raise AssertionError("unexpected prewrite OBS identity")
    if not var_curated and str(var_artifact.uid) not in EXPECTED_PREWRITE_VAR_UIDS:
        raise AssertionError("unexpected prewrite VAR identity")
    if obs_curated:
        verify_obs(obs, curated_obs)
    if var_curated:
        var_verdict = verify_var(var, source, x_var_axis)
    else:
        var_verdict = verify_var(curated_var, source, x_var_axis)
    dispositions = field_dispositions(obs if obs_curated else curated_obs)
    obs_completed = all(
        item["disposition"] in {"materialized_complete", "not_applicable", "unknown"}
        for item in dispositions.values()
    ) and all(
        dispositions[field]["disposition"] == "materialized_complete"
        for field in (
            "dataset",
            "cell_id",
            "assay",
            "modality",
            "perturbation",
            "guide_sequence",
            "is_control",
            "timepoint",
            "trajectory_id",
            "n_counts",
            "n_genes",
            "pct_mito",
            "pct_ribo",
            "is_low_quality",
        )
    )
    return {
        "obs_before": artifact_identity(obs_artifact),
        "obs_history_count": len(obs_history),
        "x": artifact_identity(x_artifact),
        "x_axis": x_receipt,
        "var_before": artifact_identity(var_artifact),
        "source_join": join_receipt,
        "source_manifest_sha256": sha256_file(SOURCE_MANIFEST_PATH),
        "canonical_field_dispositions": dispositions,
        "obs_completed": obs_completed,
        "var_verdict": var_verdict,
        "already_curated_obs": obs_curated,
        "already_curated_var": var_curated,
        "curated_obs": curated_obs,
        "curated_var": curated_var,
        "obs_artifact": obs_artifact,
        "x_artifact": x_artifact,
        "var_artifact": var_artifact,
        "x_var_axis": x_var_axis,
    }, obs_curated and var_curated


def publish(
    ln: Any, result: dict[str, Any], helper_sha256: str
) -> dict[str, list[Any]]:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-gse150062-publish-"))
    writes: dict[str, list[Any]] = {"obs": [], "var": []}
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
            description=f"{TASK_ID}: source-exhaustive GSE150062 VAR; exact 60497-feature Wu unified-reference identity and human organism; 44025 source-backed ENSG, 16401 source-native custom LH not-applicable to exact ENSG, 71 unresolved unknown; no fabrication",
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
            description=f"{TASK_ID}: source-exhaustive GSE150062 OBS; exact 78393-row Table S5 join with protospacer, target, library, trajectory, batch and QC semantics",
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
    capacity = preflight()
    emit_product("preflight", 0)
    source = load_sources()
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    result, all_curated = verify_current(ln, source)
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
            fresh, fresh_all = verify_current(ln, source)
            if fresh_all:
                result, all_curated = fresh, True
            else:
                result = fresh
                writes = publish(ln, fresh, helper_sha256)
    elif mode == "verify" and not all_curated:
        raise AssertionError("verify requested before exact OBS+VAR revisions exist")
    final, final_all = verify_current(ln, source)
    if mode in {"mutate", "verify"} and not final_all:
        raise AssertionError("terminal OBS+VAR readback failed")
    collections_after = collection_snapshot(ln)
    if collections_after != collections_before:
        raise AssertionError("Collection drift")
    collection_verdict = verify_structural_collection_reuse(
        collections_after, curated_obs_uid=str(final["obs_artifact"].uid)
    )
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    dataset_e2e = {
        "physical_member_count": 1,
        "matrix_axis_parity": True,
        "obs_completed": bool(final["obs_completed"]),
        "var_ensembl_species_completed": bool(
            final["var_verdict"]["var_ensembl_species_completed"]
        ),
        "versioned_collection_structural_reuse": bool(
            collection_verdict["structural_anchor_reused"]
        ),
        "curated_obs_is_collection_anchor": bool(
            collection_verdict["curated_obs_is_collection_anchor"]
        ),
    }
    dataset_e2e["complete"] = (
        all(
            dataset_e2e[key]
            for key in (
                "physical_member_count",
                "matrix_axis_parity",
                "obs_completed",
                "var_ensembl_species_completed",
                "versioned_collection_structural_reuse",
            )
        )
        and not dataset_e2e["curated_obs_is_collection_anchor"]
    )
    dataset_e2e["status"] = "complete" if dataset_e2e["complete"] else "failed_contract"
    receipt = {
        "format": "pert-gym.real-dataset-obs-var-curation/v3",
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "dataset_id": PREFIX,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "source_manifest_sha256": sha256_file(SOURCE_MANIFEST_PATH),
        "source_denominator": {
            "biological_datasets": 1,
            "logical_families": 1,
            "physical_members": 1,
            "observations": EXPECTED_N_OBS,
            "features": EXPECTED_N_VARS,
        },
        "member_before": strip_runtime(result),
        "member_after": strip_runtime(final),
        "dataset_e2e_v3": dataset_e2e,
        "collection_contract": collection_verdict,
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
    print("GSE150062_CURATION_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
