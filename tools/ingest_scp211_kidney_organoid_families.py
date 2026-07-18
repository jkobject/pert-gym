#!/usr/bin/env python3
"""Ingest SCP211 day-specific kidney organoid MatrixMarket families as Lamin triplets.

The selected source family is the day-specific organoid/iPSC trajectory, not the
standalone adult-kidney reference. Each family is a MatrixMarket genes x cells
matrix plus genes, barcodes, and matching metadata. The matrix is transposed to
cell x gene for AnnData and written as one same-prefix obs/X/var triplet per
family under:

    temporal_pretraining/organoid/scp211_human_kidney_organoids_atlas/<family>/...

Source files are staged under GCS/browser-auth cleanup storage and are accessed
through the verified gcsfuse mount by default.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import anndata as ad
import pandas as pd
import scipy.sparse as sp
from scipy import io as scipy_io

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

DEFAULT_SOURCE = Path(
    "/Users/jkobject/mnt/gcs/scperturb/pert-gym/staging/manual_downloads/2026-06-23/downloads_cleanup/SCP211"
)
DEFAULT_PREFIX = "temporal_pretraining/organoid/scp211_human_kidney_organoids_atlas"
STATUS_JSON = ROOT / "artifacts/schema_audit/scp211_kidney_organoid_ingestion_t_8ed1ea12_20260623.json"
STATUS_MD = ROOT / "artifacts/schema_audit/scp211_kidney_organoid_ingestion_t_8ed1ea12_20260623.md"


@dataclass(frozen=True)
class Family:
    label: str
    matrix: str
    genes: str
    barcodes: str
    metadata: str
    expected_sizes: dict[str, int]
    raw_time_label: str
    timepoint_value: int
    batch_label: str
    sample_description: str

    @property
    def prefix_label(self) -> str:
        return self.label


FAMILIES: dict[str, Family] = {
    "d51": Family(
        label="d51",
        matrix="expression/5ee010da771a5b10156f5659/gene_sorted-2020-06-05.ma51_control_organoids.mtx",
        genes="expression/5ee010da771a5b10156f5659/d51_genes.tsv",
        barcodes="expression/5ee010da771a5b10156f5659/d51_barcodes.tsv",
        metadata="cluster/d51_control_organoids_metadata.txt",
        expected_sizes={
            "expression/5ee010da771a5b10156f5659/gene_sorted-2020-06-05.ma51_control_organoids.mtx": 131_448_493,
            "expression/5ee010da771a5b10156f5659/d51_genes.tsv": 216_569,
            "expression/5ee010da771a5b10156f5659/d51_barcodes.tsv": 92_327,
            "cluster/d51_control_organoids_metadata.txt": 310_132,
        },
        raw_time_label="d51",
        timepoint_value=51,
        batch_label="ma51_control_organoids",
        sample_description="day 51 control kidney organoids",
    ),
    "d32_MA": Family(
        label="d32_MA",
        matrix="expression/5ee19042771a5b10156f57d0/gene_sorted-2020-06-05.d32_MA.mtx",
        genes="expression/5ee19042771a5b10156f57d0/d32_MA_genes.tsv",
        barcodes="expression/5ee19042771a5b10156f57d0/d32_MA_barcodes.tsv",
        metadata="cluster/2020-06-05.d32_MA_orgs_metadata.txt",
        expected_sizes={
            "expression/5ee19042771a5b10156f57d0/gene_sorted-2020-06-05.d32_MA.mtx": 280_384_959,
            "expression/5ee19042771a5b10156f57d0/d32_MA_genes.tsv": 216_569,
            "expression/5ee19042771a5b10156f57d0/d32_MA_barcodes.tsv": 255_453,
            "cluster/2020-06-05.d32_MA_orgs_metadata.txt": 629_654,
        },
        raw_time_label="d32",
        timepoint_value=32,
        batch_label="d32_MA",
        sample_description="day 32 MA kidney organoids",
    ),
    "d32_control": Family(
        label="d32_control",
        matrix="expression/5ee1a44f771a5b10156f57f4/gene_sorted-2020-06-05.d32_control_sub_organoids.mtx",
        genes="expression/5ee1a44f771a5b10156f57f4/d32_genes.tsv",
        barcodes="expression/5ee1a44f771a5b10156f57f4/d32_barcodes.tsv",
        metadata="cluster/day32.integrated_organoids_metadata.txt",
        expected_sizes={
            "expression/5ee1a44f771a5b10156f57f4/gene_sorted-2020-06-05.d32_control_sub_organoids.mtx": 1_006_368_942,
            "expression/5ee1a44f771a5b10156f57f4/d32_genes.tsv": 216_569,
            "expression/5ee1a44f771a5b10156f57f4/d32_barcodes.tsv": 188_844,
            "cluster/day32.integrated_organoids_metadata.txt": 2_612_889,
        },
        raw_time_label="d32",
        timepoint_value=32,
        batch_label="d32_control_sub_organoids",
        sample_description="day 32 control kidney organoids",
    ),
    "d0": Family(
        label="d0",
        matrix="expression/5ee02fd7771a5b10156f5695/gene_sorted-2020-06-05.d0_organoids.mtx",
        genes="expression/5ee02fd7771a5b10156f5695/d0_genes.tsv",
        barcodes="expression/5ee02fd7771a5b10156f5695/d0_barcodes.tsv",
        metadata="cluster/day0.integrated_iPSC_metadata.txt",
        expected_sizes={
            "expression/5ee02fd7771a5b10156f5695/gene_sorted-2020-06-05.d0_organoids.mtx": 1_995_091_905,
            "expression/5ee02fd7771a5b10156f5695/d0_genes.tsv": 162_055,
            "expression/5ee02fd7771a5b10156f5695/d0_barcodes.tsv": 185_157,
            "cluster/day0.integrated_iPSC_metadata.txt": 3_480_561,
        },
        raw_time_label="d0",
        timepoint_value=0,
        batch_label="d0_iPSC",
        sample_description="day 0 iPSC/organoid trajectory baseline",
    ),
    "d7": Family(
        label="d7",
        matrix="expression/5ee068be771a5b10156f56bf/gene_sorted-2020-06-05.d7_organoids.mtx",
        genes="expression/5ee068be771a5b10156f56bf/d7_genes.tsv",
        barcodes="expression/5ee068be771a5b10156f56bf/d7_barcodes.tsv",
        metadata="cluster/day7.integrated_organoids_metadata.txt",
        expected_sizes={
            "expression/5ee068be771a5b10156f56bf/gene_sorted-2020-06-05.d7_organoids.mtx": 2_825_968_784,
            "expression/5ee068be771a5b10156f56bf/d7_genes.tsv": 166_652,
            "expression/5ee068be771a5b10156f56bf/d7_barcodes.tsv": 177_166,
            "cluster/day7.integrated_organoids_metadata.txt": 4_048_339,
        },
        raw_time_label="d7",
        timepoint_value=7,
        batch_label="d7_organoids",
        sample_description="day 7 kidney organoids",
    ),
    "d15": Family(
        label="d15",
        matrix="expression/5ee081eb771a5b10156f56d5/gene_sorted-2020-06-05.d15_organoids.mtx",
        genes="expression/5ee081eb771a5b10156f56d5/d15_genes.tsv",
        barcodes="expression/5ee081eb771a5b10156f56d5/d15_barcodes.tsv",
        metadata="cluster/day15.integrated_organoids_metadata.txt",
        expected_sizes={
            "expression/5ee081eb771a5b10156f56d5/gene_sorted-2020-06-05.d15_organoids.mtx": 4_941_369_579,
            "expression/5ee081eb771a5b10156f56d5/d15_genes.tsv": 177_490,
            "expression/5ee081eb771a5b10156f56d5/d15_barcodes.tsv": 5_942_047,
            "cluster/day15.integrated_organoids_metadata.txt": 20_094_720,
        },
        raw_time_label="d15",
        timepoint_value=15,
        batch_label="d15_organoids",
        sample_description="day 15 kidney organoids",
    ),
    "d29": Family(
        label="d29",
        matrix="expression/5ee0ff31771a5b10156f5715/gene_sorted-2020-06-05.d29_organoids.mtx",
        genes="expression/5ee0ff31771a5b10156f5715/genes.tsv",
        barcodes="expression/5ee0ff31771a5b10156f5715/barcodes.tsv",
        metadata="cluster/day29.integrated_organoids_metadata.txt",
        expected_sizes={
            "expression/5ee0ff31771a5b10156f5715/gene_sorted-2020-06-05.d29_organoids.mtx": 4_858_455_186,
            "expression/5ee0ff31771a5b10156f5715/genes.tsv": 183_992,
            "expression/5ee0ff31771a5b10156f5715/barcodes.tsv": 5_193_326,
            "cluster/day29.integrated_organoids_metadata.txt": 14_973_887,
        },
        raw_time_label="d29",
        timepoint_value=29,
        batch_label="d29_organoids",
        sample_description="day 29 kidney organoids",
    ),
}

DEFAULT_ORDER = ["d51", "d32_MA", "d32_control", "d0", "d7", "d15", "d29"]
REQUIRED_OBS_COLUMNS = {
    "dataset",
    "source_accession",
    "source_title",
    "trajectory_id",
    "raw_time_label",
    "timepoint",
    "timepoint_unit",
    "timepoint_value",
    "organism",
    "tissue",
    "cell_type",
    "assay",
    "modality",
    "perturbation",
    "perturbation_type",
    "perturbation_technology",
    "is_control",
}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_gzip(path: Path) -> bool:
    with open(path, "rb") as handle:
        return handle.read(2) == b"\x1f\x8b"


def open_text(path: Path):
    return gzip.open(path, "rt") if is_gzip(path) else open(path, "rt")


def read_csv_auto(path: Path, **kwargs: Any) -> pd.DataFrame:
    return pd.read_csv(path, compression="gzip" if is_gzip(path) else None, **kwargs)


def count_lines(path: Path) -> int:
    with open_text(path) as handle:
        return sum(1 for _ in handle)


def require_family_files(source_dir: Path, family: Family) -> dict[str, int]:
    observed: dict[str, int] = {}
    missing: list[str] = []
    mismatches: list[dict[str, Any]] = []
    for rel, expected in family.expected_sizes.items():
        path = source_dir / rel
        if not path.exists():
            missing.append(rel)
            continue
        size = path.stat().st_size
        observed[rel] = int(size)
        if size != expected:
            mismatches.append({"relpath": rel, "expected": expected, "actual": size})
    if missing or mismatches:
        raise RuntimeError(f"staged file check failed for {family.label}: missing={missing} mismatches={mismatches}")
    return observed


def read_matrix_header(path: Path) -> dict[str, Any]:
    with open_text(path) as handle:
        first = handle.readline().strip()
        dims = handle.readline().strip().split()
    if not first.startswith("%%MatrixMarket matrix coordinate"):
        raise ValueError(f"unexpected MatrixMarket header for {path}: {first!r}")
    n_genes, n_cells, nnz = map(int, dims)
    return {
        "format": first,
        "n_genes": n_genes,
        "n_cells": n_cells,
        "nnz": nnz,
        "orientation": "genes_by_cells",
    }


def read_barcodes(path: Path) -> list[str]:
    with open_text(path) as handle:
        barcodes = [line.strip() for line in handle if line.strip()]
    if len(barcodes) != len(set(barcodes)):
        raise ValueError(f"barcodes are not unique in {path}")
    return barcodes


def read_var(path: Path, expected_rows: int) -> pd.DataFrame:
    var = read_csv_auto(path, sep="\t", header=None, low_memory=False)
    if len(var) != expected_rows:
        raise ValueError(f"genes rows {len(var)} != matrix genes {expected_rows} for {path}")
    if var.shape[1] == 1:
        var.columns = ["gene_id"]
        var["gene_symbol"] = var["gene_id"].astype(str)
    elif var.shape[1] == 2:
        var.columns = ["gene_id", "gene_symbol"]
    else:
        base = ["gene_id", "gene_symbol", "feature_type"]
        var.columns = base[: var.shape[1]] + [f"gene_col_{i}" for i in range(len(base), var.shape[1])]
    var["gene_id"] = var["gene_id"].astype(str)
    var["gene_symbol"] = var["gene_symbol"].astype(str)
    if var["gene_id"].nunique() == len(var):
        var.index = var["gene_id"]
    elif var["gene_symbol"].nunique() == len(var):
        var.index = var["gene_symbol"]
    else:
        raise ValueError(f"neither gene_id nor gene_symbol is unique in {path}")
    var.index = var.index.astype(str)
    var.index.name = None
    return var


def read_metadata(path: Path, barcodes: list[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    with open_text(path) as handle:
        _header = handle.readline()
        second = handle.readline().rstrip("\n").split("\t")
    skip_type_row = bool(second and second[0] == "TYPE")
    obs = read_csv_auto(path, sep="\t", skiprows=[1] if skip_type_row else None, low_memory=False)
    if "NAME" not in obs.columns:
        raise ValueError(f"metadata {path} is missing NAME column")
    obs["NAME"] = obs["NAME"].astype(str)
    if obs["NAME"].nunique() != len(obs):
        raise ValueError(f"metadata NAME values are not unique in {path}")
    missing_meta = sorted(set(barcodes) - set(obs["NAME"]))
    extra_meta = sorted(set(obs["NAME"]) - set(barcodes))
    if missing_meta or extra_meta:
        raise ValueError(
            f"metadata/barcode ID mismatch for {path}: missing_meta={len(missing_meta)} extra_meta={len(extra_meta)} "
            f"first_missing={missing_meta[:5]} first_extra={extra_meta[:5]}"
        )
    obs = obs.set_index("NAME", drop=True).loc[barcodes].copy()
    obs.index.name = None
    return obs, {"metadata_reordered_to_barcodes": True, "metadata_had_scp_type_row": skip_type_row}


def first_present(df: pd.DataFrame, names: Iterable[str], default: str = "unknown") -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name].fillna(default).astype(str)
    return pd.Series(default, index=df.index, dtype="object")


def standardize_obs(obs: pd.DataFrame, family: Family) -> pd.DataFrame:
    obs = obs.copy()
    obs["dataset"] = f"SCP211_{family.label}"
    obs["source_accession"] = "SCP211"
    obs["source_title"] = "Human Kidney Organoids Atlas"
    obs["trajectory_id"] = "scp211_kidney_organoid_maturation"
    obs["raw_time_label"] = family.raw_time_label
    obs["timepoint"] = family.raw_time_label
    obs["timepoint_unit"] = "day"
    obs["timepoint_value"] = family.timepoint_value
    obs["sample_family"] = family.label
    obs["batch_label"] = family.batch_label
    obs["sample_description"] = family.sample_description
    obs["organism"] = "Homo sapiens"
    obs["organism_ontology_id"] = "NCBITaxon_9606"
    obs["tissue"] = "kidney organoid"
    obs["tissue_ontology_id"] = "kidney organoid"
    obs["cell_type"] = first_present(obs, ["Putative_Cell_Types", "cell_type", "celltype"], "unknown")
    obs["compartment"] = first_present(obs, ["Compartment", "compartment"], "unknown")
    obs["assay"] = "10x scRNA-seq"
    obs["modality"] = "scRNA-seq"
    obs["perturbation"] = "developmental_time"
    obs["perturbation_type"] = "timecourse"
    obs["perturbation_technology"] = "natural developmental trajectory"
    obs["is_control"] = family.label in {"d51", "d32_control"}
    obs["disease"] = "normal"
    obs["cancer"] = False
    for source, target in [("nCount_RNA", "n_counts"), ("nFeature_RNA", "n_genes"), ("percentMito", "percent_mito")]:
        if source in obs.columns:
            obs[target] = pd.to_numeric(obs[source], errors="coerce")
    if "n_counts" in obs.columns:
        obs["ncounts"] = obs["n_counts"]
    if "n_genes" in obs.columns:
        obs["ngenes"] = obs["n_genes"]
    return obs


def load_tables(source_dir: Path, family: Family) -> tuple[pd.DataFrame, pd.DataFrame, list[str], dict[str, Any]]:
    matrix_path = source_dir / family.matrix
    genes_path = source_dir / family.genes
    barcodes_path = source_dir / family.barcodes
    metadata_path = source_dir / family.metadata
    header = read_matrix_header(matrix_path)
    barcodes = read_barcodes(barcodes_path)
    var = read_var(genes_path, header["n_genes"])
    if len(barcodes) != header["n_cells"]:
        raise ValueError(f"barcode rows {len(barcodes)} != matrix cells {header['n_cells']} for {family.label}")
    obs, meta_info = read_metadata(metadata_path, barcodes)
    obs = standardize_obs(obs, family)
    info = {
        "matrix_header": header,
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "barcodes_unique": True,
        **meta_info,
        "required_obs_columns_present": sorted(REQUIRED_OBS_COLUMNS.intersection(obs.columns)),
        "timepoint_counts": obs["raw_time_label"].value_counts(dropna=False).to_dict(),
        "cell_type_counts_top20": obs["cell_type"].value_counts(dropna=False).head(20).to_dict(),
    }
    return obs, var, barcodes, info


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        feature = list(ln.Feature.filter(name=name).all())
        if feature and feature[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name!r} has dtype {feature[0].dtype!r}; expected cat[Artifact]")
        if not feature:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def artifact_exists(ln: Any, key: str) -> bool:
    return ln.Artifact.filter(key=key).exists()


def family_prefix(base_prefix: str, family: Family) -> str:
    return f"{base_prefix.rstrip('/')}/{family.prefix_label}"


def verify_lamin_family(ln: Any, prefix: str) -> dict[str, Any]:
    obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
    x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs = obs_art.load()
    var = var_art.load()
    x_path_exists = bool(x_art.path.exists()) if getattr(x_art, "path", None) is not None else None
    return {
        "obs_key": obs_art.key,
        "x_key": x_art.key,
        "var_key": var_art.key,
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "x_n_observations": int(x_art.n_observations or 0),
        "x_path_exists": x_path_exists,
        "obs_to_x_link_ok": x_art.key == f"{prefix}/X.h5ad",
        "x_to_var_link_ok": var_art.key == f"{prefix}/var.parquet",
        "required_obs_columns_present": sorted(REQUIRED_OBS_COLUMNS.intersection(obs.columns)),
        "timepoint_counts": obs["raw_time_label"].value_counts(dropna=False).to_dict() if "raw_time_label" in obs.columns else {},
        "cell_type_counts_top20": obs["cell_type"].value_counts(dropna=False).head(20).to_dict() if "cell_type" in obs.columns else {},
    }


def write_status(data: dict[str, Any]) -> None:
    STATUS_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATUS_JSON.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")
    lines = [
        f"# SCP211 kidney organoid family ingestion — {data.get('status')}",
        "",
        f"Generated: {data.get('updated_at')}",
        f"Task: {data.get('task_id')}",
        f"Source: `{data.get('source_dir')}`",
        f"Lamin prefix: `{data.get('lamin_prefix')}`",
        "",
        "## Families",
    ]
    for label, result in data.get("families", {}).items():
        lines.append(f"- `{label}`: `{result.get('status')}` obs={result.get('obs_rows')} var={result.get('var_rows')} prefix=`{result.get('prefix')}`")
    if data.get("notes"):
        lines.extend(["", "## Notes"])
        for note in data["notes"]:
            lines.append(f"- {note}")
    if data.get("artifacts"):
        lines.extend(["", "## Artifacts"])
        for key, value in data["artifacts"].items():
            lines.append(f"- `{key}`: `{value}`")
    STATUS_MD.write_text("\n".join(lines) + "\n")


def ingest_one_family(ln: Any, source_dir: Path, base_prefix: str, family: Family, *, dry_run: bool, verify_only: bool, overwrite: bool) -> dict[str, Any]:
    sizes = require_family_files(source_dir, family)
    obs, var, _barcodes, preflight = load_tables(source_dir, family)
    prefix = family_prefix(base_prefix, family)
    result: dict[str, Any] = {
        "family": family.label,
        "prefix": prefix,
        "file_sizes": sizes,
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "preflight": preflight,
    }
    if dry_run:
        result["status"] = "dry_run_ok"
        return result

    existing = [suffix for suffix in ("obs.parquet", "X.h5ad", "var.parquet") if artifact_exists(ln, f"{prefix}/{suffix}")]
    if verify_only:
        verification = verify_lamin_family(ln, prefix)
        result.update(verification)
        result["status"] = "verified" if verification.get("obs_to_x_link_ok") and verification.get("x_to_var_link_ok") and verification.get("x_path_exists") else "verify_failed"
        return result
    if existing and not overwrite:
        if set(existing) == {"obs.parquet", "X.h5ad", "var.parquet"}:
            verification = verify_lamin_family(ln, prefix)
            result.update(verification)
            result["status"] = "already_exists_verified"
            return result
        raise RuntimeError(f"partial triplet exists for {prefix}: {existing}; rerun with --overwrite only after review")

    matrix_path = source_dir / family.matrix
    print(f"READ_MATRIX family={family.label} path={matrix_path}", flush=True)
    raw = scipy_io.mmread(str(matrix_path))
    matrix = raw.tocsr().T.tocsr()  # genes x cells -> cells x genes
    if matrix.shape != (obs.shape[0], var.shape[0]):
        raise ValueError(f"matrix shape {matrix.shape} != obs/var {(obs.shape[0], var.shape[0])} for {family.label}")
    if not sp.isspmatrix_csr(matrix):
        matrix = matrix.tocsr()
    print(f"MATRIX_READY family={family.label} shape={matrix.shape} nnz={matrix.nnz}", flush=True)

    prev_obs = list(ln.Artifact.filter(key=f"{prefix}/obs.parquet").all())
    prev_x = list(ln.Artifact.filter(key=f"{prefix}/X.h5ad").all())
    prev_var = list(ln.Artifact.filter(key=f"{prefix}/var.parquet").all())

    x_adata = ad.AnnData(
        X=matrix,
        obs=pd.DataFrame(index=obs.index.astype(str)),
        var=pd.DataFrame(index=var.index.astype(str)),
    )
    with tempfile.TemporaryDirectory(prefix=f"scp211_{family.label}_") as tmp_dir:
        tmp = Path(tmp_dir)
        obs_path = tmp / "obs.parquet"
        var_path = tmp / "var.parquet"
        x_path = tmp / "X.h5ad"
        obs.to_parquet(obs_path, index=True)
        var.to_parquet(var_path, index=True)
        print(f"WRITE_H5AD family={family.label} path={x_path}", flush=True)
        x_adata.write_h5ad(x_path, compression="gzip")
        print(f"REGISTER_LAMIN family={family.label} prefix={prefix}", flush=True)
        obs_art = ln.Artifact(
            obs_path,
            key=f"{prefix}/obs.parquet",
            revises=prev_obs[-1] if (overwrite and prev_obs) else None,
        ).save()
        x_art = ln.Artifact.from_anndata(
            str(x_path),
            key=f"{prefix}/X.h5ad",
            revises=prev_x[-1] if (overwrite and prev_x) else None,
        ).save()
        var_art = ln.Artifact(
            var_path,
            key=f"{prefix}/var.parquet",
            revises=prev_var[-1] if (overwrite and prev_var) else None,
            skip_hash_lookup=True,
        ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    verification = verify_lamin_family(ln, prefix)
    result.update(verification)
    result["status"] = "ingested_verified"
    return result


def parse_families(raw: str | None) -> list[str]:
    if not raw or raw == "all":
        return DEFAULT_ORDER.copy()
    labels = [part.strip() for part in raw.split(",") if part.strip()]
    unknown = [label for label in labels if label not in FAMILIES]
    if unknown:
        raise ValueError(f"unknown families {unknown}; valid={sorted(FAMILIES)}")
    return labels


def ingest(args: argparse.Namespace) -> dict[str, Any]:
    labels = parse_families(args.families)
    ln = None
    if not args.dry_run:
        ensure_project_cache()
        ln = connect_pertdata()
        ln.track(path="tools/ingest_scp211_kidney_organoid_families.py")
        assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
        assert ln.setup.settings.branch.name == "jkobject"
        ensure_link_features(ln)
    data: dict[str, Any] = {
        "task_id": "t_8ed1ea12",
        "status": "running",
        "updated_at": now_utc(),
        "source_dir": str(args.source_dir),
        "lamin_prefix": args.prefix,
        "families_requested": labels,
        "families": {},
        "notes": [
            "Selected day-specific organoid MatrixMarket families only; adult-kidney reference files intentionally excluded.",
            "Each family is written as a same-prefix obs/X/var triplet under the base SCP211 Lamin prefix.",
        ],
        "artifacts": {"status_json": str(STATUS_JSON), "status_md": str(STATUS_MD)},
    }
    try:
        for label in labels:
            family = FAMILIES[label]
            result = ingest_one_family(
                ln,
                args.source_dir,
                args.prefix,
                family,
                dry_run=args.dry_run,
                verify_only=args.verify_only,
                overwrite=args.overwrite,
            )
            data["families"][label] = result
            write_status({**data, "status": "partial"})
            if ln is not None:
                clean_cache(ROOT / ".lamin-cache")
        statuses = {result.get("status") for result in data["families"].values()}
        if args.dry_run:
            data["status"] = "dry_run_ok"
        elif args.verify_only:
            data["status"] = "verified" if statuses == {"verified"} else "verify_mixed"
        elif statuses.issubset({"ingested_verified", "already_exists_verified"}):
            data["status"] = "ingested_verified"
        else:
            data["status"] = "mixed"
        data["updated_at"] = now_utc()
        write_status(data)
        return data
    except Exception as exc:
        data["status"] = "failed"
        data["updated_at"] = now_utc()
        data["error"] = repr(exc)
        write_status(data)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--families", default="all", help="comma-separated family labels or 'all'; default ordered all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    result = ingest(args)
    print(json.dumps(result, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
