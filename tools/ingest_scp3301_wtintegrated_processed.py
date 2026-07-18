#!/usr/bin/env python3
"""Ingest SCP3301 WTintegrated processed MatrixMarket family as a Lamin triplet.

Source family staged from browser/manual SCP recovery:
- WTintegrated_processed_matrix.txt.gz      MatrixMarket genes x cells
- WTintegrated_processed_genes.tsv.gz       10x genes/features
- WTintegrated_processed_barcodes.tsv.gz    10x barcodes/cells
- WTintegrated_addendum_metadata_convention.txt.gz
- WTintegrated_addendum_clustering.txt.gz

The matrix is feature x cell and is transposed to cell x feature for AnnData.
Metadata is allowed to be unordered but must be a one-to-one set match to barcodes.
Clustering is stored as a typed obsm-style parquet auxiliary only after cell IDs match.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd
import scipy.sparse as sp
from scipy import io as scipy_io

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

DEFAULT_SOURCE = Path(
    "/Users/jkobject/mnt/gcs/scperturb/pert-gym/staging/manual_scp/2026-06-23/SCP3301"
)
DEFAULT_PREFIX = "temporal_pretraining/scp3301_anterior_segment_development_wtintegrated_processed"
STATUS_JSON = ROOT / "artifacts/schema_audit/scp3301_wtintegrated_processed_ingestion_t_08fc3955_20260623.json"
STATUS_MD = ROOT / "artifacts/schema_audit/scp3301_wtintegrated_processed_ingestion_t_08fc3955_20260623.md"

EXPECTED_FILES = {
    "WTintegrated_processed_matrix.txt.gz": 608_826_059,
    "WTintegrated_processed_genes.tsv.gz": 167_038,
    "WTintegrated_processed_barcodes.tsv.gz": 572_366,
    "WTintegrated_addendum_metadata_convention.txt.gz": 3_190_665,
    "WTintegrated_addendum_clustering.txt.gz": 3_375_784,
}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def require_files(source_dir: Path) -> dict[str, int]:
    sizes: dict[str, int] = {}
    missing: list[str] = []
    mismatches: list[dict[str, Any]] = []
    for name, expected in EXPECTED_FILES.items():
        path = source_dir / name
        if not path.exists():
            missing.append(name)
            continue
        size = path.stat().st_size
        sizes[name] = size
        if size != expected:
            mismatches.append({"filename": name, "expected": expected, "actual": size})
    if missing or mismatches:
        raise RuntimeError(f"staged file check failed: missing={missing} mismatches={mismatches}")
    return sizes


def read_barcodes(path: Path) -> list[str]:
    with gzip.open(path, "rt") as handle:
        barcodes = [line.strip() for line in handle if line.strip()]
    if len(barcodes) != len(set(barcodes)):
        raise ValueError("barcodes are not unique")
    return barcodes


def read_matrix_header(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt") as handle:
        first = handle.readline().strip()
        dims = handle.readline().strip().split()
    if first != "%%MatrixMarket matrix coordinate integer general":
        raise ValueError(f"unexpected MatrixMarket header: {first!r}")
    n_genes, n_cells, nnz = map(int, dims)
    return {"format": first, "n_genes": n_genes, "n_cells": n_cells, "nnz": nnz, "orientation": "genes_by_cells"}


def load_tables(source_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str], dict[str, Any]]:
    barcodes = read_barcodes(source_dir / "WTintegrated_processed_barcodes.tsv.gz")
    header = read_matrix_header(source_dir / "WTintegrated_processed_matrix.txt.gz")
    var = pd.read_csv(
        source_dir / "WTintegrated_processed_genes.tsv.gz",
        sep="\t",
        header=None,
        names=["gene_id", "gene_symbol", "feature_type"],
        compression="gzip",
    )
    if len(var) != header["n_genes"]:
        raise ValueError(f"genes rows {len(var)} != matrix genes {header['n_genes']}")
    if len(barcodes) != header["n_cells"]:
        raise ValueError(f"barcode rows {len(barcodes)} != matrix cells {header['n_cells']}")
    if var["gene_id"].astype(str).nunique() != len(var):
        raise ValueError("gene_id values are not unique")
    var.index = var["gene_id"].astype(str)
    var.index.name = None

    obs = pd.read_csv(
        source_dir / "WTintegrated_addendum_metadata_convention.txt.gz",
        sep="\t",
        compression="gzip",
        skiprows=[1],  # SCP TYPE row
        low_memory=False,
    )
    if "NAME" not in obs.columns:
        raise ValueError("metadata is missing NAME column")
    obs["NAME"] = obs["NAME"].astype(str)
    if obs["NAME"].nunique() != len(obs):
        raise ValueError("metadata NAME values are not unique")
    missing_meta = sorted(set(barcodes) - set(obs["NAME"]))
    extra_meta = sorted(set(obs["NAME"]) - set(barcodes))
    if missing_meta or extra_meta:
        raise ValueError(
            f"metadata/barcode ID mismatch: missing_meta={len(missing_meta)} extra_meta={len(extra_meta)} "
            f"first_missing={missing_meta[:5]} first_extra={extra_meta[:5]}"
        )
    obs = obs.set_index("NAME", drop=True).loc[barcodes].copy()
    obs.index.name = None

    clustering = pd.read_csv(
        source_dir / "WTintegrated_addendum_clustering.txt.gz",
        compression="gzip",
        skiprows=[1],  # SCP TYPE row
        low_memory=False,
    )
    if "NAME" not in clustering.columns:
        raise ValueError("clustering is missing NAME column")
    clustering["NAME"] = clustering["NAME"].astype(str)
    if clustering["NAME"].nunique() != len(clustering):
        raise ValueError("clustering NAME values are not unique")
    missing_cluster = sorted(set(barcodes) - set(clustering["NAME"]))
    extra_cluster = sorted(set(clustering["NAME"]) - set(barcodes))
    if missing_cluster or extra_cluster:
        raise ValueError(
            f"clustering/barcode ID mismatch: missing_cluster={len(missing_cluster)} extra_cluster={len(extra_cluster)} "
            f"first_missing={missing_cluster[:5]} first_extra={extra_cluster[:5]}"
        )
    clustering = clustering.set_index("NAME", drop=True).loc[barcodes].copy()
    clustering.index.name = None

    return obs, var, clustering, barcodes, header


def standardize_obs(obs: pd.DataFrame) -> pd.DataFrame:
    obs = obs.copy()
    obs["dataset"] = "SCP3301_WTintegrated_processed"
    obs["source_accession"] = "SCP3301"
    obs["source_geo_accession"] = "GSE315712"
    obs["source_title"] = "Single-Cell Characterization of Anterior Segment Development"
    obs["trajectory_id"] = "scp3301_anterior_segment_development"
    obs["raw_time_label"] = obs.get("donor_id", "unknown").astype(str).str.extract(r"(Day[^_\s-]+)", expand=False).fillna("unknown")
    obs["timepoint"] = obs["raw_time_label"]
    obs["timepoint_unit"] = "day"
    obs["timepoint_value"] = pd.to_numeric(obs["raw_time_label"].str.replace("Day", "", regex=False), errors="coerce")
    obs["organism"] = obs.get("species__ontology_label", "Mus musculus")
    obs["organism_ontology_id"] = obs.get("species", "NCBITaxon_10090")
    obs["tissue"] = obs.get("organ__ontology_label", "eye")
    obs["tissue_ontology_id"] = obs.get("organ", "UBERON:0000970")
    obs["cell_type"] = obs.get("celltype", "unknown").fillna("unknown").astype(str)
    obs["assay"] = obs.get("library_preparation_protocol__ontology_label", "10x 3' v3")
    obs["modality"] = "scRNA-seq"
    obs["perturbation"] = "developmental_time"
    obs["perturbation_type"] = "timecourse"
    obs["perturbation_technology"] = "natural developmental trajectory"
    obs["is_control"] = False
    obs["disease"] = obs.get("disease__ontology_label", "normal")
    obs["disease_ontology_id"] = obs.get("disease", "PATO_0000461")
    obs["cancer"] = False
    if "nCount_RNA" in obs.columns:
        obs["n_counts"] = pd.to_numeric(obs["nCount_RNA"], errors="coerce")
        obs["ncounts"] = obs["n_counts"]
    if "nFeature_RNA" in obs.columns:
        obs["n_genes"] = pd.to_numeric(obs["nFeature_RNA"], errors="coerce")
        obs["ngenes"] = obs["n_genes"]
    if "percentMito" in obs.columns:
        obs["percent_mito"] = pd.to_numeric(obs["percentMito"], errors="coerce")
    return obs


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var", "obsm_wtintegrated_clustering"):
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


def existing_triplet_complete(ln: Any, prefix: str) -> bool:
    return all(artifact_exists(ln, f"{prefix}/{suffix}") for suffix in ("obs.parquet", "X.h5ad", "var.parquet"))


def write_status(data: dict[str, Any]) -> None:
    STATUS_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATUS_JSON.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")
    lines = [
        f"# SCP3301 WTintegrated processed ingestion — {data.get('status')}",
        "",
        f"Generated: {data.get('updated_at')}",
        f"Task: {data.get('task_id')}",
        f"Source: `{data.get('source_dir')}`",
        f"Lamin prefix: `{data.get('lamin_prefix')}`",
        "",
        "## Verification",
    ]
    for key, value in data.get("verification", {}).items():
        lines.append(f"- `{key}`: {value}")
    if data.get("artifacts"):
        lines.extend(["", "## Artifacts"])
        for key, value in data["artifacts"].items():
            lines.append(f"- `{key}`: `{value}`")
    if data.get("notes"):
        lines.extend(["", "## Notes"])
        for note in data["notes"]:
            lines.append(f"- {note}")
    STATUS_MD.write_text("\n".join(lines) + "\n")


def verify_lamin(ln: Any, prefix: str) -> dict[str, Any]:
    obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
    x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs = obs_art.load()
    var = var_art.load()
    obsm_value = obs_art.features.get_values().get("obsm_wtintegrated_clustering")
    obsm_art = resolve_artifact(ln, obsm_value) if obsm_value is not None else None
    clustering_rows = None
    if obsm_art is not None:
        clustering_rows = int(obsm_art.load().shape[0])
    x_path_exists = bool(x_art.path.exists()) if getattr(x_art, "path", None) is not None else None
    return {
        "obs_key": obs_art.key,
        "x_key": x_art.key,
        "var_key": var_art.key,
        "obsm_key": obsm_art.key if obsm_art is not None else None,
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "x_n_observations": int(x_art.n_observations or 0),
        "x_path_exists": x_path_exists,
        "obs_to_x_link_ok": x_art.key == f"{prefix}/X.h5ad",
        "x_to_var_link_ok": var_art.key == f"{prefix}/var.parquet",
        "obsm_rows": clustering_rows,
        "required_obs_columns": sorted(
            set([
                "dataset",
                "source_accession",
                "trajectory_id",
                "raw_time_label",
                "timepoint_value",
                "organism",
                "cell_type",
                "modality",
                "assay",
                "perturbation",
                "perturbation_type",
                "is_control",
            ]).intersection(obs.columns)
        ),
        "timepoint_counts": obs["raw_time_label"].value_counts(dropna=False).head(20).to_dict() if "raw_time_label" in obs.columns else {},
        "cell_type_counts": obs["cell_type"].value_counts(dropna=False).head(20).to_dict() if "cell_type" in obs.columns else {},
    }


def ingest(args: argparse.Namespace) -> dict[str, Any]:
    source_dir = args.source_dir
    sizes = require_files(source_dir)
    obs, var, clustering, _barcodes, header = load_tables(source_dir)
    obs = standardize_obs(obs)
    preflight = {
        "file_sizes": sizes,
        "matrix_header": header,
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "clustering_rows": int(clustering.shape[0]),
        "metadata_reordered_to_barcodes": True,
        "clustering_ids_match_barcodes": True,
        "timepoint_counts": obs["raw_time_label"].value_counts(dropna=False).to_dict(),
        "cell_type_counts": obs["cell_type"].value_counts(dropna=False).to_dict(),
    }
    if args.dry_run:
        data = {
            "task_id": "t_08fc3955",
            "status": "dry_run_ok",
            "updated_at": now_utc(),
            "source_dir": str(source_dir),
            "lamin_prefix": args.prefix,
            "verification": preflight,
            "notes": ["No Lamin writes performed."],
        }
        write_status(data)
        return data

    ensure_project_cache()
    ln = connect_pertdata()
    ln.track(path="tools/ingest_scp3301_wtintegrated_processed.py")
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    ensure_link_features(ln)

    if args.verify_only:
        verification = verify_lamin(ln, args.prefix)
        data = {
            "task_id": "t_08fc3955",
            "status": "verified" if verification.get("obs_to_x_link_ok") and verification.get("x_to_var_link_ok") else "verify_failed",
            "updated_at": now_utc(),
            "source_dir": str(source_dir),
            "lamin_prefix": args.prefix,
            "verification": {**preflight, **verification},
            "artifacts": {
                "status_json": str(STATUS_JSON),
                "status_md": str(STATUS_MD),
            },
        }
        write_status(data)
        return data

    existing = [suffix for suffix in ("obs.parquet", "X.h5ad", "var.parquet") if artifact_exists(ln, f"{args.prefix}/{suffix}")]
    if existing and not args.overwrite:
        if set(existing) == {"obs.parquet", "X.h5ad", "var.parquet"}:
            verification = verify_lamin(ln, args.prefix)
            data = {
                "task_id": "t_08fc3955",
                "status": "already_exists_verified",
                "updated_at": now_utc(),
                "source_dir": str(source_dir),
                "lamin_prefix": args.prefix,
                "verification": {**preflight, **verification},
                "artifacts": {"status_json": str(STATUS_JSON), "status_md": str(STATUS_MD)},
                "notes": ["Complete triplet already existed; no overwrite requested."],
            }
            write_status(data)
            return data
        raise RuntimeError(f"partial triplet exists for {args.prefix}: {existing}; rerun with --overwrite only after review")

    print("READ_MATRIX", source_dir / "WTintegrated_processed_matrix.txt.gz", flush=True)
    raw = scipy_io.mmread(str(source_dir / "WTintegrated_processed_matrix.txt.gz"))
    matrix = raw.tocsr().T.tocsr()  # genes x cells -> cells x genes
    if matrix.shape != (obs.shape[0], var.shape[0]):
        raise ValueError(f"matrix shape {matrix.shape} != obs/var {(obs.shape[0], var.shape[0])}")
    if not sp.isspmatrix_csr(matrix):
        matrix = matrix.tocsr()
    print(f"MATRIX_READY shape={matrix.shape} nnz={matrix.nnz}", flush=True)

    prev_obs = list(ln.Artifact.filter(key=f"{args.prefix}/obs.parquet").all())
    prev_x = list(ln.Artifact.filter(key=f"{args.prefix}/X.h5ad").all())
    prev_var = list(ln.Artifact.filter(key=f"{args.prefix}/var.parquet").all())
    prev_obsm = list(ln.Artifact.filter(key=f"{args.prefix}/obsm_wtintegrated_clustering.parquet").all())

    x_adata = ad.AnnData(
        X=matrix,
        obs=pd.DataFrame(index=obs.index.astype(str)),
        var=pd.DataFrame(index=var.index.astype(str)),
    )
    with tempfile.TemporaryDirectory(prefix="scp3301_wt_") as tmp_dir:
        tmp = Path(tmp_dir)
        obs_path = tmp / "obs.parquet"
        var_path = tmp / "var.parquet"
        obsm_path = tmp / "obsm_wtintegrated_clustering.parquet"
        x_path = tmp / "X.h5ad"
        obs.to_parquet(obs_path, index=True)
        var.to_parquet(var_path, index=True)
        clustering.to_parquet(obsm_path, index=True)
        print("WRITE_H5AD", x_path, flush=True)
        x_adata.write_h5ad(x_path, compression="gzip")
        print("REGISTER_LAMIN", args.prefix, flush=True)
        obs_art = ln.Artifact(
            obs_path,
            key=f"{args.prefix}/obs.parquet",
            revises=prev_obs[-1] if (args.overwrite and prev_obs) else None,
        ).save()
        x_art = ln.Artifact.from_anndata(
            str(x_path),
            key=f"{args.prefix}/X.h5ad",
            revises=prev_x[-1] if (args.overwrite and prev_x) else None,
        ).save()
        var_art = ln.Artifact(
            var_path,
            key=f"{args.prefix}/var.parquet",
            revises=prev_var[-1] if (args.overwrite and prev_var) else None,
            skip_hash_lookup=True,
        ).save()
        obsm_art = ln.Artifact(
            obsm_path,
            key=f"{args.prefix}/obsm_wtintegrated_clustering.parquet",
            revises=prev_obsm[-1] if (args.overwrite and prev_obsm) else None,
            skip_hash_lookup=True,
        ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art, "obsm_wtintegrated_clustering": obsm_art})
    verification = verify_lamin(ln, args.prefix)
    clean_cache(ROOT / ".lamin-cache")
    data = {
        "task_id": "t_08fc3955",
        "status": "ingested_verified",
        "updated_at": now_utc(),
        "source_dir": str(source_dir),
        "lamin_prefix": args.prefix,
        "verification": {**preflight, **verification},
        "artifacts": {
            "obs": f"{args.prefix}/obs.parquet",
            "X": f"{args.prefix}/X.h5ad",
            "var": f"{args.prefix}/var.parquet",
            "obsm_wtintegrated_clustering": f"{args.prefix}/obsm_wtintegrated_clustering.parquet",
            "status_json": str(STATUS_JSON),
            "status_md": str(STATUS_MD),
        },
        "notes": ["Metadata was reordered to barcode/matrix column order before writing obs.", "Clustering IDs matched barcodes and were saved as typed obsm-style parquet auxiliary."],
    }
    write_status(data)
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    result = ingest(args)
    print(json.dumps(result, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
