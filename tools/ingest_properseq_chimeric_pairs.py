#!/usr/bin/env python3
"""Ingest PROPER-seq chimeric read-pair CSVs as a compact Lamin triplet.

The staged GSE150818 supplementary files currently available on GCS are not a
10x matrix; they are per-sample `*_chimericReadPairs.csv.gz` tables with:

    Gene1,Gene2,readCount

This script treats each gene pair / sample row as an observation and stores the
read count as a single sparse feature. It reads directly from the GCS fuse mount
so no large local raw copy is required.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
from scipy import sparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

PREFIX = "properseq/chimeric_read_pairs"
GCS_EXTRACTED_DIR = Path(
    "/mnt/gcs/scperturb/pert-gym/staging/data/main/properseq/extracted"
)
PROGRESS = ROOT / "artifacts/properseq_ingestion_progress.json"


def triplet_complete(ln: Any, prefix: str = PREFIX) -> bool:
    return all(
        ln.Artifact.filter(key=f"{prefix}/{suffix}").exists()
        for suffix in ["obs.parquet", "X.h5ad", "var.parquet"]
    )


def parse_sample(path: Path) -> dict[str, str]:
    # Example: GSM4559183_HEK1_chimericReadPairs.csv.gz
    name = path.name.removesuffix(".csv.gz")
    sample = name.removesuffix("_chimericReadPairs")
    parts = sample.split("_", 1)
    geo_sample = parts[0]
    sample_label = parts[1] if len(parts) > 1 else sample
    cell_line = sample_label.rstrip("0123456789") or sample_label
    replicate = sample_label[len(cell_line) :] or "unknown"
    aliases = {"JKT": "Jurkat", "HEK": "HEK293", "HUVEC": "HUVEC"}
    return {
        "sample": sample,
        "geo_sample": geo_sample,
        "sample_label": sample_label,
        "cell_line": aliases.get(cell_line, cell_line),
        "replicate": replicate,
    }


def load_one(path: Path) -> pd.DataFrame:
    meta = parse_sample(path)
    df = pd.read_csv(path)
    df.columns = [col.lstrip("#") for col in df.columns]
    required = {"Gene1", "Gene2", "readCount"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {sorted(missing)}")
    df = df.rename(
        columns={"Gene1": "gene_a", "Gene2": "gene_b", "readCount": "read_count"}
    )
    df["read_count"] = pd.to_numeric(df["read_count"], errors="coerce").fillna(0)
    df = df[df["read_count"] > 0].copy()
    for key, value in meta.items():
        df[key] = value
    df["gene_pair"] = df["gene_a"].astype(str) + "+" + df["gene_b"].astype(str)
    df["perturbation"] = df["gene_pair"]
    print(
        f"LOAD {path.name}: rows={len(df)} total_reads={int(df['read_count'].sum())}",
        flush=True,
    )
    return df


def build_anndata(obs: pd.DataFrame) -> ad.AnnData:
    obs = obs.reset_index(drop=True)
    obs.index = (
        obs["sample"].astype(str)
        + ":"
        + obs["gene_pair"].astype(str)
        + ":"
        + obs.index.astype(str)
    )
    obs.index = obs.index.astype(str)
    obs["perturbation_type"] = "chimeric_gene_pair"
    obs["organism"] = "human"
    obs["modality"] = "chimeric RNA readout"
    obs["assay"] = "PROPER-seq"
    obs["dataset"] = "GSE150818"
    obs["is_control"] = False
    obs["n_counts"] = obs["read_count"].astype(np.float32)
    obs["n_genes"] = 2
    obs["cancer"] = obs["cell_line"].isin(["HEK293", "Jurkat"])
    obs["disease"] = np.where(obs["cell_line"].eq("Jurkat"), "T-cell leukemia", "unknown")
    obs["tissue_type"] = np.where(obs["cell_line"].eq("HUVEC"), "umbilical vein", "unknown")

    var = pd.DataFrame(
        {
            "feature_type": ["chimeric_pair_read_count"],
            "measurement": ["read_count"],
        },
        index=["read_count"],
    )
    x = sparse.csr_matrix(obs[["read_count"]].to_numpy(dtype=np.float32))
    return ad.AnnData(X=x, obs=obs, var=var)


def save_progress(payload: dict[str, Any]) -> None:
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")


def verify(ln: Any, prefix: str = PREFIX) -> dict[str, Any]:
    obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
    x_art = ln.Artifact.get(key=f"{prefix}/X.h5ad")
    var_art = ln.Artifact.get(key=f"{prefix}/var.parquet")
    obs_link = obs_art.features.get_values().get("X")
    var_link = x_art.features.get_values().get("var")
    obs = obs_art.load()
    var = var_art.load()
    return {
        "prefix": prefix,
        "links_ok": obs_link == f"{prefix}/X.h5ad" and var_link == f"{prefix}/var.parquet",
        "obs_shape": list(obs.shape),
        "var_shape": list(var.shape),
        "total_reads": int(obs["read_count"].sum()) if "read_count" in obs.columns else None,
        "samples": sorted(obs["sample"].astype(str).unique().tolist()) if "sample" in obs.columns else [],
        "cell_lines": sorted(obs["cell_line"].astype(str).unique().tolist()) if "cell_line" in obs.columns else [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=GCS_EXTRACTED_DIR)
    parser.add_argument("--prefix", default=PREFIX)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    ensure_project_cache()
    ln = connect_pertdata()
    print(
        f"LAMIN instance={ln.setup.settings.instance.slug} branch={ln.setup.settings.branch.name}",
        flush=True,
    )

    if triplet_complete(ln, args.prefix) and not args.overwrite:
        result = verify(ln, args.prefix)
        print(f"SKIP existing triplet: {args.prefix}")
        print(json.dumps(result, indent=2))
        return 0

    files = sorted(args.input_dir.glob("*_chimericReadPairs.csv.gz"))
    if not files:
        raise FileNotFoundError(f"No chimericReadPairs csv.gz under {args.input_dir}")

    obs = pd.concat([load_one(path) for path in files], ignore_index=True)
    print(f"AGGREGATED rows={len(obs)} total_reads={int(obs['read_count'].sum())}", flush=True)
    adata = build_anndata(obs)
    print(f"ADATA shape={adata.shape}", flush=True)

    from tools.convert_triplet_artifacts import migrate_h5ad_to_triplet

    migrate_h5ad_to_triplet(
        adata,
        ln,
        dataset_prefix=args.prefix,
        replace_on_instance=args.overwrite,
    )
    result = verify(ln, args.prefix)
    if not result["links_ok"]:
        raise RuntimeError(f"Triplet links failed verification: {result}")

    result.update(
        {
            "status": "ingested",
            "input_dir": str(args.input_dir),
            "source_gcs_prefix": "gs://scperturb/pert-gym/staging/data/main/properseq/extracted/",
            "source_files": [path.name for path in files],
        }
    )
    save_progress(result)
    print(json.dumps(result, indent=2), flush=True)
    clean_cache(ROOT / ".lamin-cache" / "lamindb")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
