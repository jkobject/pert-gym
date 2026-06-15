#!/usr/bin/env python3
"""One-off memory-bounded ingestion for PRISM GSE153056 from GCS staging."""
from __future__ import annotations

from pathlib import Path
import sys

import anndata as ad

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

DATASET = "GSE153056"
PREFIX = f"prism_collection/{DATASET}"
PATH = Path("/mnt/gcs/scperturb/pert-gym/staging/data/main/prism_collection/GSE153056.h5ad")
RENAMES = {
    "gene": "perturbation",
    "gene_name": "perturbation",
    "guide_target": "perturbation",
    "pert_gene": "perturbation",
    "pert_name": "perturbation",
    "perturbation_name": "perturbation",
    "pert_type": "perturbation_type",
    "crispr_type": "perturbation_type",
    "cell_line_name": "cell_line",
    "cell_type": "tissue_type",
    "cancer_type": "disease",
}


def standardize(adata: ad.AnnData) -> ad.AnnData:
    obs = adata.obs.copy()
    obs = obs.rename(columns={k: v for k, v in RENAMES.items() if k in obs.columns})
    obs = obs.loc[:, ~obs.columns.duplicated(keep="first")]
    if "is_control" not in obs.columns and "condition" in obs.columns:
        obs["is_control"] = obs["condition"].astype(str).str.lower().eq("control")
    if "is_control" not in obs.columns and "perturbation" in obs.columns:
        ctrl_patterns = ["non-targeting", "nt", "ctrl", "control", "scramble", "safe_harbor"]
        obs["is_control"] = obs["perturbation"].astype(str).str.lower().str.contains(
            "|".join(ctrl_patterns), na=False
        )
    for col, default in [
        ("perturbation_type", "CRISPRi"),
        ("organism", "human"),
        ("cancer", True),
        ("disease", "unknown"),
        ("tissue_type", "unknown"),
    ]:
        if col not in obs.columns:
            obs[col] = default
    obs["organism"] = obs["organism"].astype(str).replace(
        {
            "human adipocytes": "human",
            "Human": "human",
            "Homo sapiens": "human",
            "Mice (Mus musculus)": "mouse",
            "Mus musculus": "mouse",
        }
    )
    if "cell_line" not in obs.columns:
        obs["cell_line"] = "unknown"
    if "dataset" not in obs.columns:
        obs["dataset"] = DATASET
    if "modality" not in obs.columns:
        obs["modality"] = "scRNA-seq"
    if "assay" not in obs.columns:
        obs["assay"] = "Perturb-seq"
    if "n_counts" in obs.columns and "ncounts" not in obs.columns:
        obs["ncounts"] = obs["n_counts"]
    if "n_genes" in obs.columns and "ngenes" not in obs.columns:
        obs["ngenes"] = obs["n_genes"]
    adata.obs = obs
    return adata


def main() -> int:
    ensure_project_cache()
    ln = connect_pertdata()
    print("LAMIN", ln.setup.settings.instance.slug, ln.setup.settings.branch.name, ln.setup.settings.branch.uid, flush=True)
    if all(ln.Artifact.filter(key=f"{PREFIX}/{s}").exists() for s in ["obs.parquet", "X.h5ad", "var.parquet"]):
        print("SKIP existing triplet", PREFIX, flush=True)
        return 0
    print("READ", PATH, PATH.stat().st_size, flush=True)
    adata = ad.read_h5ad(PATH)
    print("LOADED", adata.n_obs, adata.n_vars, flush=True)
    adata = standardize(adata)
    adata.var_names_make_unique()
    adata.obs_names_make_unique()
    print(
        "STANDARDIZED",
        "obs_dups", len(adata.obs.columns[adata.obs.columns.duplicated()]),
        "var_dups", len(adata.var.columns[adata.var.columns.duplicated()]),
        "controls", int(adata.obs["is_control"].sum()),
        flush=True,
    )
    from tools.convert_triplet_artifacts import migrate_h5ad_to_triplet

    migrate_h5ad_to_triplet(adata, ln, dataset_prefix=PREFIX, replace_on_instance=False, storage=True)
    print("MIGRATED", PREFIX, flush=True)
    obs = ln.Artifact.get(key=f"{PREFIX}/obs.parquet")
    x = obs.features.get_values()["X"]
    if isinstance(x, str):
        x = ln.Artifact.get(key=x)
    var = x.features.get_values()["var"]
    if isinstance(var, str):
        var = ln.Artifact.get(key=var)
    assert x.key == f"{PREFIX}/X.h5ad", x.key
    assert var.key == f"{PREFIX}/var.parquet", var.key
    obs_df = obs.load()
    var_df = var.load()
    print("VERIFY", obs_df.shape, x.n_observations, var_df.shape, "links_ok", True, flush=True)
    clean_cache(ROOT / ".lamin-cache" / "lamindb")
    print("DONE", PREFIX, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
