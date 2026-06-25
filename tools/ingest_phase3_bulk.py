#!/usr/bin/env python3
# ruff: noqa: E402,I001
"""Ingestion functions for Phase 3 bulk/screen sensitivity datasets.

Datasets covered:
  8. Broad PRISM Repurposing (drug sensitivity, DepMap)
  9. Sanger GDSC1 + GDSC2 (drug sensitivity, CancerRxGene)
  10. Sanger SCORE CRISPR KO (gene effect, Cell Model Passports)
  11. DepMap CCLE (bulk RNA expression + proteomics)

Data model for drug sensitivity screens (PRISM and GDSC)
---------------------------------------------------------
Both Broad PRISM and Sanger GDSC are represented as AnnData with a hybrid obs:

  obs rows  = (cell_line × drug) drug-treated rows  [is_control=False]
            + (cell_line) control / baseline rows   [is_control=True]
  var       = genes from CCLE (PRISM) or CMP (GDSC) expression
  X         = sparse: ONLY control rows filled with gene expression
              (log2 TPM+1 for PRISM; normalized log-counts for GDSC)
              drug-treated rows have empty X

Sensitivity values sit in obs:
  - PRISM: obs["lfc"]   — log fold change vs DMSO
  - GDSC:  obs["ic50"]  — LN(IC50); obs["auc"] — area under curve

This allows the same triplet-format loading pattern used for scRNA-seq
datasets, while faithfully representing that drug-treated "cells" have no
transcriptomic readout — only the matched baseline expression is available.

Usage::

    from tools.lamin_context import connect_pertdata
    from tools.ingest_phase3_bulk import ingest_broad_prism, ingest_gdsc

    ln = connect_pertdata()
    ln.track()

    ingest_broad_prism()
    ingest_gdsc()
"""

from __future__ import annotations

import re
import sys as _sys
from io import StringIO
from pathlib import Path
from urllib.parse import urlparse

import anndata as ad
import httpx
import numpy as np
import pandas as pd
import scipy.sparse as sp

_sys.path.insert(0, str(Path(__file__).parent.parent))
from tools.convert_triplet_artifacts import migrate_h5ad_to_triplet  # noqa: E402
from tools.lamin_context import connect_pertdata  # noqa: E402


ln = connect_pertdata()

DEPMAP_DOWNLOADS_API = "https://depmap.org/portal/api/download/files"
CMP_DOWNLOADS_API = (
    "https://api.cellmodelpassports.sanger.ac.uk/download_files?include=groups.files"
)


def _filename_from_url(url: str) -> str:
    return Path(urlparse(url).path).name


def depmap_download_url(
    release: str,
    filename: str,
    *,
    client: httpx.Client | None = None,
) -> str:
    """Resolve a current DepMap download URL by release and filename.

    DepMap now serves many releases through an API that may return signed URLs,
    so callers should resolve immediately before downloading.
    """
    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=60.0, follow_redirects=True)
    try:
        resp = client.get(DEPMAP_DOWNLOADS_API)
        resp.raise_for_status()
        files = pd.read_csv(StringIO(resp.text)).to_dict("records")
    finally:
        if owns_client:
            client.close()

    matches = [
        row for row in files
        if row.get("release") == release and row.get("filename") == filename
    ]
    if not matches:
        raise ValueError(f"Could not find DepMap file {filename!r} in release {release!r}")
    return matches[0]["url"]

# ---------------------------------------------------------------------------
# § 8 — Broad PRISM Repurposing
# ---------------------------------------------------------------------------

BROAD_PRISM_RELEASE = "PRISM Primary Repurposing DepMap Public 24Q2"
BROAD_PRISM_LFC_FILENAME = "Repurposing_Public_24Q2_LFC.csv"
BROAD_PRISM_META_FILENAME = "Repurposing_Public_24Q2_Treatment_Meta_Data.csv"
BROAD_PRISM_LAMIN_PREFIX = "broad_prism_repurposing"


def download_broad_prism(
    output_dir: Path = Path("data/main/broad_prism"),
) -> dict[str, Path]:
    """Download PRISM repurposing LFC matrix and compound metadata from DepMap.

    Returns
    -------
    dict[str, Path]
        Keys: ``'matrix'`` (LFC CSV), ``'meta'`` (compound metadata CSV).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    files: dict[str, Path] = {}

    with httpx.Client(timeout=300.0, follow_redirects=True) as client:
        downloads = [
            (
                "matrix",
                BROAD_PRISM_LFC_FILENAME,
                depmap_download_url(BROAD_PRISM_RELEASE, BROAD_PRISM_LFC_FILENAME, client=client),
            ),
            (
                "meta",
                BROAD_PRISM_META_FILENAME,
                depmap_download_url(BROAD_PRISM_RELEASE, BROAD_PRISM_META_FILENAME, client=client),
            ),
        ]
        for key, filename, url in downloads:
            target = output_dir / filename
            if target.exists():
                files[key] = target
                continue
            print(f"Downloading {filename} …")
            resp = client.get(url)
            resp.raise_for_status()
            target.write_bytes(resp.content)
            files[key] = target

    return files


def broad_prism_to_anndata(
    files: dict[str, Path],
    ccle_expr_file: Path | None = None,
) -> ad.AnnData:
    """Convert Broad PRISM repurposing data to AnnData using the hybrid obs model.

    Parameters
    ----------
    files:
        Output of :func:`download_broad_prism` (keys: ``'matrix'``, ``'meta'``).
    ccle_expr_file:
        Path to DepMap CCLE expression CSV
        (``OmicsExpressionProteinCodingGenesTPMLogp1.csv``).
        When provided, control rows in ``X`` are filled with log2(TPM+1);
        otherwise ``X`` is empty (0 columns).

    Returns
    -------
    ad.AnnData
        ``obs`` rows = drug-treated (cell_line × compound) + baseline controls.
        ``var`` = genes (from CCLE, or empty if not provided).
        ``X`` = sparse; only control rows carry expression values.
    """
    # Load LFC matrix: rows = depmap_id, cols = broad_id
    lfc_df = pd.read_csv(files["matrix"], index_col=0)

    # Compound metadata
    meta = pd.read_csv(files["meta"])
    if "broad_id" in meta.columns:
        meta = meta.set_index("broad_id")
    if not meta.index.is_unique:
        meta = meta[~meta.index.duplicated(keep="first")]

    cell_lines = lfc_df.index.tolist()

    # ── Drug-treated obs rows ────────────────────────────────────────────────
    lfc_stacked = lfc_df.stack(dropna=False).reset_index()
    lfc_stacked.columns = ["depmap_id", "broad_id", "lfc"]
    lfc_stacked["lfc"] = pd.to_numeric(lfc_stacked["lfc"], errors="coerce")

    drug_obs = lfc_stacked.copy()
    drug_obs["is_control"] = False
    drug_obs["perturbation"] = drug_obs["broad_id"]
    if "name" in meta.columns:
        drug_obs["perturbation_name"] = drug_obs["broad_id"].map(meta["name"])
    drug_obs["perturbation_type"] = "drug"
    drug_obs["organism"] = "human"
    drug_obs.index = (
        drug_obs["depmap_id"].astype(str) + "__" + drug_obs["broad_id"].astype(str)
    )

    # ── Control / baseline obs rows (one per cell line) ──────────────────────
    ctrl_obs = pd.DataFrame(
        {
            "depmap_id": cell_lines,
            "broad_id": None,
            "lfc": np.nan,
            "is_control": True,
            "perturbation": "control",
            "perturbation_type": "drug",
            "organism": "human",
        },
        index=[f"{cl}__control" for cl in cell_lines],
    )

    all_obs = pd.concat([ctrl_obs, drug_obs])
    n_ctrl = len(ctrl_obs)

    # ── Expression matrix (var = genes) ─────────────────────────────────────
    if ccle_expr_file is not None and Path(ccle_expr_file).exists():
        df_expr = pd.read_csv(ccle_expr_file, index_col=0)
        gene_names = df_expr.columns.str.extract(r"^(.+?) \((.+?)\)$")
        var_df = pd.DataFrame(
            {"gene_symbol": gene_names[0].values, "ensembl_id": gene_names[1].values},
            index=df_expr.columns,
        )
        # Align CCLE rows to cell_lines order; missing lines → 0
        expr_aligned = (
            df_expr.reindex(cell_lines).fillna(0.0).values.astype(np.float32)
        )
        n_genes = len(var_df)
        X = sp.lil_matrix((len(all_obs), n_genes), dtype=np.float32)
        X[:n_ctrl, :] = expr_aligned
        X = X.tocsr()
    else:
        var_df = pd.DataFrame(index=pd.Index([], name="gene"))
        X = sp.csr_matrix((len(all_obs), 0), dtype=np.float32)

    return ad.AnnData(X=X, obs=all_obs, var=var_df)


def ingest_broad_prism(
    ccle_expr_file: Path | None = None,
    overwrite: bool = False,
) -> str:
    """Download → convert → ingest Broad PRISM repurposing dataset.

    Parameters
    ----------
    ccle_expr_file:
        Optional path to the CCLE expression CSV (from ``ingest_depmap_ccle``
        or downloaded separately).  If omitted, ``X`` will be empty.
    overwrite:
        If True, revise existing artifacts.
    """
    obs_key = f"{BROAD_PRISM_LAMIN_PREFIX}/obs.parquet"
    if not overwrite and ln.Artifact.filter(key=obs_key).exists():
        print(f"Already ingested: {obs_key}")
        return obs_key

    files = download_broad_prism()
    adata = broad_prism_to_anndata(files, ccle_expr_file=ccle_expr_file)
    migrate_h5ad_to_triplet(
        adata, ln,
        dataset_prefix=BROAD_PRISM_LAMIN_PREFIX,
        replace_on_instance=overwrite,
    )
    return obs_key


# ---------------------------------------------------------------------------
# § 9 — Sanger GDSC
# ---------------------------------------------------------------------------

GDSC_URLS = {
    "GDSC1": "https://cmp.cog.sanger.ac.uk/download/GDSC1_fitted_dose_response_27Oct23.xlsx",
    "GDSC2": "https://cmp.cog.sanger.ac.uk/download/GDSC2_fitted_dose_response_27Oct23.xlsx",
}
GDSC_LAMIN_PREFIX = "sanger_gdsc"

# Cell Model Passports bulk RNA-seq expression (used as baseline for GDSC)
CMP_RNASEQ_URL = (
    "https://cog.sanger.ac.uk/cmp/download/rnaseq_latest.csv.gz"
)


def download_gdsc(
    output_dir: Path = Path("data/main/gdsc"),
) -> dict[str, Path]:
    """Download GDSC1 and GDSC2 dose-response Excel files.

    Returns
    -------
    dict[str, Path]
        Keys: ``'GDSC1'``, ``'GDSC2'``.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    with httpx.Client(timeout=300.0, follow_redirects=True) as client:
        for name, url in GDSC_URLS.items():
            target = output_dir / Path(url).name
            if target.exists():
                paths[name] = target
                continue
            print(f"Downloading {name} …")
            resp = client.get(url)
            resp.raise_for_status()
            target.write_bytes(resp.content)
            paths[name] = target

    return paths


def download_sanger_expression(
    output_dir: Path = Path("data/main/sanger_cmp"),
    url: str = CMP_RNASEQ_URL,
) -> Path:
    """Download Cell Model Passports bulk RNA-seq expression matrix.

    The file is a compressed CSV with rows = cell lines (Sanger model IDs)
    and columns = gene symbols (log-normalised counts).

    Returns
    -------
    Path
        Local path to the downloaded (still compressed) CSV.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / Path(url).name
    if target.exists():
        print(f"Already downloaded: {target.name}")
        return target

    print(f"Downloading CMP expression from {url} …")
    with httpx.Client(timeout=600.0, follow_redirects=True) as client:
        with client.stream("GET", url) as stream:
            stream.raise_for_status()
            with target.open("wb") as fh:
                for chunk in stream.iter_bytes(8 * 1024 * 1024):
                    fh.write(chunk)

    return target


def gdsc_to_anndata(
    path: Path,
    study: str,
    expr_file: Path | None = None,
) -> ad.AnnData:
    """Convert GDSC fitted dose-response Excel to AnnData using the hybrid obs model.

    Parameters
    ----------
    path:
        Path to GDSC1 or GDSC2 Excel file.
    study:
        ``'GDSC1'`` or ``'GDSC2'``.
    expr_file:
        Optional path to CMP RNA-seq expression CSV (from
        :func:`download_sanger_expression`).  When provided, control rows in
        ``X`` are filled with CMP expression; otherwise ``X`` is empty.

    Returns
    -------
    ad.AnnData
        ``obs`` rows = (cell_line × drug) drug-treated + baseline controls.
        ``var`` = genes (from CMP expression, or empty).
        ``X`` = sparse; only control rows carry expression values.
    """
    df_raw = pd.read_excel(path)

    cell_lines = df_raw["SANGER_MODEL_ID"].unique().tolist()

    # ── Drug-treated obs rows ────────────────────────────────────────────────
    drug_cols = ["SANGER_MODEL_ID", "DRUG_NAME", "DRUG_ID", "LN_IC50", "AUC", "RMSE"]
    drug_obs = df_raw[drug_cols].copy()
    drug_obs.columns = ["sanger_model_id", "perturbation", "drug_id", "ic50", "auc", "rmse"]
    drug_obs["is_control"] = False
    drug_obs["perturbation_type"] = "drug"
    drug_obs["organism"] = "human"
    drug_obs["study"] = study
    drug_obs.index = (
        drug_obs["sanger_model_id"].astype(str)
        + "__"
        + drug_obs["perturbation"].astype(str)
    )

    # ── Control / baseline obs rows ──────────────────────────────────────────
    ctrl_obs = pd.DataFrame(
        {
            "sanger_model_id": cell_lines,
            "perturbation": "control",
            "drug_id": None,
            "ic50": np.nan,
            "auc": np.nan,
            "rmse": np.nan,
            "is_control": True,
            "perturbation_type": "drug",
            "organism": "human",
            "study": study,
        },
        index=[f"{m}__control" for m in cell_lines],
    )

    all_obs = pd.concat([ctrl_obs, drug_obs])
    n_ctrl = len(ctrl_obs)

    # ── Expression matrix (var = genes) ─────────────────────────────────────
    if expr_file is not None and Path(expr_file).exists():
        df_expr = pd.read_csv(expr_file, index_col=0, compression="infer")
        var_df = pd.DataFrame(
            {"gene_symbol": df_expr.columns}, index=df_expr.columns
        )
        expr_aligned = (
            df_expr.reindex(cell_lines).fillna(0.0).values.astype(np.float32)
        )
        n_genes = len(var_df)
        X = sp.lil_matrix((len(all_obs), n_genes), dtype=np.float32)
        X[:n_ctrl, :] = expr_aligned
        X = X.tocsr()
    else:
        var_df = pd.DataFrame(index=pd.Index([], name="gene"))
        X = sp.csr_matrix((len(all_obs), 0), dtype=np.float32)

    return ad.AnnData(X=X, obs=all_obs, var=var_df)


def ingest_gdsc(
    expr_file: Path | None = None,
    overwrite: bool = False,
) -> list[str]:
    """Download → convert → ingest GDSC1 and GDSC2.

    Parameters
    ----------
    expr_file:
        Optional path to CMP RNA-seq expression CSV.  Pass the output of
        :func:`download_sanger_expression` to populate ``X`` for control rows.
    overwrite:
        If True, revise existing artifacts.
    """
    paths = download_gdsc()
    created: list[str] = []
    for study, path in paths.items():
        prefix = f"{GDSC_LAMIN_PREFIX}/{study.lower()}"
        obs_key = f"{prefix}/obs.parquet"
        if not overwrite and ln.Artifact.filter(key=obs_key).exists():
            print(f"Already ingested: {obs_key}")
            continue
        adata = gdsc_to_anndata(path, study, expr_file=expr_file)
        migrate_h5ad_to_triplet(adata, ln, dataset_prefix=prefix, replace_on_instance=overwrite)
        created.append(obs_key)
        print(f"→ saved: {obs_key}")
    return created


# ---------------------------------------------------------------------------
# § 10 — Sanger SCORE CRISPR KO
# ---------------------------------------------------------------------------

SCORE_GENE_EFFECT_URL = (
    "https://cog.sanger.ac.uk/cmp/download/"
    "Project_Score2_fitness_scores_Sanger_v2_Broad_21Q2_20250624.zip"
)
SCORE_GENE_EFFECT_FILENAME = _filename_from_url(SCORE_GENE_EFFECT_URL)
SCORE_LAMIN_PREFIX = "sanger_score_crispr"


def download_sanger_score(
    output_dir: Path = Path("data/main/sanger_score"),
) -> Path:
    """Download Sanger SCORE CRISPR KO gene effect matrix from Cell Model Passports.

    Returns
    -------
    Path
        Local path to the downloaded CSV.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / SCORE_GENE_EFFECT_FILENAME
    if target.exists():
        return target

    with httpx.Client(timeout=300.0, follow_redirects=True) as client:
        print(f"Downloading SCORE gene effect from {SCORE_GENE_EFFECT_URL} …")
        resp = client.get(SCORE_GENE_EFFECT_URL)
        resp.raise_for_status()
        target.write_bytes(resp.content)

    return target


def sanger_score_to_anndata(path: Path) -> ad.AnnData:
    """Reject the legacy fake-expression SCORE conversion.

    Sanger SCORE stores essentiality/dependency scores, not RNA expression. The
    canonical expression ``X.h5ad`` was retyped by
    ``artifacts/scripts/repair_sanger_score_crispr_retype_20260625.py`` so
    scores live in the explicit auxiliary ``X_score``/``var_score`` payload path
    with ``x_semantics=essentiality_score``. Do not regenerate a non-empty
    canonical ``X`` for this screen.
    """
    raise RuntimeError(
        "Sanger SCORE CRISPR is not an expression matrix; use the repaired "
        "X_score/var_score auxiliary payload path instead of writing scores to X.h5ad."
    )


def ingest_sanger_score(overwrite: bool = False) -> str:
    """Return the repaired SCORE obs key and refuse fake-X overwrites."""
    obs_key = f"{SCORE_LAMIN_PREFIX}/obs.parquet"
    if not overwrite and ln.Artifact.filter(key=obs_key).exists():
        print(f"Already ingested: {obs_key}")
        return obs_key

    raise RuntimeError(
        "Sanger SCORE CRISPR must not be ingested as obs/X/var expression. "
        "Run the dated retype repair script if the Lamin auxiliary score payload "
        "needs review or regeneration."
    )


# ---------------------------------------------------------------------------
# § 11 — DepMap CCLE (bulk expression + proteomics)
# ---------------------------------------------------------------------------

DEPMAP_RELEASE = "DepMap Public 26Q1"
DEPMAP_EXPR_FILENAME = "OmicsExpressionTPMLogp1HumanProteinCodingGenes.csv"
DEPMAP_PROT_FILENAME = "OmicsProteinExpressionLog2.csv"
DEPMAP_LAMIN_PREFIX = "depmap_ccle/26q1"


def download_depmap_ccle(
    output_dir: Path = Path("data/main/depmap_ccle"),
    release: str = DEPMAP_RELEASE,
) -> dict[str, Path]:
    """Download DepMap CCLE expression (and proteomics) via the DepMap API.

    Returns
    -------
    dict[str, Path]
        Keys: ``'expr'`` (expression CSV), ``'prot'`` (proteomics CSV, optional).
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    wanted = {"expr": DEPMAP_EXPR_FILENAME, "prot": DEPMAP_PROT_FILENAME}
    downloaded: dict[str, Path] = {}

    with httpx.Client(timeout=600.0, follow_redirects=True) as client:
        for key, filename in wanted.items():
            try:
                url = depmap_download_url(release, filename, client=client)
            except ValueError as exc:
                print(exc)
                continue
            target = output_dir / filename
            if target.exists():
                downloaded[key] = target
                continue
            print(f"Downloading {filename} …")
            with client.stream("GET", url) as stream:
                stream.raise_for_status()
                with target.open("wb") as fh:
                    for chunk in stream.iter_bytes(8 * 1024 * 1024):
                        fh.write(chunk)
            downloaded[key] = target

    return downloaded


def depmap_ccle_to_anndata(files: dict[str, Path]) -> ad.AnnData:
    """Convert DepMap CCLE CSV to AnnData.

    Expression columns follow the pattern ``"SYMBOL (EntrezID)"``.

    Layout:
        obs  = cell lines (depmap_id)
        var  = genes (gene_symbol, entrez_id)
        X    = log2(TPM + 1) expression
        obsm['proteomics'] = protein expression aligned to obs (if available)
        uns['proteomics_columns'] = protein names
    """
    df_expr = pd.read_csv(files["expr"], index_col=0)

    gene_mask = df_expr.columns.to_series().str.match(r"^.+? \(.+?\)$", na=False)
    gene_cols = df_expr.columns[gene_mask.to_numpy()]
    meta_cols = df_expr.columns[~gene_mask.to_numpy()]

    gene_names = gene_cols.str.extract(r"^(.+?) \((.+?)\)$")
    var_df = pd.DataFrame(
        {"gene_symbol": gene_names[0].values, "entrez_id": gene_names[1].values},
        index=gene_cols,
    )
    obs_df = df_expr.loc[:, meta_cols].copy()
    if "ModelID" in obs_df.columns:
        obs_df["depmap_id"] = obs_df["ModelID"]
    else:
        obs_df["depmap_id"] = df_expr.index.astype(str)

    adata = ad.AnnData(
        X=sp.csr_matrix(df_expr.loc[:, gene_cols].values.astype(np.float32)),
        obs=obs_df,
        var=var_df,
    )

    if "prot" in files:
        df_prot = pd.read_csv(files["prot"], index_col=0)
        prot_aligned = df_prot.reindex(adata.obs_names).values.astype(np.float32)
        adata.obsm["proteomics"] = prot_aligned
        adata.uns["proteomics_columns"] = df_prot.columns.tolist()

    adata.obs["perturbation_type"] = "baseline"
    adata.obs["organism"] = "human"

    return adata


def ingest_depmap_ccle(overwrite: bool = False) -> str:
    """Download → convert → ingest DepMap CCLE expression + proteomics."""
    obs_key = f"{DEPMAP_LAMIN_PREFIX}/obs.parquet"
    if not overwrite and ln.Artifact.filter(key=obs_key).exists():
        print(f"Already ingested: {obs_key}")
        return obs_key

    files = download_depmap_ccle()
    adata = depmap_ccle_to_anndata(files)
    migrate_h5ad_to_triplet(
        adata, ln,
        dataset_prefix=DEPMAP_LAMIN_PREFIX,
        replace_on_instance=overwrite,
    )
    return obs_key
