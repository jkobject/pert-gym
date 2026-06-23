#!/usr/bin/env python3
"""Ingestion functions for Phase 3 scRNA-seq perturbation datasets.

Datasets covered:
  1. PRISM Perturb-seq Collection (~36 h5ads, Google Drive)
  2. T-cell Genome-Wide Perturb-seq (GSE314342, AWS S3)
  3. VIPerturbSeq (Zenodo 18460279)
  4. PRoPER-seq / ProPer-seq 2026 probe-based Perturb-seq (source TBD)
  5. Sanger Dual-guide KO in CRC (Figshare 25533091)
  6. Arc VCC Perturbations (placeholder)

Usage::

    from tools.lamin_context import connect_pertdata
    from tools.ingest_phase3_scrna import ingest_prism_collection, download_prism_collection

    ln = connect_pertdata()
    ln.track()

    files = download_prism_collection()
    ingest_prism_collection(files)
"""

from __future__ import annotations

import math
import subprocess
import zipfile
from pathlib import Path

import anndata as ad
import httpx
import pandas as pd

from tools.lamin_context import connect_pertdata

ln = connect_pertdata()


def _migrate_h5ad_to_triplet(*args, **kwargs):
    """Lazy import after explicit pertdata connection to avoid Lamin registry reset."""
    from tools.convert_triplet_artifacts import migrate_h5ad_to_triplet

    return migrate_h5ad_to_triplet(*args, **kwargs)

# ---------------------------------------------------------------------------
# § 1 — PRISM Perturb-seq Collection
# ---------------------------------------------------------------------------

PRISM_GDRIVE_FOLDER_ID = "1Y0Z19JhiTmTch65kvBNNMdVtosH6QHfi"
PRISM_LAMIN_PREFIX = "prism_collection"

PRISM_DATASETS = [
    "GSE217812",
    "GSE90063_mouse",
    "GSE250378-016",
    "GSE90063_human",
    "GSE261283",
    "GSE264667",
    "GSE269596",
    "GSE270828",
    "GSE241683_cropseq",
    "GSE235325",
    "GSE225775",
    "GSE221321",
    "GSE210681",
    "GSE182308",
    "GSE165291",
    "GSE161824",
    "GSE150062",
    "GSE146194",
    "GSE278572",
    "GSE274751",
    "GSE261025",
    "GSE247599",
    "GSE225807",
    "GSE236304",
    "GSE281048",
    "GSE208240",
    "GSE205310",
    "GSE197452",
    "GSE243244",
    "GSE213511",
    "GSE212396",
    "GSE255832",
    "GSE272093",
    "GSE236057",
    "GSE263747",
    "GSE164996",
    "GSE190604",
]

_OBS_RENAMES_PRISM = {
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


def download_prism_collection(
    output_dir: Path = Path("data/main/prism_collection"),
    folder_id: str = PRISM_GDRIVE_FOLDER_ID,
) -> dict[str, Path]:
    """Download all PRISM h5ad files from Google Drive.

    Uses gdown to list and download files from the public folder.
    Skips files that already exist locally.

    Parameters
    ----------
    output_dir:
        Local directory to save downloaded files.
    folder_id:
        Google Drive folder ID.

    Returns
    -------
    dict[str, Path]
        Mapping from dataset name (without .h5ad) to local path.
    """
    import gdown  # pip install gdown

    output_dir.mkdir(parents=True, exist_ok=True)

    folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
    file_list = gdown.download_folder(
        url=folder_url,
        output=str(output_dir),
        quiet=False,
        use_cookies=False,
        skip_download=True,
    )

    downloaded: dict[str, Path] = {}
    for file_info in file_list:
        name = Path(file_info["title"]).stem
        target = output_dir / file_info["title"]
        if target.exists() and target.stat().st_size > 0:
            print(f"Already downloaded: {target.name}")
        else:
            gdown.download(id=file_info["id"], output=str(target), quiet=False)
        downloaded[name] = target

    return downloaded


def standardize_prism_obs(adata: ad.AnnData, dataset_name: str) -> ad.AnnData:
    """Rename & fill obs columns to match the pert-gym triplet schema."""
    obs = adata.obs.copy()

    obs = obs.rename(columns={k: v for k, v in _OBS_RENAMES_PRISM.items() if k in obs.columns})
    obs = obs.loc[:, ~obs.columns.duplicated(keep="first")]

    if "is_control" not in obs.columns and "condition" in obs.columns:
        obs["is_control"] = obs["condition"].astype(str).str.lower().eq("control")
    if "is_control" not in obs.columns and "perturbation" in obs.columns:
        ctrl_patterns = ["non-targeting", "nt", "ctrl", "control", "scramble", "safe_harbor"]
        obs["is_control"] = obs["perturbation"].str.lower().str.contains(
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
        obs["dataset"] = dataset_name
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


def ingest_prism_collection(
    files: dict[str, Path],
    *,
    dataset_prefix: str = PRISM_LAMIN_PREFIX,
    overwrite: bool = False,
) -> list[str]:
    """Standardise obs and ingest each PRISM h5ad as a triplet artifact."""
    created_keys: list[str] = []
    for dataset_name, h5ad_path in files.items():
        prefix = f"{dataset_prefix}/{dataset_name}"
        obs_key = f"{prefix}/obs.parquet"
        triplet_keys = {
            obs_key,
            f"{prefix}/X.h5ad",
            f"{prefix}/var.parquet",
        }

        if not overwrite and all(ln.Artifact.filter(key=key).exists() for key in triplet_keys):
            print(f"Already ingested: {obs_key}")
            continue

        print(f"Ingesting {dataset_name} …")
        adata = ad.read_h5ad(h5ad_path)
        adata = standardize_prism_obs(adata, dataset_name)

        _migrate_h5ad_to_triplet(
            adata,
            ln,
            dataset_prefix=prefix,
            replace_on_instance=overwrite,
            storage=True,
        )
        created_keys.append(obs_key)
        print(f"  → saved: {obs_key}")

    return created_keys


# ---------------------------------------------------------------------------
# § 2 — T-cell Genome-Wide Perturb-seq (GSE314342)
# ---------------------------------------------------------------------------

TCELL_S3_PREFIX = "s3://genome-scale-tcell-perturb-seq/marson2025_data/"
TCELL_LAMIN_PREFIX = "tcell_gwps"


def download_tcell_gwps(
    output_dir: Path = Path("data/main/tcell_gwps"),
    s3_prefix: str = TCELL_S3_PREFIX,
    only_h5ad: bool = True,
) -> list[Path]:
    """Sync T-cell GWPS data from public AWS S3 bucket."""
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "aws", "s3", "sync",
        "--no-sign-request",
        s3_prefix,
        str(output_dir),
    ]
    if only_h5ad:
        cmd += ["--include", "*.h5ad", "--exclude", "*pseudobulk*", "--exclude", "*DE_*"]

    subprocess.run(cmd, check=True)
    return sorted(output_dir.rglob("*.h5ad"))


def standardize_tcell_obs(adata: ad.AnnData) -> ad.AnnData:
    """Map T-cell GWPS obs columns to pert-gym schema."""
    obs = adata.obs.copy()

    renames = {
        "perturbation_name": "perturbation",
        "stimulation_condition": "tissue_type",
        "donor_id": "patient_id",
        "percent_mito": "percent_mito",
    }
    obs = obs.rename(columns={k: v for k, v in renames.items() if k in obs.columns})

    obs["perturbation_type"] = "CRISPRi"
    obs["organism"] = "human"
    obs["cell_line"] = "primary_CD4_T"
    obs["cancer"] = False
    obs["disease"] = "healthy"

    if "n_counts" in obs.columns:
        obs["ncounts"] = obs["n_counts"]
    if "n_genes" in obs.columns:
        obs["ngenes"] = obs["n_genes"]

    adata.obs = obs
    return adata


def ingest_tcell_gwps(
    h5ad_files: list[Path],
    *,
    dataset_prefix: str = TCELL_LAMIN_PREFIX,
    chunk_size: int = 500_000,
    overwrite: bool = False,
) -> list[str]:
    """Ingest T-cell GWPS h5ad files as triplet artifacts (chunked for large files)."""
    created: list[str] = []

    for h5ad_path in h5ad_files:
        dataset_name = h5ad_path.stem

        backed = ad.read_h5ad(h5ad_path, backed="r")
        n_obs = backed.n_obs
        try:
            obs_df = backed.obs.copy()
            var_df = backed.var.copy()
        finally:
            if backed.file:
                backed.file.close()

        n_chunks = max(1, math.ceil(n_obs / chunk_size))
        print(f"{dataset_name}: {n_obs:,} cells → {n_chunks} chunk(s)")

        backed = ad.read_h5ad(h5ad_path, backed="r")
        try:
            for i in range(n_chunks):
                start, end = i * chunk_size, min((i + 1) * chunk_size, n_obs)
                chunk_name = dataset_name if n_chunks == 1 else f"{dataset_name}_chunk{i:04d}"
                prefix = f"{dataset_prefix}/{chunk_name}"
                obs_key = f"{prefix}/obs.parquet"

                if not overwrite and ln.Artifact.filter(key=obs_key).exists():
                    print(f"  chunk {i}: already ingested")
                    continue

                chunk = backed[start:end, :].to_memory()
                chunk.obs = obs_df.iloc[start:end].copy()
                chunk.var = var_df
                chunk = standardize_tcell_obs(chunk)

                _migrate_h5ad_to_triplet(
                    chunk, ln,
                    dataset_prefix=prefix,
                    replace_on_instance=overwrite,
                )
                created.append(obs_key)
                print(f"  → saved chunk {i}: {obs_key}")
        finally:
            if backed.file:
                backed.file.close()

    return created


# ---------------------------------------------------------------------------
# § 3 — VIPerturbSeq (Zenodo 18460279)
# ---------------------------------------------------------------------------

VIPERTURB_ZENODO_ID = 18460279
VIPERTURB_LAMIN_PREFIX = "viperturb"
ZENODO_API_BASE = "https://zenodo.org/api/records"


def get_zenodo_files(record_id: int) -> list[dict]:
    """Fetch file metadata from a Zenodo record."""
    url = f"{ZENODO_API_BASE}/{record_id}"
    resp = httpx.get(url, timeout=30.0)
    resp.raise_for_status()
    return resp.json()["files"]


def download_viperturb(
    output_dir: Path = Path("data/main/viperturb"),
    record_id: int = VIPERTURB_ZENODO_ID,
) -> dict[str, Path]:
    """Download VIPerturbSeq h5ad files from Zenodo."""
    output_dir.mkdir(parents=True, exist_ok=True)
    files = get_zenodo_files(record_id)

    downloaded: dict[str, Path] = {}
    with httpx.Client(timeout=300.0, follow_redirects=True) as client:
        for f in files:
            if not f["key"].endswith(".h5ad"):
                continue
            target = output_dir / f["key"]
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() and target.stat().st_size > 0:
                print(f"Already downloaded: {target.name}")
                downloaded[target.stem] = target
                continue
            print(f"Downloading {f['key']} ({f['size'] / 1e9:.1f} GB) …")
            with client.stream("GET", f["links"]["self"]) as stream:
                stream.raise_for_status()
                with target.open("wb") as fh:
                    for chunk in stream.iter_bytes(8 * 1024 * 1024):
                        fh.write(chunk)
            downloaded[target.stem] = target

    return downloaded


def standardize_viperturb_obs(adata: ad.AnnData) -> ad.AnnData:
    """Map VIPerturbSeq obs columns to pert-gym schema."""
    obs = adata.obs.copy()

    renames = {
        "gene_target": "perturbation",
        "guide_target": "perturbation",
        "target_gene": "perturbation",
        "Gene_assignment": "perturbation",
        "gene": "perturbation",
        "Guide_assignment": "guide_id",
        "guide_ID": "guide_id",
        "guide": "guide_id",
        "hash.ID": "cell_line",
    }
    obs = obs.rename(columns={k: v for k, v in renames.items() if k in obs.columns})

    if "perturbation" not in obs.columns and "guide_id" in obs.columns:
        obs["perturbation"] = obs["guide_id"].astype(str).str.split("_").str[0]

    if "perturbation" in obs.columns:
        perturbation = obs["perturbation"].astype(str)
        ctrl_patterns = ["non-targeting", "no.target", "no_target", "nt", "safe_harbor", "control"]
        obs["is_control"] = perturbation.str.lower().str.contains(
            "|".join(ctrl_patterns), na=False
        )
    elif "guide_id" in obs.columns:
        guide_id = obs["guide_id"].astype(str)
        ctrl_patterns = ["non-targeting", "no.target", "no_target", "nt", "safe_harbor", "control"]
        obs["is_control"] = guide_id.str.lower().str.contains(
            "|".join(ctrl_patterns), na=False
        )

    if "nCount_RNA" in obs.columns and "n_counts" not in obs.columns:
        obs["n_counts"] = obs["nCount_RNA"]
    if "nFeature_RNA" in obs.columns and "n_genes" not in obs.columns:
        obs["n_genes"] = obs["nFeature_RNA"]

    for col, default in [
        ("perturbation_type", "CRISPRi"),
        ("organism", "human"),
        ("cell_line", "unknown"),
        ("modality", "scRNA-seq"),
        ("assay", "VIPerturb-seq"),
        ("cancer", True),
        ("disease", "unknown"),
    ]:
        if col not in obs.columns:
            obs[col] = default

    if "n_counts" in obs.columns:
        obs["ncounts"] = obs["n_counts"]
    if "n_genes" in obs.columns:
        obs["ngenes"] = obs["n_genes"]

    adata.obs = obs
    return adata


def ingest_viperturb(
    files: dict[str, Path],
    *,
    dataset_prefix: str = VIPERTURB_LAMIN_PREFIX,
    overwrite: bool = False,
) -> list[str]:
    """Standardise obs and ingest VIPerturbSeq h5ads as triplet artifacts."""
    created: list[str] = []
    for name, path in files.items():
        prefix = f"{dataset_prefix}/{name}"
        obs_key = f"{prefix}/obs.parquet"
        if not overwrite and ln.Artifact.filter(key=obs_key).exists():
            print(f"Already ingested: {obs_key}")
            continue
        adata = ad.read_h5ad(path)
        adata = standardize_viperturb_obs(adata)
        _migrate_h5ad_to_triplet(adata, ln, dataset_prefix=prefix, replace_on_instance=overwrite)
        created.append(obs_key)
        print(f"→ saved: {obs_key}")
    return created


# ---------------------------------------------------------------------------
# § 4 — PRoPER-seq / ProPer-seq 2026 probe-based Perturb-seq
# ---------------------------------------------------------------------------

PROPERSEQ_GEO_ACC: str | None = None
PROPERSEQ_GEO_FTP: str | None = None
PROPERSEQ_LAMIN_PREFIX = "properseq"


def check_properseq_in_lamin() -> bool:
    """Return True if a sourced 2026 PRoPER-seq matrix is already ingested.

    The old GSE150818 chimeric-read-pair dataset was a mistaken substitute and
    is intentionally excluded from pert-gym. Do not download or ingest it here.
    """
    scperturb = ln.Artifact.filter(key__icontains="proper").all()
    if scperturb:
        print("Possible scPerturb match:")
        for a in scperturb:
            print(f"  {a.key}")
    return False


def download_properseq(
    output_dir: Path = Path("data/main/properseq"),
) -> list[Path]:
    """Block until the actual 2026 PRoPER-seq expression source is known."""
    del output_dir
    raise RuntimeError(
        "PRoPER-seq / ProPer-seq 2026 source is TBD. The legacy GSE150818 "
        "supplementary chimeric-read-pair tables are excluded and must not be "
        "used as a substitute."
    )


def load_properseq(files: list[Path]) -> ad.AnnData:
    """Load PROPER-seq GEO files into AnnData (h5ad or MTX layout)."""
    import scanpy as sc

    h5ads = [f for f in files if f.suffix == ".h5ad"]
    if h5ads:
        return ad.read_h5ad(h5ads[0])

    parent = files[0].parent
    adata = sc.read_10x_mtx(parent, var_names="gene_symbols", cache=True)

    meta_files = list(parent.glob("*cell_metadata*")) + list(parent.glob("*obs*"))
    if meta_files:
        meta = pd.read_csv(meta_files[0], sep="\t", index_col=0)
        adata.obs = meta.reindex(adata.obs_names)

    return adata


def standardize_properseq_obs(adata: ad.AnnData) -> ad.AnnData:
    """Map PROPER-seq obs columns to pert-gym schema."""
    obs = adata.obs.copy()

    renames = {"guide_gene": "perturbation", "guide_identity": "guide_id"}
    obs = obs.rename(columns={k: v for k, v in renames.items() if k in obs.columns})

    for col, default in [
        ("perturbation_type", "CRISPRko"),
        ("cell_line", "K562"),
        ("organism", "human"),
        ("cancer", True),
        ("disease", "chronic myelogenous leukemia"),
        ("tissue_type", "bone marrow"),
    ]:
        if col not in obs.columns:
            obs[col] = default

    if "n_counts" in obs.columns:
        obs["ncounts"] = obs["n_counts"]
    if "n_genes" in obs.columns:
        obs["ngenes"] = obs["n_genes"]

    adata.obs = obs
    return adata


def ingest_properseq(
    output_dir: Path = Path("data/main/properseq"),
    overwrite: bool = False,
) -> str:
    """Block until the actual 2026 PRoPER-seq expression source is known."""
    del output_dir, overwrite
    raise RuntimeError(
        "PRoPER-seq / ProPer-seq 2026 source is TBD. Locate the real scRNA "
        "expression matrix before ingestion; do not substitute the excluded "
        "legacy GSE150818 dataset."
    )


# ---------------------------------------------------------------------------
# § 5 — Sanger Dual-guide KO in CRC (Figshare 25533091)
# ---------------------------------------------------------------------------

SANGER_DUALGUIDE_FILE_ID = 45433417
SANGER_DUALGUIDE_URL = f"https://ndownloader.figshare.com/files/{SANGER_DUALGUIDE_FILE_ID}"
SANGER_DUALGUIDE_LAMIN_PREFIX = "sanger_dual_guide_crc"


def download_sanger_dualguide(
    output_dir: Path = Path("data/main/sanger_dualguide_crc"),
) -> Path:
    """Download and extract MAPPING.zip from Figshare.

    Returns
    -------
    Path
        Path to extracted directory.
    """
    import scipy.sparse as sp

    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / "MAPPING.zip"

    if not zip_path.exists():
        print("Downloading MAPPING.zip from Figshare …")
        with httpx.Client(timeout=300.0, follow_redirects=True) as client:
            with client.stream("GET", SANGER_DUALGUIDE_URL) as stream:
                stream.raise_for_status()
                with zip_path.open("wb") as fh:
                    for chunk in stream.iter_bytes(8 * 1024 * 1024):
                        fh.write(chunk)

    extract_dir = output_dir / "extracted"
    if not extract_dir.exists():
        print("Extracting MAPPING.zip …")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)

    return extract_dir


def load_sanger_dualguide(extract_dir: Path) -> dict[str, ad.AnnData]:
    """Parse the extracted MAPPING directory into AnnData objects.

    Handles two layouts:
    a) pre-processed .h5ad files per cell line
    b) CSV/TSV matrices of genetic interaction scores per cell line
    """
    import scipy.sparse as sp

    adatas: dict[str, ad.AnnData] = {}

    for h5ad in sorted(extract_dir.rglob("*.h5ad")):
        cell_line = h5ad.stem
        adatas[cell_line] = ad.read_h5ad(h5ad)

    if adatas:
        return adatas

    for csv_file in sorted(extract_dir.rglob("*interaction_score*.csv")):
        cell_line = csv_file.stem.split("_interaction")[0]
        df = pd.read_csv(csv_file, index_col=0)

        adata = ad.AnnData(
            X=sp.csr_matrix(df.values),
            obs=pd.DataFrame({"guide_pair": df.index}, index=df.index),
            var=pd.DataFrame(index=df.columns),
        )
        guide_info = adata.obs["guide_pair"].str.extract(r"(?P<guide_a>[^+]+)\+(?P<guide_b>.+)")
        adata.obs["guide_a"] = guide_info["guide_a"]
        adata.obs["guide_b"] = guide_info["guide_b"]
        adatas[cell_line] = adata

    return adatas


def standardize_sanger_dualguide_obs(adata: ad.AnnData, cell_line: str) -> ad.AnnData:
    """Map Sanger dual-guide obs columns to pert-gym schema."""
    obs = adata.obs.copy()

    if "guide_pair" not in obs.columns and "guide_a" in obs.columns:
        obs["guide_pair"] = obs["guide_a"] + "+" + obs["guide_b"]
    if "perturbation" not in obs.columns:
        obs["perturbation"] = obs.get("guide_pair", "unknown")

    obs["perturbation_type"] = "CRISPRko_dual_guide"
    obs["cell_line"] = cell_line
    obs["organism"] = "human"
    obs["cancer"] = True
    obs["disease"] = "colorectal cancer"
    obs["tissue_type"] = "colon"

    if "n_counts" in obs.columns:
        obs["ncounts"] = obs["n_counts"]
    if "n_genes" in obs.columns:
        obs["ngenes"] = obs["n_genes"]

    adata.obs = obs
    return adata


def ingest_sanger_dualguide(overwrite: bool = False) -> list[str]:
    """Download → parse → standardise → ingest Sanger dual-guide CRC datasets."""
    extract_dir = download_sanger_dualguide()
    adatas = load_sanger_dualguide(extract_dir)

    created: list[str] = []
    for cell_line, adata in adatas.items():
        prefix = f"{SANGER_DUALGUIDE_LAMIN_PREFIX}/{cell_line}"
        obs_key = f"{prefix}/obs.parquet"
        if not overwrite and ln.Artifact.filter(key=obs_key).exists():
            print(f"Already ingested: {obs_key}")
            continue
        adata = standardize_sanger_dualguide_obs(adata, cell_line)
        _migrate_h5ad_to_triplet(adata, ln, dataset_prefix=prefix, replace_on_instance=overwrite)
        created.append(obs_key)
        print(f"→ saved: {obs_key}")

    return created


# ---------------------------------------------------------------------------
# § 6 — Arc VCC Perturbations (placeholder)
# ---------------------------------------------------------------------------

ARC_VCC_URL = "https://virtualcellchallenge.org/datasets"
ARC_VCC_LAMIN_PREFIX = "arc_vcc"


def ingest_arc_vcc(overwrite: bool = False) -> list[str]:  # noqa: ARG001
    raise NotImplementedError(
        f"Check {ARC_VCC_URL} for current download method and file format."
    )
