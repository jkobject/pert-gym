#!/usr/bin/env python3
"""VM-only bounded ingestion for GSE334273 sea lamprey 10x MTX samples.

This script is intentionally designed for pert-gym-worker-eu, not the Mac mini:
- stages the GEO RAW tar + metadata on the VM and to GCS staging;
- lists tar members and performs smoke parsing before writes;
- extracts one sample at a time to local scratch, writes same-prefix Lamin triplets,
  then deletes the extracted members.
"""
from __future__ import annotations

import argparse
import gzip
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd
import scipy.io

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TASK_ID = "t_5d39229b"
GEO_BASE = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE334nnn/GSE334273/suppl"
RAW_URL = f"{GEO_BASE}/GSE334273_RAW.tar"
METADATA_URL = f"{GEO_BASE}/GSE334273_cell_metadata.tsv.gz"
FILELIST_URL = f"{GEO_BASE}/filelist.txt"
STAGING = "gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/geo/GSE334273"
PREFIX_BASE = "temporal_pretraining/geo/GSE334273_sea_lamprey_sexual_differentiation"
ARTIFACT_JSON = ROOT / "artifacts/schema_audit/gse334273_10x_vm_t_5d39229b_20260702.json"
ARTIFACT_MD = ROOT / "artifacts/schema_audit/gse334273_10x_vm_t_5d39229b_20260702.md"


@dataclass(frozen=True)
class Sample:
    gsm: str
    sample_code: str
    tar_prefix: str
    output_slug: str
    prefix: str


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: list[str], *, check: bool = True, text: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=text, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def curl_download(url: str, dest: Path, timeout: int = 3600) -> dict[str, Any]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return {"status": "exists", "path": str(dest), "bytes": dest.stat().st_size, "url": url}
    cmd = ["curl", "-L", "--fail", "--retry", "3", "--connect-timeout", "30", "--max-time", str(timeout), "-o", str(dest), url]
    out = run(cmd)
    return {"status": "downloaded", "path": str(dest), "bytes": dest.stat().st_size, "url": url, "log_tail": out.stdout[-1000:]}


def curl_head(url: str) -> dict[str, Any]:
    try:
        out = run(["curl", "-fsSIL", "--max-time", "120", url]).stdout
        headers: dict[str, str] = {}
        for line in out.splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.lower()] = v.strip()
        return {"status": "ok", "url": url, "content_length": headers.get("content-length"), "headers_tail": out[-2000:]}
    except subprocess.CalledProcessError as exc:
        return {"status": "failed", "url": url, "output": exc.stdout[-2000:] if exc.stdout else ""}


def gcs_stage(path: Path) -> dict[str, Any]:
    target = f"{STAGING}/{path.name}"
    cp = run(["gcloud", "storage", "cp", "--billing-project=jkobject-1549353370965", str(path), target], check=False)
    return {"source": str(path), "target": target, "exit_code": cp.returncode, "output_tail": cp.stdout[-2000:]}


def parse_filelist(filelist_text: str) -> list[Sample]:
    prefixes: dict[str, set[str]] = {}
    for line in filelist_text.splitlines():
        if not line.startswith("File\t"):
            continue
        name = line.split("\t")[1]
        if not name.endswith(("barcodes.tsv.gz", "features.tsv.gz", "matrix.mtx.gz")):
            continue
        stem = name.rsplit("_", 1)[0]
        suffix = name.rsplit("_", 1)[1]
        prefixes.setdefault(stem, set()).add(suffix)
    samples: list[Sample] = []
    for stem in sorted(prefixes):
        suffixes = prefixes[stem]
        required = {"barcodes.tsv.gz", "features.tsv.gz", "matrix.mtx.gz"}
        if suffixes != required:
            raise ValueError(f"Unexpected member set for {stem}: {suffixes}")
        parts = stem.split("_")
        gsm = parts[0]
        sample_code = "_".join(parts[1:])
        slug = sample_code.lower().replace("-", "_")
        samples.append(Sample(gsm=gsm, sample_code=sample_code, tar_prefix=stem, output_slug=slug, prefix=f"{PREFIX_BASE}/{slug}"))
    return samples


def tar_members(tar_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with tarfile.open(tar_path, "r") as tf:
        for m in tf.getmembers():
            out.append({"name": m.name, "size": m.size, "type": "file" if m.isfile() else "other"})
    return out


def duplicate_status(ln: Any, prefix: str) -> dict[str, bool]:
    return {name: ln.Artifact.filter(key=f"{prefix}/{name}").exists() for name in ("obs.parquet", "X.h5ad", "var.parquet")}


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        found = list(ln.Feature.filter(name=name).all())
        if found and found[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name} dtype {found[0].dtype}; expected cat[Artifact]")
        if not found:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def read_gz_lines(path: Path) -> list[str]:
    with gzip.open(path, "rt") as handle:
        return [line.rstrip("\n") for line in handle]


def load_metadata(metadata_path: Path) -> pd.DataFrame:
    keep = [
        "barcode", "batch", "label", "CellType", "TSNE1", "TSNE2",
        "germ.cluster", "germ.broad.CellType", "germ.narrow.CellType", "germ.Pseudotime", "germ.TSNE1", "germ.TSNE2",
    ]
    compression: str | None = "gzip"
    read_path: Path | str = metadata_path
    if zipfile.is_zipfile(metadata_path):
        with zipfile.ZipFile(metadata_path) as zf:
            names = [n for n in zf.namelist() if n.endswith(".tsv")]
            if len(names) != 1:
                raise ValueError(f"Expected one TSV in metadata ZIP, found {names}")
            extracted = metadata_path.with_suffix(metadata_path.suffix + ".extracted.tsv")
            if not extracted.exists() or extracted.stat().st_size == 0:
                with zf.open(names[0]) as src, extracted.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            read_path = extracted
            compression = None
    header = pd.read_csv(read_path, sep="\t", nrows=0, compression=compression).columns.tolist()
    usecols = [c for c in keep if c in header]
    meta = pd.read_csv(read_path, sep="\t", usecols=usecols, dtype=str, na_values=["NA", ""], keep_default_na=True, compression=compression)
    if "barcode" not in meta.columns:
        raise ValueError("metadata lacks barcode column")
    meta = meta.drop_duplicates("barcode").set_index("barcode")
    return meta


def extract_sample(tar_path: Path, sample: Sample, sample_dir: Path) -> dict[str, Path]:
    sample_dir.mkdir(parents=True, exist_ok=True)
    names = {
        "barcodes": f"{sample.tar_prefix}_barcodes.tsv.gz",
        "features": f"{sample.tar_prefix}_features.tsv.gz",
        "matrix": f"{sample.tar_prefix}_matrix.mtx.gz",
    }
    with tarfile.open(tar_path, "r") as tf:
        for name in names.values():
            member = tf.getmember(name)
            tf.extract(member, path=sample_dir)
    return {k: sample_dir / v for k, v in names.items()}


def smoke_one(paths: dict[str, Path], sample: Sample, metadata: pd.DataFrame) -> dict[str, Any]:
    barcodes = read_gz_lines(paths["barcodes"])
    full_barcodes = [f"{sample.sample_code}_{b}" for b in barcodes]
    metadata_matches = sum(1 for b in full_barcodes if b in metadata.index)
    features = read_gz_lines(paths["features"])
    shape: list[int] | None = None
    with gzip.open(paths["matrix"], "rt") as handle:
        header_lines = []
        for line in handle:
            if line.startswith("%"):
                header_lines.append(line.strip())
                continue
            shape = [int(x) for x in line.split()[:3]]
            break
    if shape is None:
        raise ValueError(f"No MatrixMarket shape line found in {paths['matrix']}")
    return {
        "raw_barcodes": len(barcodes),
        "metadata_matched_barcodes_planned": int(metadata_matches),
        "features": len(features),
        "matrix_shape_header": shape,
        "matrix_header_comments": header_lines[:3],
        "shape_agrees": shape[0] == len(features) and shape[1] == len(barcodes),
    }


def make_triplet(paths: dict[str, Path], sample: Sample, metadata: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, ad.AnnData, dict[str, Any]]:
    barcodes = read_gz_lines(paths["barcodes"])
    full_barcodes = [f"{sample.sample_code}_{b}" for b in barcodes]
    feature_rows = [line.split("\t") for line in read_gz_lines(paths["features"])]
    var = pd.DataFrame(index=pd.Index([r[0] for r in feature_rows], name="gene_id"))
    var["gene_symbol"] = [r[1] if len(r) > 1 else r[0] for r in feature_rows]
    var["feature_type"] = [r[2] if len(r) > 2 else "Gene Expression" for r in feature_rows]
    var["source_accession"] = "GSE334273"
    var["organism"] = "Petromyzon marinus"

    obs = pd.DataFrame(index=pd.Index(full_barcodes, name="cell_id"))
    obs["barcode"] = full_barcodes
    obs["raw_10x_barcode"] = barcodes
    obs["dataset"] = "GSE334273"
    obs["source_accession"] = "GSE334273"
    obs["sample_accession"] = sample.gsm
    obs["sample_code"] = sample.sample_code
    obs["source_title"] = "Gonadal single-cell RNA-seq of sea lamprey sexual differentiation"
    obs["organism"] = "Petromyzon marinus"
    obs["assay"] = "10x Genomics scRNA-seq"
    obs["modality"] = "scRNA-seq"
    obs["perturbation"] = "sexual_differentiation"
    obs["perturbation_type"] = "developmental_timecourse"
    obs["source_raw_tar"] = RAW_URL
    obs["source_metadata"] = METADATA_URL
    if sample.sample_code.startswith("TF_"):
        obs["condition_hint"] = "TF"
    elif sample.sample_code.startswith("TM_"):
        obs["condition_hint"] = "TM"
    elif sample.sample_code.startswith("UND"):
        obs["condition_hint"] = "UND"
    else:
        obs["condition_hint"] = sample.sample_code.split("_", 1)[0]

    overlap = obs.index.intersection(metadata.index)
    if len(overlap):
        joined = metadata.reindex(obs.index)
        rename = {c: "metadata_" + c.replace(".", "_").replace(" ", "_") for c in joined.columns if c not in obs.columns}
        obs = pd.concat([obs, joined.rename(columns=rename)], axis=1)

    keep_positions = [i for i, cell_id in enumerate(full_barcodes) if cell_id in metadata.index]
    if not keep_positions:
        raise ValueError(f"No metadata-matched cells for {sample.sample_code}; refusing to write raw droplet matrix")
    obs = obs.iloc[keep_positions].copy()

    raw_matrix = scipy.io.mmread(paths["matrix"]).tocsc()
    matrix = raw_matrix[:, keep_positions].T.tocsr()
    adata = ad.AnnData(X=matrix, obs=pd.DataFrame(index=obs.index), var=pd.DataFrame(index=var.index))
    info = {
        "metadata_rows_matched": int(len(overlap)),
        "raw_matrix_shape_header_or_loaded": [int(raw_matrix.shape[0]), int(raw_matrix.shape[1])],
        "matrix_shape": list(matrix.shape),
        "matrix_nnz": int(matrix.nnz),
    }
    return obs, var, adata, info


def write_sample(ln: Any, sample: Sample, paths: dict[str, Path], metadata: pd.DataFrame, overwrite: bool) -> dict[str, Any]:
    dup = duplicate_status(ln, sample.prefix)
    if any(dup.values()) and not overwrite:
        raise RuntimeError(f"Duplicate/partial triplet exists for {sample.prefix}: {dup}")
    obs, var, adata, info = make_triplet(paths, sample, metadata)
    with tempfile.TemporaryDirectory(prefix=f"{sample.output_slug}_") as td:
        x_path = Path(td) / "X.h5ad"
        adata.write_h5ad(x_path, compression="gzip")
        obs_art = ln.Artifact.from_dataframe(obs, key=f"{sample.prefix}/obs.parquet").save()
        x_art = ln.Artifact.from_anndata(str(x_path), key=f"{sample.prefix}/X.h5ad").save()
        var_art = ln.Artifact.from_dataframe(var, key=f"{sample.prefix}/var.parquet", skip_hash_lookup=True).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    info.update({
        "prefix": sample.prefix,
        "duplicate_status_before_write": dup,
        "written_keys": [f"{sample.prefix}/obs.parquet", f"{sample.prefix}/X.h5ad", f"{sample.prefix}/var.parquet"],
    })
    return info


def write_reports(out: dict[str, Any]) -> None:
    ARTIFACT_JSON.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_JSON.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
    lines = [
        "# GSE334273 VM 10x MTX staging/chunking report",
        "",
        f"Generated: {out['generated_at']}",
        f"Task: {TASK_ID}",
        f"Dry run: {out['dry_run']}",
        f"Write: {out['write']}",
        f"RAW URL: {RAW_URL}",
        f"Metadata URL: {METADATA_URL}",
        f"GCS staging: {STAGING}",
        "",
        "## Samples",
    ]
    for s in out.get("samples", []):
        lines.extend([
            f"- {s['sample_code']} ({s['gsm']})",
            f"  - prefix: `{s['prefix']}`",
            f"  - smoke: `{s.get('smoke')}`",
            f"  - duplicate status: `{s.get('duplicate_status_before_write', s.get('duplicate_status'))}`",
            f"  - written keys: `{s.get('written_keys', [])}`",
        ])
    lines.extend(["", "## Verification notes", out.get("verification_notes", "")])
    ARTIFACT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--work-dir", type=Path, default=Path("data/gse334273_vm"))
    p.add_argument("--dry-run", action="store_true", help="stage/list/smoke/probe only, no artifact writes")
    p.add_argument("--write", action="store_true", help="write Lamin triplets after smoke/probe")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--only", nargs="*")
    args = p.parse_args()
    ensure_project_cache()
    args.work_dir.mkdir(parents=True, exist_ok=True)

    filelist_path = args.work_dir / "filelist.txt"
    raw_path = args.work_dir / "GSE334273_RAW.tar"
    metadata_path = args.work_dir / "GSE334273_cell_metadata.tsv.gz"

    filelist = curl_download(FILELIST_URL, filelist_path, timeout=300)
    filelist_text = filelist_path.read_text(encoding="utf-8")
    samples = parse_filelist(filelist_text)
    if args.only:
        allow = set(args.only)
        samples = [s for s in samples if s.sample_code in allow or s.gsm in allow or s.output_slug in allow]
    out: dict[str, Any] = {
        "generated_at": now(),
        "task_id": TASK_ID,
        "dry_run": args.dry_run,
        "write": args.write,
        "source": {"raw_head": curl_head(RAW_URL), "metadata_head": curl_head(METADATA_URL), "filelist_download": filelist},
        "staging_prefix": STAGING,
        "target_prefix_base": PREFIX_BASE,
        "samples": [],
    }
    raw_download = curl_download(RAW_URL, raw_path, timeout=7200)
    meta_download = curl_download(METADATA_URL, metadata_path, timeout=1800)
    out["source"].update({"raw_download": raw_download, "metadata_download": meta_download})
    out["gcs_stage"] = [gcs_stage(raw_path), gcs_stage(metadata_path), gcs_stage(filelist_path)]
    out["tar_members"] = tar_members(raw_path)

    ln = connect_pertdata()
    ln.track()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    ensure_link_features(ln)
    metadata = load_metadata(metadata_path)
    out["metadata_rows"] = int(len(metadata))

    for sample in samples:
        sample_result: dict[str, Any] = {
            "gsm": sample.gsm,
            "sample_code": sample.sample_code,
            "tar_prefix": sample.tar_prefix,
            "prefix": sample.prefix,
            "duplicate_status": duplicate_status(ln, sample.prefix),
        }
        with tempfile.TemporaryDirectory(prefix=f"gse334273_{sample.output_slug}_", dir=str(args.work_dir)) as td:
            paths = extract_sample(raw_path, sample, Path(td))
            sample_result["extracted_sizes"] = {k: v.stat().st_size for k, v in paths.items()}
            sample_result["smoke"] = smoke_one(paths, sample, metadata)
            if args.write:
                sample_result.update(write_sample(ln, sample, paths, metadata, args.overwrite))
        out["samples"].append(sample_result)
        write_reports(out)

    out["verification_notes"] = (
        "Ran on pert-gym-worker-eu with connect_pertdata(); listed tar members, staged RAW/metadata/filelist to GCS, "
        "smoke-parsed each 10x sample header against barcodes/features, probed duplicate prefixes before writes, "
        "and wrote same-prefix obs.parquet -> X.h5ad -> var.parquet triplets for samples when --write was set."
    )
    write_reports(out)
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
