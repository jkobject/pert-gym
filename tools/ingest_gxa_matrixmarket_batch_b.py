#!/usr/bin/env python3
"""Bounded VM-only ingestion for GXA MatrixMarket batch B temporal datasets.

Downloads SCEA raw MatrixMarket archives on the EU VM, streams a bounded first-cell
chunk without densifying the full matrix, and writes canonical Lamin triplets.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen
from zipfile import ZipFile

import anndata as ad
import pandas as pd
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

ARTIFACT_JSON = ROOT / "artifacts/schema_audit/gxa_matrixmarket_batch_b_t_eb4da761_20260702.json"
ARTIFACT_MD = ROOT / "artifacts/schema_audit/gxa_matrixmarket_batch_b_t_eb4da761_20260702.md"
STAGING = "gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/gxa_matrixmarket_batch_b"


@dataclass(frozen=True)
class Dataset:
    acc: str
    title: str
    prefix: str
    time_hint: str
    organism: str


DATASETS = [
    Dataset("E-MTAB-9304", "Drosophila embryo dorsal-ventral patterning scRNA-seq", "temporal_pretraining/gxa/E-MTAB-9304_drosophila_dorsal_ventral_patterning/chunk_0000", "developmental/time labels from experiment design", "drosophila melanogaster"),
    Dataset("E-MTAB-8060", "Single cell RNA-seq of in vitro cultured human embryos", "temporal_pretraining/gxa/E-MTAB-8060_in_vitro_cultured_human_embryos/chunk_0000", "embryonic day/time labels from experiment design", "homo sapiens"),
    Dataset("E-MTAB-8894", "Human fetal lateral ganglionic eminence at 7, 9, 11 pcw", "temporal_pretraining/gxa/E-MTAB-8894_human_fetal_lge/chunk_0000", "post-conceptional week labels from experiment design", "homo sapiens"),
    Dataset("E-GEOD-234602", "Organogenetic transcriptomes of Drosophila embryo at single-cell resolution", "temporal_pretraining/gxa/E-GEOD-234602_drosophila_organogenetic_transcriptomes/chunk_0000", "embryo organogenesis/stage labels from experiment design", "drosophila melanogaster"),
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def url(acc: str, kind: str) -> str:
    base = f"https://www.ebi.ac.uk/gxa/sc/experiment/{acc}/download"
    if kind == "raw":
        return base + "/zip?fileType=quantification-raw&accessKey="
    if kind == "design":
        return base + "?fileType=experiment-design&accessKey="
    if kind == "metadata":
        return base + "/zip?fileType=experiment-metadata&accessKey="
    raise ValueError(kind)


def download(target: Path, source_url: str, timeout: int = 1800) -> dict[str, Any]:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.stat().st_size > 0:
        return {"path": str(target), "bytes": target.stat().st_size, "status": "exists"}
    subprocess.run(["curl", "-L", "--fail", "--retry", "3", "--connect-timeout", "30", "--max-time", str(timeout), "-o", str(target), source_url], check=True)
    return {"path": str(target), "bytes": target.stat().st_size, "status": "downloaded"}




def gcs_stage(local_path: Path, gcs_uri: str) -> dict[str, Any]:
    """Stage a file and return durable source/staging evidence for review."""
    cp = subprocess.run(
        ["gcloud", "storage", "cp", "--billing-project=jkobject-1549353370965", str(local_path), gcs_uri],
        check=False,
        capture_output=True,
        text=True,
    )
    stat = subprocess.run(
        ["gcloud", "storage", "ls", "-L", "--billing-project=jkobject-1549353370965", gcs_uri],
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        "uri": gcs_uri,
        "cp_returncode": cp.returncode,
        "cp_stdout_tail": cp.stdout.strip()[-1000:],
        "cp_stderr_tail": cp.stderr.strip()[-1000:],
        "stat_returncode": stat.returncode,
        "stat_stdout_tail": stat.stdout.strip()[-2000:],
        "stat_stderr_tail": stat.stderr.strip()[-1000:],
    }

def head(source_url: str) -> dict[str, Any]:
    req = Request(source_url, method="HEAD")
    last_error: str | None = None
    for _attempt in range(3):
        try:
            with urlopen(req, timeout=120) as r:
                return {"status": r.status, "url": source_url, "content_length": r.headers.get("Content-Length"), "content_type": r.headers.get("Content-Type")}
        except (TimeoutError, URLError) as exc:
            last_error = repr(exc)
    return {"status": "head_failed", "url": source_url, "error": last_error}


def member_names(zip_path: Path) -> dict[str, str]:
    with ZipFile(zip_path) as zf:
        names = zf.namelist()
    mtx = next(n for n in names if n.endswith(".mtx"))
    cols = next(n for n in names if n.endswith(".mtx_cols"))
    rows = next(n for n in names if n.endswith(".mtx_rows"))
    return {"mtx": mtx, "cols": cols, "rows": rows}


def read_zip_lines(zip_path: Path, member: str, limit: int | None = None) -> list[str]:
    out = subprocess.check_output(["unzip", "-p", str(zip_path), member], text=True)
    lines = out.splitlines()
    return lines if limit is None else lines[:limit]




def safe_obs_column(raw: str) -> str:
    name = raw.strip()
    name = name.replace("Sample Characteristic Ontology Term", "ontology_term")
    name = name.replace("Sample Characteristic", "sample_characteristic")
    name = name.replace("Factor Value Ontology Term", "factor_value_ontology_term")
    name = name.replace("Factor Value", "factor_value")
    name = re.sub(r"[\[\]]", "_", name)
    name = re.sub(r"[^0-9A-Za-z_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return f"design_{name}"


def design_obs(design_path: Path, keep_cols: list[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    design = pd.read_csv(design_path, sep="\t", dtype=str)
    if "Assay" not in design.columns:
        raise ValueError(f"{design_path} has no Assay column")
    keep = pd.DataFrame(index=pd.Index(keep_cols, name="cell_id"))
    design = design.set_index("Assay", drop=False)
    aligned = design.reindex(keep.index)
    missing = int(aligned["Assay"].isna().sum())
    wanted_tokens = (
        "age",
        "developmental stage",
        "time",
        "stage",
        "pcw",
        "day",
        "individual",
        "organism part",
        "strain",
        "genotype",
        "replicate",
        "inferred cell type",
    )
    selected = [c for c in design.columns if c == "Assay" or any(tok in c.lower() for tok in wanted_tokens)]
    for raw_col in selected:
        if raw_col == "Assay":
            continue
        keep[safe_obs_column(raw_col)] = aligned[raw_col].astype("string")

    priority = [
        "Factor Value[age]",
        "Sample Characteristic[age]",
        "Factor Value[developmental stage]",
        "Sample Characteristic[developmental stage]",
    ]
    norm = pd.Series(pd.NA, index=keep.index, dtype="string")
    norm_source = None
    for col in priority:
        if col in aligned.columns:
            values = aligned[col].astype("string")
            if values.notna().any():
                norm = norm.fillna(values)
                norm_source = norm_source or col
    keep["developmental_time_label"] = norm
    keep["developmental_time_label_source"] = norm_source or "unresolved"

    distribution_cols = [
        c
        for c in keep.columns
        if ("age" in c or "developmental_stage" in c or c == "developmental_time_label")
        and "ontology" not in c
    ]
    distributions: dict[str, dict[str, int]] = {}
    for col in distribution_cols:
        vc = keep[col].fillna("<NA>").astype(str).value_counts(dropna=False).head(20)
        distributions[col] = {str(k): int(v) for k, v in vc.items()}
    evidence = {
        "design_rows": int(len(design)),
        "design_columns": list(design.columns),
        "selected_design_columns": selected,
        "obs_design_columns": list(keep.columns),
        "missing_design_rows_for_kept_cells": missing,
        "developmental_time_label_source": norm_source,
        "value_distributions_top20": distributions,
    }
    return keep, evidence

def stream_bounded_matrix(zip_path: Path, member: str, n_rows: int, n_cols_keep: int) -> sp.csr_matrix:
    cmd = ["unzip", "-p", str(zip_path), member]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, bufsize=1024 * 1024)
    assert proc.stdout is not None
    rows: list[int] = []
    cols: list[int] = []
    vals: list[float] = []
    shape_seen = False
    for line in proc.stdout:
        if line.startswith("%"):
            continue
        parts = line.split()
        if not parts:
            continue
        if not shape_seen:
            n_features, n_cells, _nnz = map(int, parts[:3])
            if n_features != n_rows:
                raise ValueError(f"matrix rows {n_features} != row labels {n_rows}")
            if n_cols_keep > n_cells:
                n_cols_keep = n_cells
            shape_seen = True
            continue
        r = int(parts[0]) - 1
        c = int(parts[1]) - 1
        if c < n_cols_keep:
            rows.append(c)
            cols.append(r)
            vals.append(float(parts[2]) if len(parts) > 2 else 1.0)
    rc = proc.wait()
    if rc:
        raise RuntimeError(f"unzip stream failed exit={rc}")
    return sp.coo_matrix((vals, (rows, cols)), shape=(n_cols_keep, n_rows)).tocsr()


def duplicate_status(ln: Any, prefix: str) -> dict[str, bool]:
    return {s: ln.Artifact.filter(key=f"{prefix}/{s}").exists() for s in ("obs.parquet", "X.h5ad", "var.parquet")}


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        found = list(ln.Feature.filter(name=name).all())
        if found and found[0].dtype != "cat[Artifact]":
            raise ValueError(f"Feature {name} dtype {found[0].dtype}; expected cat[Artifact]")
        if not found:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def ingest_one(ds: Dataset, work: Path, max_cells: int, dry_run: bool, overwrite: bool) -> dict[str, Any]:
    zip_path = work / f"{ds.acc}.quantification_raw.zip"
    design_path = work / f"{ds.acc}.experiment_design.tsv"
    metadata_path = work / f"{ds.acc}.experiment_metadata.zip"
    source_urls = {"raw": url(ds.acc, "raw"), "design": url(ds.acc, "design"), "metadata": url(ds.acc, "metadata")}
    source = {
        "urls": source_urls,
        "head": {kind: head(source_url) for kind, source_url in source_urls.items()},
    }
    source["downloads"] = {
        "raw": download(zip_path, source_urls["raw"]),
        "design": download(design_path, source_urls["design"], timeout=600),
        "metadata": download(metadata_path, source_urls["metadata"], timeout=600),
    }
    source["staging"] = {
        "raw": gcs_stage(zip_path, f"{STAGING}/{ds.acc}/{zip_path.name}"),
        "design": gcs_stage(design_path, f"{STAGING}/{ds.acc}/{design_path.name}"),
        "metadata": gcs_stage(metadata_path, f"{STAGING}/{ds.acc}/{metadata_path.name}"),
    }

    members = member_names(zip_path)
    all_cols = read_zip_lines(zip_path, members["cols"])
    row_lines = read_zip_lines(zip_path, members["rows"])
    keep_cols = all_cols[:max_cells]
    var = pd.DataFrame(index=pd.Index(row_lines, name="gene_id"))
    var["source_accession"] = ds.acc
    obs = pd.DataFrame(index=pd.Index(keep_cols, name="cell_id"))
    obs["dataset"] = ds.acc
    obs["source_accession"] = ds.acc
    obs["source_title"] = ds.title
    obs["organism"] = ds.organism
    obs["assay"] = "GXA Single Cell Expression Atlas MatrixMarket raw counts"
    obs["modality"] = "scRNA-seq"
    obs["perturbation"] = "developmental_time"
    obs["perturbation_type"] = "developmental_timecourse"
    obs["timepoint_source_hint"] = ds.time_hint
    obs["source_experiment_design_path"] = str(design_path)
    obs["source_raw_zip_path"] = str(zip_path)
    parsed_design, design_evidence = design_obs(design_path, keep_cols)
    for col in parsed_design.columns:
        obs[col] = parsed_design[col]

    ln = connect_pertdata()
    ln.track()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    dup = duplicate_status(ln, ds.prefix)
    result = {"accession": ds.acc, "prefix": ds.prefix, "source": source, "zip_members": members, "n_obs_total": len(all_cols), "n_vars": len(var), "n_obs_written_or_planned": len(obs), "duplicate_status": dup, "design_evidence": design_evidence, "dry_run": dry_run}
    if dry_run:
        return result
    if any(dup.values()) and not overwrite:
        raise RuntimeError(f"duplicate/partial triplet for {ds.prefix}: {dup}")
    ensure_link_features(ln)
    matrix = stream_bounded_matrix(zip_path, members["mtx"], len(var), len(obs))
    with tempfile.TemporaryDirectory(prefix=f"{ds.acc}_") as tmp:
        x_path = Path(tmp) / "X.h5ad"
        ad.AnnData(X=matrix, obs=pd.DataFrame(index=obs.index), var=pd.DataFrame(index=var.index)).write_h5ad(x_path, compression="gzip")
        obs_art = ln.Artifact.from_dataframe(obs, key=f"{ds.prefix}/obs.parquet").save()
        x_art = ln.Artifact.from_anndata(str(x_path), key=f"{ds.prefix}/X.h5ad").save()
        var_art = ln.Artifact.from_dataframe(var, key=f"{ds.prefix}/var.parquet", skip_hash_lookup=True).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    result.update({"matrix_shape": list(matrix.shape), "matrix_nnz": int(matrix.nnz), "written_keys": [f"{ds.prefix}/obs.parquet", f"{ds.prefix}/X.h5ad", f"{ds.prefix}/var.parquet"]})
    return result


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--work-dir", type=Path, default=Path("data/gxa_batch_b"))
    p.add_argument("--max-cells", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--only", nargs="*")
    args = p.parse_args()
    ensure_project_cache()
    selected = [d for d in DATASETS if not args.only or d.acc in args.only]
    out = {"generated_at": now(), "task_id": "t_eb4da761", "staging_prefix": STAGING, "max_cells": args.max_cells, "dry_run": args.dry_run, "datasets": []}
    args.work_dir.mkdir(parents=True, exist_ok=True)
    for ds in selected:
        out["datasets"].append(ingest_one(ds, args.work_dir, args.max_cells, args.dry_run, args.overwrite))
        ARTIFACT_JSON.parent.mkdir(parents=True, exist_ok=True)
        ARTIFACT_JSON.write_text(json.dumps(out, indent=2, sort_keys=True))
    lines = ["# GXA MatrixMarket batch B ingestion verification", "", f"Generated: {out['generated_at']}", f"Dry run: {out['dry_run']}", f"Max cells per dataset: {out['max_cells']}", f"Staging prefix: `{STAGING}`", ""]
    for d in out["datasets"]:
        lines += [
            f"## {d['accession']}",
            f"- prefix: `{d['prefix']}`",
            f"- raw url: `{d['source']['urls']['raw']}`",
            f"- design url: `{d['source']['urls']['design']}`",
            f"- raw bytes: {d['source']['downloads']['raw']['bytes']}",
            f"- design bytes: {d['source']['downloads']['design']['bytes']}",
            f"- metadata bytes: {d['source']['downloads']['metadata']['bytes']}",
            f"- staged raw: `{d['source']['staging']['raw']['uri']}`",
            f"- staged design: `{d['source']['staging']['design']['uri']}`",
            f"- staged metadata: `{d['source']['staging']['metadata']['uri']}`",
            f"- planned/written shape: {d.get('matrix_shape', [d['n_obs_written_or_planned'], d['n_vars']])}",
            f"- duplicate status before write: `{d['duplicate_status']}`",
            f"- design rows matched missing: {d['design_evidence']['missing_design_rows_for_kept_cells']}",
            f"- developmental/time label source: `{d['design_evidence']['developmental_time_label_source']}`",
            f"- developmental/time distributions: `{d['design_evidence']['value_distributions_top20']}`",
            f"- written keys: `{d.get('written_keys', [])}`",
            "",
        ]
    ARTIFACT_MD.write_text("\n".join(lines))
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
