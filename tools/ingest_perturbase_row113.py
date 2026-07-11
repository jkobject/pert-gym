#!/usr/bin/env python3
"""Convert PerturBase row113 / GSE216481 filtered RNA components to Lamin triplets.

The source archives are the filtered PerturBase objects staged under:
    gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/perturbase_t29/filtered_objects_20260630/

Only the active RNA components are allowed here: 201218_RNA and 210322_TFAtlas.
ATAC, failed, combinatorial, or raw GEO components are rejected by construction.
"""
from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.clean_lamin_cache import clean_cache  # noqa: E402
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

ALLOWED_COMPONENTS = {
    "201218_RNA": {
        "expected_n_obs": 56857,
        "expected_modality": "scRNA-seq",
        "assay": "Perturb-seq / multiome filtered RNA",
    },
    "210322_TFAtlas": {
        "expected_n_obs": 527594,
        "expected_modality": "scRNA-seq",
        "assay": "TFAtlas Perturb-seq filtered RNA",
    },
}
DEFAULT_GCS_PREFIX = (
    "gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/"
    "perturbase_t29/filtered_objects_20260630"
)
DEFAULT_PREFIX_ROOT = "temporal_pretraining/perturbase/gse216481_row113"
EXPECTED_MEMBER = "mixscape_hvg_filter.h5ad"


@dataclass
class ComponentResult:
    component: str
    status: str
    prefix_root: str
    n_obs: int
    n_vars: int
    expected_n_obs: int
    chunks: list[dict[str, Any]]
    source_archive: str


def localize_archive(component: str, source: str, cache_dir: Path) -> Path:
    """Return a local archive path, copying a gs:// source into cache when needed."""
    if component not in ALLOWED_COMPONENTS:
        raise ValueError(
            f"Component {component!r} is not allowed. Allowed RNA components: "
            f"{sorted(ALLOWED_COMPONENTS)}"
        )
    cache_dir.mkdir(parents=True, exist_ok=True)
    source_path = f"{source.rstrip('/')}/{component}.filter.tar.gz" if source.startswith("gs://") else source
    if source_path.startswith("gs://"):
        dest = cache_dir / f"{component}.filter.tar.gz"
        if not dest.exists():
            cmd = [
                "gcloud",
                "storage",
                "cp",
                "--billing-project=jkobject-1549353370965",
                source_path,
                str(dest),
            ]
            subprocess.run(cmd, check=True)
        return dest
    path = Path(source_path)
    if path.is_dir():
        path = path / f"{component}.filter.tar.gz"
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def extract_h5ad(archive: Path, tmp_dir: Path) -> Path:
    """Safely extract the single expected h5ad member from a filtered tarball."""
    with tarfile.open(archive, "r:gz") as tf:
        names = tf.getnames()
        if EXPECTED_MEMBER not in names:
            raise ValueError(f"{archive} does not contain {EXPECTED_MEMBER}; members={names[:10]}")
        member = tf.getmember(EXPECTED_MEMBER)
        target = tmp_dir / EXPECTED_MEMBER
        target_abs = target.resolve()
        if not str(target_abs).startswith(str(tmp_dir.resolve())):
            raise ValueError(f"Unsafe tar member path: {EXPECTED_MEMBER}")
        src = tf.extractfile(member)
        if src is None:
            raise ValueError(f"Could not open tar member {EXPECTED_MEMBER}")
        with src, target.open("wb") as dst:
            shutil.copyfileobj(src, dst)
        return target


def split_tf_label(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    tf = series.astype(str)
    target = tf.str.split("-", n=1).str[-1]
    guide = tf.str.split("-", n=1).str[0]
    return target, guide


def standardize_obs(obs: pd.DataFrame, *, component: str) -> pd.DataFrame:
    """Project PerturBase filtered obs to the pert-gym row113 schema."""
    out = obs.copy()
    out = out.loc[:, ~out.columns.duplicated(keep="first")]
    out["source_component"] = component
    out["dataset"] = "PerturBase row113 / GSE216481"
    out["source"] = "PerturBase"
    out["source_accession"] = "GSE216481"
    out["organism"] = "human"
    out["modality"] = ALLOWED_COMPONENTS[component]["expected_modality"]
    out["assay"] = ALLOWED_COMPONENTS[component]["assay"]
    out["perturbation_type"] = "overexpression"
    out["perturbation_technology"] = "MORF/TF ORF overexpression"
    out["perturbation_library"] = "Joung et al. MORF Data S1"
    out["cell_type"] = out.get("cell_type", "embryonic stem cell derived differentiation")
    out["cell_line"] = out.get("cell_line", "hESC")
    out["is_bulk"] = False
    out["is_pseudobulk"] = False

    if "TF" not in out.columns and "gene" not in out.columns:
        raise ValueError(
            f"{component} lacks both TF and gene columns; cannot derive perturbation labels"
        )
    if "TF" in out.columns:
        target, guide = split_tf_label(out["TF"])
        out["guide_id"] = guide
        out["perturbation_target"] = target
    else:
        out["perturbation_target"] = out["gene"].astype(str)
    out["perturbation"] = out["perturbation_target"].astype(str)
    if "gene" in out.columns:
        # Keep the source's post-Mixscape target assignment for auditability.
        source_gene = out["gene"].astype(str)
        out["perturbation_target_source_gene"] = source_gene
        control_mask = source_gene.str.lower().isin({"ctrl", "control", "non-targeting", "nt"})
        out.loc[control_mask, "perturbation_target"] = "CTRL"
        out.loc[control_mask, "perturbation"] = "CTRL"

    if "timept" in out.columns:
        out["timepoint_original"] = out["timept"].astype(str)
        day_num = out["timept"].astype(str).str.extract(r"(\d+(?:\.\d+)?)", expand=False)
        out["timepoint"] = pd.to_numeric(day_num, errors="coerce") * 24 * 60
    if "nCount_RNA" in out.columns and "n_counts" not in out.columns:
        out["n_counts"] = out["nCount_RNA"]
    if "nFeature_RNA" in out.columns and "n_genes" not in out.columns:
        out["n_genes"] = out["nFeature_RNA"]
    if "percent_mito" in out.columns and "pct_mito" not in out.columns:
        out["pct_mito"] = out["percent_mito"]
    if "pct_counts_mt" in out.columns and "pct_mito" not in out.columns:
        out["pct_mito"] = out["pct_counts_mt"]
    out["is_control"] = out["perturbation"].astype(str).str.lower().isin(
        {"control", "ctrl", "nt", "non-targeting", "nan", "none"}
    )
    out["cell_id"] = out.index.astype(str)
    return out


def standardize_var(var: pd.DataFrame) -> pd.DataFrame:
    out = var.copy()
    out = out.loc[:, ~out.columns.duplicated(keep="first")]
    if "gene_symbol" not in out.columns:
        out["gene_symbol"] = out.index.astype(str)
    if "gene_id" not in out.columns:
        if "ENSEMBL" in out.columns:
            out["gene_id"] = out["ENSEMBL"].astype(str)
        else:
            out["gene_id"] = out.index.astype(str)
    out.index = out.index.astype(str)
    return out


def ensure_artifact_features(ln: Any) -> None:
    for name in ("X", "var"):
        feature = list(ln.Feature.filter(name=name).all())
        if feature and feature[0].dtype != "cat[Artifact]":
            raise ValueError(
                f"Feature {name!r} has dtype {feature[0].dtype!r}; expected cat[Artifact]."
            )
        if not feature:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def triplet_status(ln: Any, prefix: str) -> set[str]:
    return {
        suffix
        for suffix in ("obs.parquet", "X.h5ad", "var.parquet")
        if ln.Artifact.filter(key=f"{prefix}/{suffix}").exists()
    }


def write_triplet(
    ln: Any,
    *,
    prefix: str,
    chunk: ad.AnnData,
    overwrite: bool,
) -> dict[str, Any]:
    status = triplet_status(ln, prefix)
    if status == {"obs.parquet", "X.h5ad", "var.parquet"} and not overwrite:
        return {"status": "exists", "prefix": prefix, "n_obs": int(chunk.n_obs), "n_vars": int(chunk.n_vars)}
    if status and not overwrite:
        raise RuntimeError(f"Partial triplet exists for {prefix}: {sorted(status)}")

    obs_key = f"{prefix}/obs.parquet"
    x_key = f"{prefix}/X.h5ad"
    var_key = f"{prefix}/var.parquet"
    prev_obs = list(ln.Artifact.filter(key=obs_key).all())
    prev_x = list(ln.Artifact.filter(key=x_key).all())
    prev_var = list(ln.Artifact.filter(key=var_key).all())

    x_adata = ad.AnnData(
        X=chunk.X.copy(),
        obs=pd.DataFrame(index=chunk.obs_names.astype(str)),
        var=pd.DataFrame(index=chunk.var_names.astype(str)),
    )
    obs_art = ln.Artifact.from_dataframe(
        chunk.obs.copy(),
        key=obs_key,
        revises=prev_obs[-1] if (overwrite and prev_obs) else None,
        skip_hash_lookup=True,
    ).save()
    with tempfile.TemporaryDirectory(prefix="perturbase_row113_x_") as td:
        x_path = Path(td) / "X.h5ad"
        x_adata.write_h5ad(x_path, compression="gzip")
        x_art = ln.Artifact.from_anndata(
            str(x_path),
            key=x_key,
            revises=prev_x[-1] if (overwrite and prev_x) else None,
            skip_hash_lookup=True,
        ).save()
    var_art = ln.Artifact.from_dataframe(
        chunk.var.copy(),
        key=var_key,
        revises=prev_var[-1] if (overwrite and prev_var) else None,
        skip_hash_lookup=True,
    ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    linked_x = resolve_artifact(ln, obs_art.features.get_values()["X"])
    linked_var = resolve_artifact(ln, linked_x.features.get_values()["var"])
    if linked_x.key != x_key or linked_var.key != var_key:
        raise RuntimeError(f"Bad triplet links for {prefix}: {linked_x.key=} {linked_var.key=}")
    return {"status": "ingested", "prefix": prefix, "n_obs": int(chunk.n_obs), "n_vars": int(chunk.n_vars)}


def iter_chunks(adata: ad.AnnData, obs: pd.DataFrame, var: pd.DataFrame, chunk_size: int):
    n_obs = int(adata.n_obs)
    for chunk_i, start in enumerate(range(0, n_obs, chunk_size)):
        end = min(n_obs, start + chunk_size)
        sliced = adata[start:end, :].to_memory()
        sliced.obs = obs.iloc[start:end].copy()
        sliced.var = var.copy()
        yield chunk_i, start, end, sliced


def convert_component(
    *,
    component: str,
    archive: Path,
    prefix_root: str,
    chunk_size: int,
    max_chunks: int | None,
    dry_run: bool,
    overwrite: bool,
    ln: Any | None,
) -> ComponentResult:
    with tempfile.TemporaryDirectory(prefix=f"{component}_") as td:
        h5ad_path = extract_h5ad(archive, Path(td))
        source = ad.read_h5ad(h5ad_path, backed="r")
        try:
            n_obs, n_vars = int(source.n_obs), int(source.n_vars)
            expected_n_obs = int(ALLOWED_COMPONENTS[component]["expected_n_obs"])
            if n_obs != expected_n_obs:
                raise ValueError(f"{component} n_obs {n_obs} != expected {expected_n_obs}")
            obs = standardize_obs(source.obs.copy(), component=component)
            var = standardize_var(source.var.copy())
            if len(obs) != n_obs or len(var) != n_vars:
                raise ValueError(f"Metadata shape mismatch for {component}")
            if obs["perturbation"].isna().any() or obs["perturbation"].astype(str).str.len().eq(0).any():
                raise ValueError(f"{component} has empty perturbation labels")

            total_chunks = math.ceil(n_obs / chunk_size)
            wanted_chunks = total_chunks if max_chunks is None else min(total_chunks, max_chunks)
            chunks: list[dict[str, Any]] = []
            for chunk_i, start, end, chunk in iter_chunks(source, obs, var, chunk_size):
                if chunk_i >= wanted_chunks:
                    break
                prefix = f"{prefix_root.rstrip('/')}/{component}/chunk_{chunk_i:04d}"
                entry = {"chunk_index": chunk_i, "start": start, "end": end, "prefix": prefix}
                if dry_run:
                    entry.update({"status": "dry_run", "n_obs": int(chunk.n_obs), "n_vars": int(chunk.n_vars)})
                else:
                    if ln is None:
                        raise RuntimeError("Lamin handle required when dry_run=False")
                    entry.update(write_triplet(ln, prefix=prefix, chunk=chunk, overwrite=overwrite))
                    clean_cache(ROOT / ".lamin-cache" / "lamindb")
                chunks.append(entry)
            return ComponentResult(
                component=component,
                status="dry_run" if dry_run else "ingested",
                prefix_root=f"{prefix_root.rstrip('/')}/{component}",
                n_obs=n_obs,
                n_vars=n_vars,
                expected_n_obs=expected_n_obs,
                chunks=chunks,
                source_archive=str(archive),
            )
        finally:
            source.file.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=DEFAULT_GCS_PREFIX, help="GCS prefix, local directory, or single archive")
    parser.add_argument("--component", action="append", choices=sorted(ALLOWED_COMPONENTS), help="Allowed RNA component to ingest; repeatable. Defaults to both.")
    parser.add_argument("--prefix-root", default=DEFAULT_PREFIX_ROOT)
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "data/gcs_cache/perturbase_t29/filtered_objects_20260630")
    parser.add_argument("--chunk-size", type=int, default=25000)
    parser.add_argument("--max-chunks", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--report-json", type=Path, default=None)
    args = parser.parse_args()

    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")
    components = args.component or sorted(ALLOWED_COMPONENTS)
    if not set(components).issubset(ALLOWED_COMPONENTS):
        raise ValueError(f"Only active RNA components are allowed: {sorted(ALLOWED_COMPONENTS)}")

    ln = None
    if not args.dry_run:
        ensure_project_cache()
        ln = connect_pertdata()
        ln.track(path="tools/ingest_perturbase_row113.py")
        ensure_artifact_features(ln)
        print("LAMIN", ln.setup.settings.instance.slug, ln.setup.settings.branch.name, ln.setup.settings.branch.uid, flush=True)

    results: list[ComponentResult] = []
    for component in components:
        archive = localize_archive(component, args.source, args.cache_dir)
        print(f"CONVERT {component} archive={archive} dry_run={args.dry_run}", flush=True)
        result = convert_component(
            component=component,
            archive=archive,
            prefix_root=args.prefix_root,
            chunk_size=args.chunk_size,
            max_chunks=args.max_chunks,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
            ln=ln,
        )
        results.append(result)
        print(f"DONE {component} status={result.status} chunks={len(result.chunks)}", flush=True)

    payload = {
        "dataset": "PerturBase row113 / GSE216481",
        "allowed_components": sorted(ALLOWED_COMPONENTS),
        "excluded_components": ["180124_perturb", "210715_combinatorial", "PRJNA893678", "ATAC", "failed"],
        "prefix_root": args.prefix_root,
        "dry_run": args.dry_run,
        "results": [asdict(result) for result in results],
    }
    if args.report_json:
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
    print(json.dumps(payload, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
