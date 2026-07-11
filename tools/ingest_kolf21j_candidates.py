#!/usr/bin/env python3
"""Build append-only KOLF2.1J canonical triplet candidates on an approved EU VM.

Downloads directly to the capacity VM with resume + MD5 verification, reads each
source in backed mode, emits same-prefix obs/X/var triplets, and only then writes
candidate Lamin artifacts on branch ``jkobject``.  It refuses Mac/local runs,
existing candidate keys, bad checksums, source schema drift, or a target-count
drift from the validated KOLF2.1J contract.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anndata as ad
import h5py
import pandas as pd
from scipy import sparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata  # noqa: E402
from tools.pert_gym_vm_runner import require_heavy_vm  # noqa: E402

FIGSHARE_ARTICLE = 27261219
LICENSE = "CC BY 4.0"
DOI = "10.25452/figshare.plus.27261219"
SOURCE_URL = "https://ndownloader.figshare.com/files/{file_id}"


@dataclass(frozen=True)
class Variant:
    dataset_id: str
    file_id: int
    filename: str
    size_bytes: int
    md5: str
    expected_target_denominator: int

    @property
    def prefix(self) -> str:
        return f"kolf21j/{self.dataset_id}"


VARIANTS = (
    Variant(
        dataset_id="kolf21j_pan_genome_qc_filtered",
        file_id=64650261,
        filename="KOLF_Pan_Genome_QC_Filtered.h5ad",
        size_bytes=189393177972,
        md5="afd30fde1e6ad32969c29868394385d1",
        expected_target_denominator=11692,
    ),
    Variant(
        dataset_id="kolf21j_strong_perturbations",
        file_id=64650852,
        filename="KOLF_Strong_Perturbations.h5ad",
        size_bytes=46718752086,
        md5="28bcfff0679e7c6c35bdd584f3626362",
        expected_target_denominator=11739,
    ),
)


def file_md5(path: Path) -> str:
    digest = hashlib.md5()  # nosec B324: source publisher specifies MD5
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download_verified(variant: Variant, source_dir: Path) -> Path:
    source_dir.mkdir(parents=True, exist_ok=True)
    path = source_dir / variant.filename
    if not path.exists() or path.stat().st_size != variant.size_bytes:
        subprocess.run(
            [
                "curl", "--fail", "--location", "--retry", "8", "--retry-all-errors",
                "--connect-timeout", "30", "--continue-at", "-", "--output", str(path),
                SOURCE_URL.format(file_id=variant.file_id),
            ],
            check=True,
        )
    if path.stat().st_size != variant.size_bytes:
        raise RuntimeError(f"source size mismatch for {variant.filename}: {path.stat().st_size}")
    actual = file_md5(path)
    if actual != variant.md5:
        raise RuntimeError(f"source MD5 mismatch for {variant.filename}: {actual}")
    return path


def build_canonical_obs(source_obs: pd.DataFrame, *, dataset_id: str) -> pd.DataFrame:
    required = {"gene_target", "gene_target_ensembl_id", "gRNA", "perturbed"}
    missing = sorted(required - set(source_obs.columns))
    if missing:
        raise ValueError(f"KOLF source is missing required obs fields: {missing}")
    obs = source_obs.copy()
    target = obs["gene_target"].astype(str)
    perturbed = obs["perturbed"].astype(str).str.lower().eq("true")
    obs["dataset_id"] = dataset_id
    obs["source_accession"] = f"figshare:{FIGSHARE_ARTICLE}"
    obs["perturbation"] = target
    obs["perturbation_target"] = target
    obs["perturbation_target_id"] = obs["gene_target_ensembl_id"].astype(str)
    obs["guide_id"] = obs["gRNA"].astype(str)
    obs["is_control"] = target.eq("NTC")
    obs["is_perturbed"] = perturbed
    obs["perturbation_type"] = "CRISPRi"
    obs["organism"] = "human"
    obs["cell_line"] = "KOLF2.1J"
    obs["modality"] = "scRNA-seq"
    obs["assay"] = "Perturb-seq"
    return obs


def target_denominator(obs: pd.DataFrame) -> int:
    values = obs.loc[~obs["is_control"], "perturbation_target_id"].dropna().astype(str)
    values = values[~values.isin({"", "nan", "None", "NTC"})]
    return int(values.nunique())


def write_x_only_h5ad(source_path: Path, output_path: Path) -> None:
    """Copy only the on-disk X group, preserving sparse encoding without RAM loading."""
    if output_path.exists():
        raise FileExistsError(f"refusing to overwrite X candidate {output_path}")
    source = ad.read_h5ad(source_path, backed="r")
    try:
        shell = ad.AnnData(
            X=sparse.csr_matrix((int(source.n_obs), int(source.n_vars)), dtype=source.X.dtype),
            obs=pd.DataFrame(index=source.obs_names.copy()),
            var=pd.DataFrame(index=source.var_names.copy()),
        )
        shell.write_h5ad(output_path)
        with h5py.File(source_path, "r") as raw, h5py.File(output_path, "r+") as target:
            del target["X"]
            raw.copy("X", target)
    finally:
        source.file.close()


def candidate_keys(variant: Variant) -> tuple[str, str, str]:
    return tuple(f"{variant.prefix}/{name}" for name in ("obs.parquet", "X.h5ad", "var.parquet"))


def assert_no_existing_triplet(ln: Any, variant: Variant) -> None:
    existing = [key for key in candidate_keys(variant) if ln.Artifact.filter(key=key).exists()]
    if existing:
        raise FileExistsError(f"refusing duplicate/no-overwrite KOLF candidate keys: {existing}")


def register_triplet(ln: Any, variant: Variant, obs: pd.DataFrame, var: pd.DataFrame, x_path: Path) -> dict[str, Any]:
    obs_key, x_key, var_key = candidate_keys(variant)
    obs_art = ln.Artifact.from_dataframe(obs, key=obs_key).save()
    x_art = ln.Artifact.from_anndata(ad.read_h5ad(x_path, backed="r"), key=x_key).save()
    var_art = ln.Artifact.from_dataframe(var, key=var_key, skip_hash_lookup=True).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    linked_x = obs_art.features.get_values()["X"]
    if isinstance(linked_x, str):
        linked_x = ln.Artifact.get(key=linked_x)
    linked_var = linked_x.features.get_values()["var"]
    if isinstance(linked_var, str):
        linked_var = ln.Artifact.get(key=linked_var)
    if linked_x.key != x_key or linked_var.key != var_key:
        raise RuntimeError(f"KOLF triplet link readback failed for {variant.prefix}")
    return {"obs_key": obs_key, "x_key": x_key, "var_key": var_key, "obs_uid": str(obs_art.uid)}


def build_variant(variant: Variant, *, source_dir: Path, output_root: Path, dry_run: bool) -> dict[str, Any]:
    source_path = download_verified(variant, source_dir)
    source = ad.read_h5ad(source_path, backed="r")
    try:
        obs = build_canonical_obs(source.obs, dataset_id=variant.dataset_id)
        var = source.var.copy()
        if not obs.index.is_unique or not var.index.is_unique:
            raise ValueError("KOLF source obs/var indices must be unique")
        actual_denominator = target_denominator(obs)
        if actual_denominator != variant.expected_target_denominator:
            raise RuntimeError(
                f"target denominator mismatch for {variant.dataset_id}: "
                f"{actual_denominator} != {variant.expected_target_denominator}"
            )
        report: dict[str, Any] = {
            "dataset_id": variant.dataset_id,
            "prefix": variant.prefix,
            "source": {"article_id": FIGSHARE_ARTICLE, "doi": DOI, "file_id": variant.file_id,
                       "filename": variant.filename, "size_bytes": variant.size_bytes, "md5": variant.md5,
                       "license": LICENSE},
            "shape": [int(source.n_obs), int(source.n_vars)],
            "target_denominator": actual_denominator,
            "control_cells": int(obs["is_control"].sum()),
            "perturbed_cells": int(obs["is_perturbed"].sum()),
            "planned_keys": candidate_keys(variant),
            "lamin_writes": 0,
        }
        if dry_run:
            return report
        output_dir = output_root / variant.dataset_id
        output_dir.mkdir(parents=True, exist_ok=False)
        x_path = output_dir / "X.h5ad"
        write_x_only_h5ad(source_path, x_path)
    finally:
        source.file.close()
    ln = connect_pertdata()
    ln.track(path=__file__)
    assert_no_existing_triplet(ln, variant)
    report["triplet"] = register_triplet(ln, variant, obs, var, x_path)
    report["lamin_writes"] = 3
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    require_heavy_vm()
    reports = [build_variant(v, source_dir=args.source_dir, output_root=args.output_root, dry_run=args.dry_run) for v in VARIANTS]
    payload = {"schema": "pert-gym.kolf21j-candidates.v1", "dry_run": args.dry_run, "variants": reports}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
