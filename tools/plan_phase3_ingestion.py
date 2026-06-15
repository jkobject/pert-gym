#!/usr/bin/env python3
"""Build a local ingestion manifest for missing perturbation datasets.

This tool is intentionally local-only: it does not import lamindb and does not
write artifacts. Use it before Lamin ingestion to see which dataset families
have download/conversion code and which local files are already present.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class DatasetPlan:
    name: str
    family: str
    modality: str
    perturbation_axis: str
    priority: int
    lamin_prefix: str
    local_dir: str
    ingestion_entrypoint: str
    status: str
    expected_outputs: list[str]
    next_action: str
    notes: str


DATASETS: tuple[DatasetPlan, ...] = (
    DatasetPlan(
        name="XAtlas/Orion",
        family="genome-wide Perturb-seq",
        modality="scRNA-seq",
        perturbation_axis="CRISPRi dual-guide; dose/dependency-like response",
        priority=1,
        lamin_prefix="xatlas/orion",
        local_dir="data/main/xatlas_orion",
        ingestion_entrypoint="tools.ingest_xatlas_orion.run_xatlas_orion_pipeline",
        status="script_ready_large_download",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Download HCT116/HEK293T h5ad files, preprocess backed-mode obs/var, then save chunked triplets.",
        notes="Very large files (~500 GB total); run only when storage and Lamin branch are confirmed.",
    ),
    DatasetPlan(
        name="PRISM Perturb-seq Collection",
        family="standardized Perturb-seq collection",
        modality="scRNA-seq",
        perturbation_axis="CRISPRi/CRISPRko",
        priority=1,
        lamin_prefix="prism_collection",
        local_dir="data/main/prism_collection",
        ingestion_entrypoint="tools.ingest_phase3_scrna.download_prism_collection + ingest_prism_collection",
        status="script_ready_needs_download",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Install gdown if missing, download public Google Drive h5ads, audit obs columns, ingest one dataset first.",
        notes="Use overlap audit against existing scPerturb keys before full batch ingestion.",
    ),
    DatasetPlan(
        name="T-cell GWPS",
        family="genome-wide primary T-cell Perturb-seq",
        modality="scRNA-seq",
        perturbation_axis="CRISPRi; rest/TCR/TCR+IL2 state transitions",
        priority=2,
        lamin_prefix="tcell_gwps",
        local_dir="data/main/tcell_gwps",
        ingestion_entrypoint="tools.ingest_phase3_scrna.download_tcell_gwps + ingest_tcell_gwps",
        status="script_ready_large_download",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Sync h5ad files from public S3 with aws CLI, then chunk ingest.",
        notes="~22M cells; must use chunking and branch isolation.",
    ),
    DatasetPlan(
        name="VIPerturbSeq",
        family="genome-wide CRISPRi with enriched phenotypes",
        modality="scRNA-seq",
        perturbation_axis="CRISPRi",
        priority=2,
        lamin_prefix="viperturb",
        local_dir="data/main/viperturb",
        ingestion_entrypoint="tools.ingest_phase3_scrna.download_viperturb + ingest_viperturb",
        status="script_ready_needs_download",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Query Zenodo record, download h5ads, inspect obs, ingest.",
        notes="Good candidate for first end-to-end remote download if files are modest.",
    ),
    DatasetPlan(
        name="PROPER-seq",
        family="regulatory-element/ORF perturbation",
        modality="scRNA-seq",
        perturbation_axis="CRISPRko / ORF",
        priority=2,
        lamin_prefix="properseq",
        local_dir="data/main/properseq",
        ingestion_entrypoint="tools.ingest_phase3_scrna.ingest_properseq",
        status="script_ready_check_existing_first",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Check existing Lamin/scPerturb artifacts before downloading GEO files.",
        notes="May already exist through scPerturb.",
    ),
    DatasetPlan(
        name="Sanger Dual-guide KO CRC",
        family="genetic interaction screen",
        modality="scRNA-seq or score matrix",
        perturbation_axis="dual-guide CRISPRko",
        priority=3,
        lamin_prefix="sanger_dual_guide_crc",
        local_dir="data/main/sanger_dualguide_crc",
        ingestion_entrypoint="tools.ingest_phase3_scrna.download_sanger_dualguide + ingest_sanger_dualguide",
        status="script_ready_format_tbc",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Download Figshare zip and inspect extracted layout before conversion.",
        notes="Likely needs custom score-to-sensitivity mapping.",
    ),
    DatasetPlan(
        name="Broad PRISM Repurposing",
        family="drug sensitivity screen",
        modality="bulk/sensitivity",
        perturbation_axis="chemical perturbation",
        priority=1,
        lamin_prefix="broad_prism_repurposing",
        local_dir="data/main/broad_prism",
        ingestion_entrypoint="tools.ingest_phase3_bulk.ingest_broad_prism",
        status="script_ready_url_needs_validation",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Validate DepMap download URL/API, then convert to hybrid obs with lfc sensitivity.",
        notes="Core depmap-like target for sensitivity normalization.",
    ),
    DatasetPlan(
        name="Sanger GDSC",
        family="drug dose-response screen",
        modality="bulk/sensitivity",
        perturbation_axis="chemical perturbation",
        priority=2,
        lamin_prefix="sanger_gdsc",
        local_dir="data/main/gdsc",
        ingestion_entrypoint="tools.ingest_phase3_bulk.ingest_gdsc",
        status="script_ready_needs_download",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Download GDSC1/GDSC2 Excel files and optional CMP expression baseline.",
        notes="Use LN_IC50/AUC as sensitivity fields.",
    ),
    DatasetPlan(
        name="Sanger SCORE CRISPR KO",
        family="gene essentiality screen",
        modality="bulk/sensitivity",
        perturbation_axis="CRISPRko",
        priority=2,
        lamin_prefix="sanger_score_crispr",
        local_dir="data/main/sanger_score",
        ingestion_entrypoint="tools.ingest_phase3_bulk.ingest_sanger_score",
        status="script_ready_url_needs_validation",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Resolve current Cell Model Passports gene-effect download URL.",
        notes="Direct depmap-like gene effect target.",
    ),
    DatasetPlan(
        name="DepMap CCLE",
        family="baseline molecular profiles",
        modality="bulk RNA/protein",
        perturbation_axis="baseline covariates",
        priority=1,
        lamin_prefix="depmap_ccle/25q2",
        local_dir="data/main/depmap_ccle",
        ingestion_entrypoint="tools.ingest_phase3_bulk.ingest_depmap_ccle",
        status="script_ready_needs_figshare_article",
        expected_outputs=["obs.parquet", "X.h5ad", "var.parquet"],
        next_action="Set DepMap Figshare article id for the target release before download.",
        notes="Needed to contextualize PRISM/GDSC/SCORE sensitivity triplets.",
    ),
)


def local_file_summary(root: Path, local_dir: str) -> dict[str, object]:
    directory = root / local_dir
    files = [path for path in directory.rglob("*") if path.is_file()] if directory.exists() else []
    suffix_counts: dict[str, int] = {}
    total_bytes = 0
    examples: list[str] = []
    for path in files:
        suffix = ".csv.gz" if path.name.endswith(".csv.gz") else path.suffix
        suffix_counts[suffix or "<none>"] = suffix_counts.get(suffix or "<none>", 0) + 1
        total_bytes += path.stat().st_size
        if len(examples) < 5:
            examples.append(str(path.relative_to(root)))
    return {
        "exists": directory.exists(),
        "file_count": len(files),
        "total_bytes": total_bytes,
        "suffix_counts": suffix_counts,
        "examples": examples,
    }


def build_manifest(root: Path) -> list[dict[str, object]]:
    manifest: list[dict[str, object]] = []
    for dataset in DATASETS:
        item = asdict(dataset)
        item["local_files"] = local_file_summary(root, dataset.local_dir)
        manifest.append(item)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--json", type=Path, default=None, help="Optional output JSON path.")
    args = parser.parse_args()

    root = args.root.resolve()
    manifest = build_manifest(root)

    if args.json is not None:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    for item in manifest:
        files = item["local_files"]
        print(
            f"P{item['priority']} {item['name']}: {item['status']} | "
            f"local_files={files['file_count']} | prefix={item['lamin_prefix']}"
        )
        print(f"  next: {item['next_action']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
