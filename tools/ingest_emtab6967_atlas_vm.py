#!/usr/bin/env python3
"""VM-only bounded ingestion for E-MTAB-6967 whole-mouse-embryo atlas tarball.

Designed for pert-gym-worker-eu near gs://scperturb. It stages/lists the tarball,
smoke-parses MatrixMarket metadata without full materialization, then streams the
matrix for selected stage-sized chunks and writes same-prefix Lamin triplets.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TASK_ID = "t_d030d588"
SOURCE_URI = "gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/E-MTAB-6967/atlas_data.tar.gz"
BILLING_PROJECT = "jkobject-1549353370965"
PREFIX_BASE = "temporal_pretraining/arrayexpress/E-MTAB-6967_whole_mouse_embryo"
ARTIFACT_JSON = (
    ROOT / "artifacts/schema_audit/emtab6967_atlas_vm_t_d030d588_20260702.json"
)
ARTIFACT_MD = ROOT / "artifacts/schema_audit/emtab6967_atlas_vm_t_d030d588_20260702.md"


@dataclass(frozen=True)
class ChunkPlan:
    stage: str
    prefix: str
    selected_positions: list[int]
    total_stage_cells: int
    written_cells: int
    limited: bool
    range_start: int
    range_end: int


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )


def gcs_stat() -> dict[str, Any]:
    out = run(
        [
            "gcloud",
            "storage",
            "ls",
            "--billing-project",
            BILLING_PROJECT,
            "-L",
            SOURCE_URI,
        ]
    ).stdout
    return {"uri": SOURCE_URI, "listing": out[-4000:]}


def gcs_cp_if_needed(tar_path: Path) -> dict[str, Any]:
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    if tar_path.exists() and tar_path.stat().st_size > 0:
        return {
            "status": "exists",
            "path": str(tar_path),
            "bytes": tar_path.stat().st_size,
        }
    out = run(
        [
            "gcloud",
            "storage",
            "cp",
            "--billing-project",
            BILLING_PROJECT,
            SOURCE_URI,
            str(tar_path),
        ]
    ).stdout
    return {
        "status": "copied",
        "path": str(tar_path),
        "bytes": tar_path.stat().st_size,
        "output_tail": out[-2000:],
    }


def tar_summary(tar_path: Path) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    with tarfile.open(tar_path, "r:gz") as tf:
        for m in tf.getmembers():
            members.append(
                {
                    "name": m.name,
                    "size": int(m.size),
                    "type": "file" if m.isfile() else "other",
                }
            )
    return members


def extract_small_members(tar_path: Path, dest: Path) -> dict[str, str]:
    wanted = [
        "atlas/genes.tsv",
        "atlas/meta.csv",
        "atlas/README.txt",
        "atlas/sizefactors.tab",
    ]
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "r:gz") as tf:
        for name in wanted:
            tf.extract(name, path=dest)
    return {name: str(dest / name) for name in wanted}


def read_matrix_header(tar_path: Path) -> dict[str, Any]:
    comments: list[str] = []
    with tarfile.open(tar_path, "r:gz") as tf:
        handle = tf.extractfile("atlas/raw_counts.mtx")
        if handle is None:
            raise FileNotFoundError("atlas/raw_counts.mtx")
        for raw in handle:
            line = raw.decode("utf-8").strip()
            if line.startswith("%"):
                comments.append(line)
                continue
            n_genes, n_cells, nnz = map(int, line.split()[:3])
            return {
                "comments": comments[:5],
                "n_genes": n_genes,
                "n_cells": n_cells,
                "nnz": nnz,
            }
    raise ValueError("MatrixMarket shape line not found")


def duplicate_status(ln: Any, prefix: str) -> dict[str, bool]:
    return {
        name: ln.Artifact.filter(key=f"{prefix}/{name}").exists()
        for name in ("obs.parquet", "X.h5ad", "var.parquet")
    }


def ensure_link_features(ln: Any) -> None:
    for name in ("X", "var"):
        found = list(ln.Feature.filter(name=name).all())
        if found and found[0].dtype != "cat[Artifact]":
            raise ValueError(
                f"Feature {name} dtype {found[0].dtype}; expected cat[Artifact]"
            )
        if not found:
            ln.Feature(name=name, dtype="cat[Artifact]").save()


def slug_stage(stage: str) -> str:
    return stage.lower().replace(".", "p").replace(" ", "_").replace("/", "_")


def build_chunk_plan(
    meta: pd.DataFrame,
    stage: str,
    max_cells: int,
    start_cell: int = 0,
    end_cell: int | None = None,
) -> ChunkPlan:
    positions_all = [
        int(i)
        for i, value in enumerate(meta["stage"].astype(str).tolist())
        if value == stage
    ]
    if start_cell < 0:
        raise ValueError(f"start_cell must be non-negative, got {start_cell}")
    requested_end = end_cell if end_cell is not None else start_cell + max_cells
    if requested_end <= start_cell:
        raise ValueError(
            f"end_cell must be greater than start_cell, got {start_cell}:{requested_end}"
        )
    if requested_end > len(positions_all):
        raise ValueError(
            f"requested stage range {start_cell}:{requested_end} exceeds {stage} cell count {len(positions_all)}"
        )
    if requested_end - start_cell > max_cells:
        raise ValueError(
            f"requested range length {requested_end - start_cell} exceeds --max-cells {max_cells}"
        )
    selected = positions_all[start_cell:requested_end]
    slug = slug_stage(stage)
    limited = start_cell != 0 or requested_end < len(positions_all)
    suffix = f"{slug}_cells_{start_cell:05d}_{requested_end:05d}" if limited else slug
    return ChunkPlan(
        stage=stage,
        prefix=f"{PREFIX_BASE}/{suffix}",
        selected_positions=selected,
        total_stage_cells=len(positions_all),
        written_cells=len(selected),
        limited=limited,
        range_start=start_cell,
        range_end=requested_end,
    )


def load_var(genes_path: Path) -> pd.DataFrame:
    genes = pd.read_csv(
        genes_path, sep="\t", header=None, names=["gene_id", "gene_symbol"], dtype=str
    )
    var = genes.set_index("gene_id", drop=True)
    var.index.name = "gene_id"
    var["source_accession"] = "E-MTAB-6967"
    var["organism"] = "Mus musculus"
    return var


def load_obs(
    meta: pd.DataFrame, sizefactors_path: Path, plan: ChunkPlan
) -> pd.DataFrame:
    obs = meta.iloc[plan.selected_positions].copy()
    sf = pd.read_csv(sizefactors_path, sep="\t", header=None, names=["size_factor"])
    obs["size_factor"] = sf.iloc[plan.selected_positions, 0].to_numpy()
    obs = obs.set_index("cell", drop=False)
    obs.index.name = "cell_id"
    obs["dataset"] = "E-MTAB-6967"
    obs["source_accession"] = "E-MTAB-6967"
    obs["source_title"] = "Whole-mouse-embryo timecourse E6.5-E8.5"
    obs["organism"] = "Mus musculus"
    obs["assay"] = "10x Genomics scRNA-seq"
    obs["modality"] = "scRNA-seq"
    obs["perturbation"] = "developmental_time"
    obs["perturbation_type"] = "developmental_timecourse"
    obs["chunk_stage"] = plan.stage
    obs["chunk_limited"] = bool(plan.limited)
    obs["source_gcs_uri"] = SOURCE_URI
    return obs


def stream_matrix_chunk(
    tar_path: Path, n_genes: int, selected_positions: list[int]
) -> sp.csr_matrix:
    selected = {
        pos + 1: i for i, pos in enumerate(selected_positions)
    }  # MatrixMarket columns are 1-based
    rows: list[int] = []
    cols: list[int] = []
    vals: list[int] = []
    shape_seen = False
    with tarfile.open(tar_path, "r:gz") as tf:
        handle = tf.extractfile("atlas/raw_counts.mtx")
        if handle is None:
            raise FileNotFoundError("atlas/raw_counts.mtx")
        for raw in handle:
            line = raw.decode("utf-8")
            if line.startswith("%"):
                continue
            parts = line.split()
            if not parts:
                continue
            if not shape_seen:
                matrix_genes, _matrix_cells, _matrix_nnz = map(int, parts[:3])
                if matrix_genes != n_genes:
                    raise ValueError(
                        f"matrix genes {matrix_genes} != var rows {n_genes}"
                    )
                shape_seen = True
                continue
            gene_idx = int(parts[0]) - 1
            cell_col = int(parts[1])
            out_row = selected.get(cell_col)
            if out_row is None:
                continue
            rows.append(out_row)
            cols.append(gene_idx)
            vals.append(int(parts[2]))
    return sp.coo_matrix(
        (vals, (rows, cols)), shape=(len(selected_positions), n_genes)
    ).tocsr()


def write_chunk(
    ln: Any,
    plan: ChunkPlan,
    obs: pd.DataFrame,
    var: pd.DataFrame,
    matrix: sp.csr_matrix,
    overwrite: bool,
) -> dict[str, Any]:
    dup = duplicate_status(ln, plan.prefix)
    if any(dup.values()) and not overwrite:
        raise RuntimeError(f"Duplicate/partial triplet exists for {plan.prefix}: {dup}")
    with tempfile.TemporaryDirectory(prefix="emtab6967_") as tmp:
        x_path = Path(tmp) / "X.h5ad"
        ad.AnnData(
            X=matrix,
            obs=pd.DataFrame(index=obs.index),
            var=pd.DataFrame(index=var.index),
        ).write_h5ad(x_path, compression="gzip")
        obs_art = ln.Artifact.from_dataframe(
            obs, key=f"{plan.prefix}/obs.parquet"
        ).save()
        x_art = ln.Artifact.from_anndata(
            str(x_path), key=f"{plan.prefix}/X.h5ad"
        ).save()
        var_art = ln.Artifact.from_dataframe(
            var, key=f"{plan.prefix}/var.parquet", skip_hash_lookup=True
        ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    return {
        "prefix": plan.prefix,
        "duplicate_status_before_write": dup,
        "matrix_shape": list(matrix.shape),
        "matrix_nnz": int(matrix.nnz),
        "written_keys": [
            f"{plan.prefix}/obs.parquet",
            f"{plan.prefix}/X.h5ad",
            f"{plan.prefix}/var.parquet",
        ],
    }


def write_reports(out: dict[str, Any]) -> None:
    ARTIFACT_JSON.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_JSON.write_text(
        json.dumps(out, indent=2, sort_keys=True), encoding="utf-8"
    )
    lines = [
        "# E-MTAB-6967 VM atlas tarball chunking report",
        "",
        f"Generated: {out['generated_at']}",
        f"Task: {TASK_ID}",
        f"Source: `{SOURCE_URI}`",
        f"Dry run: {out['dry_run']}",
        f"Write: {out['write']}",
        f"Target prefix base: `{PREFIX_BASE}`",
        "",
        "## Source/staging manifest",
        f"- local tar: `{out['local_tar']['path']}` ({out['local_tar']['bytes']} bytes)",
        f"- matrix header: `{out['matrix_header']}`",
        f"- archive members: `{out['tar_members']}`",
        "",
        "## Chunk plan/results",
    ]
    for chunk in out.get("chunks", []):
        lines.extend(
            [
                f"- stage `{chunk['stage']}` -> `{chunk['prefix']}`",
                f"  - stage range: {chunk.get('range_start')}:{chunk.get('range_end')}",
                f"  - planned cells: {chunk['written_cells']} of {chunk['total_stage_cells']} (limited={chunk['limited']})",
                f"  - duplicate status: `{chunk.get('duplicate_status')}`",
                f"  - smoke: `{chunk.get('smoke')}`",
                f"  - written keys: `{chunk.get('written_keys', [])}`",
            ]
        )
    lines.extend(["", "## Verification note", out.get("verification_note", "")])
    ARTIFACT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--work-dir", type=Path, default=Path("data/etab6967_vm"))
    p.add_argument(
        "--stage",
        nargs="+",
        default=["E6.5"],
        help="stage labels to chunk; use small first stage for bounded smoke",
    )
    p.add_argument("--max-cells", type=int, default=5000)
    p.add_argument(
        "--start-cell",
        type=int,
        default=0,
        help="0-based offset within each selected stage; used for residual chunks",
    )
    p.add_argument(
        "--end-cell",
        type=int,
        default=None,
        help="exclusive 0-based end offset within each selected stage; defaults to start + max-cells",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--write", action="store_true")
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()

    ensure_project_cache()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    tar_path = args.work_dir / "atlas_data.tar.gz"
    out: dict[str, Any] = {
        "generated_at": now(),
        "task_id": TASK_ID,
        "source_uri": SOURCE_URI,
        "dry_run": bool(args.dry_run),
        "write": bool(args.write),
        "bounded_parameters": {
            "stages": args.stage,
            "max_cells_per_stage": args.max_cells,
            "start_cell": args.start_cell,
            "end_cell": args.end_cell,
            "full_dataset_cells": 139331,
        },
        "gcs_stat": gcs_stat(),
        "local_tar": gcs_cp_if_needed(tar_path),
        "chunks": [],
    }
    out["tar_members"] = tar_summary(tar_path)
    small_paths = extract_small_members(tar_path, args.work_dir / "small_members")
    out["small_members"] = small_paths
    out["matrix_header"] = read_matrix_header(tar_path)

    meta = pd.read_csv(small_paths["atlas/meta.csv"])
    var = load_var(Path(small_paths["atlas/genes.tsv"]))
    out["metadata"] = {
        "obs_rows": int(len(meta)),
        "var_rows": int(len(var)),
        "stage_counts": {
            str(k): int(v)
            for k, v in meta["stage"].value_counts(dropna=False).sort_index().items()
        },
    }

    ln = connect_pertdata()
    ln.track()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    ensure_link_features(ln)

    for stage in args.stage:
        plan = build_chunk_plan(meta, stage, args.max_cells, args.start_cell, args.end_cell)
        if not plan.selected_positions:
            raise ValueError(f"No cells selected for stage {stage!r}")
        dup = duplicate_status(ln, plan.prefix)
        chunk: dict[str, Any] = {
            "stage": stage,
            "prefix": plan.prefix,
            "total_stage_cells": plan.total_stage_cells,
            "written_cells": plan.written_cells,
            "limited": plan.limited,
            "range_start": plan.range_start,
            "range_end": plan.range_end,
            "duplicate_status": dup,
        }
        obs = load_obs(meta, Path(small_paths["atlas/sizefactors.tab"]), plan)
        chunk["smoke"] = {
            "obs_rows": int(len(obs)),
            "var_rows": int(len(var)),
            "first_cell_ids": obs.index[:3].tolist(),
            "stage_values": sorted(obs["stage"].astype(str).unique().tolist()),
            "source_matrix_header_agrees": out["matrix_header"]["n_genes"] == len(var)
            and out["matrix_header"]["n_cells"] == len(meta),
        }
        if args.write:
            matrix = stream_matrix_chunk(tar_path, len(var), plan.selected_positions)
            chunk.update(write_chunk(ln, plan, obs, var, matrix, args.overwrite))
        out["chunks"].append(chunk)
        write_reports(out)

    out["verification_note"] = (
        "Ran on pert-gym-worker-eu using tools.lamin_context.connect_pertdata() on laminlabs/pertdata jkobject. "
        "The script copied the staged GCS tarball to VM-local scratch, listed archive members, extracted only small metadata members, "
        "read the MatrixMarket header through tar streaming, smoke-validated obs/var/stage counts, probed duplicate prefixes before each write, "
        "and when --write is set streams atlas/raw_counts.mtx once per bounded stage chunk without dense/full-matrix materialization. "
        "Non-zero stage offsets are explicit via --start-cell/--end-cell and are reflected in the same-prefix chunk key."
    )
    write_reports(out)
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
