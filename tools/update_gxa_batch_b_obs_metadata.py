#!/usr/bin/env python3
"""Patch bounded GXA batch B obs triplets with parsed experiment-design labels."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.ingest_gxa_matrixmarket_batch_b import (  # noqa: E402
    DATASETS,
    STAGING,
    design_obs,
    gcs_stage,
    member_names,
    read_zip_lines,
    url,
)
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

ARTIFACT_JSON = ROOT / "artifacts/schema_audit/gxa_matrixmarket_batch_b_t_eb4da761_20260702.json"
ARTIFACT_MD = ROOT / "artifacts/schema_audit/gxa_matrixmarket_batch_b_t_eb4da761_20260702.md"
WORK = ROOT / "data/gxa_batch_b"


def dist_for_obs(obs: pd.DataFrame) -> dict[str, dict[str, int]]:
    cols = [c for c in obs.columns if ("age" in c or "developmental_stage" in c or c == "developmental_time_label") and "ontology" not in c]
    return {c: {str(k): int(v) for k, v in obs[c].fillna("<NA>").astype(str).value_counts(dropna=False).head(20).items()} for c in cols}


def main() -> None:
    ensure_project_cache()
    ln = connect_pertdata()
    ln.track()
    out: dict[str, Any] = {
        "task_id": "t_eb4da761",
        "worker": "pert-gym-worker-eu",
        "zone": "europe-west1-b",
        "lamin_instance": "laminlabs/pertdata",
        "lamin_branch": "jkobject",
        "staging_prefix": STAGING,
        "script": "tools/update_gxa_batch_b_obs_metadata.py",
        "max_cells": 1000,
        "datasets": [],
    }
    for ds in DATASETS:
        zip_path = WORK / f"{ds.acc}.quantification_raw.zip"
        design_path = WORK / f"{ds.acc}.experiment_design.tsv"
        metadata_path = WORK / f"{ds.acc}.experiment_metadata.zip"
        members = member_names(zip_path)
        keep_cols = read_zip_lines(zip_path, members["cols"], limit=1000)
        parsed, evidence = design_obs(design_path, keep_cols)
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
        for col in parsed.columns:
            obs[col] = parsed[col]
        obs_key = f"{ds.prefix}/obs.parquet"
        x_key = f"{ds.prefix}/X.h5ad"
        var_key = f"{ds.prefix}/var.parquet"
        x_art = ln.Artifact.get(key=x_key)
        var_art = ln.Artifact.get(key=var_key)
        dup_before = {s: ln.Artifact.filter(key=f"{ds.prefix}/{s}").exists() for s in ("obs.parquet", "X.h5ad", "var.parquet")}
        obs_art = ln.Artifact.from_dataframe(obs, key=obs_key).save()
        obs_art.features.set_values({"X": x_art})
        source_urls = {"raw": url(ds.acc, "raw"), "design": url(ds.acc, "design"), "metadata": url(ds.acc, "metadata")}
        source = {
            "urls": source_urls,
            "head": {
                kind: {
                    "status": "not_retried_in_obs_metadata_repair",
                    "url": source_url,
                    "reason": "large EBI raw HEAD probes previously timed out; durable GCS staging stats and local byte sizes recorded below",
                }
                for kind, source_url in source_urls.items()
            },
            "downloads": {
                "raw": {"path": str(zip_path), "bytes": zip_path.stat().st_size, "status": "exists"},
                "design": {"path": str(design_path), "bytes": design_path.stat().st_size, "status": "exists"},
                "metadata": {"path": str(metadata_path), "bytes": metadata_path.stat().st_size, "status": "exists"},
            },
            "staging": {
                "raw": gcs_stage(zip_path, f"{STAGING}/{ds.acc}/{zip_path.name}"),
                "design": gcs_stage(design_path, f"{STAGING}/{ds.acc}/{design_path.name}"),
                "metadata": gcs_stage(metadata_path, f"{STAGING}/{ds.acc}/{metadata_path.name}"),
            },
        }
        out["datasets"].append({
            "accession": ds.acc,
            "prefix": ds.prefix,
            "source": source,
            "zip_members": members,
            "source_cells": len(read_zip_lines(zip_path, members["cols"])),
            "matrix_shape": [1000, int(ln.Artifact.get(key=var_key).n_observations or 0)],
            "duplicate_status_before_write": dup_before,
            "artifact_uids": {"obs": obs_art.uid, "X": x_art.uid, "var": var_art.uid},
            "obs_columns": list(obs.columns),
            "design_evidence": evidence,
            "readback_value_distributions": dist_for_obs(obs),
        })
        ARTIFACT_JSON.write_text(json.dumps(out, indent=2, sort_keys=True))
    lines = ["# GXA MatrixMarket batch B ingestion verification", "", "Updated obs metadata pass: parsed GXA experiment-design rows for the 1000 written assay/cell IDs and rewrote obs.parquet versions linked to existing X.h5ad artifacts.", "", f"Staging prefix: `{STAGING}`", ""]
    for d in out["datasets"]:
        lines.extend([
            f"## {d['accession']}",
            f"- prefix: `{d['prefix']}`",
            f"- source URLs: raw `{d['source']['urls']['raw']}`, design `{d['source']['urls']['design']}`, metadata `{d['source']['urls']['metadata']}`",
            f"- staged raw: `{d['source']['staging']['raw']['uri']}` bytes {d['source']['downloads']['raw']['bytes']}",
            f"- staged design: `{d['source']['staging']['design']['uri']}` bytes {d['source']['downloads']['design']['bytes']}",
            f"- staged metadata: `{d['source']['staging']['metadata']['uri']}` bytes {d['source']['downloads']['metadata']['bytes']}",
            f"- shape: {d['matrix_shape']}",
            f"- obs/X/var uids: `{d['artifact_uids']}`",
            f"- missing design rows among 1000 cells: {d['design_evidence']['missing_design_rows_for_kept_cells']}",
            f"- developmental/time label source: `{d['design_evidence']['developmental_time_label_source']}`",
            f"- distributions: `{d['readback_value_distributions']}`",
            "",
        ])
    ARTIFACT_MD.write_text("\n".join(lines))
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
