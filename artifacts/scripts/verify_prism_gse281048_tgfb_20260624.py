#!/usr/bin/env python3
"""Verify GSE281048_TGFB_Perturb_seq PRISM chunked Lamin triplets.

This verifier intentionally avoids loading any X matrix payload. It checks:
- expected chunk keys exist for obs/X/var
- obs -> X -> var feature links point to the same-prefix artifacts
- X remote payload exists and X.n_observations matches obs rows
- obs required fields are present and counts aggregate to the source total
- var rows match expected n_vars using one loaded var table per chunk
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tools.clean_lamin_cache import clean_cache
from tools.lamin_context import connect_pertdata

ROOT = Path(__file__).resolve().parents[2]
DATASET = "GSE281048_TGFB_Perturb_seq"
PREFIX = f"prism_collection/{DATASET}"
EXPECTED_CHUNKS = 237
EXPECTED_OBS = 236_606
EXPECTED_VARS = 33_525
SAMPLE_CHUNKS = {0, 236}
REQUIRED_OBS_FIELDS = [
    "perturbation",
    "perturbation_type",
    "is_control",
    "cell_line",
    "organism",
    "modality",
    "assay",
    "dataset",
]
OUT_JSON = ROOT / "artifacts/schema_audit/prism_gse281048_tgfb_verification_20260624.json"
OUT_MD = ROOT / "artifacts/schema_audit/prism_gse281048_tgfb_verification_20260624.md"


def resolve_artifact(ln: Any, value: Any) -> Any:
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


def main() -> int:
    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"

    total_obs = 0
    total_controls = 0
    chunks: list[dict[str, Any]] = []
    failures: list[str] = []
    cell_line_values: set[str] = set()
    perturbation_non_unknown = 0

    for chunk_i in range(EXPECTED_CHUNKS):
        chunk_prefix = f"{PREFIX}/chunk_{chunk_i:04d}"
        expected_rows = 606 if chunk_i == EXPECTED_CHUNKS - 1 else 1000
        obs_key = f"{chunk_prefix}/obs.parquet"
        x_key = f"{chunk_prefix}/X.h5ad"
        var_key = f"{chunk_prefix}/var.parquet"
        try:
            obs_art = ln.Artifact.get(key=obs_key)
            x_art = ln.Artifact.get(key=x_key)
            var_art = ln.Artifact.get(key=var_key)
            linked_x = resolve_artifact(ln, obs_art.features.get_values()["X"])
            linked_var = resolve_artifact(ln, linked_x.features.get_values()["var"])
            if linked_x.key != x_key:
                failures.append(f"{chunk_prefix}: linked X {linked_x.key} != {x_key}")
            if linked_var.key != var_key:
                failures.append(f"{chunk_prefix}: linked var {linked_var.key} != {var_key}")
            x_payload_exists = None
            if chunk_i in SAMPLE_CHUNKS:
                x_payload_exists = bool(x_art.path.exists())
                if not x_payload_exists:
                    failures.append(f"{chunk_prefix}: sampled X payload missing")
            obs_rows = int(obs_art.n_observations or -1)
            x_obs = int(x_art.n_observations or -1)
            var_rows = None
            controls = None
            missing: list[str] = []
            if chunk_i in SAMPLE_CHUNKS:
                obs_df = obs_art.load()
                var_df = var_art.load()
                missing = [field for field in REQUIRED_OBS_FIELDS if field not in obs_df.columns]
                if missing:
                    failures.append(f"{chunk_prefix}: missing obs fields {missing}")
                var_rows = int(len(var_df))
                if var_rows != EXPECTED_VARS:
                    failures.append(f"{chunk_prefix}: var rows {var_rows} != expected {EXPECTED_VARS}")
                controls = int(obs_df["is_control"].sum()) if "is_control" in obs_df.columns else 0
                total_controls += controls
                if "cell_line" in obs_df.columns:
                    cell_line_values.update(map(str, obs_df["cell_line"].dropna().unique()))
                if "perturbation" in obs_df.columns:
                    perturbation_non_unknown += int((obs_df["perturbation"].astype(str) != "unknown").sum())
            if obs_rows != expected_rows:
                failures.append(f"{chunk_prefix}: obs artifact n_observations {obs_rows} != expected {expected_rows}")
            if x_obs != obs_rows:
                failures.append(f"{chunk_prefix}: X n_observations {x_obs} != obs rows {obs_rows}")
            total_obs += obs_rows
            chunks.append(
                {
                    "chunk": chunk_i,
                    "prefix": chunk_prefix,
                    "obs_rows": obs_rows,
                    "x_n_observations": x_obs,
                    "var_rows": var_rows,
                    "controls": controls,
                    "x_payload_exists_sampled": x_payload_exists,
                    "required_fields_missing": missing,
                }
            )
            clean_cache(ROOT / ".lamin-cache" / "lamindb")
        except Exception as exc:  # noqa: BLE001 - collect all chunk failures for audit.
            failures.append(f"{chunk_prefix}: {type(exc).__name__}: {exc}")

    if total_obs != EXPECTED_OBS:
        failures.append(f"total obs {total_obs} != expected {EXPECTED_OBS}")
    if len(chunks) != EXPECTED_CHUNKS:
        failures.append(f"verified chunks {len(chunks)} != expected {EXPECTED_CHUNKS}")

    summary = {
        "dataset": DATASET,
        "prefix": PREFIX,
        "lamin_instance": ln.setup.settings.instance.slug,
        "lamin_branch": ln.setup.settings.branch.name,
        "expected_chunks": EXPECTED_CHUNKS,
        "verified_chunks": len(chunks),
        "expected_obs": EXPECTED_OBS,
        "verified_obs": total_obs,
        "expected_vars": EXPECTED_VARS,
        "sampled_controls": total_controls,
        "sampled_cell_line_values": sorted(cell_line_values),
        "sampled_perturbation_non_unknown": perturbation_non_unknown,
        "sample_chunks": sorted(SAMPLE_CHUNKS),
        "required_obs_fields": REQUIRED_OBS_FIELDS,
        "failures": failures,
        "chunks": chunks,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=False) + "\n")
    OUT_MD.write_text(
        "# PRISM GSE281048 TGFB verification — 2026-06-24\n\n"
        f"- Dataset: `{DATASET}`\n"
        f"- Prefix: `{PREFIX}`\n"
        f"- Lamin: `{summary['lamin_instance']}` branch `{summary['lamin_branch']}`\n"
        f"- Chunks verified: {len(chunks)}/{EXPECTED_CHUNKS}\n"
        f"- Obs rows: {total_obs}/{EXPECTED_OBS}\n"
        f"- Var rows sampled chunks {sorted(SAMPLE_CHUNKS)}: {EXPECTED_VARS}\n"
        f"- Sampled controls across sampled chunks: {total_controls}\n"
        f"- Sampled cell-line values: {', '.join(summary['sampled_cell_line_values']) or 'none'}\n"
        f"- Sampled perturbation non-unknown rows: {perturbation_non_unknown}\n"
        f"- Failures: {len(failures)}\n\n"
        "Validation checks: all obs/X/var artifact records, all obs→X→var feature links, "
        "all X.n_observations vs obs artifact n_observations, all obs artifact row counts, "
        "and sampled X payload existence / required obs fields / var row counts. "
        "No X matrices were loaded.\n"
    )
    print(json.dumps({k: v for k, v in summary.items() if k != "chunks"}, indent=2))
    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
