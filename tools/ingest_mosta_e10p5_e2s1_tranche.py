#!/usr/bin/env python3
"""Register the MOSTA/STDS0000058 E10.5_E2S1 next exact-clear small-file triplet.

Guarded for Kanban t_580bbd4b. Runs only on pert-gym-worker-eu. Current tranche is exactly one next exact-clear already-staged small H5AD,
E10.5_E2S1, chosen per Kanban t_580bbd4b after accepted E9.5_E1S1; preferred next sample per Kanban t_580bbd4b.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import socket
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

TASK_ID = "t_580bbd4b"
BILLING_PROJECT = "jkobject-1549353370965"
DATE = datetime.now(timezone.utc).strftime("%Y%m%d")
REPORT_JSON = ROOT / f"artifacts/schema_audit/mosta_E10p5_E2S1_tranche_{DATE}.json"
REPORT_MD = ROOT / f"artifacts/schema_audit/mosta_E10p5_E2S1_tranche_{DATE}.md"

TRANCHE = [
    {
        "sample": "E10.5_E2S1",
        "stage": "E10.5",
        "section": "E2S1",
        "source_uri": "gs://scperturb/pert-gym/staging/temporal_pretraining/mosta_stds0000058_20260703/stomics/E10.5_E2S1.MOSTA.h5ad",
        "prefix": "temporal_pretraining/mosta_stds0000058/E10.5_E2S1",
        "expected_gcs_size_bytes": 400197383,
        "local_source": "/tmp/pert-gym-mosta-tranche/E10.5_E2S1.MOSTA.h5ad",
    }
]

PREVIOUS_PREFIXES = [
    "temporal_pretraining/mosta_stds0000058/E9.5_E1S1",
    "temporal_pretraining/mosta_stds0000058/E9.5_E2S1",
    "temporal_pretraining/mosta_stds0000058/E9.5_E2S2",
    "temporal_pretraining/mosta_stds0000058/E9.5_E2S3",
    "temporal_pretraining/mosta_stds0000058/E9.5_E2S4",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=check
    )


def assert_vm() -> None:
    host = socket.gethostname()
    if "pert-gym-worker-eu" not in host:
        raise RuntimeError(
            f"Refusing to run outside pert-gym-worker-eu; hostname={host!r}"
        )


def gcs_describe(uri: str) -> dict[str, Any]:
    cp = run(
        [
            "gcloud",
            "storage",
            "objects",
            "describe",
            uri,
            "--billing-project",
            BILLING_PROJECT,
            "--format=json",
        ]
    )
    return json.loads(cp.stdout)


def stage_source(item: dict[str, Any]) -> dict[str, Any]:
    local_path = Path(item["local_source"])
    local_path.parent.mkdir(parents=True, exist_ok=True)
    expected = int(gcs_describe(item["source_uri"]).get("size", 0))
    if local_path.exists() and local_path.stat().st_size == expected:
        status = "exists"
    else:
        if local_path.exists():
            local_path.unlink()
        run(
            [
                "gcloud",
                "storage",
                "cp",
                "--billing-project",
                BILLING_PROJECT,
                item["source_uri"],
                str(local_path),
            ]
        )
        status = "copied"
    first8 = local_path.open("rb").read(8).hex()
    if first8 != "894844460d0a1a0a":
        raise RuntimeError(f"HDF5 signature mismatch for {local_path}: {first8}")
    return {
        "status": status,
        "path": str(local_path),
        "bytes": local_path.stat().st_size,
        "expected_bytes": expected,
        "hdf5_first8_hex": first8,
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


def exact_key_counts(ln: Any, prefix: str) -> dict[str, int]:
    return {
        name: int(ln.Artifact.filter(key=f"{prefix}/{name}").count())
        for name in ("obs.parquet", "X.h5ad", "var.parquet")
    }


def resolve_artifact(ln: Any, value: Any) -> Any:
    return ln.Artifact.get(key=value) if isinstance(value, str) else value


def hash_index(values: pd.Index) -> str:
    h = hashlib.sha256()
    for value in values.astype(str):
        h.update(value.encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


def choose_x(source: ad.AnnData) -> tuple[Any, dict[str, Any]]:
    if "count" in source.layers:
        x = source.layers["count"]
        semantics = "raw_counts_from_count_layer"
        source_matrix = "layers['count']"
    else:
        x = source.X
        semantics = "source_X_as_provided_processed_expression"
        source_matrix = "X"
    if not sp.issparse(x):
        x = sp.csr_matrix(x)
    else:
        x = x.tocsr()
    return x, {
        "recorded_semantics": semantics,
        "source_matrix": source_matrix,
        "dtype": str(x.dtype),
        "sparse": "CSR",
        "nnz": int(x.nnz),
        "raw_present": source.raw is not None,
        "layers": list(source.layers.keys()),
    }


def make_obs(
    source: ad.AnnData, item: dict[str, Any], x_semantics: str
) -> pd.DataFrame:
    obs = source.obs.copy()
    obs.index = obs.index.astype(str)
    obs.index.name = obs.index.name or "spot_id"
    spatial_policy = "no obsm['spatial'] present"
    if "spatial" in source.obsm:
        spatial = np.asarray(source.obsm["spatial"])
        if spatial.shape != (source.n_obs, 2):
            raise ValueError(
                f"Expected spatial shape {(source.n_obs, 2)}, got {spatial.shape}"
            )
        obs["spatial_x"] = spatial[:, 0]
        obs["spatial_y"] = spatial[:, 1]
        spatial_policy = "obsm['spatial'] preserved as obs.spatial_x/obs.spatial_y"
    obs["dataset"] = "MOSTA/STDS0000058"
    obs["source_accession"] = "STDS0000058"
    obs["source_sample"] = item["sample"]
    obs["source_title"] = f"MOSTA mouse embryo {item['sample']} Stereo-seq"
    obs["organism"] = "Mus musculus"
    obs["assay"] = "STOmics/Stereo-seq spatial transcriptomics"
    obs["modality"] = "spatial_transcriptomics"
    obs["perturbation"] = "developmental_time"
    obs["perturbation_type"] = "developmental_timecourse"
    obs["developmental_stage"] = item["stage"]
    obs["section_id"] = item["section"]
    obs["source_gcs_uri"] = item["source_uri"]
    obs["x_semantics"] = x_semantics
    obs["spatial_metadata_policy"] = spatial_policy
    return obs


def make_var(source: ad.AnnData) -> pd.DataFrame:
    var = source.var.copy()
    var.index = var.index.astype(str)
    var.index.name = var.index.name or "gene_id"
    var["source_accession"] = "STDS0000058"
    var["organism"] = "Mus musculus"
    return var


def load_source(
    item: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, sp.csr_matrix, dict[str, Any]]:
    source = ad.read_h5ad(item["local_source"])
    if source.n_obs > 100_000:
        raise RuntimeError(
            f"Refusing unreviewed full load/write for {item['sample']}: n_obs={source.n_obs} > 100000"
        )
    x, x_info = choose_x(source)
    if x.shape != (source.n_obs, source.n_vars):
        raise ValueError(
            f"X shape {x.shape} != source shape {(source.n_obs, source.n_vars)}"
        )
    obs = make_obs(source, item, x_info["recorded_semantics"])
    var = make_var(source)
    summary = {
        "sample": item["sample"],
        "n_obs": int(source.n_obs),
        "n_vars": int(source.n_vars),
        "obs_rows": int(len(obs)),
        "var_rows": int(len(var)),
        "obs_cols": list(obs.columns),
        "var_cols": list(var.columns),
        "obsm_keys": list(source.obsm.keys()),
        "var_index_sha256": hash_index(var.index),
        "x_semantics": x_info,
    }
    return obs, var, x, summary


def save_triplet(
    ln: Any,
    item: dict[str, Any],
    obs: pd.DataFrame,
    var: pd.DataFrame,
    x: sp.csr_matrix,
) -> dict[str, Any]:
    dup = exact_key_counts(ln, item["prefix"])
    if any(dup.values()):
        raise RuntimeError(
            f"Refusing overwrite for {item['sample']}; exact target keys already exist: {dup}"
        )
    with tempfile.TemporaryDirectory(prefix=f"mosta_{item['sample']}_") as tmp:
        x_path = Path(tmp) / "X.h5ad"
        ad.AnnData(
            X=x.copy(),
            obs=pd.DataFrame(index=obs.index.astype(str).copy()),
            var=pd.DataFrame(index=var.index.astype(str).copy()),
        ).write_h5ad(x_path, compression="gzip")
        obs_art = ln.Artifact.from_dataframe(
            obs.copy(),
            key=f"{item['prefix']}/obs.parquet",
            description=f"MOSTA STDS0000058 {item['sample']} obs metadata",
        ).save()
        x_art = ln.Artifact.from_anndata(
            str(x_path),
            key=f"{item['prefix']}/X.h5ad",
            description=f"MOSTA STDS0000058 {item['sample']} expression matrix",
        ).save()
        var_art = ln.Artifact.from_dataframe(
            var.copy(),
            key=f"{item['prefix']}/var.parquet",
            description=f"MOSTA STDS0000058 {item['sample']} var metadata",
            skip_hash_lookup=True,
        ).save()
    x_art.features.set_values({"var": var_art})
    obs_art.features.set_values({"X": x_art})
    return {"obs_uid": obs_art.uid, "X_uid": x_art.uid, "var_uid": var_art.uid}


def verify_triplet(ln: Any, item: dict[str, Any]) -> dict[str, Any]:
    counts = exact_key_counts(ln, item["prefix"])
    obs_art = ln.Artifact.get(key=f"{item['prefix']}/obs.parquet")
    x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    obs = obs_art.load()
    var = var_art.load()
    return {
        "sample": item["sample"],
        "prefix": item["prefix"],
        "exact_key_counts": counts,
        "keys": {"obs": obs_art.key, "X": x_art.key, "var": var_art.key},
        "uids": {"obs": obs_art.uid, "X": x_art.uid, "var": var_art.uid},
        "obs_rows": int(obs.shape[0]),
        "var_rows": int(var.shape[0]),
        "x_n_observations": int(x_art.n_observations or 0),
        "same_prefix_link_ok": x_art.key == f"{item['prefix']}/X.h5ad"
        and var_art.key == f"{item['prefix']}/var.parquet",
        "row_count_ok": int(obs.shape[0]) == int(x_art.n_observations or 0),
        "var_count_ok": int(var.shape[0]) > 0,
    }


def verify_existing_prefix(ln: Any, prefix: str) -> dict[str, Any]:
    counts = exact_key_counts(ln, prefix)
    result: dict[str, Any] = {"prefix": prefix, "exact_key_counts": counts}
    if counts != {"obs.parquet": 1, "X.h5ad": 1, "var.parquet": 1}:
        result["link_ok"] = False
        result["note"] = "not exactly one complete triplet"
        return result
    obs_art = ln.Artifact.get(key=f"{prefix}/obs.parquet")
    x_art = resolve_artifact(ln, obs_art.features.get_values()["X"])
    var_art = resolve_artifact(ln, x_art.features.get_values()["var"])
    result.update(
        {
            "keys": {"obs": obs_art.key, "X": x_art.key, "var": var_art.key},
            "uids": {"obs": obs_art.uid, "X": x_art.uid, "var": var_art.uid},
            "x_n_observations": int(x_art.n_observations or 0),
            "link_ok": x_art.key == f"{prefix}/X.h5ad"
            and var_art.key == f"{prefix}/var.parquet",
        }
    )
    return result


def write_reports(report: dict[str, Any]) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# MOSTA E10.5_E2S1 next exact-clear small-file triplet report",
        "",
        f"Generated: {report['generated_at']}",
        f"Task: `{TASK_ID}`",
        f"Host: `{report['host']}`",
        "",
        "## Scope",
        "- One next exact-clear already-staged H5AD: `E10.5_E2S1.MOSTA.h5ad`.",
        "- This is not full MOSTA completion, Collection promotion, or model-ready completion.",
        "- Image/TIFF/Bin1/barcode sidecars were not read or registered.",
        "",
        "## Safety",
        "- Ran on `pert-gym-worker-eu`; no Mac full download.",
        "- Connected with `tools.lamin_context.connect_pertdata()` to `laminlabs/pertdata` branch `jkobject`; global Lamin CLI not used.",
        f"- Requester Pays billing project: `{BILLING_PROJECT}`",
        "",
        "## Results",
        f"- Duplicate probes before write: `{report.get('duplicate_probe_before_write')}`",
        f"- Source summaries: `{report.get('source_summaries')}`",
        f"- Saved: `{report.get('saved')}`",
        f"- Verification: `{report.get('verification')}`",
        "",
        "## Commands",
    ]
    lines.extend([f"- `{cmd}`" for cmd in report.get("commands", [])])
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--verify-only", action="store_true")
    args = ap.parse_args()
    assert_vm()
    ensure_project_cache()
    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    report: dict[str, Any] = {
        "generated_at": now(),
        "task_id": TASK_ID,
        "host": socket.gethostname(),
        "cwd": str(Path.cwd()),
        "dry_run": args.dry_run,
        "write": args.write,
        "verify_only": args.verify_only,
        "tranche": TRANCHE,
        "commands": [
            "uv run python tools/ingest_mosta_e10p5_e2s1_tranche.py --dry-run",
            "uv run python tools/ingest_mosta_e10p5_e2s1_tranche.py --write",
            "uv run python tools/ingest_mosta_e10p5_e2s1_tranche.py --verify-only",
        ],
        "lamin": {
            "instance": ln.setup.settings.instance.slug,
            "branch": ln.setup.settings.branch.name,
            "branch_uid": ln.setup.settings.branch.uid,
        },
    }
    report["protected_previous_prefixes_readback"] = [
        verify_existing_prefix(ln, prefix) for prefix in PREVIOUS_PREFIXES
    ]
    if not args.verify_only:
        ln.track(path="tools/ingest_mosta_e10p5_e2s1_tranche.py")
        ensure_link_features(ln)
    report["duplicate_probe_before_write"] = {
        i["sample"]: exact_key_counts(ln, i["prefix"]) for i in TRANCHE
    }
    if args.verify_only:
        report["verification"] = [verify_triplet(ln, i) for i in TRANCHE]
        write_reports(report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    if any(any(v.values()) for v in report["duplicate_probe_before_write"].values()):
        raise RuntimeError(
            f"Refusing overwrite; duplicate probe found {report['duplicate_probe_before_write']}"
        )
    report["gcs_objects"] = {
        i["sample"]: gcs_describe(i["source_uri"]) for i in TRANCHE
    }
    report["local_sources"] = {i["sample"]: stage_source(i) for i in TRANCHE}
    loaded = [load_source(i) for i in TRANCHE]
    report["source_summaries"] = [x[3] for x in loaded]
    if args.write:
        report["saved"] = [
            save_triplet(ln, item, obs, var, x)
            for item, (obs, var, x, _) in zip(TRANCHE, loaded)
        ]
        report["verification"] = [verify_triplet(ln, i) for i in TRANCHE]
    else:
        report["saved"] = None
        report["verification"] = {
            "not_run": "pass --write to register, or --verify-only after write"
        }
    write_reports(report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
