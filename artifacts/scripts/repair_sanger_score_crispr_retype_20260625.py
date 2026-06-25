#!/usr/bin/env python3
"""Retype Sanger SCORE CRISPR so canonical X is not expression-like scores.

The pre-repair `sanger_score_crispr/X.h5ad` payload stores Project Score
essentiality/dependency scores.  This script preserves that matrix as an
explicit auxiliary `X_score.h5ad`/`var_score.parquet` payload with
`x_semantics=essentiality_score`, then revises the canonical triplet so
`obs -> X -> var` resolves to an empty placeholder X/var.  Loaders can still
project essentiality scores through the typed auxiliary feature link, but must
not treat the canonical X as RNA expression.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd
import scipy.sparse as sp

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lamin_context import connect_pertdata  # noqa: E402

PREFIX = "sanger_score_crispr"
OBS_KEY = f"{PREFIX}/obs.parquet"
CANONICAL_X_KEY = f"{PREFIX}/X.h5ad"
CANONICAL_VAR_KEY = f"{PREFIX}/var.parquet"
SCORE_X_KEY = f"{PREFIX}/X_score.h5ad"
SCORE_VAR_KEY = f"{PREFIX}/var_score.parquet"
REPORT_STEM = "sanger_score_crispr_retype_20260625"
REPORT_DIR = ROOT / "artifacts" / "schema_audit"


class RepairStateError(RuntimeError):
    """Raised when the live Lamin state is not safe to rewrite."""


def resolve_artifact(ln: Any, value: Any) -> Any:
    """Resolve a Lamin artifact feature value that may be a key or object."""
    if getattr(value, "key", None):
        return value
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    raise TypeError(f"unsupported artifact reference: {type(value)!r}")


def matrix_nnz(matrix: Any) -> int:
    """Return sparse/dense non-zero count without assuming sparse type."""
    nnz = getattr(matrix, "nnz", None)
    if nnz is not None:
        return int(nnz)
    return int((matrix != 0).sum())


def latest_or_none(ln: Any, key: str) -> Any | None:
    """Return latest artifact for key, or None when absent."""
    artifacts = list(ln.Artifact.filter(key=key).all())
    return sorted(artifacts, key=lambda artifact: artifact.created_at)[-1] if artifacts else None


def ensure_artifact_feature(ln: Any, name: str) -> None:
    """Ensure a feature exists for Artifact-valued links."""
    matches = list(ln.Feature.filter(name=name).all())
    if matches:
        dtype = str(matches[0].dtype)
        if dtype != "cat[Artifact]":
            raise RepairStateError(f"Feature {name!r} has dtype {dtype!r}, expected cat[Artifact]")
        return
    ln.Feature(name=name, dtype="cat[Artifact]").save()


def canonical_triplet_state(ln: Any) -> dict[str, Any]:
    """Inspect the current canonical obs/X/var links without broad scans."""
    obs = ln.Artifact.get(key=OBS_KEY)
    x = resolve_artifact(ln, obs.features.get_values()["X"])
    var = resolve_artifact(ln, x.features.get_values()["var"])
    x_adata = x.load()
    obs_df = obs.load()
    var_df = var.load()
    return {
        "obs_artifact": obs,
        "x_artifact": x,
        "var_artifact": var,
        "x_shape": tuple(map(int, x_adata.shape)),
        "x_nnz": matrix_nnz(x_adata.X),
        "obs_shape": tuple(map(int, obs_df.shape)),
        "var_shape": tuple(map(int, var_df.shape)),
    }


def build_empty_canonical_payloads(obs_df: pd.DataFrame) -> tuple[ad.AnnData, pd.DataFrame]:
    """Build an empty canonical X/var payload aligned to obs rows."""
    empty_obs = pd.DataFrame(index=obs_df.index.copy())
    empty_var = pd.DataFrame(index=pd.Index([], name="empty_feature_id"))
    empty_x = ad.AnnData(X=sp.csr_matrix((len(obs_df), 0), dtype="float32"), obs=empty_obs, var=empty_var.copy())
    empty_x.uns["x_semantics"] = "empty"
    empty_x.uns["retyped_from"] = CANONICAL_X_KEY
    empty_x.uns["score_payload_key"] = SCORE_X_KEY
    empty_x.uns["repair_task"] = "t_5fbcbcd0"
    return empty_x, empty_var


def build_score_payload(score_adata: ad.AnnData) -> ad.AnnData:
    """Normalize the preserved score matrix as an explicitly typed auxiliary payload."""
    payload = score_adata.copy()
    payload.obs = pd.DataFrame(index=score_adata.obs_names.copy())
    payload.var = pd.DataFrame(index=score_adata.var_names.copy())
    payload.uns["x_semantics"] = "essentiality_score"
    payload.uns["artifact_role"] = "X_score"
    payload.uns["canonical_expression_X"] = False
    payload.uns["score_type"] = "Project Score CRISPR essentiality/dependency score"
    payload.uns["repair_task"] = "t_5fbcbcd0"
    return payload


def prepare_repair_payload(ln: Any) -> dict[str, Any]:
    """Load only the target Sanger SCORE artifacts and prepare replacement payloads."""
    obs_artifact = ln.Artifact.get(key=OBS_KEY)
    current_x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    current_var_artifact = resolve_artifact(ln, current_x_artifact.features.get_values()["var"])

    score_x_existing = latest_or_none(ln, SCORE_X_KEY)
    if score_x_existing is not None:
        raise RepairStateError(
            f"{SCORE_X_KEY} already exists; refusing to re-run destructive retype without a bespoke follow-up"
        )
    if current_x_artifact.key != CANONICAL_X_KEY:
        raise RepairStateError(f"unexpected canonical X link: {current_x_artifact.key!r}")
    if current_var_artifact.key != CANONICAL_VAR_KEY:
        raise RepairStateError(f"unexpected canonical var link: {current_var_artifact.key!r}")

    obs_df = obs_artifact.load()
    var_df = current_var_artifact.load()
    score_adata = current_x_artifact.load()
    score_nnz = matrix_nnz(score_adata.X)
    if score_adata.n_obs != len(obs_df):
        raise RepairStateError(
            f"score X obs count {score_adata.n_obs} does not match obs rows {len(obs_df)}"
        )
    if score_adata.n_vars != len(var_df):
        raise RepairStateError(
            f"score X var count {score_adata.n_vars} does not match var rows {len(var_df)}"
        )
    if score_nnz <= 0:
        raise RepairStateError("current canonical X is already empty; no score matrix to preserve")

    empty_x, empty_var = build_empty_canonical_payloads(obs_df)
    score_payload = build_score_payload(score_adata)
    return {
        "obs_artifact": obs_artifact,
        "current_x_artifact": current_x_artifact,
        "current_var_artifact": current_var_artifact,
        "obs_df": obs_df,
        "var_df": var_df,
        "score_payload": score_payload,
        "empty_x": empty_x,
        "empty_var": empty_var,
        "before": {
            "canonical_x_key": current_x_artifact.key,
            "canonical_var_key": current_var_artifact.key,
            "score_shape": list(map(int, score_adata.shape)),
            "score_nnz": int(score_nnz),
            "obs_shape": list(map(int, obs_df.shape)),
            "var_shape": list(map(int, var_df.shape)),
        },
    }


def write_repair(ln: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Revise live Lamin artifacts and return saved artifact handles."""
    for feature_name in ("X", "var", "X_score"):
        ensure_artifact_feature(ln, feature_name)

    obs_df = payload["obs_df"].copy()
    obs_df["canonical_X_semantics"] = "empty"
    obs_df["score_matrix_key"] = SCORE_X_KEY
    obs_df["score_matrix_semantics"] = "essentiality_score"
    obs_df["baseline_expression_note"] = "baseline RNA must be loaded separately; canonical X is not expression"

    prev_obs = latest_or_none(ln, OBS_KEY)
    prev_x = latest_or_none(ln, CANONICAL_X_KEY)
    prev_var = latest_or_none(ln, CANONICAL_VAR_KEY)
    prev_score_x = latest_or_none(ln, SCORE_X_KEY)
    prev_score_var = latest_or_none(ln, SCORE_VAR_KEY)

    score_var_artifact = ln.Artifact.from_dataframe(
        payload["var_df"],
        key=SCORE_VAR_KEY,
        revises=prev_score_var,
        skip_hash_lookup=True,
    ).save()
    score_x_artifact = ln.Artifact.from_anndata(
        payload["score_payload"],
        key=SCORE_X_KEY,
        revises=prev_score_x,
    ).save()
    canonical_var_artifact = ln.Artifact.from_dataframe(
        payload["empty_var"],
        key=CANONICAL_VAR_KEY,
        revises=prev_var,
        skip_hash_lookup=True,
    ).save()
    canonical_x_artifact = ln.Artifact.from_anndata(
        payload["empty_x"],
        key=CANONICAL_X_KEY,
        revises=prev_x,
    ).save()
    obs_artifact = ln.Artifact.from_dataframe(
        obs_df,
        key=OBS_KEY,
        revises=prev_obs,
    ).save()

    score_x_artifact.features.set_values({"var": score_var_artifact})
    canonical_x_artifact.features.set_values({"var": canonical_var_artifact})
    obs_artifact.features.set_values({"X": canonical_x_artifact, "X_score": score_x_artifact})

    return {
        "obs_artifact": obs_artifact,
        "canonical_x_artifact": canonical_x_artifact,
        "canonical_var_artifact": canonical_var_artifact,
        "score_x_artifact": score_x_artifact,
        "score_var_artifact": score_var_artifact,
    }


def verify_repair(ln: Any) -> dict[str, Any]:
    """Read back the repaired target links and bounded shapes."""
    obs_artifact = ln.Artifact.get(key=OBS_KEY)
    obs_features = obs_artifact.features.get_values()
    canonical_x = resolve_artifact(ln, obs_features["X"])
    score_x = resolve_artifact(ln, obs_features["X_score"])
    canonical_var = resolve_artifact(ln, canonical_x.features.get_values()["var"])
    score_var = resolve_artifact(ln, score_x.features.get_values()["var"])

    obs_df = obs_artifact.load()
    canonical_adata = canonical_x.load()
    score_adata = score_x.load()
    canonical_var_df = canonical_var.load()
    score_var_df = score_var.load()

    canonical_nnz = matrix_nnz(canonical_adata.X)
    score_nnz = matrix_nnz(score_adata.X)
    result = {
        "obs_key": obs_artifact.key,
        "canonical_x_key": canonical_x.key,
        "canonical_var_key": canonical_var.key,
        "score_x_key": score_x.key,
        "score_var_key": score_var.key,
        "obs_shape": list(map(int, obs_df.shape)),
        "canonical_x_shape": list(map(int, canonical_adata.shape)),
        "canonical_x_nnz": int(canonical_nnz),
        "canonical_var_shape": list(map(int, canonical_var_df.shape)),
        "score_x_shape": list(map(int, score_adata.shape)),
        "score_x_nnz": int(score_nnz),
        "score_var_shape": list(map(int, score_var_df.shape)),
        "score_x_semantics": score_adata.uns.get("x_semantics"),
        "obs_has_score_matrix_key": "score_matrix_key" in obs_df.columns,
    }
    if result["canonical_x_key"] != CANONICAL_X_KEY:
        raise RepairStateError(f"bad canonical X link: {result['canonical_x_key']}")
    if result["canonical_x_shape"][1] != 0 or result["canonical_x_nnz"] != 0:
        raise RepairStateError(f"canonical X is not empty: {result['canonical_x_shape']} nnz={canonical_nnz}")
    if result["score_x_key"] != SCORE_X_KEY or result["score_x_semantics"] != "essentiality_score":
        raise RepairStateError("score auxiliary payload is not explicitly typed as essentiality_score")
    if result["score_x_nnz"] <= 0:
        raise RepairStateError("score auxiliary payload lost non-zero score values")
    return result


def write_reports(before: dict[str, Any], after: dict[str, Any], *, dry_run: bool) -> dict[str, str]:
    """Write dated JSON/TSV/MD repair artifacts."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report = {
        "task": "t_5fbcbcd0",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "instance": "laminlabs/pertdata",
        "branch": "jkobject",
        "decision": "Option B: retype score matrix as X_score/var_score auxiliary and revise canonical X to empty",
        "before": before,
        "after": after,
        "loader_contract": {
            "canonical_expression_X": "empty placeholder; must not be selected as expression/model-ready",
            "essentiality_scores": "load through obs feature link X_score -> var_score; X_score.uns['x_semantics'] == 'essentiality_score'",
            "baseline_expression": "DepMap/CCLE baseline RNA remains separate at depmap_ccle/26q1",
            "sanger_gdsc": "drug response metrics are projected from obs; X remains empty placeholder",
        },
    }
    json_path = REPORT_DIR / f"{REPORT_STEM}.json"
    tsv_path = REPORT_DIR / f"{REPORT_STEM}.tsv"
    md_path = REPORT_DIR / f"{REPORT_STEM}.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pd.DataFrame(
        [
            {"field": "dry_run", "value": dry_run},
            {"field": "before_canonical_x_shape", "value": before.get("score_shape")},
            {"field": "before_canonical_x_nnz", "value": before.get("score_nnz")},
            {"field": "after_canonical_x_shape", "value": after.get("canonical_x_shape")},
            {"field": "after_canonical_x_nnz", "value": after.get("canonical_x_nnz")},
            {"field": "after_score_x_shape", "value": after.get("score_x_shape")},
            {"field": "after_score_x_nnz", "value": after.get("score_x_nnz")},
            {"field": "score_x_semantics", "value": after.get("score_x_semantics")},
        ]
    ).to_csv(tsv_path, sep="\t", index=False)
    md_path.write_text(
        "# Sanger SCORE CRISPR retype repair — 20260625\n\n"
        f"- Task: `t_5fbcbcd0`\n"
        f"- Dry run: `{dry_run}`\n"
        "- Instance/branch: `laminlabs/pertdata` / `jkobject`\n"
        "- Decision: Option B — preserve the score matrix as typed auxiliary "
        "`sanger_score_crispr/X_score.h5ad` linked to `var_score.parquet`, "
        "and revise canonical `X.h5ad` to an empty placeholder.\n\n"
        "## Before\n\n"
        f"- Canonical X: `{CANONICAL_X_KEY}` shape `{before.get('score_shape')}`, nnz `{before.get('score_nnz')}`.\n"
        f"- Canonical var: `{CANONICAL_VAR_KEY}` shape `{before.get('var_shape')}`.\n\n"
        "## After\n\n"
        f"- obs→X now resolves to `{after.get('canonical_x_key')}` with shape `{after.get('canonical_x_shape')}` and nnz `{after.get('canonical_x_nnz')}`.\n"
        f"- canonical X→var resolves to `{after.get('canonical_var_key')}` with shape `{after.get('canonical_var_shape')}`.\n"
        f"- obs→X_score resolves to `{after.get('score_x_key')}` with shape `{after.get('score_x_shape')}`, nnz `{after.get('score_x_nnz')}`, `x_semantics={after.get('score_x_semantics')}`.\n"
        f"- X_score→var resolves to `{after.get('score_var_key')}` with shape `{after.get('score_var_shape')}`.\n\n"
        "## Loader contract\n\n"
        "- `sanger_score_crispr` must not be selected as canonical expression/model-ready data from `X.h5ad`.\n"
        "- Essentiality scores are available only through the typed auxiliary score payload path.\n"
        "- DepMap/CCLE RNA remains a separate baseline expression artifact (`depmap_ccle/26q1`).\n"
        "- Sanger GDSC loaders should continue to project IC50/AUC/RMSE response metrics from obs with empty X.\n",
        encoding="utf-8",
    )
    return {"json": str(json_path), "tsv": str(tsv_path), "md": str(md_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Apply the Lamin repair; otherwise only preflight.")
    args = parser.parse_args()

    ln = connect_pertdata()
    if args.write:
        ln.track(path=str(Path(__file__).relative_to(ROOT)))
    payload = prepare_repair_payload(ln)
    if args.write:
        write_repair(ln, payload)
        after = verify_repair(ln)
    else:
        after = {
            "planned_canonical_x_shape": [payload["before"]["score_shape"][0], 0],
            "planned_canonical_x_nnz": 0,
            "planned_score_x_shape": payload["before"]["score_shape"],
            "planned_score_x_nnz": payload["before"]["score_nnz"],
            "score_x_semantics": "essentiality_score",
        }
    paths = write_reports(payload["before"], after, dry_run=not args.write)
    print(json.dumps({"status": "wrote" if args.write else "dry_run", "reports": paths, "after": after}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
