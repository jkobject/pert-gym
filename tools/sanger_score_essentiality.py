#!/usr/bin/env python3
"""Bounded Sanger SCORE essentiality converter.

Project Score/Sanger SCORE CRISPR payloads are gene-essentiality response
matrices, not RNA expression matrices.  This converter exposes a bounded sample
as explicit obs+var tables:

- obs rows are model/cell-line x perturbation-gene response observations;
- score values live in obs response columns;
- var rows describe perturbation genes;
- no X.h5ad is emitted, so dependency scores cannot be mistaken for expression.

The implementation supports the SCORE2 zip layout used by
``Project_Score2_fitness_scores_Sanger_v2_Broad_21Q2_20250624.zip`` and small
CSV/TSV matrix fixtures for local smoke tests.  Full production conversion of
the SCORE2 matrix should run on a bounded worker or be chunked; this module's
CLI defaults to explicit row/gene limits for smoke artifacts.
"""

from __future__ import annotations

import argparse
import json
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

SANGER_SCORE_LAMIN_PREFIX = "sanger_score_crispr"
SANGER_SCORE_RELEASE = "Project Score2 Sanger v2 Broad 21Q2"
SANGER_SCORE_SOURCE_URL = (
    "https://cog.sanger.ac.uk/cmp/download/"
    "Project_Score2_fitness_scores_Sanger_v2_Broad_21Q2_20250624.zip"
)
BASELINE_JOIN = {
    "baseline_lamin_prefix": "depmap_ccle/26q1",
    "join_keys": ["sanger_model_id", "model_name"],
    "note": "Baseline RNA remains a separate expression artifact; SCORE scores are not RNA X.",
}
OBS_COLUMNS = (
    "essentiality_observation_id",
    "model_name",
    "sanger_model_id",
    "score_source",
    "qc_pass",
    "perturbation_gene",
    "perturbation_gene_id",
    "ensembl_id",
    "score",
    "response_metric",
    "response_value",
    "dataset_release",
    "perturbation_type",
    "readout_modality",
    "source",
    "source_filename",
    "baseline_lamin_prefix",
    "baseline_join_id",
)
VAR_COLUMNS = (
    "perturbation_gene_id",
    "perturbation_gene",
    "ensembl_id",
    "organism",
    "dataset_release",
    "source",
)


@dataclass(frozen=True)
class ScoreMatrix:
    values: pd.DataFrame
    model_meta: pd.DataFrame
    gene_meta: pd.DataFrame
    score_metric: str
    source_member: str


def _first_existing(columns: Iterable[str], choices: Iterable[str]) -> str | None:
    column_set = set(columns)
    for choice in choices:
        if choice in column_set:
            return choice
    return None


def _score2_member_name(zf: zipfile.ZipFile) -> str:
    candidates = [
        info.filename
        for info in zf.infolist()
        if "fold_change_values" in info.filename and info.filename.endswith((".tsv", ".csv"))
    ]
    if not candidates:
        sample = [info.filename for info in zf.infolist()[:10]]
        raise ValueError(f"No SCORE fold_change_values TSV/CSV found; sample members={sample}")
    return candidates[0]


def _parse_score2_raw(raw: pd.DataFrame, source_member: str) -> ScoreMatrix:
    if raw.shape[0] < 6 or raw.shape[1] < 4:
        raise ValueError("SCORE2 matrix is too small to contain expected metadata rows")

    model_meta = pd.DataFrame(
        {
            "model_name": raw.iloc[0, 3:].to_numpy(dtype=str),
            "sanger_model_id": raw.iloc[1, 3:].to_numpy(dtype=str),
            "score_source": raw.iloc[2, 3:].to_numpy(dtype=str),
            "qc_pass": raw.iloc[3, 3:].astype(str).str.upper().eq("TRUE").to_numpy(),
        }
    )
    model_meta.index = (
        model_meta["sanger_model_id"].astype(str)
        + "__"
        + model_meta["score_source"].astype(str)
        + "__"
        + model_meta.groupby(["sanger_model_id", "score_source"]).cumcount().astype(str)
    )

    gene_meta = raw.iloc[5:, :3].copy()
    gene_meta.columns = ["perturbation_gene_id", "perturbation_gene", "ensembl_id"]
    gene_meta = gene_meta.reset_index(drop=True)
    gene_meta.index = gene_meta["perturbation_gene_id"].astype(str)

    values = raw.iloc[5:, 3:].apply(pd.to_numeric, errors="coerce").T
    values.index = model_meta.index
    values.columns = gene_meta.index
    return ScoreMatrix(
        values=values,
        model_meta=model_meta,
        gene_meta=gene_meta,
        score_metric="fold_change",
        source_member=source_member,
    )


def load_sanger_score_matrix(path: Path) -> ScoreMatrix:
    """Load SCORE2 zip or a simple model x gene CSV/TSV fixture into metadata parts."""

    path = Path(path)
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as zf:
            member = _score2_member_name(zf)
            with zf.open(member) as handle:
                raw = pd.read_csv(handle, sep="\t", header=None, low_memory=False)
        return _parse_score2_raw(raw, member)

    sep = "\t" if path.suffix in {".tsv", ".txt"} else ","
    matrix = pd.read_csv(path, sep=sep)
    model_col = _first_existing(matrix.columns, ["sanger_model_id", "model_id", "model_name"])
    if model_col is None:
        model_col = str(matrix.columns[0])
    matrix = matrix.set_index(model_col)
    gene_columns = list(matrix.columns)
    values = matrix.apply(pd.to_numeric, errors="coerce")
    if not isinstance(values, pd.DataFrame):
        values = values.to_frame()
    model_meta = pd.DataFrame(
        {
            "model_name": values.index.astype(str),
            "sanger_model_id": values.index.astype(str),
            "score_source": "fixture",
            "qc_pass": True,
        },
        index=values.index.astype(str),
    )
    gene_meta = pd.DataFrame(
        {
            "perturbation_gene_id": gene_columns,
            "perturbation_gene": gene_columns,
            "ensembl_id": pd.NA,
        },
        index=pd.Index(gene_columns, dtype=str),
    )
    values.index = model_meta.index
    values.columns = gene_meta.index
    return ScoreMatrix(
        values=values,
        model_meta=model_meta,
        gene_meta=gene_meta,
        score_metric="gene_effect",
        source_member=path.name,
    )


def score_matrix_to_obs_var(
    matrix: ScoreMatrix,
    *,
    max_models: int | None = None,
    max_genes: int | None = None,
    dataset_release: str = SANGER_SCORE_RELEASE,
    source_filename: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convert a bounded SCORE matrix slice into obs+var response tables."""

    values = matrix.values
    if max_models is not None:
        values = values.iloc[:max_models, :]
    if max_genes is not None:
        values = values.iloc[:, :max_genes]

    gene_meta = matrix.gene_meta.reindex(values.columns).copy()
    gene_meta["organism"] = "human"
    gene_meta["dataset_release"] = dataset_release
    gene_meta["source"] = "Sanger Project Score"
    var = gene_meta.loc[:, list(VAR_COLUMNS)].reset_index(drop=True)

    model_meta = matrix.model_meta.reindex(values.index).copy()
    obs = values.rename_axis("model_key").reset_index().melt(
        id_vars="model_key",
        var_name="perturbation_gene_id",
        value_name="score",
    )
    obs = obs.merge(model_meta, left_on="model_key", right_index=True, how="left")
    obs = obs.merge(
        var[["perturbation_gene_id", "perturbation_gene", "ensembl_id"]],
        on="perturbation_gene_id",
        how="left",
    )
    obs["response_metric"] = matrix.score_metric
    obs["response_value"] = pd.to_numeric(obs["score"], errors="coerce")
    obs["dataset_release"] = dataset_release
    obs["perturbation_type"] = "CRISPRko"
    obs["readout_modality"] = "Project_Score_CRISPR_screen"
    obs["source"] = "Sanger Project Score"
    obs["source_filename"] = source_filename or matrix.source_member
    obs["baseline_lamin_prefix"] = BASELINE_JOIN["baseline_lamin_prefix"]
    obs["baseline_join_id"] = obs["sanger_model_id"].fillna(obs["model_name"])
    obs["essentiality_observation_id"] = (
        "SCORE:"
        + obs["dataset_release"].astype(str)
        + ":"
        + obs["model_key"].astype(str)
        + ":"
        + obs["perturbation_gene_id"].astype(str)
        + ":"
        + obs["response_metric"].astype(str)
    )
    obs = obs.loc[:, list(OBS_COLUMNS)]
    return obs, var


def write_table(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        df.to_parquet(path, index=False)
    elif path.suffix == ".tsv":
        df.to_csv(path, sep="\t", index=False)
    elif path.suffix == ".csv":
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"Unsupported output suffix: {path}")
    return path


def write_smoke_manifest(
    path: Path,
    *,
    source_path: Path,
    obs_path: Path,
    var_path: Path,
    obs: pd.DataFrame,
    var: pd.DataFrame,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "dataset_id": SANGER_SCORE_LAMIN_PREFIX,
        "classification": "converter-smoke-ready",
        "artifact_contract": "essentiality_obs_var_only",
        "forbidden_outputs": ["X.h5ad for SCORE dependency/fold-change scores"],
        "source_url": SANGER_SCORE_SOURCE_URL,
        "source_path": str(source_path),
        "outputs": {"obs": str(obs_path), "var": str(var_path)},
        "rows": {"obs": int(len(obs)), "var": int(len(var))},
        "response_metrics": sorted(obs["response_metric"].dropna().astype(str).unique().tolist()),
        "perturbation_types": sorted(obs["perturbation_type"].dropna().astype(str).unique().tolist()),
        "readout_modalities": sorted(obs["readout_modality"].dropna().astype(str).unique().tolist()),
        "baseline_join": BASELINE_JOIN,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--obs-output", type=Path, required=True)
    parser.add_argument("--var-output", type=Path, required=True)
    parser.add_argument("--manifest-output", type=Path)
    parser.add_argument("--max-models", type=int, default=3)
    parser.add_argument("--max-genes", type=int, default=5)
    args = parser.parse_args(argv)

    matrix = load_sanger_score_matrix(args.input)
    obs, var = score_matrix_to_obs_var(
        matrix,
        max_models=args.max_models,
        max_genes=args.max_genes,
        source_filename=args.input.name,
    )
    write_table(obs, args.obs_output)
    write_table(var, args.var_output)
    if args.manifest_output is not None:
        write_smoke_manifest(
            args.manifest_output,
            source_path=args.input,
            obs_path=args.obs_output,
            var_path=args.var_output,
            obs=obs,
            var=var,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
