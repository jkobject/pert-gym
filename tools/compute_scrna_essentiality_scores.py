"""Compute conservative essentiality proxy scores for scRNA-seq perturbation data.

The output is intentionally conservative: a dataset/gene receives a numeric score
only when both perturbed cells and controls are identifiable in loaded obs
metadata.  When controls, perturbation genes, or experimental design information
are missing, the script emits `not_applicable`/inventory rows instead of
inventing essentiality.

Score definition
----------------
For each loaded dataset and perturbation gene:

    score = log2((n_cells_perturbed + pseudocount) / (n_controls + pseudocount))

This is an end-point abundance/cell-count proxy relative to controls, not a
validated viability estimate.  It should be interpreted as low-confidence unless
library baseline abundance, replicate structure, and sampling design are known.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT_STR = str(REPO_ROOT)
if sys.path[0] != REPO_ROOT_STR:
    sys.path.insert(0, REPO_ROOT_STR)

import pandas as pd  # noqa: E402

from tools.lamin_context import connect_pertdata, ensure_project_cache  # noqa: E402

DEFAULT_MANIFEST = REPO_ROOT / "artifacts/schema_audit/unified_collection_manifest_20260621.tsv"
DEFAULT_OUTPUT = REPO_ROOT / "artifacts/essentiality_scores_scRNAseq.tsv"

COMPATIBLE_PERTURBATION_RE = re.compile(
    r"crispr|crispri|crisprko|ko|knock\s*out|kd|knock\s*down|guide|genetic|orf|perturb-seq|crop-seq",
    re.IGNORECASE,
)
CONTROL_TOKEN_RE = re.compile(
    r"^(control|ctrl|non[-_ ]?target(?:ing)?|ntc|neg(?:ative)?|safe[-_ ]?targeting|scramble(?:d)?|mock|vehicle|unperturbed|no[-_ ]?guide|empty)$",
    re.IGNORECASE,
)
CONTROL_SUBSTRING_RE = re.compile(
    r"control|non[-_ ]?target|ntc|safe[-_ ]?target|scramble|unperturbed|no[-_ ]?guide",
    re.IGNORECASE,
)
GENE_COLUMNS = (
    "perturbation_gene",
    "target_gene",
    "target",
    "gene",
    "pert_target",
    "perturbation",
    "pert_genetic",
    "pert_name",
    "guide_target",
    "sgRNA_target",
    "sgRNA_gene",
    "guide_gene",
)
PERTURBATION_TYPE_COLUMNS = (
    "perturbation_type",
    "pert_type",
    "perturbation_type_2",
    "intervention_type",
)
CONTROL_COLUMNS = (
    "is_control",
    "control",
    "is_ctrl",
    "ctrl",
    "perturbation",
    "pert_target",
    "pert_genetic",
    "pert_name",
    "target_gene",
    "gene",
)
GUIDE_ID_COLUMNS = {
    "perturbation",
    "pert_genetic",
    "pert_name",
    "guide_target",
    "sgrna_target",
    "sgrna_gene",
    "guide_gene",
}
UNKNOWN_TOKENS = {"", "unknown", "nan", "none", "null", "na", "n/a"}


@dataclass(frozen=True)
class CandidateDataset:
    dataset: str
    artifact_keys: tuple[str, ...]
    perturbation_type: str
    total_manifest_cells: int | None = None
    manifest_control_availability: str = "unknown"
    source: str = "unknown"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Unified collection manifest TSV used for cheap dataset inventory.",
    )
    parser.add_argument(
        "--artifact-key",
        action="append",
        default=[],
        help="Explicit obs.parquet artifact key to score. Can be repeated.",
    )
    parser.add_argument(
        "--dataset-id",
        action="append",
        default=[],
        help="Restrict manifest scoring to these dataset_id values. Can be repeated.",
    )
    parser.add_argument(
        "--dataset-contains",
        default=None,
        help="Restrict manifest scoring to dataset ids containing this substring.",
    )
    parser.add_argument(
        "--max-datasets-to-score",
        type=int,
        default=1,
        help=(
            "Safety cap for artifact loading when using a manifest. Inventory rows are "
            "still emitted for compatible datasets not scored. Use 0 to inventory only; "
            "use -1 to score all selected datasets."
        ),
    )
    parser.add_argument(
        "--max-members-per-dataset",
        type=int,
        default=1,
        help="Maximum obs artifacts/chunks to load per dataset; -1 means all members.",
    )
    parser.add_argument("--pseudocount", type=float, default=0.5)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--write-json-summary",
        action="store_true",
        help="Also write a compact JSON summary next to the TSV output.",
    )
    return parser.parse_args()


def is_missing(value: Any) -> bool:
    return value is None or str(value).strip().lower() in UNKNOWN_TOKENS


def normalize_gene(value: Any, *, strip_guide_suffixes: bool = False) -> str | None:
    if is_missing(value):
        return None
    text = str(value).strip()
    if CONTROL_SUBSTRING_RE.search(text):
        return None
    if strip_guide_suffixes:
        # Common guide encodings in CROP/ECCITE/VIPerturb metadata.  Apply only
        # to guide/perturbation ID columns, not clean gene-symbol columns where
        # trailing numeric components can be part of real symbols (e.g. MIR-21).
        text = re.sub(r"^Tcrlibrary_", "", text, flags=re.IGNORECASE)
        text = re.sub(r"[_-]g\d+$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"[_-]\d+$", "", text)
    text = text.strip()
    if is_missing(text) or CONTROL_SUBSTRING_RE.search(text):
        return None
    return text


def available_column(columns: Iterable[str], preferred: Sequence[str]) -> str | None:
    column_map = {column.lower(): column for column in columns}
    for column in preferred:
        if column.lower() in column_map:
            return column_map[column.lower()]
    return None


def boolish_control(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip().str.lower()
    return text.isin({"true", "1", "yes", "y", "control", "ctrl"})


def infer_control_mask(obs: pd.DataFrame) -> tuple[pd.Series, str]:
    masks: list[tuple[str, pd.Series]] = []
    for column in CONTROL_COLUMNS:
        if column not in obs.columns:
            continue
        values = obs[column]
        if column.lower().startswith(("is_", "control", "ctrl")):
            mask = boolish_control(values)
        else:
            valid_values = ~values.map(is_missing)
            text = values.astype(str).str.strip()
            mask = valid_values & (
                text.str.match(CONTROL_TOKEN_RE, na=False)
                | text.str.contains(CONTROL_SUBSTRING_RE, na=False)
            )
        if bool(mask.any()):
            masks.append((column, mask.fillna(False)))
    if not masks:
        return pd.Series(False, index=obs.index), "no_control_column_or_tokens_found"
    combined = masks[0][1].copy()
    source_columns = [masks[0][0]]
    for column, mask in masks[1:]:
        combined |= mask
        source_columns.append(column)
    return combined.fillna(False), "control_detected_from:" + ",".join(source_columns)


def infer_gene_series(obs: pd.DataFrame, control_mask: pd.Series) -> tuple[pd.Series, str | None]:
    for column in GENE_COLUMNS:
        if column not in obs.columns:
            continue
        strip_guide_suffixes = column.lower() in GUIDE_ID_COLUMNS
        genes = obs[column].map(lambda value: normalize_gene(value, strip_guide_suffixes=strip_guide_suffixes))
        genes = genes.mask(control_mask, None)
        if int(genes.notna().sum()) > 0:
            return genes, column
    return pd.Series([None] * len(obs), index=obs.index), None


def infer_perturbation_type(obs: pd.DataFrame, manifest_type: str = "unknown") -> str:
    values: list[str] = []
    for column in PERTURBATION_TYPE_COLUMNS:
        if column in obs.columns:
            values.extend(
                sorted(
                    {
                        str(value).strip()
                        for value in obs[column].dropna().unique().tolist()
                        if not is_missing(value)
                    }
                )
            )
    if values:
        return ";".join(sorted(set(values))[:6])
    return manifest_type or "unknown"


def load_manifest_candidates(path: Path) -> list[CandidateDataset]:
    if not path.exists():
        return []
    manifest = pd.read_csv(path, sep="\t", keep_default_na=False)
    if "modality" in manifest.columns:
        manifest = manifest.loc[
            manifest["modality"].astype(str).str.contains("scrna", case=False, na=False)
            | manifest["modality"].astype(str).str.contains("scRNA-seq", case=False, na=False)
        ].copy()
    text_columns = [
        column
        for column in ("perturbation_type", "perturbation_technology", "dataset_id", "source", "assay")
        if column in manifest.columns
    ]
    compatible = pd.Series(False, index=manifest.index)
    for column in text_columns:
        compatible |= manifest[column].astype(str).str.contains(COMPATIBLE_PERTURBATION_RE, na=False)
    manifest = manifest.loc[compatible].copy()
    if manifest.empty:
        return []

    if "n_obs" in manifest.columns:
        manifest["n_obs"] = pd.to_numeric(manifest["n_obs"], errors="coerce")
    candidates: list[CandidateDataset] = []
    for dataset, group in manifest.groupby("dataset_id", dropna=False):
        group = group.sort_values(["chunk_index", "artifact_key"] if "chunk_index" in group.columns else ["artifact_key"])
        perturbation_types = sorted(
            {
                str(value).strip()
                for value in group.get("perturbation_type", pd.Series(dtype=str)).tolist()
                if not is_missing(value)
            }
        )
        control_values = sorted(
            {
                str(value).strip()
                for value in group.get("control_availability", pd.Series(dtype=str)).tolist()
                if not is_missing(value)
            }
        )
        source_values = sorted(
            {
                str(value).strip()
                for value in group.get("source", pd.Series(dtype=str)).tolist()
                if not is_missing(value)
            }
        )
        n_obs = None
        if "n_obs" in group.columns:
            n_obs = int(group["n_obs"].fillna(0).sum())
        candidates.append(
            CandidateDataset(
                dataset=str(dataset),
                artifact_keys=tuple(group["artifact_key"].astype(str).tolist()),
                perturbation_type=";".join(perturbation_types) or "unknown",
                total_manifest_cells=n_obs,
                manifest_control_availability=";".join(control_values) or "unknown",
                source=";".join(source_values) or "unknown",
            )
        )
    return candidates


def artifact_dataset_name(artifact_key: str) -> str:
    if artifact_key.endswith("/obs.parquet"):
        return artifact_key[: -len("/obs.parquet")]
    return artifact_key


def score_loaded_obs(
    *,
    dataset: str,
    obs_frames: Sequence[pd.DataFrame],
    perturbation_type_hint: str,
    artifact_keys: Sequence[str],
    pseudocount: float,
    limitations_prefix: str = "",
) -> list[dict[str, Any]]:
    if not obs_frames:
        return [
            row(
                dataset=dataset,
                perturbation_gene="not_applicable",
                perturbation_type=perturbation_type_hint,
                n_cells_perturbed=0,
                n_controls=0,
                score="not_applicable",
                score_method="not_applicable_no_obs_loaded",
                confidence="none",
                limitations=limitations_prefix + "no obs metadata could be loaded",
                artifact_keys=artifact_keys,
            )
        ]
    obs = pd.concat(obs_frames, axis=0, copy=False)
    control_mask, control_method = infer_control_mask(obs)
    n_controls = int(control_mask.sum())
    gene_series, gene_column = infer_gene_series(obs, control_mask)
    perturbation_type = infer_perturbation_type(obs, perturbation_type_hint)

    if n_controls <= 0:
        return [
            row(
                dataset=dataset,
                perturbation_gene="not_applicable",
                perturbation_type=perturbation_type,
                n_cells_perturbed=int(gene_series.notna().sum()),
                n_controls=0,
                score="not_applicable",
                score_method="not_applicable_no_identifiable_controls",
                confidence="none",
                limitations=(
                    limitations_prefix
                    + f"{control_method}; cannot compute cell-count ratio without controls"
                ),
                artifact_keys=artifact_keys,
            )
        ]
    if gene_column is None or int(gene_series.notna().sum()) <= 0:
        return [
            row(
                dataset=dataset,
                perturbation_gene="not_applicable",
                perturbation_type=perturbation_type,
                n_cells_perturbed=0,
                n_controls=n_controls,
                score="not_applicable",
                score_method="not_applicable_no_perturbation_gene_column",
                confidence="none",
                limitations=limitations_prefix + "controls found but no perturbation gene column was identifiable",
                artifact_keys=artifact_keys,
            )
        ]

    rows: list[dict[str, Any]] = []
    counts = gene_series.dropna().value_counts().sort_index()
    for gene, n_cells in counts.items():
        score = math.log2((int(n_cells) + pseudocount) / (n_controls + pseudocount))
        rows.append(
            row(
                dataset=dataset,
                perturbation_gene=str(gene),
                perturbation_type=perturbation_type,
                n_cells_perturbed=int(n_cells),
                n_controls=n_controls,
                score=round(score, 6),
                score_method=f"cell_count_log2_ratio_vs_controls;pseudocount={pseudocount};gene_column={gene_column};{control_method}",
                confidence="low",
                limitations=(
                    limitations_prefix
                    + "end-point abundance proxy only; no baseline guide library abundance, replicate normalization, or survival assay calibration applied"
                ),
                artifact_keys=artifact_keys,
            )
        )
    return rows


def row(**kwargs: Any) -> dict[str, Any]:
    artifact_keys = kwargs.pop("artifact_keys", ())
    return {
        "dataset": kwargs["dataset"],
        "perturbation_gene": kwargs["perturbation_gene"],
        "perturbation_type": kwargs["perturbation_type"],
        "n_cells_perturbed": kwargs["n_cells_perturbed"],
        "n_controls": kwargs["n_controls"],
        "score": kwargs["score"],
        "score_method": kwargs["score_method"],
        "confidence/limitations": f"{kwargs['confidence']}: {kwargs['limitations']}",
        "artifact_keys": ";".join(artifact_keys),
    }


def inventory_row(candidate: CandidateDataset, reason: str) -> dict[str, Any]:
    return row(
        dataset=candidate.dataset,
        perturbation_gene="not_evaluated",
        perturbation_type=candidate.perturbation_type,
        n_cells_perturbed=candidate.total_manifest_cells or 0,
        n_controls=0,
        score="not_applicable",
        score_method="inventory_only_not_loaded",
        confidence="inventory_only",
        limitations=(
            f"{reason}; manifest source={candidate.source}; "
            f"manifest_control_availability={candidate.manifest_control_availability}; "
            "run with --dataset-id/--dataset-contains and sufficient member limits to score"
        ),
        artifact_keys=candidate.artifact_keys[:3],
    )


def selected_candidates(candidates: list[CandidateDataset], args: argparse.Namespace) -> tuple[list[CandidateDataset], set[str]]:
    selected = candidates
    explicit_ids = {str(value) for value in args.dataset_id}
    if explicit_ids:
        selected = [candidate for candidate in selected if candidate.dataset in explicit_ids]
    if args.dataset_contains:
        needle = args.dataset_contains.lower()
        selected = [candidate for candidate in selected if needle in candidate.dataset.lower()]
    if args.max_datasets_to_score >= 0:
        selected = selected[: args.max_datasets_to_score]
    return selected, {candidate.dataset for candidate in selected}


def load_obs_for_candidate(ln: Any, candidate: CandidateDataset, max_members: int) -> tuple[list[pd.DataFrame], list[str], list[str]]:
    keys = list(candidate.artifact_keys)
    if max_members >= 0:
        keys = keys[:max_members]
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    for key in keys:
        try:
            artifact = ln.Artifact.get(key=key)
            loaded = artifact.load()
            if not isinstance(loaded, pd.DataFrame):
                loaded = pd.DataFrame(loaded)
            frames.append(loaded)
        except Exception as exc:  # noqa: BLE001 - preserve per-artifact failures in output
            errors.append(f"{key}: {type(exc).__name__}: {exc}")
    return frames, keys, errors


def main() -> int:
    args = parse_args()
    ensure_project_cache()

    candidates = load_manifest_candidates(args.manifest)
    explicit_candidates = [
        CandidateDataset(
            dataset=artifact_dataset_name(key),
            artifact_keys=(key,),
            perturbation_type="unknown",
            manifest_control_availability="unknown",
            source="explicit_artifact_key",
        )
        for key in args.artifact_key
    ]
    all_candidates = candidates + explicit_candidates
    if not all_candidates:
        raise SystemExit(
            "No compatible candidates found. Provide --manifest with a unified manifest or at least one --artifact-key."
        )

    to_score, selected_names = selected_candidates(candidates, args)
    to_score.extend(explicit_candidates)

    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if candidate.dataset not in selected_names:
            rows.append(inventory_row(candidate, "compatible scRNA-seq genetic perturbation dataset inventoried but not loaded by safety cap"))

    if to_score:
        ln = connect_pertdata()
        if ln.setup.settings.instance.slug != "laminlabs/pertdata" or ln.setup.settings.branch.name != "jkobject":
            raise RuntimeError("Unexpected Lamin instance/branch after connect_pertdata().")
        for candidate in to_score:
            frames, keys, errors = load_obs_for_candidate(ln, candidate, args.max_members_per_dataset)
            limitation_prefix = ""
            if args.max_members_per_dataset >= 0 and len(candidate.artifact_keys) > len(keys):
                limitation_prefix += f"partial scan {len(keys)}/{len(candidate.artifact_keys)} obs members; "
            if errors:
                limitation_prefix += "load_errors=" + " | ".join(errors[:3]) + "; "
            rows.extend(
                score_loaded_obs(
                    dataset=candidate.dataset,
                    obs_frames=frames,
                    perturbation_type_hint=candidate.perturbation_type,
                    artifact_keys=keys,
                    pseudocount=args.pseudocount,
                    limitations_prefix=limitation_prefix,
                )
            )

    output = args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    result = pd.DataFrame(rows)
    result = result.sort_values(["score_method", "dataset", "perturbation_gene"]).reset_index(drop=True)
    result.to_csv(output, sep="\t", index=False)

    summary = {
        "output": str(output),
        "rows": int(len(result)),
        "compatible_datasets_inventoried": int(len(candidates)),
        "datasets_scored_or_attempted": int(len(to_score)),
        "numeric_scores": int(pd.to_numeric(result["score"], errors="coerce").notna().sum()),
        "not_applicable_rows": int((result["score"] == "not_applicable").sum()),
    }
    if args.write_json_summary:
        import json

        summary_path = output.with_suffix(".summary.json")
        summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        summary["summary_json"] = str(summary_path)
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
