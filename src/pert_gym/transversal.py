"""Bounded tagged batches for scientifically distinct PRISM and STRAND inputs.

This module deliberately does not merge targets: Broad PRISM direct LFC is a
numeric response task conditioned on exact release-matched baseline expression,
while STRAND is source-native guide-to-label-set mapping supervision.  Consumers
must route by ``dataset_tag`` and ``task_tag`` rather than treating STRAND as a
viability, survival, or expression-response dataset.
"""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .benchmarks import load_response_screen_with_baseline

PRISM_DATASET_TAG = "broad_prism_24q2"
PRISM_TASK_TAG = "direct_lfc"
STRAND_DATASET_TAG = "strand_perturbqa"
STRAND_TASK_TAG = "guide_to_source_native_label_set"


@dataclass(frozen=True)
class TaggedBatch:
    """One dataset/task/split batch without coercing task-specific targets."""

    dataset_tag: str
    task_tag: str
    split: str
    row_ids: tuple[str, ...]
    features: tuple[tuple[float, ...], ...]
    feature_names: tuple[str, ...]
    numeric_targets: tuple[float, ...] | None
    categorical_targets: tuple[tuple[str, ...], ...] | None
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.row_ids or len(self.row_ids) != len(self.features):
            raise ValueError(
                "row_ids and features must be non-empty and have matching rows"
            )
        if any(len(row) != len(self.feature_names) for row in self.features):
            raise ValueError("feature rows must match feature_names")
        if not all(math.isfinite(value) for row in self.features for value in row):
            raise ValueError("features must be finite")
        if (self.numeric_targets is None) == (self.categorical_targets is None):
            raise ValueError(
                "exactly one task-specific target representation is required"
            )
        if self.numeric_targets is not None:
            if len(self.numeric_targets) != len(self.row_ids):
                raise ValueError("numeric targets must have one value per row")
            if not all(math.isfinite(value) for value in self.numeric_targets):
                raise ValueError("numeric targets must be finite")
        if self.categorical_targets is not None and len(
            self.categorical_targets
        ) != len(self.row_ids):
            raise ValueError("categorical targets must have one value per row")


@dataclass(frozen=True)
class TransversalBatches:
    """Dataset-tagged, deterministic batches for the two reviewed surfaces."""

    by_split: Mapping[str, tuple[TaggedBatch, ...]]
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        if set(self.by_split) != {"train", "val", "test"}:
            raise ValueError("transversal batches require train/val/test keys")
        prism_by_split: dict[str, TaggedBatch] = {}
        for split, batches in self.by_split.items():
            for batch in batches:
                if batch.split != split:
                    raise ValueError("batch split key must match batch.split")
                if batch.dataset_tag == PRISM_DATASET_TAG:
                    if (
                        batch.task_tag != PRISM_TASK_TAG
                        or batch.numeric_targets is None
                    ):
                        raise ValueError("PRISM must remain a numeric direct-LFC task")
                    prism_by_split[split] = batch
                if batch.dataset_tag == STRAND_DATASET_TAG:
                    if (
                        batch.task_tag != STRAND_TASK_TAG
                        or batch.categorical_targets is None
                    ):
                        raise ValueError(
                            "STRAND must remain mapping-sidecar supervision"
                        )
                    if batch.metadata.get("source_native_split") is not True:
                        raise ValueError(
                            "STRAND source-native split limitation must be retained"
                        )
        if set(prism_by_split) != {"train", "val", "test"}:
            raise ValueError("PRISM must provide all deterministic splits")
        _validate_prism_leakage(prism_by_split)


def load_transversal_batches(
    *,
    prism_subset_path: Path | str,
    prism_baseline_rows: Sequence[Mapping[str, Any]],
    prism_baseline_feature_names: Sequence[str],
    strand_join_path: Path | str,
) -> TransversalBatches:
    """Load reviewed PRISM + STRAND inputs into non-coerced tagged batches.

    ``prism_baseline_rows`` must be read from the immutable DepMap 26Q1 baseline
    generation named in the PRISM manifest.  STRAND reads the reviewed join table
    directly and intentionally emits only source-native mapping labels.
    """

    prism_rows = _read_tsv(prism_subset_path)
    strand_rows = _read_tsv(strand_join_path)
    prism_batches = _build_prism_batches(
        prism_rows, prism_baseline_rows, prism_baseline_feature_names
    )
    strand_batches = _build_strand_batches(strand_rows)
    by_split = {
        split: tuple(prism_batches[split] + strand_batches.get(split, []))
        for split in ("train", "val", "test")
    }
    return TransversalBatches(
        by_split=by_split,
        metadata={
            "contract": "tagged_multitask_no_target_coercion",
            "prism": {
                "dataset_tag": PRISM_DATASET_TAG,
                "task_tag": PRISM_TASK_TAG,
                "target_semantics": "direct raw LFC",
                "baseline_join": "exact stable DepMap ModelID",
                "source_rows": len(prism_rows),
            },
            "strand": {
                "dataset_tag": STRAND_DATASET_TAG,
                "task_tag": STRAND_TASK_TAG,
                "target_semantics": "source-native guide-to-label-set mapping only",
                "source_native_split_limitation": True,
                "task_rows": len(strand_rows),
            },
        },
    )


def _build_prism_batches(
    rows: Sequence[Mapping[str, str]],
    baseline_rows: Sequence[Mapping[str, Any]],
    feature_names: Sequence[str],
) -> dict[str, list[TaggedBatch]]:
    required = {
        "source_row_identifier",
        "perturbation_id",
        "depmap_id",
        "response_value",
        "split",
    }
    _require_columns(rows, required, "PRISM subset")
    batches: dict[str, list[TaggedBatch]] = {}
    for split in ("train", "val", "test"):
        split_rows = [row for row in rows if row["split"] == split]
        if not split_rows:
            raise ValueError(f"PRISM subset is missing {split} rows")
        response = load_response_screen_with_baseline(
            response_rows=split_rows,
            baseline_rows=baseline_rows,
            feature_names=feature_names,
        )
        assert response.target_response is not None
        batches[split] = [
            TaggedBatch(
                dataset_tag=PRISM_DATASET_TAG,
                task_tag=PRISM_TASK_TAG,
                split=split,
                row_ids=tuple(row["source_row_identifier"] for row in split_rows),
                features=tuple(tuple(row) for row in response.X),
                feature_names=tuple(response.feature_names),
                numeric_targets=tuple(row[0] for row in response.target_response),
                categorical_targets=None,
                metadata={
                    "model_ids": tuple(
                        row["depmap_id"].split("::", 1)[0] for row in split_rows
                    ),
                    "compound_ids": tuple(row["perturbation_id"] for row in split_rows),
                    "response_metric": "lfc",
                    "source_row_ids_are_immutable": True,
                },
            )
        ]
    return batches


def _build_strand_batches(
    rows: Sequence[Mapping[str, str]],
) -> dict[str, list[TaggedBatch]]:
    required = {
        "join_row_id",
        "perturbqa_split",
        "perturbqa_target_label_sample",
        "guide_raw_token_count",
        "guide_parsed_non_control_count",
        "guide_control_token_count",
        "guide_tss_proxy_true_count",
        "model_ready_status",
    }
    _require_columns(rows, required, "STRAND join table")
    if any(row["model_ready_status"] != "loader_projectable_only" for row in rows):
        raise ValueError("STRAND rows must remain loader_projectable_only")
    batches: dict[str, list[TaggedBatch]] = {}
    for split in ("train", "val", "test"):
        split_rows = [row for row in rows if row["perturbqa_split"] == split]
        if not split_rows:
            continue
        targets = tuple(
            tuple(
                label
                for label in row["perturbqa_target_label_sample"].split(";")
                if label
            )
            for row in split_rows
        )
        if any(not target for target in targets):
            raise ValueError("STRAND source-native label samples must be non-empty")
        batches[split] = [
            TaggedBatch(
                dataset_tag=STRAND_DATASET_TAG,
                task_tag=STRAND_TASK_TAG,
                split=split,
                row_ids=tuple(row["join_row_id"] for row in split_rows),
                features=tuple(
                    tuple(
                        float(row[field])
                        for field in (
                            "guide_raw_token_count",
                            "guide_parsed_non_control_count",
                            "guide_control_token_count",
                            "guide_tss_proxy_true_count",
                        )
                    )
                    for row in split_rows
                ),
                feature_names=(
                    "guide_raw_token_count",
                    "guide_parsed_non_control_count",
                    "guide_control_token_count",
                    "guide_tss_proxy_true_count",
                ),
                numeric_targets=None,
                categorical_targets=targets,
                metadata={
                    "source_native_split": True,
                    "global_leakage_audited": False,
                    "not_viability_or_survival": True,
                    "not_expression_response": True,
                },
            )
        ]
    return batches


def _read_tsv(path: Path | str) -> list[dict[str, str]]:
    with Path(path).open(newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _require_columns(
    rows: Sequence[Mapping[str, str]], required: set[str], name: str
) -> None:
    if not rows:
        raise ValueError(f"{name} is empty")
    missing = required - set(rows[0])
    if missing:
        raise ValueError(f"{name} missing required columns: {sorted(missing)}")


def _validate_prism_leakage(batches: Mapping[str, TaggedBatch]) -> None:
    values = list(batches.values())
    for field in ("row_ids",):
        sets = [set(getattr(batch, field)) for batch in values]
        if any(left & right for i, left in enumerate(sets) for right in sets[i + 1 :]):
            raise ValueError(f"PRISM {field} leak across splits")
    for key in ("compound_ids",):
        sets = [set(batch.metadata[key]) for batch in values]
        if any(left & right for i, left in enumerate(sets) for right in sets[i + 1 :]):
            raise ValueError(f"PRISM {key} leak across splits")
