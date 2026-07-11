#!/usr/bin/env python3
"""Run a bounded tagged PRISM + STRAND batch/optimizer smoke.

The optimizer has two independent heads.  Its STRAND loss is deliberately a
source-native target-label-count proxy, never a viability, survival, or
expression-response label.  This is an execution smoke, not a performance
benchmark.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pert_gym.depmap_baseline_fixture import validate_fixture_for_manifest
from pert_gym.transversal import (
    PRISM_DATASET_TAG,
    STRAND_DATASET_TAG,
    TaggedBatch,
    load_transversal_batches,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _mse_step(
    features: tuple[tuple[float, ...], ...], targets: tuple[float, ...]
) -> dict[str, float]:
    """One actual full-batch SGD update for a linear scalar head."""
    n_features = len(features[0])
    weights = [0.0] * n_features
    bias = 0.0
    predictions = [bias for _ in features]
    before = sum(
        (prediction - target) ** 2 for prediction, target in zip(predictions, targets)
    ) / len(targets)
    learning_rate = 1e-6 if n_features > 100 else 1e-3
    scale = 2.0 / len(targets)
    grad_weights = [0.0] * n_features
    grad_bias = 0.0
    for row, prediction, target in zip(features, predictions, targets):
        error = prediction - target
        grad_bias += scale * error
        for index, value in enumerate(row):
            grad_weights[index] += scale * error * value
    weights = [
        weight - learning_rate * gradient
        for weight, gradient in zip(weights, grad_weights)
    ]
    bias -= learning_rate * grad_bias
    after_predictions = [
        sum(weight * value for weight, value in zip(weights, row)) + bias
        for row in features
    ]
    after = sum(
        (prediction - target) ** 2
        for prediction, target in zip(after_predictions, targets)
    ) / len(targets)
    return {
        "loss_before": before,
        "loss_after": after,
        "learning_rate": learning_rate,
        "updated": True,
    }


def _strand_proxy_targets(batch: TaggedBatch) -> tuple[float, ...]:
    assert batch.categorical_targets is not None
    return tuple(float(len(target)) for target in batch.categorical_targets)


def _batch_summary(batch: TaggedBatch) -> dict[str, object]:
    return {
        "dataset_tag": batch.dataset_tag,
        "task_tag": batch.task_tag,
        "rows": len(batch.row_ids),
        "feature_shape": [len(batch.features), len(batch.feature_names)],
        "numeric_target_shape": [len(batch.numeric_targets), 1]
        if batch.numeric_targets
        else None,
        "categorical_target_rows": len(batch.categorical_targets)
        if batch.categorical_targets
        else None,
        "numeric_values_finite": batch.numeric_targets is None
        or all(math.isfinite(value) for value in batch.numeric_targets),
        "metadata": dict(batch.metadata),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prism-subset", type=Path, required=True)
    parser.add_argument("--prism-manifest", type=Path, required=True)
    parser.add_argument("--prism-baselines", type=Path, required=True)
    parser.add_argument("--strand-join", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    started = time.monotonic()
    baseline_payload = json.loads(args.prism_baselines.read_text())
    prism_manifest = json.loads(args.prism_manifest.read_text())
    inputs = {
        "prism_subset": {
            "path": str(args.prism_subset),
            "sha256": _sha256(args.prism_subset),
        },
        "prism_manifest": {
            "path": str(args.prism_manifest),
            "sha256": _sha256(args.prism_manifest),
        },
        "prism_baselines": {
            "path": str(args.prism_baselines),
            "sha256": _sha256(args.prism_baselines),
        },
        "strand_join": {
            "path": str(args.strand_join),
            "sha256": _sha256(args.strand_join),
        },
    }
    try:
        validate_fixture_for_manifest(
            baseline_payload, prism_manifest, args.prism_subset
        )
        batches = load_transversal_batches(
            prism_subset_path=args.prism_subset,
            prism_baseline_rows=baseline_payload["rows"],
            prism_baseline_feature_names=baseline_payload["feature_names"],
            strand_join_path=args.strand_join,
        )
    except ValueError as exc:
        report = {
            "schema_version": "transversal_multitask_smoke.v1",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "rejected",
            "inputs": inputs,
            "prism_immutable_manifest": prism_manifest,
            "rejection": {
                "type": type(exc).__name__,
                "message": str(exc),
                "policy": "non-identical duplicate baseline RNA vectors fail closed",
            },
            "commands": [" ".join(sys.argv)],
            "runtime_seconds": time.monotonic() - started,
            "runtime": {"python": sys.version, "platform": platform.platform()},
        }
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        raise
    summaries = {
        split: [_batch_summary(batch) for batch in split_batches]
        for split, split_batches in batches.by_split.items()
    }
    prism_train = next(
        batch
        for batch in batches.by_split["train"]
        if batch.dataset_tag == PRISM_DATASET_TAG
    )
    strand_train = next(
        batch
        for batch in batches.by_split["train"]
        if batch.dataset_tag == STRAND_DATASET_TAG
    )
    assert prism_train.numeric_targets is not None
    report = {
        "schema_version": "transversal_multitask_smoke.v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        "seed": 0,
        "inputs": inputs,
        "contract": dict(batches.metadata),
        "prism_immutable_manifest": prism_manifest,
        "splits": summaries,
        "optimizer_smoke": {
            "implementation": "stdlib two-head full-batch linear SGD",
            "prism_direct_lfc": _mse_step(
                prism_train.features, prism_train.numeric_targets
            ),
            "strand_source_native_target_label_count_proxy": _mse_step(
                strand_train.features, _strand_proxy_targets(strand_train)
            ),
        },
        "commands": [" ".join(sys.argv)],
        "runtime_seconds": time.monotonic() - started,
        "runtime": {"python": sys.version, "platform": platform.platform()},
        "limitations": [
            "PRISM is only the immutable 126-row/118-ModelID loader-projectable subset, not full PRISM.",
            "STRAND is source-native guide-to-label-set mapping/pretraining-sidecar supervision only; it is not viability, survival, or expression response supervision.",
            "STRAND retains source-native splits and makes no global leakage-safety claim.",
            "The reviewed STRAND join table has 16,446 aggregated perturbation-task rows; its 16,488 source label rows are represented by source-native label-count metadata rather than flattened into fabricated response rows.",
            "The optimizer smoke proves tagged routing and one update only; it makes no useful benchmark-performance claim.",
        ],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "out": str(args.out),
                "runtime_seconds": report["runtime_seconds"],
                "optimizer_smoke": report["optimizer_smoke"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
