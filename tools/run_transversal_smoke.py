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
import subprocess
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

EXPECTED_PRISM_SUBSET_SHA256 = (
    "b4f9abda6162d3e8a13149f384417a26a7589acd840a32bb47abfc9deeba51a1"
)
EXPECTED_DEPMAP_FIXTURE_SHA256 = (
    "2d055813bcd4e00ae7aecd86c00a772e2342a036181cf6107488444e7790d3bf"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_immutable_json_report(out: Path, report: dict[str, object]) -> Path:
    """Write one report and a readback checksum sidecar without overwriting either."""

    sidecar = out.with_suffix(out.suffix + ".sha256")
    if out.exists() or sidecar.exists():
        raise FileExistsError("refusing to overwrite an immutable smoke report")
    out.parent.mkdir(parents=True, exist_ok=True)
    report_text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    created: list[Path] = []
    try:
        with out.open("x", encoding="utf-8") as handle:
            created.append(out)
            handle.write(report_text)
        report_sha256 = _sha256(out)
        with sidecar.open("x", encoding="utf-8") as handle:
            created.append(sidecar)
            handle.write(f"{report_sha256}  {out.name}\n")
    except BaseException:
        for path in reversed(created):
            path.unlink(missing_ok=True)
        raise
    return sidecar


def _commit() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


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
    parameter_delta_l2 = math.sqrt(sum(weight**2 for weight in weights) + bias**2)
    return {
        "loss_before": before,
        "loss_after": after,
        "learning_rate": learning_rate,
        "parameter_delta_l2": parameter_delta_l2,
        "updated": True,
    }


def _pairwise_overlaps(
    values_by_split: dict[str, set[str]],
) -> dict[str, dict[str, object]]:
    """Return deterministic overlap witnesses for every train/val/test pair."""

    return {
        f"{left}/{right}": {
            "count": len(values_by_split[left] & values_by_split[right]),
            "values": sorted(values_by_split[left] & values_by_split[right]),
        }
        for left, right in (("train", "val"), ("train", "test"), ("val", "test"))
    }


def _prism_leakage_report(batches: dict[str, TaggedBatch]) -> dict[str, object]:
    """Expose zero-required held-out overlap checks and ModelID context separately."""

    source_rows = {split: set(batch.row_ids) for split, batch in batches.items()}
    held_out_entities = {
        split: set(batch.metadata["compound_ids"]) for split, batch in batches.items()
    }
    model_ids = {
        split: set(batch.metadata["model_ids"]) for split, batch in batches.items()
    }
    required = {
        "source_row_identifier": _pairwise_overlaps(source_rows),
        "held_out_perturbation_id": _pairwise_overlaps(held_out_entities),
    }
    if any(
        item["count"] != 0
        for overlaps in required.values()
        for item in overlaps.values()
    ):
        raise ValueError("PRISM required held-out entity/source-row leakage detected")
    return {
        "required_zero_overlap": required,
        "context_model_id_overlap_not_a_held_out_entity_policy": _pairwise_overlaps(
            model_ids
        ),
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
    parser.add_argument("--strand-metadata", type=Path, required=True)
    parser.add_argument("--prism-subset-uri", required=True)
    parser.add_argument("--prism-subset-generation", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    started = time.monotonic()
    baseline_payload = json.loads(args.prism_baselines.read_text())
    prism_manifest = json.loads(args.prism_manifest.read_text())
    strand_metadata = json.loads(args.strand_metadata.read_text())
    inputs = {
        "prism_subset": {
            "uri": args.prism_subset_uri,
            "generation": args.prism_subset_generation,
            "sha256": _sha256(args.prism_subset),
        },
        "prism_manifest": {
            "path": str(args.prism_manifest),
            "sha256": _sha256(args.prism_manifest),
        },
        "prism_baseline_fixture": {
            "path": str(args.prism_baselines),
            "sha256": _sha256(args.prism_baselines),
            "source": baseline_payload.get("provenance", {}).get("source"),
        },
        "strand_join": {
            "path": str(args.strand_join),
            "sha256": _sha256(args.strand_join),
            "metadata_path": str(args.strand_metadata),
            "metadata_sha256": _sha256(args.strand_metadata),
        },
    }
    try:
        if inputs["prism_subset"]["sha256"] != EXPECTED_PRISM_SUBSET_SHA256:
            raise ValueError(
                "PRISM subset SHA256 differs from the reviewed 126-row input"
            )
        if inputs["prism_baseline_fixture"]["sha256"] != EXPECTED_DEPMAP_FIXTURE_SHA256:
            raise ValueError("DepMap fixture SHA256 differs from the reviewed input")
        strand_counts = strand_metadata.get("counts")
        if not isinstance(strand_counts, dict) or {
            "unmatched_unique_perturbation_rows_before_resolution": strand_counts.get(
                "unmatched_unique_perturbation_rows_before_resolution"
            ),
            "resolved_unique_perturbation_rows": strand_counts.get(
                "resolved_unique_perturbation_rows"
            ),
            "unresolved_unique_perturbation_rows_after_resolution": strand_counts.get(
                "unresolved_unique_perturbation_rows_after_resolution"
            ),
        } != {
            "unmatched_unique_perturbation_rows_before_resolution": 403,
            "resolved_unique_perturbation_rows": 401,
            "unresolved_unique_perturbation_rows_after_resolution": 2,
        }:
            raise ValueError(
                "STRAND metadata does not preserve the reviewed 403/401/2 policy"
            )
        exclusions = strand_metadata.get("loader_exclusions", {}).get(
            "unresolved_by_file"
        )
        if not isinstance(exclusions, dict) or {
            (mapping_file, item.get("perturbation"))
            for mapping_file, items in exclusions.items()
            for item in items
        } != {("k562-de.csv", "ELOB"), ("k562-dir.csv", "ELOB")}:
            raise ValueError(
                "STRAND metadata does not preserve exactly two k562 ELOB exclusions"
            )
        if (
            strand_metadata.get("outputs", {}).get("table_tsv_sha256")
            != inputs["strand_join"]["sha256"]
        ):
            raise ValueError(
                "STRAND metadata table SHA256 does not match the joined input"
            )
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
            "schema_version": "transversal_multitask_smoke.v2",
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
        _write_immutable_json_report(args.out, report)
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
    prism_batches = {
        split: next(
            batch for batch in split_batches if batch.dataset_tag == PRISM_DATASET_TAG
        )
        for split, split_batches in batches.by_split.items()
    }
    leakage_checks = _prism_leakage_report(prism_batches)
    optimizer_smoke = {
        "implementation": "stdlib two-head full-batch linear SGD",
        "prism_direct_lfc": _mse_step(
            prism_train.features, prism_train.numeric_targets
        ),
        "strand_source_native_target_label_count_proxy": _mse_step(
            strand_train.features, _strand_proxy_targets(strand_train)
        ),
    }
    if any(
        not math.isfinite(value)
        for result in optimizer_smoke.values()
        if isinstance(result, dict)
        for value in result.values()
        if isinstance(value, float)
    ) or any(
        result["parameter_delta_l2"] <= 0.0
        for result in optimizer_smoke.values()
        if isinstance(result, dict)
    ):
        raise ValueError(
            "optimizer smoke did not produce finite nonzero update witnesses"
        )
    report = {
        "schema_version": "transversal_multitask_smoke.v2",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        "seed": 0,
        "inputs": inputs,
        "contract": dict(batches.metadata),
        "prism_immutable_manifest": prism_manifest,
        "selection": {
            "prism_rows_expected": 126,
            "prism_rows_selected": sum(
                len(batch.row_ids) for batch in prism_batches.values()
            ),
            "prism_unique_model_ids": len(
                set().union(
                    *(
                        set(batch.metadata["model_ids"])
                        for batch in prism_batches.values()
                    )
                )
            ),
            "strand_join_rows_selected": sum(
                len(batch.row_ids)
                for split_batches in batches.by_split.values()
                for batch in split_batches
                if batch.dataset_tag == STRAND_DATASET_TAG
            ),
            "strand_join_rows_denominator": strand_counts["table_rows"],
            "strand_alias_policy": "403 residual -> 401 accepted aliases; exactly two k562 ELOB exclusions",
        },
        "splits": summaries,
        "leakage_checks": leakage_checks,
        "optimizer_smoke": optimizer_smoke,
        "commands": [" ".join(sys.argv)],
        "commit": _commit(),
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
    sidecar = _write_immutable_json_report(args.out, report)
    print(
        json.dumps(
            {
                "out": str(args.out),
                "readback_sha256": _sha256(args.out),
                "readback_sha256_sidecar": str(sidecar),
                "runtime_seconds": report["runtime_seconds"],
                "optimizer_smoke": report["optimizer_smoke"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
