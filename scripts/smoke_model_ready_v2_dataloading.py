#!/usr/bin/env python
"""Metadata-only smoke for model_ready_v2 heterogeneous dataloading.

This script intentionally reads only the accepted manifest artifact. It does not
connect to Lamin, bulk-download payloads, or load large X/image matrices on the
Mac. Each emitted row is a loader batch contract smoke carrying handles plus the
standard pert-gym fields downstream loaders need.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

from pert_gym.benchmarks import (
    load_model_ready_v2_adapters,
    load_model_ready_v2_batches,
)

DEFAULT_MANIFEST = Path("artifacts/schema_audit/model_ready_v2_manifest_20260708.json")


def _counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def build_summary(manifest_path: Path) -> dict[str, Any]:
    adapters = load_model_ready_v2_adapters(manifest_path=manifest_path)
    batches = load_model_ready_v2_batches(manifest_path=manifest_path)
    batch_dicts = [asdict(batch) for batch in batches]
    return {
        "manifest_path": str(manifest_path),
        "batch_count": len(batches),
        "adapter_counts": {
            "responses": len(adapters.responses),
            "expressions": len(adapters.expressions),
            "images": len(adapters.images),
            "mappings": len(adapters.mappings),
            "skipped": len(adapters.skipped),
        },
        "by_modality": _counter_dict(Counter(batch.modality for batch in batches)),
        "by_source_dataset": _counter_dict(Counter(batch.source_dataset for batch in batches)),
        "target_masks": _counter_dict(Counter(str(batch.target_mask).lower() for batch in batches)),
        "required_field_failures": _required_field_failures(batch_dicts),
        "skipped": dict(adapters.skipped),
        "sample_batches": batch_dicts[:10],
    }


def _required_field_failures(batch_dicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    required = (
        "features",
        "perturbation",
        "source_dataset",
        "split",
        "organism",
        "modality",
        "target_mask",
    )
    failures: list[dict[str, Any]] = []
    for idx, batch in enumerate(batch_dicts):
        missing = [field for field in required if batch.get(field) in (None, "", {})]
        if missing:
            failures.append(
                {
                    "batch_index": idx,
                    "source_dataset": batch.get("source_dataset"),
                    "modality": batch.get("modality"),
                    "missing": missing,
                }
            )
    return failures


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    summary = build_summary(args.manifest)
    text = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
    print(text, end="")
    if summary["required_field_failures"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
