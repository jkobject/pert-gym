#!/usr/bin/env python3
"""Executable bounded-read example for a logical pert-gym manifest.

Example:
    uv run python tools/example_logical_loader.py \
      /path/to/manifest.json --rows 100:132 --blocks 2,3
"""

from __future__ import annotations

import argparse
import json
from typing import Sequence

from pert_gym.logical_dataset import open_logical_dataset


def _slice(value: str) -> slice:
    parts = value.split(":")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError("rows must be START:END")
    try:
        return slice(
            int(parts[0]) if parts[0] else None, int(parts[1]) if parts[1] else None
        )
    except ValueError as error:
        raise argparse.ArgumentTypeError("row bounds must be integers") from error


def _blocks(value: str) -> Sequence[int]:
    try:
        return [int(part) for part in value.split(",") if part]
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "blocks must be comma-separated integers"
        ) from error


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read one bounded slice from a local or remote logical dataset"
    )
    parser.add_argument("manifest", help="manifest or promotion-marker path/URI")
    parser.add_argument("--rows", type=_slice, default=slice(0, 8), help="START:END")
    parser.add_argument(
        "--blocks", type=_blocks, help="optional comma-separated block indexes"
    )
    args = parser.parse_args()

    dataset = open_logical_dataset(args.manifest)
    batch = dataset.read(rows=args.rows, blocks=args.blocks)
    print(
        json.dumps(
            {
                "dataset": dataset.name,
                "dataset_shape": list(dataset.shape),
                "block_count": dataset.block_count,
                "selected_blocks": list(batch.block_indexes),
                "selected_rows": [batch.start, batch.end],
                "batch_shape": list(batch.X.shape),
                "obs_index": batch.obs.index.astype(str).tolist(),
                "var_index": batch.var.index.astype(str).tolist(),
                "target_split": (
                    batch.obs["target_split"].astype(str).tolist()
                    if "target_split" in batch.obs
                    else None
                ),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
