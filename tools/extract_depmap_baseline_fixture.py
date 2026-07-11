#!/usr/bin/env python3
"""Extract a provenance-safe exact-ModelID DepMap baseline fixture.

Run this against the reviewed immutable 26Q1 expression CSV on the EU worker;
do not substitute an ad-hoc JSON baseline or select/aggregate duplicate ModelIDs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path

from pert_gym.depmap_baseline_fixture import (
    extract_exact_modelid_baseline,
    sha256_file,
)


def _stable_model_id(value: str) -> str:
    return value.split("::", 1)[0].strip()


def _requested_model_ids(subset_path: Path) -> set[str]:
    with subset_path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not reader.fieldnames or "depmap_id" not in reader.fieldnames:
            raise ValueError("PRISM subset TSV must have a depmap_id column")
        requested = {_stable_model_id(str(row["depmap_id"])) for row in reader}
    requested.discard("")
    if not requested:
        raise ValueError("PRISM subset TSV has no exact stable ModelIDs")
    return requested


def _commit() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prism-subset", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--source-generation", required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists() or args.report.exists():
        raise FileExistsError(
            "refusing to overwrite an existing immutable fixture or report"
        )

    requested = _requested_model_ids(args.prism_subset)
    command = " ".join(sys.argv)
    fixture = extract_exact_modelid_baseline(
        source_path=args.source,
        requested_model_ids=requested,
        source_uri=args.source_uri,
        source_generation=args.source_generation,
        expected_source_sha256=args.source_sha256,
        extraction_command=command,
        commit=_commit(),
    )
    fixture["provenance"]["inputs"] = {
        "prism_subset": str(args.prism_subset),
        "prism_subset_sha256": sha256_file(args.prism_subset),
    }
    fixture["provenance"]["extraction"]["requested_unique_model_ids"] = len(requested)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")
    report = {
        "schema_version": "depmap_exact_modelid_baseline_extraction_report.v1",
        "fixture": {
            "path": str(args.out),
            "sha256": sha256_file(args.out),
            "rows": len(fixture["rows"]),
            "feature_count": len(fixture["feature_names"]),
        },
        "provenance": fixture["provenance"],
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
