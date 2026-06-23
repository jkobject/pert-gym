#!/usr/bin/env python3
"""Validate the row113/GSE216481 metadata contract before any Lamin write.

This is a bounded smoke/guardrail, not an ingester. It checks the existing staged
probe artifact and fails fast until the missing TF barcode/ORF-symbol map plus
filtered-cell contract are supplied.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROBE = ROOT / "artifacts/schema_audit/temporal_t29_gse216481_row113_contract_input_20260623.json"
DEFAULT_OUT = ROOT / "artifacts/schema_audit/temporal_t29_gse216481_row113_contract_validation_20260623.json"

EXPECTED_COMPONENTS = {
    "201218_RNA": {"filter_cells": 56857, "filter_genes": 36844, "filter_perturbations": 139},
    "210322_TFAtlas": {"filter_cells": 527594, "filter_genes": 16873, "filter_perturbations": 1183},
}
EXCLUDED_COMPONENTS = {"PRJNA893678_ATAC", "180124_perturb", "210715_combinatorial"}
REQUIRED_OBS_FIELDS = [
    "dataset_id",
    "source",
    "source_repository",
    "geo_accession",
    "bioproject",
    "component",
    "sample_title",
    "sample_id",
    "encoded_cell_id",
    "r1_coordinate",
    "r2_coordinate",
    "r3_coordinate",
    "plate_coordinate",
    "tfmap_coordinate",
    "tfmap_sequence",
    "tfmap_numeric_id",
    "orf_id",
    "tf_symbol",
    "perturbation",
    "perturbation_type",
    "organism",
    "cell_type",
    "assay",
    "modality",
    "timepoint_raw",
    "timepoint",
    "timepoint_unit",
    "is_control",
    "is_baseline",
    "filter_contract_source",
    "label_contract_source",
    "qc_note",
]


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def nonempty_existing_path(value: str | None) -> bool:
    if not value:
        return False
    p = Path(value)
    if not p.is_absolute():
        p = ROOT / p
    return p.exists() and p.stat().st_size > 0


def validate(args: argparse.Namespace) -> dict[str, Any]:
    probe = load_json(args.probe_json)
    errors: list[str] = []
    warnings: list[str] = []

    source = probe.get("source", {})
    if source.get("staged_gcs_target") != "gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/perturbase_t29/GSE216481_RAW.tar":
        errors.append("probe source.staged_gcs_target does not match expected GSE216481 staged tar")

    filelist = probe.get("filelist", {})
    if filelist.get("member_count") != 167:
        errors.append(f"expected 167 tar members, got {filelist.get('member_count')!r}")
    if filelist.get("expected_tar_bytes") != 17_908_162_560:
        errors.append(f"expected staged tar byte size 17908162560, got {filelist.get('expected_tar_bytes')!r}")

    components = probe.get("components", {})
    for component, expected in EXPECTED_COMPONENTS.items():
        rec = (components.get(component) or {}).get("perturbase_record") or {}
        if rec.get("qc") != "Pass" or rec.get("modality") != "RNA":
            errors.append(f"{component} is not recorded as QC-pass RNA in probe artifact")
        for key, value in expected.items():
            if rec.get(key) != value:
                errors.append(f"{component}.{key} expected {value}, got {rec.get(key)!r}")

    decisions = probe.get("decisions", {})
    active = set(decisions.get("rna_components_identified") or [])
    excluded = set(decisions.get("excluded_from_canonical_x") or [])
    if set(EXPECTED_COMPONENTS) - active:
        errors.append(f"missing active RNA components in probe decisions: {sorted(set(EXPECTED_COMPONENTS) - active)}")
    if EXCLUDED_COMPONENTS - excluded:
        errors.append(f"missing excluded components in probe decisions: {sorted(EXCLUDED_COMPONENTS - excluded)}")

    missing_required_inputs: list[str] = []
    if not nonempty_existing_path(args.label_map):
        missing_required_inputs.append("label_map: barcode/sequence/numeric-id to ORF/TF-symbol lookup")
    if not nonempty_existing_path(args.filtered_cells):
        missing_required_inputs.append("filtered_cells: component-specific filtered-cell inclusion table/predicate")

    if args.label_map and not nonempty_existing_path(args.label_map):
        warnings.append(f"label_map path was supplied but does not exist or is empty: {args.label_map}")
    if args.filtered_cells and not nonempty_existing_path(args.filtered_cells):
        warnings.append(f"filtered_cells path was supplied but does not exist or is empty: {args.filtered_cells}")

    status = "ready_for_converter_smoke" if not errors and not missing_required_inputs else "contract_incomplete"
    if errors:
        status = "probe_inconsistent"

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": status,
        "probe_json": str(args.probe_json.relative_to(ROOT) if args.probe_json.is_relative_to(ROOT) else args.probe_json),
        "active_components": sorted(EXPECTED_COMPONENTS),
        "excluded_from_canonical_x": sorted(EXCLUDED_COMPONENTS),
        "required_obs_fields": REQUIRED_OBS_FIELDS,
        "missing_required_inputs": missing_required_inputs,
        "errors": errors,
        "warnings": warnings,
        "next_action": (
            "Recover PerturBase repository 1 filtered object/metadata export or original TF atlas supplementary/library table containing barcode/sequence/numeric-id to ORF/TF-symbol mapping and filtered cell inclusion."
            if missing_required_inputs else
            "Proceed to a one-sample/one-chunk converter smoke for 201218_RNA, then verify obs->X->var links before full chunking."
        ),
    }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe-json", type=Path, default=DEFAULT_PROBE)
    parser.add_argument("--label-map", help="Path to recovered barcode/sequence/numeric-id to ORF/TF-symbol map")
    parser.add_argument("--filtered-cells", help="Path to recovered filtered-cell inclusion table/predicate")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--allow-incomplete", action="store_true", help="Write status JSON and exit 0 even when required contract inputs are missing")
    args = parser.parse_args()

    if not args.probe_json.exists():
        raise SystemExit(f"missing probe JSON: {args.probe_json}")

    result = validate(args)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))

    if result["errors"]:
        return 2
    if result["missing_required_inputs"] and not args.allow_incomplete:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
