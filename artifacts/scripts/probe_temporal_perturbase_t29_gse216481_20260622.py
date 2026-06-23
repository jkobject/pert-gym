#!/usr/bin/env python3
"""Probe GSE216481 / PerturBase T29 row113 without full-loading matrices.

This script deliberately performs source/manifest/metadata inspection only:
- fetches GEO filelist and SOFT metadata;
- computes TAR member byte offsets from the listed member order/sizes;
- range-reads selected gzip members just far enough to inspect headers/examples;
- duplicate-checks Lamin keys on laminlabs/pertdata branch jkobject;
- writes a JSON + Markdown status artifact.

It does not register Lamin artifacts and does not materialize the 17.9 GB tar locally.
"""
from __future__ import annotations

import csv
import gzip
import importlib.util
import io
import json
import math
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
OUT_JSON = ROOT / "artifacts/schema_audit/temporal_t29_gse216481_row113_probe_20260622.json"
OUT_MD = ROOT / "artifacts/schema_audit/temporal_t29_gse216481_row113_probe_20260622.md"
OUT_FILELIST = ROOT / "artifacts/schema_audit/temporal_t29_gse216481_filelist_20260622.txt"

GEO_SUPPL = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE216nnn/GSE216481/suppl/"
RAW_TAR_URL = GEO_SUPPL + "GSE216481_RAW.tar"
FILELIST_URL = GEO_SUPPL + "filelist.txt"
SOFT_URL = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE216nnn/GSE216481/soft/GSE216481_family.soft.gz"
EXPECTED_TAR_BYTES = 17_908_162_560
STAGED_GCS = "gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/perturbase_t29/GSE216481_RAW.tar"

PERTURBASE_RECORDS = {
    "201218_RNA": {
        "id": 1,
        "modality": "RNA",
        "qc": "Pass",
        "filter_cells": 56857,
        "filter_genes": 36844,
        "filter_perturbations": 139,
        "raw_cells": 69085,
        "raw_genes": 36844,
        "raw_perturbations": 196,
        "notes": "201218 directed-differentiation RNA, H1 hESC overexpression, D4/D7.",
    },
    "210322_TFAtlas": {
        "id": 32,
        "modality": "RNA",
        "qc": "Pass",
        "filter_cells": 527594,
        "filter_genes": 16873,
        "filter_perturbations": 1183,
        "raw_cells": 623153,
        "raw_genes": 16873,
        "raw_perturbations": 1604,
        "notes": "Large TFAtlas RNA component; expression CSVs are dense gene x encoded-cell tables.",
    },
    "PRJNA893678_ATAC": {
        "id": 73,
        "modality": "ATAC",
        "qc": "Pass",
        "filter_cells": 69085,
        "filter_genes": 865996,
        "filter_perturbations": 196,
        "raw_cells": 69085,
        "raw_genes": 865996,
        "raw_perturbations": 196,
        "notes": "ATAC component; exclude from canonical RNA X.h5ad unless represented later as typed auxiliary modality.",
    },
    "180124_perturb": {"id": 98, "modality": "RNA", "qc": "Failed", "filter_cells": 0, "filter_genes": 0, "filter_perturbations": 0},
    "210715_combinatorial": {"id": 99, "modality": "RNA", "qc": "Failed", "filter_cells": 0, "filter_genes": 0, "filter_perturbations": 0},
}


def fetch_text(url: str, timeout: int = 60) -> str:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_filelist(text: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    tar_offset = 0
    for line in text.splitlines():
        if line.startswith("Archive\t"):
            continue
        if not line.startswith("File\t"):
            continue
        _, name, timestamp, size_s, typ = line.split("\t")[:5]
        size = int(size_s)
        entries.append(
            {
                "name": name,
                "timestamp": timestamp,
                "size_bytes": size,
                "type": typ,
                "tar_header_offset": tar_offset,
                "tar_data_offset": tar_offset + 512,
                "tar_data_end_inclusive": tar_offset + 512 + size - 1,
            }
        )
        tar_offset += 512 + math.ceil(size / 512) * 512
    return entries


def parse_soft_samples() -> dict[str, dict[str, str]]:
    r = requests.get(SOFT_URL, timeout=60)
    r.raise_for_status()
    text = gzip.decompress(r.content).decode("utf-8", "replace")
    samples: dict[str, dict[str, str]] = {}
    acc: str | None = None
    current: dict[str, str] = {}
    characteristics: list[str] = []
    for line in text.splitlines():
        if line.startswith("^SAMPLE"):
            if acc:
                current["characteristics"] = "; ".join(characteristics)
                samples[acc] = current
            acc = line.split("=", 1)[1].strip()
            current = {"geo_accession": acc}
            characteristics = []
        elif acc and line.startswith("!Sample_title"):
            current["title"] = line.split("=", 1)[1].strip()
        elif acc and line.startswith("!Sample_source_name_ch1"):
            current["source_name"] = line.split("=", 1)[1].strip()
        elif acc and line.startswith("!Sample_description"):
            current["description"] = line.split("=", 1)[1].strip()
        elif acc and line.startswith("!Sample_characteristics_ch1"):
            characteristics.append(line.split("=", 1)[1].strip())
    if acc:
        current["characteristics"] = "; ".join(characteristics)
        samples[acc] = current
    return samples


def component_for(name: str) -> str:
    if "201218_RNA" in name:
        return "201218_RNA"
    if "210322_TFAtlas" in name:
        return "210322_TFAtlas"
    if "201218_ATAC" in name:
        return "PRJNA893678_ATAC"
    if "180124_perturb" in name:
        return "180124_perturb"
    if "210715_combinatorial" in name:
        return "210715_combinatorial"
    if "201218_TFmap" in name:
        return "201218_TFmap"
    if "210322_TFmap" in name:
        return "210322_TFmap"
    return "other"


def sample_id_from_member(name: str) -> str:
    m = re.match(r"(GSM\d+)_", name)
    return m.group(1) if m else ""


def range_response(entry: dict[str, Any], stream: bool = True) -> requests.Response:
    headers = {"Range": f"bytes={entry['tar_data_offset']}-{entry['tar_data_end_inclusive']}"}
    r = requests.get(RAW_TAR_URL, headers=headers, stream=stream, timeout=120)
    r.raise_for_status()
    if r.status_code != 206:
        raise RuntimeError(f"range request did not return 206 for {entry['name']}: {r.status_code}")
    return r


def read_gzip_first_line(entry: dict[str, Any]) -> str:
    r = range_response(entry, stream=True)
    try:
        with gzip.GzipFile(fileobj=r.raw) as gz:
            return gz.readline().decode("utf-8", "replace").rstrip("\r\n")
    finally:
        r.close()


def read_gzip_first_rows(entry: dict[str, Any], n: int = 5) -> list[str]:
    r = range_response(entry, stream=True)
    rows: list[str] = []
    try:
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for _ in range(n):
                line = gz.readline()
                if not line:
                    break
                rows.append(line.decode("utf-8", "replace").rstrip("\r\n"))
    finally:
        r.close()
    return rows


def count_gzip_rows_small(entry: dict[str, Any]) -> int:
    # Only used for small TFmap files, not dense expression matrices.
    r = range_response(entry, stream=True)
    count = 0
    try:
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for _ in gz:
                count += 1
    finally:
        r.close()
    return count


def csv_header_column_count(line: str) -> int:
    # Expression files are tab-delimited gene x cell tables. First field is "gene".
    return max(0, len(line.split("\t")) - 1)


def build_duplicate_probe() -> dict[str, Any]:
    lamin_spec = importlib.util.spec_from_file_location(
        "pert_gym_lamin_context", ROOT / "tools" / "lamin_context.py"
    )
    if lamin_spec is None or lamin_spec.loader is None:
        raise RuntimeError("could not load tools/lamin_context.py")
    lamin_module = importlib.util.module_from_spec(lamin_spec)
    lamin_spec.loader.exec_module(lamin_module)
    connect_pertdata = lamin_module.connect_pertdata

    ln = connect_pertdata()
    assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
    assert ln.setup.settings.branch.name == "jkobject"
    terms = [
        "GSE216481",
        "PRJNA893678",
        "201218_RNA",
        "210322_TFAtlas",
        "PRJNA893678_ATAC",
        "180124_perturb",
        "210715_combinatorial",
        "temporal_pretraining/perturbase/gse216481",
        "tfatlas",
    ]
    out: dict[str, Any] = {
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "terms": {},
    }
    for term in terms:
        rows = []
        for a in ln.Artifact.filter(key__icontains=term)[:100]:
            rows.append({"uid": a.uid, "key": a.key, "suffix": a.suffix, "n_observations": getattr(a, "n_observations", None)})
        out["terms"][term] = {"count_first100": len(rows), "rows": rows}
    return out


def main() -> None:
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    filelist_text = fetch_text(FILELIST_URL)
    OUT_FILELIST.write_text(filelist_text)
    entries = parse_filelist(filelist_text)
    by_name = {e["name"]: e for e in entries}
    samples = parse_soft_samples()

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in entries:
        e["component"] = component_for(e["name"])
        e["geo_accession"] = sample_id_from_member(e["name"])
        if e["geo_accession"] in samples:
            e["sample_title"] = samples[e["geo_accession"]].get("title", "")
            e["sample_characteristics"] = samples[e["geo_accession"]].get("characteristics", "")
            e["sample_description"] = samples[e["geo_accession"]].get("description", "")
        grouped[e["component"]].append(e)

    component_summary: dict[str, Any] = {}
    for component, rows in sorted(grouped.items()):
        types = defaultdict(int)
        bytes_by_type = defaultdict(int)
        accessions = set()
        for e in rows:
            types[e["type"]] += 1
            bytes_by_type[e["type"]] += e["size_bytes"]
            if e.get("geo_accession"):
                accessions.add(e["geo_accession"])
        component_summary[component] = {
            "file_count": len(rows),
            "bytes": sum(e["size_bytes"] for e in rows),
            "types": dict(types),
            "bytes_by_type": dict(bytes_by_type),
            "geo_accessions": sorted(accessions),
            "perturbase_record": PERTURBASE_RECORDS.get(component),
        }

    expression_header_probe: dict[str, Any] = {}
    for component in ["201218_RNA", "210322_TFAtlas"]:
        expr_entries = [e for e in grouped.get(component, []) if e["name"].endswith(".csv.gz")]
        sample_rows = []
        total_header_cells = 0
        for e in expr_entries:
            first = read_gzip_first_line(e)
            cells = csv_header_column_count(first)
            total_header_cells += cells
            sample_rows.append(
                {
                    "name": e["name"],
                    "geo_accession": e.get("geo_accession"),
                    "sample_title": e.get("sample_title"),
                    "size_bytes": e["size_bytes"],
                    "header_cell_columns": cells,
                    "first_three_cell_ids": first.split("\t")[1:4],
                    "characteristics": e.get("sample_characteristics", ""),
                }
            )
        expression_header_probe[component] = {
            "expression_files": len(expr_entries),
            "sum_header_cell_columns": total_header_cells,
            "samples": sample_rows,
        }

    tfmap_probe: dict[str, Any] = {}
    for component in ["201218_TFmap", "210322_TFmap"]:
        rows = []
        for e in grouped.get(component, []):
            first_rows = read_gzip_first_rows(e, n=3)
            count = count_gzip_rows_small(e)
            parsed_first = []
            for line in first_rows:
                parsed_first.append(next(csv.reader([line])))
            rows.append(
                {
                    "name": e["name"],
                    "geo_accession": e.get("geo_accession"),
                    "sample_title": e.get("sample_title"),
                    "description": e.get("sample_description"),
                    "size_bytes": e["size_bytes"],
                    "rows": count,
                    "first_rows": parsed_first,
                }
            )
        tfmap_probe[component] = {"files": len(rows), "rows": rows, "total_rows": sum(r["rows"] for r in rows)}

    selected_examples: dict[str, Any] = {}
    for name in [
        "GSM6706657_201218_RNA_D4_S1.csv.gz",
        "GSM6706673_201218_TFmap_S1.csv.gz",
        "GSM6719950_210322_TFAtlas_S05.csv.gz",
        "GSM6719974_210322_TFmap_S05.csv.gz",
        "GSM6674255_191108_TFv2d56_4w_S1_matrix.mtx.gz",
        "GSM6674255_191108_TFv2d56_4w_S1_features.tsv.gz",
    ]:
        e = by_name.get(name)
        if e:
            selected_examples[name] = {
                "component": e["component"],
                "size_bytes": e["size_bytes"],
                "tar_data_offset": e["tar_data_offset"],
                "first_rows": read_gzip_first_rows(e, n=4),
            }

    duplicate_probe = build_duplicate_probe()

    total_member_bytes = sum(e["size_bytes"] for e in entries)
    computed_tar_covered = max(e["tar_data_end_inclusive"] for e in entries) + 1 if entries else 0
    payload = {
        "generated_at": generated_at,
        "scope": "T29F-Mac row113 GSE216481 range/source probe; no Lamin writes",
        "source": {
            "geo": "GSE216481",
            "raw_tar_url": RAW_TAR_URL,
            "filelist_url": FILELIST_URL,
            "soft_url": SOFT_URL,
            "expected_tar_bytes": EXPECTED_TAR_BYTES,
            "staged_gcs_target": STAGED_GCS,
            "filelist_artifact": str(OUT_FILELIST.relative_to(ROOT)),
        },
        "filelist": {
            "member_count": len(entries),
            "member_payload_bytes": total_member_bytes,
            "computed_tar_data_end_plus_one": computed_tar_covered,
            "expected_tar_bytes": EXPECTED_TAR_BYTES,
            "computed_vs_expected_slack_bytes": EXPECTED_TAR_BYTES - computed_tar_covered,
        },
        "components": component_summary,
        "expression_header_probe": expression_header_probe,
        "tfmap_probe": tfmap_probe,
        "selected_range_examples": selected_examples,
        "duplicate_probe": duplicate_probe,
        "decisions": {
            "canonical_writes_performed": False,
            "rna_components_identified": ["201218_RNA", "210322_TFAtlas"],
            "excluded_from_canonical_x": ["PRJNA893678_ATAC", "180124_perturb", "210715_combinatorial"],
            "blocker": "Expression matrix components are identifiable and range-readable, but perturbation labels are not yet safe: TFmap files map encoded R1/R2/R3 cell barcode coordinates to 24nt sequences plus numeric values, while the probed GEO/PerturBase metadata does not expose a verified barcode/ORF-to-TF-symbol library map. Canonical ingestion with perturbation labels would risk wrong labels.",
        },
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, sort_keys=True))

    lines: list[str] = []
    lines.append("# T29F row113 GSE216481 TF atlas staged/range probe — 2026-06-22")
    lines.append("")
    lines.append("Scope: source/component/range inspection only; no Lamin writes and no full matrix loads.")
    lines.append("")
    lines.append("## Source/staging")
    lines.append(f"- GEO RAW tar: `{RAW_TAR_URL}`")
    lines.append(f"- Expected size: `{EXPECTED_TAR_BYTES}` bytes.")
    lines.append(f"- GCS staging target: `{STAGED_GCS}` (verified by the worker after upload completes).")
    lines.append(f"- Filelist saved at `{OUT_FILELIST.relative_to(ROOT)}`.")
    lines.append(f"- Filelist members: {len(entries)} files, payload bytes {total_member_bytes:,}; computed tar coverage {computed_tar_covered:,} bytes with {EXPECTED_TAR_BYTES - computed_tar_covered:,} bytes slack/end padding vs HEAD size.")
    lines.append("")
    lines.append("## Components from filelist")
    for component, info in component_summary.items():
        rec = info.get("perturbase_record") or {}
        rec_txt = ""
        if rec:
            rec_txt = f"; PerturBase id={rec.get('id')} modality={rec.get('modality')} qc={rec.get('qc')} filtered={rec.get('filter_cells')}×{rec.get('filter_genes')} perts={rec.get('filter_perturbations')}"
        lines.append(f"- `{component}`: {info['file_count']} files, {info['bytes']:,} bytes, types={info['types']}{rec_txt}")
    lines.append("")
    lines.append("## RNA expression header probe")
    for component, info in expression_header_probe.items():
        rec = PERTURBASE_RECORDS[component]
        lines.append(f"- `{component}`: {info['expression_files']} dense gene×cell CSV.gz files; header cell columns sum to {info['sum_header_cell_columns']:,}; PerturBase filtered cells {rec['filter_cells']:,}, genes {rec['filter_genes']:,}, perturbations {rec['filter_perturbations']:,}.")
        for sample in info["samples"][:6]:
            lines.append(f"  - `{sample['name']}`: {sample['header_cell_columns']:,} encoded cell columns; examples {sample['first_three_cell_ids']}")
        if len(info["samples"]) > 6:
            lines.append(f"  - ... {len(info['samples']) - 6} more samples in JSON.")
    lines.append("")
    lines.append("## TFmap probe")
    for component, info in tfmap_probe.items():
        lines.append(f"- `{component}`: {info['files']} small CSV.gz files, {info['total_rows']:,} rows total.")
        for row in info["rows"][:4]:
            lines.append(f"  - `{row['name']}`: {row['rows']:,} rows; description={row.get('description')!r}; first row={row['first_rows'][0] if row['first_rows'] else []}")
        if len(info["rows"]) > 4:
            lines.append(f"  - ... {len(info['rows']) - 4} more TFmap files in JSON.")
    lines.append("")
    lines.append("## Duplicate probe")
    hit_terms = []
    for term, res in duplicate_probe["terms"].items():
        if res["count_first100"]:
            hit_terms.append(f"`{term}`={res['count_first100']}")
    if hit_terms:
        lines.append("- Potential Lamin key hits: " + ", ".join(hit_terms))
    else:
        lines.append("- No Lamin key hits in first 100 results for GSE216481 / PRJNA893678 / component-prefix terms on `laminlabs/pertdata` branch `jkobject`.")
    lines.append("")
    lines.append("## Decision")
    lines.append("- RNA components are identifiable: QC-pass `201218_RNA` and large QC-pass `210322_TFAtlas`.")
    lines.append("- Exclude ATAC and failed/combinatorial components from canonical RNA `X.h5ad`; ATAC would need a typed auxiliary modality contract if ever represented.")
    lines.append("- No canonical Lamin write was performed. The blocker is perturbation-label safety, not source size: expression cell IDs are encoded (`R1.*,R2.*,R3.*,P1.*`) and TFmap files expose encoded barcode/sequence/numeric rows, but no verified barcode/ORF-to-TF-symbol library map was found in the GEO/PerturBase metadata probed here. Writing `perturbation` now would risk wrong labels.")
    lines.append("- A future converter should first resolve that library map or obtain the PerturBase filtered object/metadata contract, then ingest in sample/component chunks with dense-CSV streaming/transposition; do not full-load the 527k-cell TFAtlas component.")
    OUT_MD.write_text("\n".join(lines) + "\n")
    print(json.dumps({"json": str(OUT_JSON), "md": str(OUT_MD), "members": len(entries)}, indent=2))


if __name__ == "__main__":
    main()
