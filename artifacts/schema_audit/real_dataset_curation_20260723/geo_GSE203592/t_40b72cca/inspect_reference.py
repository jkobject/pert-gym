#!/usr/bin/env python3
"""Read-only probe of the GSE203592 mouse feature-axis source mapping."""

from __future__ import annotations

import gzip
import hashlib
import json
import platform
import re
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

PREFIX = "prism_collection/GSE203592"
EXPECTED_VAR_UID = "N0CJ8e8f2rE4PjL10001"
EXPECTED_N_VARS = 31_053
GTF_URL = (
    "https://ftp.ensembl.org/pub/release-93/gtf/mus_musculus/"
    "Mus_musculus.GRCm38.93.gtf.gz"
)
REPORT = Path(__file__).with_name("reference_inspection_report.json")
ATTR_RE = re.compile(r'([A-Za-z0-9_]+) "([^"]*)";')


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    records = list(ln.Artifact.filter(key=value).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"cannot resolve linked artifact: {value}")
    return records[-1]


def download(url: str, destination: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with (
        urllib.request.urlopen(url, timeout=120) as response,
        destination.open("wb") as handle,
    ):
        while chunk := response.read(1024 * 1024):
            handle.write(chunk)
            digest.update(chunk)
            size += len(chunk)
    return {"url": url, "bytes": size, "sha256": digest.hexdigest()}


def parse_gtf(path: Path) -> tuple[dict[str, set[str]], dict[str, set[str]], int]:
    by_symbol: dict[str, set[str]] = defaultdict(set)
    by_gene_id: dict[str, set[str]] = defaultdict(set)
    gene_rows = 0
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 9 or fields[2] != "gene":
                continue
            attributes = dict(ATTR_RE.findall(fields[8]))
            gene_id = attributes.get("gene_id", "").split(".", maxsplit=1)[0]
            symbol = attributes.get("gene_name", "")
            if gene_id and symbol:
                by_symbol[symbol].add(gene_id)
                by_gene_id[gene_id].add(symbol)
                gene_rows += 1
    return by_symbol, by_gene_id, gene_rows


def main() -> int:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    preflight()
    ln = connect_pertdata()
    obs_records = list(ln.Artifact.filter(key=f"{PREFIX}/obs.parquet").all())
    obs_records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    obs = obs_records[-1]
    x = resolve_artifact(ln, obs.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x.features.get_values()["var"])
    if str(var_artifact.uid) != EXPECTED_VAR_UID:
        raise AssertionError("VAR identity drift")
    var = var_artifact.load()
    if len(var) != EXPECTED_N_VARS:
        raise AssertionError("VAR denominator drift")

    destination = Path("/tmp/Mus_musculus.GRCm38.93.gtf.gz")
    source = download(GTF_URL, destination)
    by_symbol, by_gene_id, gene_rows = parse_gtf(destination)
    symbols = var.index.astype(str)
    unique_matches = {
        symbol: next(iter(by_symbol[symbol]))
        for symbol in symbols
        if len(by_symbol.get(symbol, set())) == 1
    }
    ambiguous = {
        symbol: sorted(by_symbol[symbol])
        for symbol in symbols
        if len(by_symbol.get(symbol, set())) > 1
    }
    unmapped = [symbol for symbol in symbols if symbol not in by_symbol]
    mapped_ids = [
        unique_matches[symbol] for symbol in symbols if symbol in unique_matches
    ]
    duplicate_mapped_ids = len(mapped_ids) - len(set(mapped_ids))
    existing_mouse_ids = int(
        var["stable_feature_id"]
        .astype("string")
        .str.fullmatch(r"ENSMUSG\d{11}", na=False)
        .sum()
    )
    report = {
        "format": "pert-gym.gse203592-mouse-reference-inspection/v1",
        "source": source,
        "gene_rows": gene_rows,
        "unique_gtf_gene_ids": len(by_gene_id),
        "unique_gtf_symbols": len(by_symbol),
        "axis_rows": len(var),
        "unique_symbol_matches": len(unique_matches),
        "coverage": len(unique_matches) / len(var),
        "ambiguous_symbol_count": len(ambiguous),
        "ambiguous_symbols": ambiguous,
        "unmapped_symbol_count": len(unmapped),
        "unmapped_symbols": unmapped,
        "duplicate_mapped_ids": duplicate_mapped_ids,
        "existing_mouse_stable_ids": existing_mouse_ids,
        "writes": 0,
    }
    REPORT.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print("GSE203592_REFERENCE_INSPECTION=" + json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
