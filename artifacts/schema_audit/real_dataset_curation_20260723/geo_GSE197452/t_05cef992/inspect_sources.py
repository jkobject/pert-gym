#!/usr/bin/env python3
"""Read-only source payload inspection for the GSE197452 Perturb-seq subset."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import platform
import subprocess
import tempfile
import zipfile
from collections import Counter
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pandas as pd

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import preflight

TASK_ID = "t_05cef992"
PREFIX = "prism_collection/GSE197452_Perturb-seq"
ROOT = "https://ftp.ncbi.nlm.nih.gov/geo/samples/GSM6297nnn"
FILES = {
    "GSM6297384_cells_counts_Pert_Ill.txt.gz": ("GSM6297384", 90_886),
    "GSM6297384_expression_counts_Pert_Ill.txt.gz": ("GSM6297384", 193_966_012),
    "GSM6297384_genes_counts_Pert_Ill.txt.gz": ("GSM6297384", 243_917),
    "GSM6297385_cells_counts_Pert_Ult.txt.gz": ("GSM6297385", 91_334),
    "GSM6297385_expression_counts_Pert_Ult.txt.gz": ("GSM6297385", 193_163_948),
    "GSM6297385_genes_counts_Pert_Ult.txt.gz": ("GSM6297385", 243_917),
    "GSM6297388_filtered_feature_bc_matrix.pert.ill.h5": ("GSM6297388", 88_119_419),
    "GSM6297388_filtered_feature_bc_matrix.pert.ult.h5": ("GSM6297388", 86_235_772),
}
SUPPLEMENTARY_URL = (
    "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC9931582/supplementaryFiles"
)
SUPPLEMENTARY_TABLE = "41587_2022_1452_MOESM4_ESM.xlsx"
SUPPLEMENTARY_TABLE_SHA256 = (
    "ec1380c72943075b9ecd3e2753d32b14c42951c769e862c7a87c609129f9b23a"
)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_sha256(values: pd.Index) -> str:
    return hashlib.sha256("\n".join(values.astype(str)).encode()).hexdigest()


def url_for(name: str, sample: str) -> str:
    return f"{ROOT}/{sample}/suppl/{name}"


def download_sources() -> tuple[dict[str, Path], dict[str, Any]]:
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-gse197452-sources"
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    receipts: dict[str, Any] = {}
    for name, (sample, expected_size) in FILES.items():
        url = url_for(name, sample)
        path = root / name
        if not path.exists() or path.stat().st_size != expected_size:
            subprocess.run(
                [
                    "curl", "--silent", "--show-error", "--location", "--fail",
                    "--retry", "3", "--output", str(path), url,
                ],
                check=True,
                timeout=3600,
            )
        if path.stat().st_size != expected_size:
            raise AssertionError(f"source size drift: {name}")
        paths[name] = path
        receipts[name] = {
            "url": url,
            "size": expected_size,
            "sha256": sha256_file(path),
        }
    return paths, receipts


def text_preview(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", errors="replace") as handle:
        lines = [handle.readline().rstrip("\n") for _ in range(8)]
    return {"lines": lines, "field_counts": [len(line.split("\t")) for line in lines]}


def matrix_market_header(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt") as handle:
        banner = handle.readline().strip()
        while True:
            line = handle.readline().strip()
            if not line.startswith("%"):
                break
    dimensions = [int(value) for value in line.split()]
    if len(dimensions) != 3:
        raise AssertionError(f"unexpected matrix header: {path.name}")
    return {
        "banner": banner,
        "rows": dimensions[0],
        "columns": dimensions[1],
        "entries": dimensions[2],
    }


def read_one_column(path: Path) -> pd.Index:
    frame = pd.read_csv(path, sep="\t", header=None, dtype="string")
    if frame.shape[1] != 1:
        raise AssertionError(f"expected one-column source axis: {path.name}={frame.shape}")
    return pd.Index(frame.iloc[:, 0].astype(str))


def read_gene_table(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep="\t", header=None, dtype="string")
    if frame.shape[1] != 3:
        raise AssertionError(f"expected three-column source gene axis: {path.name}={frame.shape}")
    frame.columns = ["gene_id", "gene_symbol", "feature_type"]
    return frame


def decode(values: Any) -> pd.Index:
    return pd.Index(
        [item.decode() if isinstance(item, bytes) else str(item) for item in values]
    )


def h5_summary(path: Path) -> tuple[dict[str, Any], pd.Index]:
    with h5py.File(path, "r") as handle:
        matrix = handle["matrix"]
        barcodes = decode(matrix["barcodes"][:])
        features = matrix["features"]
        feature_names = decode(features["name"][:])
        feature_ids = decode(features["id"][:])
        feature_types = decode(features["feature_type"][:])
        shape = [int(value) for value in matrix["shape"][:]]
        type_samples: dict[str, list[dict[str, str]]] = {}
        for feature_type in sorted(set(feature_types)):
            positions = [i for i, value in enumerate(feature_types) if value == feature_type]
            type_samples[feature_type] = [
                {"id": feature_ids[i], "name": feature_names[i]}
                for i in positions[:20]
            ]
        return {
            "shape": shape,
            "barcodes": len(barcodes),
            "barcodes_unique": bool(barcodes.is_unique),
            "barcodes_sha256": ordered_sha256(barcodes),
            "barcode_sample": barcodes[:12].tolist(),
            "features": len(feature_names),
            "feature_ids_unique": bool(feature_ids.is_unique),
            "feature_names_unique": bool(feature_names.is_unique),
            "feature_type_counts": dict(Counter(feature_types)),
            "feature_type_samples": type_samples,
            "feature_samples": [
                {"id": feature_ids[i], "name": feature_names[i], "type": feature_types[i]}
                for i in range(min(20, len(feature_names)))
            ],
            "nnz": int(len(matrix["data"])),
        }, barcodes


def load_feature_table() -> tuple[pd.DataFrame, dict[str, Any]]:
    path = Path(tempfile.gettempdir()) / f"{TASK_ID}-PMC9931582-supplementary.zip"
    if not path.exists():
        subprocess.run(
            [
                "curl", "--silent", "--show-error", "--location", "--fail",
                "--retry", "3", "--output", str(path), SUPPLEMENTARY_URL,
            ],
            check=True,
            timeout=1800,
        )
    with zipfile.ZipFile(path) as archive:
        payload = archive.read(SUPPLEMENTARY_TABLE)
    if hashlib.sha256(payload).hexdigest() != SUPPLEMENTARY_TABLE_SHA256:
        raise AssertionError("Supplementary Table 4 identity drift")
    table = pd.read_excel(io.BytesIO(payload), sheet_name="table S4")
    if (
        list(table.columns)
        != ["id", "name", "read", "pattern", "sequence", "feature_type"]
        or len(table) != 6_155
        or not table["id"].is_unique
        or not table["sequence"].is_unique
    ):
        raise AssertionError("Supplementary Table 4 schema/content drift")
    return table, {
        "url": SUPPLEMENTARY_URL,
        "archive_size": path.stat().st_size,
        "archive_sha256": sha256_file(path),
        "member": SUPPLEMENTARY_TABLE,
        "member_size": len(payload),
        "member_sha256": SUPPLEMENTARY_TABLE_SHA256,
        "rows": len(table),
        "feature_type_counts": table["feature_type"].value_counts().to_dict(),
    }


def feature_assignments(
    path: Path, obs: pd.DataFrame, feature_table: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, Any]]:
    with h5py.File(path, "r") as handle:
        matrix = handle["matrix"]
        barcodes = decode(matrix["barcodes"][:])
        features = matrix["features"]
        names = np.asarray(decode(features["name"][:]), dtype=object)
        types = np.asarray(decode(features["feature_type"][:]), dtype=object)
        indices = matrix["indices"][:]
        indptr = matrix["indptr"][:]
        data = matrix["data"][:]
    accepted = pd.Index(obs["original_obs_index"].astype(str))
    if not barcodes.equals(accepted):
        raise AssertionError("Illumina feature-barcode/accepted OBS order drift")

    assignments: dict[str, list[Any]] = {
        "source_guide_top": [],
        "source_guide_top_count": [],
        "source_guide_detected_count": [],
        "source_guide_top_ties": [],
        "source_hash_top": [],
        "source_hash_top_count": [],
        "source_hash_detected_count": [],
        "source_hash_top_ties": [],
    }
    for column in range(len(barcodes)):
        start, stop = int(indptr[column]), int(indptr[column + 1])
        cell_indices = indices[start:stop]
        cell_values = data[start:stop]
        for label, feature_type in (
            ("guide", "CRISPR Guide Capture"),
            ("hash", "Custom"),
        ):
            selected = types[cell_indices] == feature_type
            selected_indices = cell_indices[selected]
            selected_values = cell_values[selected]
            if not len(selected_values):
                assignments[f"source_{label}_top"].append(pd.NA)
                assignments[f"source_{label}_top_count"].append(0)
                assignments[f"source_{label}_detected_count"].append(0)
                assignments[f"source_{label}_top_ties"].append(0)
                continue
            maximum = int(selected_values.max())
            maxima = selected_indices[selected_values == maximum]
            assignments[f"source_{label}_top"].append(str(names[int(maxima[0])]))
            assignments[f"source_{label}_top_count"].append(maximum)
            assignments[f"source_{label}_detected_count"].append(len(selected_values))
            assignments[f"source_{label}_top_ties"].append(len(maxima))
    frame = pd.DataFrame(assignments, index=obs.index)
    frame["source_guide_top"] = frame["source_guide_top"].astype("string")
    frame["source_hash_top"] = frame["source_hash_top"].astype("string")
    sequences = feature_table.set_index("id")["sequence"].astype("string")
    current = obs["guide"].astype("string")
    if not current.dropna().isin(sequences.index).all():
        raise AssertionError("accepted guide absent from Supplementary Table 4")
    frame["source_guide_sequence"] = current.map(sequences).astype("string")
    comparable = current.notna() & frame["source_guide_top"].notna()
    matches = current.eq(frame["source_guide_top"]) & comparable
    mismatch = comparable & ~matches
    return frame, {
        "current_guide_non_null": int(current.notna().sum()),
        "current_guide_sequence_joined": int(frame["source_guide_sequence"].notna().sum()),
        "top_guide_non_null": int(frame["source_guide_top"].notna().sum()),
        "current_equals_top_guide": int(matches.sum()),
        "current_differs_top_guide": int(mismatch.sum()),
        "current_differs_top_sample": [
            {
                "barcode": str(obs.loc[index, "original_obs_index"]),
                "current": str(current.loc[index]),
                "top": str(frame.loc[index, "source_guide_top"]),
            }
            for index in frame.index[mismatch][:20]
        ],
        "guide_top_tie_rows": int((frame["source_guide_top_ties"] > 1).sum()),
        "hash_top_non_null": int(frame["source_hash_top"].notna().sum()),
        "hash_top_tie_rows": int((frame["source_hash_top_ties"] > 1).sum()),
        "hash_top_counts": frame["source_hash_top"].value_counts(dropna=False).to_dict(),
    }


def main() -> None:
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    paths, receipts = download_sources()
    feature_table, feature_table_receipt = load_feature_table()

    cells_ill = read_one_column(paths["GSM6297384_cells_counts_Pert_Ill.txt.gz"])
    cells_ult = read_one_column(paths["GSM6297385_cells_counts_Pert_Ult.txt.gz"])
    genes_ill = read_gene_table(paths["GSM6297384_genes_counts_Pert_Ill.txt.gz"])
    genes_ult = read_gene_table(paths["GSM6297385_genes_counts_Pert_Ult.txt.gz"])
    matrix_ill = matrix_market_header(paths["GSM6297384_expression_counts_Pert_Ill.txt.gz"])
    matrix_ult = matrix_market_header(paths["GSM6297385_expression_counts_Pert_Ult.txt.gz"])
    h5_ill, h5_barcodes_ill = h5_summary(
        paths["GSM6297388_filtered_feature_bc_matrix.pert.ill.h5"]
    )
    h5_ult, h5_barcodes_ult = h5_summary(
        paths["GSM6297388_filtered_feature_bc_matrix.pert.ult.h5"]
    )

    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    obs_records = list(ln.Artifact.filter(key=f"{PREFIX}/obs.parquet").all())
    obs_records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    obs = obs_records[-1].load()
    accepted = pd.Index(obs["original_obs_index"].astype(str))
    _, assignment_receipt = feature_assignments(
        paths["GSM6297388_filtered_feature_bc_matrix.pert.ill.h5"],
        obs,
        feature_table,
    )

    axes = {
        "expression_cells_ill": cells_ill,
        "expression_cells_ult": cells_ult,
        "feature_matrix_barcodes_ill": h5_barcodes_ill,
        "feature_matrix_barcodes_ult": h5_barcodes_ult,
    }
    relations = {
        name: {
            "rows": len(axis),
            "unique": bool(axis.is_unique),
            "ordered_equals_accepted": axis.equals(accepted),
            "set_equals_accepted": len(axis) == len(accepted) and set(axis) == set(accepted),
            "accepted_missing_from_source": len(accepted.difference(axis)),
            "source_missing_from_accepted": len(axis.difference(accepted)),
            "accepted_missing_sample": accepted.difference(axis)[:20].tolist(),
            "source_missing_sample": axis.difference(accepted)[:20].tolist(),
            "sha256": ordered_sha256(axis),
        }
        for name, axis in axes.items()
    }
    report = {
        "format": "pert-gym.gse197452-source-inspection/v1",
        "task_id": TASK_ID,
        "host": capacity.hostname,
        "pid": os.getpid(),
        "capacity": {
            "free_disk_bytes": capacity.free_disk_bytes,
            "available_memory_bytes": capacity.available_memory_bytes,
        },
        "sources": receipts,
        "supplementary_table_4": feature_table_receipt,
        "text_previews": {
            name: text_preview(path)
            for name, path in paths.items()
            if name.endswith(".txt.gz") and "expression_counts" not in name
        },
        "expression": {
            "ill": matrix_ill,
            "ult": matrix_ult,
            "gene_tables_equal": genes_ill.equals(genes_ult),
            "gene_ids_unique": bool(genes_ill["gene_id"].is_unique),
            "gene_symbols_unique": bool(genes_ill["gene_symbol"].is_unique),
            "gene_id_axis_sha256": ordered_sha256(pd.Index(genes_ill["gene_id"])),
            "gene_symbol_axis_sha256": ordered_sha256(pd.Index(genes_ill["gene_symbol"])),
            "gene_table_sample": genes_ill.head(12).to_dict(orient="records"),
        },
        "feature_matrices": {"ill": h5_ill, "ult": h5_ult},
        "accepted_obs": {
            "uid": str(obs_records[-1].uid),
            "rows": len(accepted),
            "axis_sha256": ordered_sha256(accepted),
        },
        "feature_assignments": assignment_receipt,
        "axis_relations": relations,
        "invariants": {"writes": 0, "downloaded_bytes": sum(item["size"] for item in receipts.values())},
    }
    print("GSE197452_SOURCE_INSPECTION=" + canonical(report), flush=True)


if __name__ == "__main__":
    main()
