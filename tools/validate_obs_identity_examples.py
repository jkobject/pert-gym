#!/usr/bin/env python3
"""Validate representative global obs identity payloads.

This script is intentionally local-only: it constructs small in-memory examples
for PRISM chunks, scRNA cells, and non-scRNA table-like rows, then applies the
same helper ingestion/repair scripts should use before writing obs.parquet.
"""

from __future__ import annotations

import json

import pandas as pd

from pert_gym.obs_identity import add_obs_identity, validate_obs_identity


def build_examples() -> dict[str, pd.DataFrame]:
    prism = pd.DataFrame(
        {"cell_line": ["THP-1", "THP-1"], "drug": ["drug_a", "drug_b"]},
        index=pd.Index(["well_A01", "well_A02"], name="source_well"),
    )
    prism = add_obs_identity(
        prism,
        dataset_id="prism_collection/GSE221321",
        prefix="prism_collection/GSE221321/chunk_0042",
        source_accession="GSE221321",
        sample_column="cell_line",
        chunk_id="chunk_0042",
        row_kind="drug_response_row",
    )

    scrna = pd.DataFrame(
        {
            "cell_barcode": ["AAAC-1", "AAAC-2", "TTTG-1"],
            "sample_id": ["donor1", "donor1", "donor2"],
            "perturbation": ["ctrl", "KRAS", "ctrl"],
        },
        index=[101, 102, 103],
    )
    scrna = add_obs_identity(
        scrna,
        dataset_id="viperturb/genome_wide_filtered",
        prefix="viperturb/genome_wide_filtered/chunk_0001",
        source_accession="VIPerturbSeq",
        sample_column="sample_id",
        barcode_column="cell_barcode",
        chunk_id="chunk_0001",
        row_kind="scrna_cell",
    )

    essentiality = pd.DataFrame(
        {
            "depmap_id": ["ACH-000001", "ACH-000002"],
            "gene_symbol": ["KRAS", "BRAF"],
            "dependency_score": [-0.8, -0.1],
        }
    )
    essentiality = add_obs_identity(
        essentiality,
        dataset_id="depmap/avana_2024_q4",
        prefix="depmap/avana_2024_q4/gene_dependency",
        sample_column="depmap_id",
        row_kind="essentiality_row",
    )

    return {
        "prism_chunk": prism,
        "scrna_chunk": scrna,
        "essentiality_table": essentiality,
    }


def main() -> int:
    examples = build_examples()
    summary = {}
    all_uuids = []
    for name, obs in examples.items():
        validate_obs_identity(obs)
        all_uuids.extend(obs["obs_uuid"].tolist())
        summary[name] = {
            "n_obs": int(len(obs)),
            "first_original_obs_index": str(obs["original_obs_index"].iloc[0]),
            "first_obs_uuid": str(obs["obs_uuid"].iloc[0]),
        }

    if len(all_uuids) != len(set(all_uuids)):
        raise ValueError("example obs_uuid values are not globally unique")

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
