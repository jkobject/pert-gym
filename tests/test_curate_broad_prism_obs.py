from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from tools import curate_broad_prism_obs as curate


def _source_database(path: Path) -> None:
    database = sqlite3.connect(path)
    database.execute(
        "CREATE TABLE source_rows ("
        "source_index INTEGER PRIMARY KEY, row_id TEXT NOT NULL UNIQUE, "
        "profile_id TEXT NOT NULL, lfc TEXT, lfc_cb TEXT, pass_value TEXT NOT NULL)"
    )
    database.executemany(
        "INSERT INTO source_rows VALUES (?, ?, ?, ?, ?, ?)",
        [
            (0, "ACH-000001::P1::W1::R1", "P1_120H_A", "-1.5", "-1.2", "TRUE"),
            (1, "ACH-000002::P2::W2::R2", "P2_120H_B", "0.5", "0.4", "FALSE"),
        ],
    )
    database.commit()
    database.close()


def _treatment(profile_id: str, name: str, treatment_type: str) -> dict[str, str]:
    return {
        "profile_id": profile_id,
        "prism_replicate": "replicate",
        "perturbation_well": "A01",
        "culture": "adherent",
        "perturbation_type": treatment_type,
        "dose": "2.5",
        "broad_id": f"BRD-{name}",
        "name": name,
        "compound_plate": "plate",
        "rep": "1",
        "screen": "primary",
    }


def test_expected_legacy_axis_layout() -> None:
    assert [curate.expected_source_index_and_role(i, 2) for i in range(10)] == [
        (0, "legacy_synthetic_control"),
        (1, "legacy_synthetic_control"),
        (0, "profile_id"),
        (0, "LFC"),
        (0, "LFC_cb"),
        (0, "PASS"),
        (1, "profile_id"),
        (1, "LFC"),
        (1, "LFC_cb"),
        (1, "PASS"),
    ]


def test_source_rows_for_indices_handles_control_field_wrap(tmp_path: Path) -> None:
    database_path = tmp_path / "source.sqlite"
    _source_database(database_path)
    database = sqlite3.connect(database_path)
    try:
        rows = curate.source_rows_for_indices(database, [1, 0, 0, 1])
    finally:
        database.close()
    assert sorted(rows) == [0, 1]
    assert rows[0][1] == "ACH-000001::P1::W1::R1"
    assert rows[1][1] == "ACH-000002::P2::W2::R2"


def test_build_candidate_preserves_axis_and_materializes_only_lfc(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(curate, "EXPECTED_SOURCE_ROWS", 2)
    monkeypatch.setattr(curate, "EXPECTED_OBS_ROWS", 10)
    source_database = tmp_path / "source.sqlite"
    _source_database(source_database)

    row_ids = [
        "ACH-000001::P1::W1::R1",
        "ACH-000002::P2::W2::R2",
        *(["ACH-000001::P1::W1::R1"] * 4),
        *(["ACH-000002::P2::W2::R2"] * 4),
    ]
    broad_ids = [None, None, *curate.SOURCE_FIELDS, *curate.SOURCE_FIELDS]
    predecessor = tmp_path / "predecessor.parquet"
    pq.write_table(
        pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "depmap_id": row_ids,
                    "broad_id": broad_ids,
                    "original_obs_index": [f"row-{i}" for i in range(10)],
                    "obs_uuid": [f"uuid-{i}" for i in range(10)],
                }
            ),
            preserve_index=False,
        ),
        predecessor,
        row_group_size=3,
    )
    output = tmp_path / "candidate.parquet"
    summary = curate.build_candidate(
        predecessor_path=predecessor,
        source_database_path=source_database,
        treatments={
            "P1_120H_A": _treatment("P1_120H_A", "drug-a", "trt_cp"),
            "P2_120H_B": _treatment("P2_120H_B", "vehicle", "ctl_vehicle"),
        },
        cell_lines={
            "ACH-000001": {
                "ccle_name": "CELL_A",
                "primary_tissue": "lung",
            }
        },
        output_path=output,
    )

    candidate = pq.read_table(output).to_pandas()
    assert summary["rows"] == 10
    assert summary["legacy_order_mismatch"] == 0
    assert summary["role_counts"]["LFC"] == 2
    assert candidate["original_obs_index"].tolist() == [f"row-{i}" for i in range(10)]
    assert candidate["source_row_id"].tolist() == row_ids
    assert candidate["source_field_role"].tolist() == [
        "legacy_synthetic_control",
        "legacy_synthetic_control",
        *curate.SOURCE_FIELDS,
        *curate.SOURCE_FIELDS,
    ]
    direct = candidate[candidate["source_field_role"] == "LFC"]
    assert direct["response_value"].tolist() == [-1.5, 0.5]
    assert direct["response_metric"].tolist() == ["lfc", "lfc"]
    assert pd.isna(candidate.loc[0, "response_value"])
    assert candidate.loc[3, "loader_projectable"]
    assert not candidate.loc[7, "loader_projectable"]
    assert candidate.loc[3, "timepoint"] == "7200"
    assert candidate.loc[3, "tissue_type"] == "lung"
    assert candidate.loc[7, "tissue_type"] == "unknown"
    assert candidate.loc[7, "is_control"]
    assert candidate.loc[3, "quality_flag"] == "accepted_lfc"
    assert candidate.loc[7, "quality_flag"] == "source_qc_failed"
