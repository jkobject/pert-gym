from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from scipy import sparse

from pert_gym.logical_sparse_zarr import (
    MigrationInterrupted,
    read_logical_sparse_revision,
)
from pert_gym.perturbai_sparse_parquet import (
    REQUESTER_PAYS_PROJECT,
    PerturbAISource,
    build_perturbai_revision,
    requester_pays_storage_options,
    validate_perturbai_sources,
)
from tools.migrate_perturbai_sparse_parquet import _var as read_perturbai_var


def _write_source(
    path: Path,
    *,
    cells: tuple[str | None, ...] = ("c0", "c1"),
    genes: list[object] | None = None,
    expressions: list[object] | None = None,
) -> None:
    table = pa.table(
        {
            "cell_id": list(cells),
            "batch": ["b"] * len(cells),
            "genes": genes if genes is not None else [[0, 2], [1]][: len(cells)],
            "expressions": (
                expressions if expressions is not None else [[2, 3], [5]][: len(cells)]
            ),
        }
    )
    pq.write_table(table, path, row_group_size=1)


def _source(path: Path, stem: str) -> PerturbAISource:
    return PerturbAISource(
        stem=stem,
        source_uri=f"gs://example/data/{stem}.parquet",
        source_commit="a7c65dc0a64da4bd47cf6ef5f4dec6c7ef745e87",
        source_object_id=f"data/{stem}.parquet",
        local_path=path,
    )


def _var() -> pd.DataFrame:
    return pd.DataFrame(
        {"gene_token_id": [0, 1, 2], "gene_name": ["a", "b", "c"]},
        index=["g0", "g1", "g2"],
    )


@pytest.mark.parametrize(
    ("gene_token_ids", "error"),
    [
        ([0.0, 1.9, 2.0], "integer-valued"),
        ([0.0, None, 2.0], "non-null"),
        ([False, True, False], "must not be boolean"),
        ([0.0, float("nan"), 2.0], "finite"),
        ([0.0, float("inf"), 2.0], "finite"),
        ([0, -1, 2], "non-negative"),
        ([0, float(2**63), 2], "supported range"),
        ([0, 1, 1], "duplicate"),
        ([0, 2, 3], "contiguous"),
    ],
)
def test_migration_rejects_lossy_or_invalid_gene_token_ids(
    tmp_path: Path, gene_token_ids: list[object], error: str
) -> None:
    metadata = tmp_path / "genes.parquet"
    pd.DataFrame(
        {
            "gene_token_id": gene_token_ids,
            "gene_name": ["a", "b", "c"],
            "gene_ids": ["g0", "g1", "g2"],
        }
    ).to_parquet(metadata)

    with pytest.raises(ValueError, match=error):
        read_perturbai_var(metadata)


def test_migration_accepts_exact_integer_valued_gene_token_ids(tmp_path: Path) -> None:
    metadata = tmp_path / "genes.parquet"
    pd.DataFrame(
        {
            "gene_token_id": [0.0, 1, np.int64(2)],
            "gene_name": ["a", "b", "c"],
            "gene_ids": ["g0", "g1", "g2"],
        }
    ).to_parquet(metadata)

    var = read_perturbai_var(metadata)

    assert var["gene_token_id"].tolist() == [0, 1, 2]
    assert var.index.tolist() == ["g0", "g1", "g2"]


def test_source_validation_accepts_non_numeric_stems_in_exact_part_order(
    tmp_path: Path,
) -> None:
    first = tmp_path / "two.parquet"
    second = tmp_path / "three.parquet"
    _write_source(first)
    _write_source(second)
    sources = (
        _source(first, "WB8588_2_1_part-2"),
        _source(second, "WB8588_2_1_part-3"),
    )

    assert validate_perturbai_sources(sources) == sources


@pytest.mark.parametrize(
    "stems, error",
    [
        (("WB8588_2_1_part-2", "WB8588_2_1_part-2"), "duplicate"),
        (("WB8588_2_1_part-2", "WB8588_2_1_part-4"), "missing"),
        (("WB8588_2_1_part-3", "WB8588_2_1_part-2"), "out of order"),
    ],
)
def test_source_validation_rejects_duplicate_missing_and_out_of_order_parts(
    tmp_path: Path, stems: tuple[str, str], error: str
) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _write_source(first)
    _write_source(second)

    with pytest.raises(ValueError, match=error):
        validate_perturbai_sources(
            (_source(first, stems[0]), _source(second, stems[1]))
        )


def test_requester_pays_requires_exact_project() -> None:
    assert requester_pays_storage_options(REQUESTER_PAYS_PROJECT) == {
        "project": REQUESTER_PAYS_PROJECT,
        "requester_pays": True,
    }
    with pytest.raises(ValueError, match="billing project"):
        requester_pays_storage_options("other-project")


def test_adapter_streams_sparse_rows_and_binds_source_provenance(
    tmp_path: Path,
) -> None:
    first = tmp_path / "two.parquet"
    second = tmp_path / "three.parquet"
    _write_source(first)
    _write_source(second, cells=("c2", "c3"))
    sources = (
        _source(first, "WB8588_2_1_part-2"),
        _source(second, "WB8588_2_1_part-3"),
    )

    manifest = build_perturbai_revision(
        root=tmp_path / "out",
        logical_key="perturbai/wholebrain",
        revision="r1",
        sources=sources,
        var=_var(),
        schema_fingerprint="perturbai-gene-metadata/v1",
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=2,
        parquet_batch_rows=1,
    )

    source_identity = manifest["source_identity"]
    assert source_identity["kind"] == "perturbai-sparse-parquet/v1"
    assert [item["stem"] for item in source_identity["sources"]] == [
        "WB8588_2_1_part-2",
        "WB8588_2_1_part-3",
    ]
    assert [item["row_range"] for item in source_identity["sources"]] == [
        [0, 2],
        [2, 4],
    ]
    assert all("sha256" in item for item in source_identity["sources"])

    _surface, matrix, obs, var = read_logical_sparse_revision(
        tmp_path / "out", "perturbai/wholebrain", "r1"
    )
    expected = sparse.csr_matrix(np.array([[2, 0, 3], [0, 5, 0], [2, 0, 3], [0, 5, 0]]))
    assert (matrix != expected).nnz == 0
    assert obs.index.tolist() == ["c0", "c1", "c2", "c3"]
    assert var.index.tolist() == ["g0", "g1", "g2"]
    assert max(item["max_batch_rows"] for item in source_identity["sources"]) == 1


def test_adapter_rejects_malformed_sparse_rows_before_candidate_write(
    tmp_path: Path,
) -> None:
    path = tmp_path / "malformed.parquet"
    pq.write_table(
        pa.table(
            {
                "cell_id": ["c0"],
                "batch": ["b"],
                "genes": [[0, 1]],
                "expressions": [[1]],
            }
        ),
        path,
    )

    with pytest.raises(ValueError, match="length mismatch"):
        build_perturbai_revision(
            root=tmp_path / "out",
            logical_key="perturbai/wholebrain",
            revision="r1",
            sources=(_source(path, "WB8588_2_1_part-2"),),
            var=_var(),
            schema_fingerprint="perturbai-gene-metadata/v1",
            ingestion_run_id="test-run",
            max_rss_bytes=10**12,
            min_rows=1,
            max_rows=2,
        )
    assert not (tmp_path / "out/perturbai/wholebrain/revisions/r1").exists()


@pytest.mark.parametrize(
    ("genes", "expressions", "error"),
    [
        ([[1.9]], [[1]], "gene token must be integer-valued"),
        ([[0]], [[1.9]], "expression value must be integer-valued"),
        ([[0]], [[3_000_000_000]], "expression value outside"),
        ([[0]], [[float("nan")]], "expression value must be finite"),
        ([[float("inf")]], [[1]], "gene token must be finite"),
        ([[True]], [[1]], "gene token must not be boolean"),
        ([None], [[1]], "genes must be a one-dimensional sequence"),
        ([[0]], [None], "expressions must be a one-dimensional sequence"),
        ([[[0]]], [[1]], "genes must be a one-dimensional sequence"),
    ],
)
def test_adapter_rejects_invalid_sparse_values_before_coercion(
    tmp_path: Path, genes: list[object], expressions: list[object], error: str
) -> None:
    path = tmp_path / "invalid.parquet"
    _write_source(path, cells=("c0",), genes=genes, expressions=expressions)

    with pytest.raises(ValueError, match=error):
        build_perturbai_revision(
            root=tmp_path / "out",
            logical_key="perturbai/wholebrain",
            revision="r1",
            sources=(_source(path, "WB8588_2_1_part-2"),),
            var=_var(),
            schema_fingerprint="perturbai-gene-metadata/v1",
            ingestion_run_id="test-run",
            max_rss_bytes=10**12,
            min_rows=1,
            max_rows=2,
        )
    assert not (tmp_path / "out/perturbai/wholebrain/revisions/r1").exists()


@pytest.mark.parametrize(
    "cells, error",
    [((None,), "cell_id must be non-null"), ((" ",), "cell_id must be non-blank")],
)
def test_adapter_rejects_null_or_blank_cell_ids_before_candidate_write(
    tmp_path: Path, cells: tuple[str | None, ...], error: str
) -> None:
    path = tmp_path / "invalid-cell-id.parquet"
    _write_source(path, cells=cells, genes=[[0]], expressions=[[1]])

    with pytest.raises(ValueError, match=error):
        build_perturbai_revision(
            root=tmp_path / "out",
            logical_key="perturbai/wholebrain",
            revision="r1",
            sources=(_source(path, "WB8588_2_1_part-2"),),
            var=_var(),
            schema_fingerprint="perturbai-gene-metadata/v1",
            ingestion_run_id="test-run",
            max_rss_bytes=10**12,
            min_rows=1,
            max_rows=2,
        )


def test_adapter_accepts_int32_expression_boundary(tmp_path: Path) -> None:
    path = tmp_path / "boundary.parquet"
    _write_source(
        path,
        cells=("c0",),
        genes=[[2]],
        expressions=[[np.iinfo(np.int32).max]],
    )

    manifest = build_perturbai_revision(
        root=tmp_path / "out",
        logical_key="perturbai/wholebrain",
        revision="r1",
        sources=(_source(path, "WB8588_2_1_part-2"),),
        var=_var(),
        schema_fingerprint="perturbai-gene-metadata/v1",
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=2,
    )

    assert manifest["nnz"] == 1
    _surface, matrix, _obs, _var_frame = read_logical_sparse_revision(
        tmp_path / "out", "perturbai/wholebrain", "r1"
    )
    assert matrix[0, 2] == np.iinfo(np.int32).max


def test_adapter_resume_refuses_var_drift_and_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "source.parquet"
    _write_source(path)
    kwargs = dict(
        root=tmp_path / "out",
        logical_key="perturbai/wholebrain",
        revision="r1",
        sources=(_source(path, "WB8588_2_1_part-2"),),
        var=_var(),
        schema_fingerprint="perturbai-gene-metadata/v1",
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=1,
        max_rows=1,
    )
    with pytest.raises(MigrationInterrupted):
        build_perturbai_revision(**kwargs, stop_after_chunks=1)
    with pytest.raises(RuntimeError, match="checkpoint mismatch"):
        build_perturbai_revision(
            **(kwargs | {"var": _var().rename(index={"g0": "drift"})})
        )
    build_perturbai_revision(**kwargs)
    with pytest.raises(RuntimeError, match="already completed"):
        build_perturbai_revision(**kwargs)
