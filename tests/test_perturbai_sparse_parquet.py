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
    _build_obs,
    _SparseParquetMatrix,
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


def _write_many_row_source(path: Path, rows: int = 100) -> None:
    table = pa.table(
        {
            "cell_id": [f"c{row}" for row in range(rows)],
            "batch": ["b"] * rows,
            "genes": [[row % 3] for row in range(rows)],
            "expressions": [[row + 1] for row in range(rows)],
        }
    )
    pq.write_table(table, path, row_group_size=10)


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


@pytest.mark.parametrize(
    ("gene_token_ids", "error"),
    [
        ([1, 0, 2], "align exactly with matrix columns"),
        ([0, 1, 1], "unique"),
        (None, "missing required column"),
        (pd.Series([0, 1], index=["g0", "g1"]), "finite"),
        ([0, 1, 3], "outside supported range"),
        ([0, 1.5, 2], "integer-valued"),
    ],
)
def test_public_builder_rejects_invalid_gene_token_to_column_identity(
    tmp_path: Path,
    gene_token_ids: list[object] | pd.Series | None,
    error: str,
) -> None:
    source_path = tmp_path / "source.parquet"
    _write_source(source_path)
    var = _var()
    if gene_token_ids is None:
        var = var.drop(columns="gene_token_id")
    else:
        var["gene_token_id"] = gene_token_ids

    with pytest.raises(ValueError, match=error):
        build_perturbai_revision(
            root=tmp_path / "out",
            logical_key="perturbai/wholebrain",
            revision="r1",
            sources=(_source(source_path, "WB8588_2_1_part-2"),),
            var=var,
            schema_fingerprint="perturbai-gene-metadata/v1",
            ingestion_run_id="test-run",
            max_rss_bytes=10**12,
            min_rows=1,
            max_rows=2,
        )
    assert not (tmp_path / "out/perturbai/wholebrain/revisions/r1").exists()


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


def test_obs_adapter_keeps_only_window_sized_metadata_and_disk_unique_index(
    tmp_path: Path,
) -> None:
    first = tmp_path / "two.parquet"
    second = tmp_path / "three.parquet"
    _write_source(first, cells=("c0", "c1"))
    _write_source(second, cells=("c2", "c3"))
    sources = (
        _source(first, "WB8588_2_1_part-2"),
        _source(second, "WB8588_2_1_part-3"),
    )
    matrix = _SparseParquetMatrix(sources, n_vars=len(_var()), batch_rows=1)

    obs = _build_obs(matrix, sources)

    assert not isinstance(obs, pd.DataFrame)
    assert len(obs) == 4
    assert not any(
        isinstance(value, (pd.DataFrame, list, set)) for value in vars(obs).values()
    )
    obs.logical_sparse_obs_identity()
    window = obs.iloc[1:3]

    assert window.index.tolist() == ["c1", "c2"]
    assert obs.max_live_rows == 2


def test_obs_adapter_accepts_nullable_metadata_split_across_row_groups(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nullable-metadata.parquet"
    pq.write_table(
        pa.table(
            {
                "cell_id": ["c0", "c1"],
                "numeric_metadata": pa.array([1, None], type=pa.int64()),
                "genes": [[0], [1]],
                "expressions": [[2], [3]],
            }
        ),
        path,
        row_group_size=1,
    )
    sources = (_source(path, "WB8588_2_1_part-2"),)
    build_perturbai_revision(
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
    _surface, _matrix, obs, _var_frame = read_logical_sparse_revision(
        tmp_path / "out", "perturbai/wholebrain", "r1"
    )

    assert obs["numeric_metadata"].tolist() == [1, pd.NA]


def test_obs_adapter_rejects_genuinely_incompatible_metadata_schemas(
    tmp_path: Path,
) -> None:
    first = tmp_path / "integer-metadata.parquet"
    second = tmp_path / "string-metadata.parquet"
    for path, metadata, cell in (
        (first, pa.array([1], type=pa.int64()), "c0"),
        (second, pa.array(["one"], type=pa.string()), "c1"),
    ):
        pq.write_table(
            pa.table(
                {
                    "cell_id": [cell],
                    "metadata": metadata,
                    "genes": [[0]],
                    "expressions": [[2]],
                }
            ),
            path,
        )
    sources = (
        _source(first, "WB8588_2_1_part-2"),
        _source(second, "WB8588_2_1_part-3"),
    )
    matrix = _SparseParquetMatrix(sources, n_vars=len(_var()), batch_rows=1)
    obs = _build_obs(matrix, sources)

    with pytest.raises(ValueError, match="schema changes"):
        obs.logical_sparse_obs_identity()


def test_sparse_matrix_reads_only_overlapping_row_groups_for_early_and_late_slices(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "source.parquet"
    _write_many_row_source(path)
    matrix = _SparseParquetMatrix(
        (_source(path, "WB8588_2_1_part-2"),), n_vars=3, batch_rows=10
    )
    original_batches = matrix._batches
    reads = 0

    def counted_batches(
        source: PerturbAISource,
        *,
        start_row: int = 0,
        end_row: int | None = None,
    ):
        nonlocal reads
        for frame in original_batches(source, start_row=start_row, end_row=end_row):
            reads += 1
            yield frame

    monkeypatch.setattr(matrix, "_batches", counted_batches)

    early = matrix[0:10]
    assert reads == 1
    assert early.toarray().tolist() == [
        [1, 0, 0],
        [0, 2, 0],
        [0, 0, 3],
        [4, 0, 0],
        [0, 5, 0],
        [0, 0, 6],
        [7, 0, 0],
        [0, 8, 0],
        [0, 0, 9],
        [10, 0, 0],
    ]

    reads = 0
    late = matrix[90:100]
    assert reads == 1
    assert late.toarray().tolist() == [
        [91, 0, 0],
        [0, 92, 0],
        [0, 0, 93],
        [94, 0, 0],
        [0, 95, 0],
        [0, 0, 96],
        [97, 0, 0],
        [0, 98, 0],
        [0, 0, 99],
        [100, 0, 0],
    ]


def test_adapter_multi_chunk_build_scans_each_row_group_a_bounded_number_of_times(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "source.parquet"
    _write_many_row_source(path)
    original_batches = _SparseParquetMatrix._batches
    batch_reads = 0

    def counted_batches(
        self: _SparseParquetMatrix,
        source: PerturbAISource,
        *,
        start_row: int = 0,
        end_row: int | None = None,
    ):
        nonlocal batch_reads
        for frame in original_batches(
            self, source, start_row=start_row, end_row=end_row
        ):
            batch_reads += 1
            yield frame

    monkeypatch.setattr(_SparseParquetMatrix, "_batches", counted_batches)

    build_perturbai_revision(
        root=tmp_path / "out",
        logical_key="perturbai/wholebrain",
        revision="r1",
        sources=(_source(path, "WB8588_2_1_part-2"),),
        var=_var(),
        schema_fingerprint="perturbai-gene-metadata/v1",
        ingestion_run_id="test-run",
        max_rss_bytes=10**12,
        min_rows=10,
        max_rows=10,
        parquet_batch_rows=10,
    )

    # Matrix validation, streamed obs identity, and paired X/obs chunk writes
    # each scan every row group only a bounded number of times.
    assert batch_reads == 41
    _surface, matrix, _obs, _var_frame = read_logical_sparse_revision(
        tmp_path / "out", "perturbai/wholebrain", "r1"
    )
    expected = sparse.csr_matrix(
        (
            np.arange(1, 101),
            (np.arange(100), np.arange(100) % 3),
        ),
        shape=(100, 3),
    )
    assert (matrix != expected).nnz == 0


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
