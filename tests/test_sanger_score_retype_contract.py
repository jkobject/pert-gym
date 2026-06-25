from pathlib import Path

import anndata as ad
import pandas as pd
import scipy.sparse as sp

from artifacts.scripts.repair_sanger_score_crispr_retype_20260625 import (
    build_empty_canonical_payloads,
    build_score_payload,
    matrix_nnz,
)
from tools.plan_phase3_ingestion import DATASETS


def test_sanger_score_legacy_converter_refuses_fake_expression_x() -> None:
    source = Path("tools/ingest_phase3_bulk.py").read_text(encoding="utf-8")

    assert "Reject the legacy fake-expression SCORE conversion" in source
    assert "not an expression matrix" in source
    assert "X_score/var_score auxiliary payload" in source


def test_repair_payload_builds_empty_canonical_x_and_typed_score_auxiliary() -> None:
    obs = pd.DataFrame(index=pd.Index(["SIDM1", "SIDM2"], name="model"))
    score = ad.AnnData(
        X=sp.csr_matrix([[-1.2, 0.0], [0.4, -0.7]], dtype="float32"),
        obs=pd.DataFrame(index=obs.index.copy()),
        var=pd.DataFrame(index=pd.Index(["GENE1", "GENE2"], name="gene_id")),
    )

    empty_x, empty_var = build_empty_canonical_payloads(obs)
    score_payload = build_score_payload(score)

    assert empty_x.shape == (2, 0)
    assert matrix_nnz(empty_x.X) == 0
    assert empty_x.uns["x_semantics"] == "empty"
    assert empty_var.empty
    assert score_payload.shape == (2, 2)
    assert matrix_nnz(score_payload.X) == 3
    assert score_payload.uns["x_semantics"] == "essentiality_score"
    assert score_payload.uns["canonical_expression_X"] is False


def test_phase3_plan_marks_sanger_score_as_auxiliary_not_model_ready_expression() -> None:
    sanger = next(dataset for dataset in DATASETS if dataset.name == "Sanger SCORE CRISPR KO")
    depmap = next(dataset for dataset in DATASETS if dataset.name == "DepMap CCLE")

    assert "X_score.h5ad" in sanger.expected_outputs
    assert "X.h5ad(empty)" in sanger.expected_outputs
    assert "x_semantics=essentiality_score" in sanger.notes
    assert "expression" not in sanger.notes.split(".")[0].lower()

    assert depmap.expected_outputs == ["obs.parquet", "X.h5ad", "var.parquet"]
    assert depmap.lamin_prefix.startswith("depmap_ccle/")
