from __future__ import annotations

import importlib
import sys
from pathlib import Path

import numpy as np


def test_depmap_ccle_to_anndata_emits_strict_baseline_contract(
    tmp_path: Path,
    monkeypatch,
):
    expr = tmp_path / "expression.csv"
    expr.write_text(
        "ModelID,A1BG (1),TP53 (7157),CellLineName\n"
        "ACH-000001,1.5,2.5,OVCAR3\n"
        "ACH-000002,3.5,4.5,HL60\n",
        encoding="utf-8",
    )

    import tools.lamin_context as lamin_context

    monkeypatch.setattr(lamin_context, "connect_pertdata", lambda: object())
    sys.modules.pop("tools.ingest_phase3_bulk", None)
    try:
        bulk = importlib.import_module("tools.ingest_phase3_bulk")
        adata = bulk.depmap_ccle_to_anndata({"expr": expr})
    finally:
        sys.modules.pop("tools.ingest_phase3_bulk", None)

    assert adata.obs_names.tolist() == ["ACH-000001", "ACH-000002"]
    assert adata.obs["depmap_id"].tolist() == adata.obs_names.tolist()
    assert adata.obs["model_id"].tolist() == adata.obs_names.tolist()
    assert adata.obs["baseline_join_id"].tolist() == adata.obs_names.tolist()
    assert set(adata.obs["dataset_release"]) == {"DepMap Public 26Q1"}
    assert set(adata.obs["baseline_lamin_prefix"]) == {"depmap_ccle/26q1"}
    assert set(adata.obs["artifact_role"]) == {"matched_baseline_expression"}
    assert adata.var["gene_symbol"].tolist() == ["A1BG", "TP53"]
    assert adata.var["entrez_id"].tolist() == ["1", "7157"]
    np.testing.assert_allclose(adata.X.toarray(), [[1.5, 2.5], [3.5, 4.5]])
    assert "CellLineName" not in adata.var_names
    assert "effect_score" not in adata.obs.columns
    assert "score" not in adata.obs.columns
