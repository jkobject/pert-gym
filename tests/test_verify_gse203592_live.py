from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

SCRIPT = Path(__file__).parents[1] / "tools/verify_gse203592_live.py"
spec = importlib.util.spec_from_file_location("verify_gse203592_live", SCRIPT)
assert spec is not None and spec.loader is not None
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def test_obs_receipt_requires_source_identity_and_control_parity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(verifier, "EXPECTED_N_OBS", 2)
    obs = pd.DataFrame(
        {
            "cell_barcode": ["c1", "c2"],
            "original_obs_index": ["c1", "c2"],
            "is_control": [False, True],
            "condition": ["test", "control"],
            "guide_sequence_state": ["unknown", "unknown"],
            "guide_sequence_source": ["source search", "source search"],
        },
        index=pd.Index(["c1", "c2"]),
    )

    receipt = verifier.obs_receipt(obs)

    assert receipt["rows"] == 2
    assert receipt["control_rows"] == 1
    assert receipt["canonical_state_column_count"] == 1
    obs.loc["c1", "condition"] = "control"
    with pytest.raises(AssertionError, match="control semantics"):
        verifier.obs_receipt(obs)


def test_var_receipt_rejects_axis_drift_and_reports_mouse_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(verifier, "EXPECTED_N_VARS", 2)
    var = pd.DataFrame(
        {
            "stable_feature_id": ["ENSMUSG00000000001", pd.NA],
            "stable_feature_id_mapping_status": ["mapped", "ambiguous"],
            "feature_index": ["ENSMUSG00000000001", "Gene.1"],
            "organism": ["Mus musculus", "Mus musculus"],
        },
        index=pd.Index(["Gene", "Gene.1"]),
    )

    receipt = verifier.var_receipt(var, var.index)

    assert receipt["mapped_ensmusg_rows"] == 1
    assert receipt["unresolved_rows"] == 1
    assert receipt["feature_index_complete_unique"] is True
    with pytest.raises(AssertionError, match="feature axis ordering"):
        verifier.var_receipt(var, pd.Index(["Gene.1", "Gene"]))
