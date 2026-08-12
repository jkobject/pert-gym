from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "artifacts"
    / "schema_audit"
    / "dataset_full_dod_20260812"
    / "depmap_ccle_26q1"
    / "t_c90d1146"
    / "verify_full_dod.py"
)
SPEC = importlib.util.spec_from_file_location("depmap_ccle_26q1_full_dod", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_verifier_binds_exact_dataset_payload_and_staging_generation() -> None:
    assert MODULE.TASK_ID == "t_c90d1146"
    assert MODULE.DATASET_ID == "depmap_ccle/26q1"
    assert MODULE.EXPECTED_ARTIFACTS == {
        "obs": {
            "uid": "kCNSxyUJoJJKRSgE0004",
            "key": "depmap_ccle/26q1/obs.parquet",
            "hash": "Zm-yc0UfSwnYI1DnDfoccQ",
            "n_observations": 1719,
        },
        "x": {
            "uid": "fUSYT9ArHdQye5qv0001",
            "key": "depmap_ccle/26q1/X.h5ad",
            "hash": "I1DppOQzGK8jczy2Lh_J9O",
            "n_observations": 1719,
        },
        "var": {
            "uid": "0S0wAPqgigynI4Av0003",
            "key": "depmap_ccle/26q1/var.parquet",
            "hash": "5wjqSsaFA7D0kcZSts--ig",
            "n_observations": 19215,
        },
    }
    assert MODULE.STAGING_MANIFEST_URI.endswith("manifest.json#1784226218253256")
    assert len(MODULE.STAGING_MANIFEST_SHA256) == 64


def test_verifier_refuses_non_generation_qualified_manifest() -> None:
    with pytest.raises(ValueError, match="generation-qualified"):
        MODULE.gcs_get_exact("gs://scperturb/path/manifest.json")


def test_receipt_digest_excludes_only_its_signature() -> None:
    receipt = {"status": "PASS", "nested": {"writes": 0}}
    first = MODULE.receipt_sha256(receipt)
    receipt["canonical_sha256"] = first
    assert MODULE.receipt_sha256(receipt) == first
    receipt["nested"]["writes"] = 1
    assert MODULE.receipt_sha256(receipt) != first
