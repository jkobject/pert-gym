from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
REVIEW = ROOT / "artifacts/review/temporal-v4-099-parquet-parity-v1"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_review_artifact_is_bound_to_exact_writer_and_helper() -> None:
    authorization = json.loads((REVIEW / "authorization.json").read_text())
    writer = REVIEW / "write_component.py"
    helper = REVIEW / "parquet_frame_parity.py"

    assert authorization["writer_sha256"] == sha256(writer)
    assert authorization["parquet_frame_parity_sha256"] == sha256(helper)
    assert (
        helper.read_bytes()
        == (ROOT / "src/pert_gym/parquet_frame_parity.py").read_bytes()
    )


def test_review_authorization_is_fail_closed_and_bounded() -> None:
    authorization = json.loads((REVIEW / "authorization.json").read_text())

    assert (
        authorization["protocol"] == "temporal-v4-099-category-safe-parquet-parity/v2"
    )
    assert authorization["shape"] == [10224, 35552]
    assert authorization["accepted_components"] == {
        "current": 2,
        "denominator": 153,
    }
    assert authorization["accepted_components_credit"] == 0
    assert authorization["failed_candidate_denylist"] == [
        "temporal-v4-099-20260715T135852Z-d36b0c6d"
    ]
    assert authorization["fresh_immutable_revision_required"] is True
    assert authorization["single_writer_lease"] == "global-plus-legacy-exclusive"
    assert authorization["manifest_last"] is True
    assert authorization["forbidden_actions"] == [
        "cleanup",
        "deletion",
        "promotion",
        "collection_mutation",
    ]
    assert authorization["execution_timeout_seconds"] == 7200
    assert authorization["max_rss_bytes"] == 24 * 1024**3
    assert authorization["min_available_bytes"] == 4 * 1024**3
    assert authorization["writer_host"] == "pert-gym-worker-eu"
    assert authorization["writer_region"] == "europe-west1-b"


def test_writer_records_obs_and_shared_var_parity_without_dataframe_equals() -> None:
    writer = (REVIEW / "write_component.py").read_text()

    assert (
        "obs_parity = frame_parity.assert_parquet_frame_parity(obs, remote_obs)"
        in writer
    )
    assert (
        "var_parity = frame_parity.assert_parquet_frame_parity(var, remote_var)"
        in writer
    )
    assert (
        "existing_shared_var_parity = frame_parity.assert_parquet_frame_parity(var, remote_var)"
        in writer
    )
    assert '"parquet_frame_parity": {' in writer
    assert "remote_obs.equals(obs)" not in writer
    assert "remote_var.equals(var)" not in writer
    assert "ordered_var_identity(list(map(str, remote_var.index)))" in writer
