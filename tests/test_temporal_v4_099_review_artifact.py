from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
REVIEW = ROOT / "artifacts/review/temporal-v4-099-parquet-parity-v1"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_review_artifact_binds_authorization_to_current_writer() -> None:
    authorization = json.loads((REVIEW / "authorization.json").read_text())
    writer = REVIEW / "write_component.py"
    contract = REVIEW / "writer_contract.py"
    helper = REVIEW / "parquet_frame_parity.py"

    assert authorization["writer_sha256"] == sha256(writer)
    assert authorization["writer_contract_sha256"] == sha256(contract)
    assert authorization["parquet_frame_parity_sha256"] == sha256(helper)
    assert (
        helper.read_bytes()
        == (ROOT / "src/pert_gym/parquet_frame_parity.py").read_bytes()
    )


def test_review_authorization_is_fail_closed_and_bounded() -> None:
    authorization = json.loads((REVIEW / "authorization.json").read_text())
    config = json.loads((REVIEW / "row-99-config.json").read_text())

    assert authorization["protocol"] == "cellxgene-category-safe-logical-sparse-zarr/v1"
    assert config["shape"] == [10224, 35552]
    assert config["accepted_components"] == {
        "current": 2,
        "denominator": 153,
        "credit": 0,
    }
    assert config["revision"]["failed_candidate_denylist"] == [
        "temporal-v4-099-20260715T135852Z-d36b0c6d"
    ]
    assert config["revision"]["fresh_immutable_required"] is True
    assert config["execution"]["single_writer_lease"] == "global-plus-legacy-exclusive"
    assert config["storage"]["manifest_last"] is True
    assert config["forbidden_actions"] == [
        "cleanup",
        "deletion",
        "promotion",
        "collection_mutation",
        "lamin_main",
        "vm_lifecycle_change",
    ]
    assert config["execution"]["timeout_seconds"] == 7200
    assert config["execution"]["max_rss_bytes"] == 24 * 1024**3
    assert config["execution"]["min_available_bytes"] == 4 * 1024**3
    assert config["execution"]["host"] == "pert-gym-worker-eu"
    assert config["execution"]["zone"] == "europe-west1-b"
    assert authorization["execution_authorized"] is False


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
