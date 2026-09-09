from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "tools" / "run_arc_vcc_obs_var_eu.sh"


def test_arc_vcc_remote_runner_fails_closed_and_emits_payload_heartbeats() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "set -euo pipefail" in text
    assert "write_heartbeat preflight 0" in text
    assert 'write_heartbeat "${MODE}_source_join" 0' in text
    assert "write_heartbeat terminal 6" in text
    assert "os.replace(temporary, path)" in text
    assert 'python3 - "$$"' in text
    assert "if pid in ancestors or pgid == current_pgid:" in text
    assert '"pid=,ppid=,pgid=,args="' in text

    assert "CONFLICTING_ARC_WRITER" in text
    assert "pgrep -af" not in text


def test_arc_vcc_remote_runner_records_all_three_primary_h5ad_identities() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    for path in (
        "test/adata_Test.h5ad",
        "train/adata_Training.h5ad",
        "validation/adata_Validation.h5ad",
    ):
        assert path in text
    assert "json(generation,size,crc32c,md5Hash,updateTime)" in text