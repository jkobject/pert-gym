from __future__ import annotations

import subprocess

from tools import ingest_xatlas_orion as xatlas


def test_active_huge_ingestions_ignores_prompt_mentions(monkeypatch):
    ps_output = "\n".join(
        [
            "USER PID %CPU %MEM VSZ RSS TT STAT STARTED TIME COMMAND",
            "jk 101 0.0 0.0 1 1 ?? S 1:00PM 0:00.01 /Users/jkobject/.hermes/hermes-agent/venv/bin/python -m hermes worker --prompt 'review tools/ingest_xatlas_orion.py and run_prism_ingestion_batch.py'",
            "jk 202 0.0 0.0 1 1 ?? S 1:00PM 0:00.01 uv run python tools/ingest_prism_large_h5ad_chunks.py gs://bucket/source.h5ad --dataset GSE --max-chunks 1",
        ]
    )

    def fake_check_output(cmd, text=True):
        if cmd == ["ps", "auxww"]:
            return ps_output
        raise AssertionError(cmd)

    monkeypatch.setattr(subprocess, "check_output", fake_check_output)
    monkeypatch.setattr(xatlas, "current_process_family_pids", lambda: {999})

    matches = xatlas.active_huge_ingestions()

    assert len(matches) == 1
    assert "ingest_prism_large_h5ad_chunks.py" in matches[0]
    assert "--prompt" not in matches[0]


def test_invokes_huge_ingestion_script_supports_shell_c():
    assert xatlas.invokes_huge_ingestion_script(
        "bash -lc 'uv run python tools/ingest_xatlas_orion.py /mnt/file --dataset hct116'"
    )
    assert not xatlas.invokes_huge_ingestion_script(
        "python -m hermes worker --prompt 'uv run python tools/ingest_xatlas_orion.py /mnt/file'"
    )
