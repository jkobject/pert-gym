import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import h5py
import numpy as np
import pytest

SCRIPT = (
    Path(__file__).resolve().parents[1] / "tools" / "ingest_xatlas_orion_chunk0000.py"
)
ROOT = SCRIPT.parents[1]

spec = importlib.util.spec_from_file_location(
    "_xatlas_orion_chunk0000_under_test", SCRIPT
)
assert spec is not None and spec.loader is not None
chunker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(chunker)


def test_cli_help_starts_under_uv_python_environment() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "--chunk-index" in result.stdout
    assert "--chunk-size" in result.stdout


def _args(**overrides):
    values = {
        "dataset": chunker.DEFAULT_DATASET,
        "source_uri": None,
        "prefix_root": chunker.DEFAULT_PREFIX_ROOT,
        "chunk_index": chunker.DEFAULT_CHUNK_INDEX,
        "chunk_size": chunker.DEFAULT_CHUNK_SIZE,
        "billing_project": chunker.DEFAULT_BILLING_PROJECT,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_chunker_loads_repo_local_helpers_without_tools_package_import() -> None:
    text = SCRIPT.read_text()

    assert 'load_repo_tool_module("lamin_context")' in text
    assert 'load_repo_tool_module("clean_lamin_cache")' in text
    assert "from tools.lamin_context" not in text
    assert "from tools.clean_lamin_cache" not in text


def test_validate_smoke_constraints_returns_authorized_hct116_target():
    source_uri, prefix, chunk_index, chunk_size, cell_line = (
        chunker.validate_smoke_constraints(_args(chunk_index=1))
    )

    assert source_uri == chunker.DATASETS[chunker.DEFAULT_DATASET]["source_uri"]
    assert prefix == "xatlas/orion/hct116_filtered_dual_guide_cells/chunk_0001"
    assert chunk_index == 1
    assert chunk_size == 1000
    assert cell_line == "HCT116"


def test_validate_smoke_constraints_returns_authorized_hek293t_target():
    source_uri, prefix, chunk_index, chunk_size, cell_line = (
        chunker.validate_smoke_constraints(
            _args(dataset="hek293t_filtered_dual_guide_cells", chunk_index=0)
        )
    )

    assert (
        source_uri
        == chunker.DATASETS["hek293t_filtered_dual_guide_cells"]["source_uri"]
    )
    assert prefix == "xatlas/orion/hek293t_filtered_dual_guide_cells/chunk_0000"
    assert chunk_index == 0
    assert chunk_size == 1000
    assert cell_line == "HEK293T"


@pytest.mark.parametrize(
    "override",
    [
        {"dataset": "unknown_cells"},
        {"source_uri": "gs://scperturb/other.h5ad"},
        {"prefix_root": "xatlas/orion/test"},
        {"chunk_index": -1},
        {"chunk_size": 999},
        {"billing_project": "other-project"},
    ],
)
def test_validate_smoke_constraints_rejects_ambiguous_or_unsafe_overrides(override):
    with pytest.raises(ValueError, match="NO_GO"):
        chunker.validate_smoke_constraints(_args(**override))


def test_duplicate_probe_queries_exact_triplet_keys_only():
    class ArtifactRecord:
        def __init__(self, key):
            self.key = key

    class Query:
        def __init__(self, keys):
            self.keys = keys

        def all(self):
            assert self.keys == [
                "prefix/chunk_0000/obs.parquet",
                "prefix/chunk_0000/X.h5ad",
                "prefix/chunk_0000/var.parquet",
            ]
            return [ArtifactRecord(self.keys[2]), ArtifactRecord(self.keys[0])]

    class FakeArtifact:
        @staticmethod
        def filter(key__in):
            return Query(key__in)

    class FakeLn:
        Artifact = FakeArtifact

    assert chunker.duplicate_probe(FakeLn(), "prefix/chunk_0000") == [
        "prefix/chunk_0000/obs.parquet",
        "prefix/chunk_0000/var.parquet",
    ]


def test_read_csr_rows_reads_only_requested_row_window(tmp_path):
    path = tmp_path / "x.h5"
    with h5py.File(path, "w") as h5:
        x = h5.create_group("X")
        x.attrs["shape"] = (3, 4)
        x.create_dataset("data", data=np.array([1, 2, 3, 4, 5], dtype=np.float32))
        x.create_dataset("indices", data=np.array([0, 2, 1, 3, 0], dtype=np.int64))
        x.create_dataset("indptr", data=np.array([0, 2, 3, 5], dtype=np.int64))

    with h5py.File(path, "r") as h5:
        rows = chunker.read_csr_rows(h5["X"], 1, 3)

    assert rows.shape == (2, 4)
    assert rows.toarray().tolist() == [[0, 3, 0, 0], [5, 0, 0, 4]]


def test_write_status_preserves_prior_attempts(tmp_path, monkeypatch):
    monkeypatch.setattr(chunker, "STATUS_DIR", tmp_path)

    first = {"status": "no_go", "no_go_reason": "first"}
    second = {"status": "no_go", "no_go_reason": "second"}

    first_path = chunker.write_status(first)
    second_path = chunker.write_status(second)

    assert first_path != second_path
    assert first_path.exists()
    assert second_path.exists()
    assert json.loads(first_path.read_text())["no_go_reason"] == "first"
    assert json.loads(second_path.read_text())["no_go_reason"] == "second"
    assert json.loads(first_path.read_text())["status_path"] == str(first_path)
    assert json.loads(second_path.read_text())["status_path"] == str(second_path)


def test_main_rejects_duplicate_target_before_any_source_read_or_write(
    tmp_path, monkeypatch, capsys
):
    events = []
    existing = [
        "xatlas/orion/hct116_filtered_dual_guide_cells/chunk_0000/obs.parquet",
    ]

    class Query:
        def all(self):
            events.append("duplicate_probe")
            return [SimpleNamespace(key=key) for key in existing]

    class FakeArtifact:
        @staticmethod
        def filter(key__in):
            assert key__in == chunker.artifact_keys_for(chunker.DEFAULT_TARGET_PREFIX)
            return Query()

        @staticmethod
        def from_dataframe(*args, **kwargs):  # pragma: no cover - must not be called
            raise AssertionError("write attempted after duplicate probe")

        @staticmethod
        def from_anndata(*args, **kwargs):  # pragma: no cover - must not be called
            raise AssertionError("write attempted after duplicate probe")

    fake_ln = SimpleNamespace(
        Artifact=FakeArtifact,
        setup=SimpleNamespace(
            settings=SimpleNamespace(
                instance=SimpleNamespace(slug="laminlabs/pertdata"),
                branch=SimpleNamespace(name="jkobject", uid="branch-uid"),
            )
        ),
    )

    def fail_url_to_filesystem(
        *args, **kwargs
    ):  # pragma: no cover - must not be called
        raise AssertionError("source read attempted despite duplicate target")

    monkeypatch.setattr(chunker, "STATUS_DIR", tmp_path)
    monkeypatch.setattr(chunker, "ensure_project_cache", lambda: events.append("cache"))
    monkeypatch.setattr(
        chunker,
        "connect_pertdata",
        lambda: events.append("connect_pertdata") or fake_ln,
    )
    monkeypatch.setattr(chunker, "url_to_filesystem", fail_url_to_filesystem)
    monkeypatch.setattr(sys, "argv", ["ingest_xatlas_orion_chunk0000.py", "--dry-run"])

    assert chunker.main() == 2

    out = capsys.readouterr().out
    assert "LAMIN laminlabs/pertdata jkobject branch-uid" in out
    assert (
        "NO_GO DUPLICATE_TARGET xatlas/orion/hct116_filtered_dual_guide_cells/chunk_0000"
        in out
    )
    assert events == ["cache", "connect_pertdata", "duplicate_probe"]
    status_paths = list(tmp_path.glob("*.json"))
    assert len(status_paths) == 1
    status = json.loads(status_paths[0].read_text())
    assert status["status"] == "no_go"
    assert status["prefix"] == chunker.DEFAULT_TARGET_PREFIX
    assert status["existing_exact_keys_before"] == existing
    assert status["dry_run"] is True
    assert status["constraints"]["overwrite"] is False


def test_main_records_no_go_for_unsafe_precondition_without_connecting(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setattr(chunker, "STATUS_DIR", tmp_path)
    monkeypatch.setattr(
        chunker,
        "connect_pertdata",
        lambda: (_ for _ in ()).throw(AssertionError("connect should not happen")),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["ingest_xatlas_orion_chunk0000.py", "--chunk-index", "-1", "--dry-run"],
    )

    assert chunker.main() == 2

    out = capsys.readouterr().out
    assert "NO_GO NO_GO: chunk index must be non-negative" in out
    status_paths = list(tmp_path.glob("*.json"))
    assert len(status_paths) == 1
    status = json.loads(status_paths[0].read_text())
    assert status["status"] == "no_go"
    assert "non-negative" in status["no_go_reason"]
    assert status["constraints"]["chunk_id"] == "chunk_0000"
    assert status["constraints"]["chunk_size_rows"] == 1000
