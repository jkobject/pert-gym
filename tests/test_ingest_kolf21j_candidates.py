from __future__ import annotations

import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
import pytest
from scipy import sparse

from tools import pert_gym_vm_runner as runner

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "ingest_kolf21j_candidates", ROOT / "tools" / "ingest_kolf21j_candidates.py"
)
assert SPEC is not None and SPEC.loader is not None
kolf = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = kolf
SPEC.loader.exec_module(kolf)


class _TripletFeatures:
    def __init__(self) -> None:
        self.values: dict[str, _TripletArtifact] = {}

    def set_values(self, values: dict[str, _TripletArtifact]) -> None:
        self.values.update(values)

    def get_values(self) -> dict[str, _TripletArtifact]:
        return dict(self.values)


@dataclass
class _TripletArtifact:
    manager: "_TripletArtifacts"
    key: str
    uid: str

    def __post_init__(self) -> None:
        self.features = _TripletFeatures()

    def save(self) -> "_TripletArtifact":
        self.manager.saved[self.key] = self
        return self


class _TripletQuery:
    def __init__(self, records: list[_TripletArtifact]) -> None:
        self.records = records

    def all(self) -> list[_TripletArtifact]:
        return self.records

    def exists(self) -> bool:
        return bool(self.records)


class _TripletArtifacts:
    def __init__(self) -> None:
        self.saved: dict[str, _TripletArtifact] = {}

    def filter(self, *, key: str) -> _TripletQuery:
        artifact = self.saved.get(key)
        return _TripletQuery([] if artifact is None else [artifact])

    def _new(self, key: str) -> _TripletArtifact:
        return _TripletArtifact(self, key, f"uid-{key}")

    def from_dataframe(
        self, _frame: pd.DataFrame, *, key: str, **_kwargs: Any
    ) -> _TripletArtifact:
        return self._new(key)

    def from_anndata(
        self, _adata: Any, *, key: str, **_kwargs: Any
    ) -> _TripletArtifact:
        return self._new(key)


class _TripletLn:
    def __init__(self) -> None:
        self.Artifact = _TripletArtifacts()


@pytest.mark.parametrize("stage", ["obs", "x", "var", "var-link", "x-link"])
def test_register_triplet_resumes_after_each_remote_mutation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, stage: str
) -> None:
    variant = kolf.VARIANTS[0]
    obs = pd.DataFrame({"value": [1]}, index=["cell-1"])
    var = pd.DataFrame({"value": [2]}, index=["gene-1"])
    x_path = tmp_path / "X.h5ad"
    x_path.write_bytes(b"candidate X")
    ln = _TripletLn()
    monkeypatch.setattr(kolf.ad, "read_h5ad", lambda *_args, **_kwargs: object())

    with pytest.raises(RuntimeError, match=f"intentional crash after {stage}"):
        kolf.register_triplet(ln, variant, obs, var, x_path, stop_after_stage=stage)

    obs_key, x_key, var_key = kolf.candidate_keys(variant)
    expected_uid = {
        "obs": f"uid-{obs_key}",
        "x": f"uid-{x_key}",
        "var": f"uid-{var_key}",
        "var-link": f"uid-{x_key}",
        "x-link": f"uid-{obs_key}",
    }[stage]
    journal = json.loads((tmp_path / "publication-journal.json").read_text())
    assert journal["pending_stage"] == {"stage": stage, "uid": expected_uid}

    result = kolf.register_triplet(ln, variant, obs, var, x_path)

    assert result == {
        "obs_key": obs_key,
        "x_key": x_key,
        "var_key": var_key,
        "obs_uid": f"uid-{obs_key}",
    }
    assert set(ln.Artifact.saved) == {obs_key, x_key, var_key}
    assert ln.Artifact.saved[obs_key].features.get_values()["X"].key == x_key
    assert ln.Artifact.saved[x_key].features.get_values()["var"].key == var_key
    journal = json.loads((tmp_path / "publication-journal.json").read_text())
    assert journal["artifact_uids"] == {
        "obs": f"uid-{obs_key}",
        "x": f"uid-{x_key}",
        "var": f"uid-{var_key}",
    }


@pytest.mark.parametrize("stage", ["obs", "x", "var", "var-link", "x-link"])
def test_register_triplet_rebinds_pending_stage_after_crash_before_remote_mutation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, stage: str
) -> None:
    variant = kolf.VARIANTS[0]
    obs = pd.DataFrame({"value": [1]}, index=["cell-1"])
    var = pd.DataFrame({"value": [2]}, index=["gene-1"])
    x_path = tmp_path / "X.h5ad"
    x_path.write_bytes(b"candidate X")
    ln = _TripletLn()
    monkeypatch.setattr(kolf.ad, "read_h5ad", lambda *_args, **_kwargs: object())
    begin_stage = kolf._begin_stage

    def crash_after_durable_begin(
        path: Path, journal: dict[str, object], current_stage: str, artifact: object
    ) -> None:
        begin_stage(path, journal, current_stage, artifact)
        if current_stage == stage:
            raise RuntimeError(f"intentional crash before {stage} remote mutation")

    with monkeypatch.context() as context:
        context.setattr(kolf, "_begin_stage", crash_after_durable_begin)
        with pytest.raises(
            RuntimeError, match=f"intentional crash before {stage} remote mutation"
        ):
            kolf.register_triplet(ln, variant, obs, var, x_path)

    obs_key, x_key, var_key = kolf.candidate_keys(variant)
    journal_path = tmp_path / "publication-journal.json"
    journal = json.loads(journal_path.read_text())
    assert journal["pending_stage"] is not None
    assert journal["pending_stage"]["stage"] == stage
    expected_saved_stages = kolf._TRIPLET_STAGES.index(stage)
    assert set(ln.Artifact.saved) == set(
        (obs_key, x_key, var_key)[:expected_saved_stages]
    )

    result = kolf.register_triplet(ln, variant, obs, var, x_path)

    assert result == {
        "obs_key": obs_key,
        "x_key": x_key,
        "var_key": var_key,
        "obs_uid": f"uid-{obs_key}",
    }
    assert set(ln.Artifact.saved) == {obs_key, x_key, var_key}
    assert ln.Artifact.saved[obs_key].features.get_values()["X"].key == x_key
    assert ln.Artifact.saved[x_key].features.get_values()["var"].key == var_key
    journal = json.loads(journal_path.read_text())
    assert journal["completed_stages"] == list(kolf._TRIPLET_STAGES)
    assert journal["pending_stage"] is None


def test_register_triplet_rejects_unbound_same_key_foreign_artifact(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    variant = kolf.VARIANTS[0]
    obs = pd.DataFrame({"value": [1]}, index=["cell-1"])
    var = pd.DataFrame({"value": [2]}, index=["gene-1"])
    x_path = tmp_path / "X.h5ad"
    x_path.write_bytes(b"candidate X")
    ln = _TripletLn()
    obs_key, _, _ = kolf.candidate_keys(variant)
    ln.Artifact.saved[obs_key] = _TripletArtifact(ln.Artifact, obs_key, "foreign-uid")
    monkeypatch.setattr(kolf.ad, "read_h5ad", lambda *_args, **_kwargs: object())

    with pytest.raises(RuntimeError, match="unbound remote stage"):
        kolf.register_triplet(ln, variant, obs, var, x_path)

    assert set(ln.Artifact.saved) == {obs_key}


def test_register_triplet_rejects_journal_artifact_uid_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    variant = kolf.VARIANTS[0]
    obs = pd.DataFrame({"value": [1]}, index=["cell-1"])
    var = pd.DataFrame({"value": [2]}, index=["gene-1"])
    x_path = tmp_path / "X.h5ad"
    x_path.write_bytes(b"candidate X")
    ln = _TripletLn()
    monkeypatch.setattr(kolf.ad, "read_h5ad", lambda *_args, **_kwargs: object())
    kolf.register_triplet(ln, variant, obs, var, x_path)

    journal_path = tmp_path / "publication-journal.json"
    journal = json.loads(journal_path.read_text())
    journal["artifact_uids"]["obs"] = "foreign-uid"
    journal_path.write_text(json.dumps(journal))

    with pytest.raises(RuntimeError, match="UID mismatch"):
        kolf.register_triplet(ln, variant, obs, var, x_path)


def test_register_triplet_rejects_save_uid_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    variant = kolf.VARIANTS[0]
    obs = pd.DataFrame({"value": [1]}, index=["cell-1"])
    var = pd.DataFrame({"value": [2]}, index=["gene-1"])
    x_path = tmp_path / "X.h5ad"
    x_path.write_bytes(b"candidate X")
    ln = _TripletLn()
    monkeypatch.setattr(kolf.ad, "read_h5ad", lambda *_args, **_kwargs: object())

    def save_under_foreign_uid(self: _TripletArtifact) -> _TripletArtifact:
        saved = _TripletArtifact(self.manager, self.key, "foreign-uid")
        self.manager.saved[self.key] = saved
        return saved

    monkeypatch.setattr(_TripletArtifact, "save", save_under_foreign_uid)

    with pytest.raises(RuntimeError, match="artifact save UID mismatch"):
        kolf.register_triplet(ln, variant, obs, var, x_path)


@pytest.mark.parametrize("contents", ["{", "[]", '{"completed_stages": []}'])
def test_register_triplet_rejects_malformed_or_torn_journal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, contents: str
) -> None:
    variant = kolf.VARIANTS[0]
    obs = pd.DataFrame({"value": [1]}, index=["cell-1"])
    var = pd.DataFrame({"value": [2]}, index=["gene-1"])
    x_path = tmp_path / "X.h5ad"
    x_path.write_bytes(b"candidate X")
    (tmp_path / "publication-journal.json").write_text(contents)
    monkeypatch.setattr(kolf.ad, "read_h5ad", lambda *_args, **_kwargs: object())

    with pytest.raises(RuntimeError, match="malformed"):
        kolf.register_triplet(_TripletLn(), variant, obs, var, x_path)


def test_write_journal_replaces_a_fully_flushed_temporary_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "publication-journal.json"
    replacements: list[tuple[object, object]] = []
    real_replace = os.replace

    def record_replace(source: object, destination: object) -> None:
        replacements.append((source, destination))
        real_replace(source, destination)

    monkeypatch.setattr(kolf, "os", os, raising=False)
    monkeypatch.setattr(kolf.os, "replace", record_replace)

    kolf._write_journal(path, {"completed_stages": ["obs"]})

    assert replacements
    assert json.loads(path.read_text()) == {"completed_stages": ["obs"]}


def test_write_journal_ignores_directory_fsync_failure_after_atomic_replace(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "publication-journal.json"
    real_fsync = os.fsync
    calls = 0

    def fail_directory_fsync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("directory fsync unavailable")
        real_fsync(descriptor)

    monkeypatch.setattr(kolf.os, "fsync", fail_directory_fsync)

    kolf._write_journal(path, {"completed_stages": ["obs"]})

    assert calls == 2
    assert json.loads(path.read_text()) == {"completed_stages": ["obs"]}


def test_register_triplet_rejects_non_contiguous_journal_before_remote_save(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    variant = kolf.VARIANTS[0]
    obs = pd.DataFrame({"value": [1]}, index=["cell-1"])
    var = pd.DataFrame({"value": [2]}, index=["gene-1"])
    x_path = tmp_path / "X.h5ad"
    x_path.write_bytes(b"candidate X")
    ln = _TripletLn()
    monkeypatch.setattr(kolf.ad, "read_h5ad", lambda *_args, **_kwargs: object())
    kolf.register_triplet(ln, variant, obs, var, x_path)

    journal_path = tmp_path / "publication-journal.json"
    journal = json.loads(journal_path.read_text())
    journal["completed_stages"] = ["x"]
    journal_path.write_text(json.dumps(journal))
    saved_before = dict(ln.Artifact.saved)

    with pytest.raises(RuntimeError, match="journal stages must form a contiguous"):
        kolf.register_triplet(ln, variant, obs, var, x_path)

    assert ln.Artifact.saved == saved_before


def test_prepare_x_candidate_reuses_existing_payload_for_publication_resume(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output_dir = tmp_path / "candidate"
    output_dir.mkdir()
    x_path = output_dir / "X.h5ad"
    x_path.write_bytes(b"already-built X")

    monkeypatch.setattr(
        kolf,
        "write_x_only_h5ad",
        lambda *_args, **_kwargs: pytest.fail("resume must not rewrite X"),
    )

    assert kolf.prepare_x_candidate(tmp_path / "source.h5ad", output_dir) == x_path


def test_build_canonical_obs_maps_pilot_fields_without_mutating_source() -> None:
    source = pd.DataFrame(
        {
            "gene_target": ["NTC", "TGFBR2"],
            "gene_target_ensembl_id": ["NTC", "ENSG00000163513"],
            "gRNA": ["NTC_1", "TGFBR2_1"],
            "perturbed": ["False", "True"],
            "channel": ["Channel-A", "Channel-B"],
            "batch": ["Channel", "Channel"],
        },
        index=pd.Index(["cell-1", "cell-2"], name="barcode"),
    )

    result = kolf.build_canonical_obs(
        source, dataset_id="kolf21j_pan_genome_qc_filtered"
    )

    assert result["is_control"].tolist() == [True, False]
    assert result["is_perturbed"].tolist() == [False, True]
    assert result["perturbation"].tolist() == ["NTC", "TGFBR2"]
    assert result["perturbation_target_id"].tolist() == ["NTC", "ENSG00000163513"]
    assert result["guide_id"].tolist() == ["NTC_1", "TGFBR2_1"]
    assert result["dataset_id"].tolist() == ["kolf21j_pan_genome_qc_filtered"] * 2
    assert "is_control" not in source


def test_write_x_only_h5ad_preserves_sparse_x_without_copying_layers(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.h5ad"
    output_path = tmp_path / "X.h5ad"
    matrix = sparse.csr_matrix(np.array([[0, 3], [4, 0]], dtype=np.float32))
    source = ad.AnnData(
        X=matrix,
        obs=pd.DataFrame(index=["a", "b"]),
        var=pd.DataFrame(index=["g1", "g2"]),
        layers={"counts": matrix.copy()},
    )
    source.write_h5ad(source_path)

    kolf.write_x_only_h5ad(source_path, output_path)

    result = ad.read_h5ad(output_path, backed="r")
    try:
        assert result.shape == (2, 2)
        assert list(result.layers.keys()) == []
        assert (result.X[:, :].toarray() == matrix.toarray()).all()
    finally:
        result.file.close()
