from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

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


def test_main_rejects_capacity_vm_before_building_candidates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(runner.socket, "gethostname", lambda: "pert-gym-capacity-eu-v2")
    metadata = {
        "project/project-id": runner.EXPECTED_GCE_PROJECT,
        "instance/zone": f"projects/1/zones/{runner.EXPECTED_ZONE}",
        "instance/name": "pert-gym-capacity-eu-v2",
    }
    monkeypatch.setattr(runner, "_metadata_value", metadata.__getitem__)
    monkeypatch.setattr(
        kolf.sys,
        "argv",
        [
            "ingest_kolf21j_candidates.py",
            "--source-dir",
            str(tmp_path / "source"),
            "--output-root",
            str(tmp_path / "output"),
            "--report",
            str(tmp_path / "report.json"),
        ],
    )

    def fail_if_candidate_build_starts(*args: object, **kwargs: object) -> None:
        raise AssertionError("candidate build must not start on the capacity VM")

    monkeypatch.setattr(kolf, "build_variant", fail_if_candidate_build_starts)

    with pytest.raises(RuntimeError, match="pert-gym-capacity-eu-v2"):
        kolf.main()

    assert not (tmp_path / "source").exists()
    assert not (tmp_path / "output").exists()
    assert not (tmp_path / "report.json").exists()
