from __future__ import annotations

import gzip
import importlib.util
from pathlib import Path

import pandas as pd

SCRIPT = Path(__file__).parents[1] / "tools" / "build_stt0000071_component.py"


def load_module():
    spec = importlib.util.spec_from_file_location("build_stt0000071_component", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_row(section: str, filename: str, index: int) -> dict[str, str]:
    relative = f"STSA0000734/{section}/{filename}"
    return {
        "relative_path": relative,
        "sample_id": "STSA0000734",
        "timepoint": "uninjured_1",
        "section_id": section,
        "section_label": "T1_C1",
        "filename": filename,
        "gcs_uri": f"gs://scperturb/pert-gym/staging/temporal_pretraining/stt0000071_cngb_non_tiff_20260630/{relative}",
        "gcs_generation": str(1000 + index),
        "gcs_size": "10",
    }


def test_classify_sections_requires_exact_cellbin_metadata_and_raw(monkeypatch) -> None:
    module = load_module()
    rows = []
    for index in range(46):
        section = f"STTS{index:07d}"
        rows.extend(
            [
                source_row(section, f"S{index}.70.gem.gz", index * 3),
                source_row(section, f"S{index}.70.tsv.gz", index * 3 + 1),
                source_row(section, f"S{index}.gem.gz", index * 3 + 2),
            ]
        )
    sections = module.classify_sections(rows)
    assert len(sections) == 46
    assert sections[0]["cell_bin"]["filename"].endswith(".70.gem.gz")
    assert sections[0]["metadata"]["filename"].endswith(".70.tsv.gz")
    assert sections[0]["raw_unbinned"]["filename"].endswith(".gem.gz")


def test_convert_section_preserves_coordinate_obs_x_parity(tmp_path: Path) -> None:
    module = load_module()
    gem = pd.DataFrame(
        {
            "geneID": ["g2", "g1", "g1"],
            "x": [10.123456789012, 10.123456789013, 20.0],
            "y": [30.0, 30.0, 40.0],
            "MIDCounts": [2, 1, 4],
        }
    )
    metadata = pd.DataFrame(
        {
            "x": [10.123456789014, 20.0],
            "y": [30.0, 40.0],
            "nGenes": [2, 1],
            "nUMI": [3, 4],
            "sid": ["s1", "s1"],
            "3D_x": [1.0, 2.0],
            "3D_y": [3.0, 4.0],
            "3D_z": [5.0, 6.0],
            "annotation": ["A", "B"],
            "cellname": ["c1", "c2"],
            "cid": ["section", "section"],
        }
    )
    gem_path = tmp_path / "section.70.gem.gz"
    metadata_path = tmp_path / "section.70.tsv.gz"
    gem.to_csv(gem_path, sep="\t", index=False, compression="gzip")
    metadata.to_csv(metadata_path, sep="\t", index=False, compression="gzip")
    section = {
        "sample_id": "STSA0000734",
        "section_id": "STTS0001152",
        "timepoint": "uninjured_1",
    }
    obs, matrix, stats = module.convert_section(
        gem_path, metadata_path, genes=["g1", "g2"], section=section
    )
    assert list(matrix.shape) == [2, 2]
    assert matrix.toarray().tolist() == [[1, 2], [4, 0]]
    assert stats == {
        "n_obs": 2,
        "n_vars": 2,
        "section_present_genes": 2,
        "nnz": 3,
        "sum": 7,
        "metadata_n_counts_sum": 7,
    }
    assert obs.index.tolist() == ["c1", "c2"]
    assert obs["age"].isna().all()
    assert set(obs["age_missingness_reason"]) == {"source_not_reported"}
    assert obs["timepoint"].tolist() == [0, 0]


def test_sparse_zarr_zip_roundtrip(tmp_path: Path) -> None:
    module = load_module()
    matrix = module.sparse.csr_matrix([[0, 2], [3, 0]], dtype="int64")
    path = tmp_path / "X.zarr.zip"
    module.write_sparse_zarr_zip(path, matrix)
    loaded = module.read_sparse_zarr_zip(path)
    assert loaded.shape == matrix.shape
    assert loaded.nnz == matrix.nnz
    assert (loaded != matrix).nnz == 0
