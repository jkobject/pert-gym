from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
sp = pytest.importorskip("scipy.sparse")
h5py = pytest.importorskip("h5py")
if importlib.util.find_spec("anndata") is None:
    sys.modules["anndata"] = types.ModuleType("anndata")

SCRIPT = (
    Path(__file__).parents[1]
    / "artifacts/schema_audit/real_dataset_curation_20260723/geo_GSE197452/t_05cef992/curate_obs_var.py"
)
EVIDENCE = SCRIPT.parent
SOURCE_SCRIPT = EVIDENCE / "inspect_sources.py"
SOURCE_MANIFEST = EVIDENCE / "source_manifest.json"
DECISION_NOTEBOOK = EVIDENCE / "GSE197452_processing_decisions.ipynb"
sys.path.insert(0, str(EVIDENCE))
SPEC = importlib.util.spec_from_file_location("gse197452_curation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
curation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(curation)
SOURCE_SPEC = importlib.util.spec_from_file_location("gse197452_sources", SOURCE_SCRIPT)
assert SOURCE_SPEC is not None and SOURCE_SPEC.loader is not None
sources = importlib.util.module_from_spec(SOURCE_SPEC)
SOURCE_SPEC.loader.exec_module(sources)


def test_source_manifest_binds_geo_publication_and_all_relevant_payloads() -> None:
    manifest = json.loads(SOURCE_MANIFEST.read_text())
    assert manifest["real_dataset_id"] == "geo:GSE197452"
    assert (
        manifest["source_authorities"]["publication"]["doi"]
        == "10.1038/s41587-022-01452-6"
    )
    assert manifest["source_authorities"]["publication"]["pmcid"] == "PMC9931582"
    assert set(manifest["payloads"]) == {
        "GSM6297384_cells_counts_Pert_Ill.txt.gz",
        "GSM6297384_expression_counts_Pert_Ill.txt.gz",
        "GSM6297384_genes_counts_Pert_Ill.txt.gz",
        "GSM6297385_cells_counts_Pert_Ult.txt.gz",
        "GSM6297385_expression_counts_Pert_Ult.txt.gz",
        "GSM6297385_genes_counts_Pert_Ult.txt.gz",
        "GSM6297388_filtered_feature_bc_matrix.pert.ill.h5",
        "GSM6297388_filtered_feature_bc_matrix.pert.ult.h5",
    }
    assert all(len(item["sha256"]) == 64 for item in manifest["payloads"].values())
    table = manifest["source_authorities"]["publication"]["supplementary_table_4"]
    assert table["rows"] == 6155
    assert table["guide_rows"] == 6127
    assert (
        table["member_sha256"]
        == "ec1380c72943075b9ecd3e2753d32b14c42951c769e862c7a87c609129f9b23a"
    )


def _obs_fixture() -> tuple[Any, Any, Any]:
    index = pd.Index(["row-a", "row-b", "row-c"])
    obs = pd.DataFrame(
        {
            "guide": ["GENE1_1", "NO_SITE_1", pd.NA],
            "perturbation": ["GENE1", "Non-targeting", "unknown"],
            "condition": ["test", "control", "test"],
            "is_control": [False, True, False],
            "dataset": "GSE197452",
            "cell_line": "A375",
            "disease": "Cancer",
            "tissue_type": "Melanoma",
            "organism": "Humans (Homo sapiens)",
            "perturbation_type": ["CRISPR KO", pd.NA, pd.NA],
            "assay": "Perturb-seq",
            "modality": "scRNA-seq",
            "original_obs_index": ["AA-1", "BB-1", "CC-1"],
            "obs_uuid": ["uuid-a", "uuid-b", "uuid-c"],
        },
        index=index,
    )
    assignments = pd.DataFrame(
        {
            "source_guide_top": pd.Series(
                ["GENE1_1", "NO_SITE_1", pd.NA], index=index, dtype="string"
            ),
            "source_guide_top_count": [11, 8, 0],
            "source_guide_detected_count": [1, 2, 0],
            "source_guide_top_ties": [1, 1, 0],
            "source_hash_top": pd.Series(
                ["Hashing_1", "Hashing_3", pd.NA], index=index, dtype="string"
            ),
            "source_hash_top_count": [20, 9, 0],
            "source_hash_detected_count": [1, 2, 0],
            "source_hash_top_ties": [1, 2, 0],
            "source_guide_sequence": pd.Series(
                ["A" * 20, "C" * 20, pd.NA], index=index, dtype="string"
            ),
        },
        index=index,
    )
    qc = pd.DataFrame(
        {
            "n_counts": [100, 200, 300],
            "n_genes": [10, 20, 30],
            "pct_mito": [1.0, 2.0, 3.0],
            "pct_ribo": [4.0, 5.0, 6.0],
        }
    )
    return obs, assignments, qc


def test_curate_obs_materializes_exact_guides_sequences_qc_and_missingness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    obs, assignments, qc = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    curated, receipt = curation.curate_obs(obs, assignments, qc)
    assert receipt == {
        "rows": 3,
        "guide_rows": 2,
        "guide_sequence_rows": 2,
        "control_rows": 1,
        "hash_top_known_rows": 2,
        "hash_top_tie_rows": 1,
    }
    assert curated["sample"].unique().tolist() == ["GSM6297384"]
    assert curated["guide_sequence"].tolist()[:2] == ["A" * 20, "C" * 20]
    assert pd.isna(curated["guide_sequence"].iloc[2])
    assert curated["is_control"].tolist() == [False, True, False]
    assert curated["n_counts"].tolist() == [100, 200, 300]
    assert curated["source_hash_top"].tolist()[:2] == ["Hashing_1", "Hashing_3"]
    assert curated["combination_size"].tolist() == [2, 2, 1]
    assert curated["dose"].unique().tolist() == [2.0]
    assert curated["timepoint"].unique().tolist() == [960.0]
    dispositions = curation.field_dispositions(curated)
    assert set(dispositions) == set(curation.CANONICAL_OBS_FIELDS)
    assert dispositions["guide_sequence"]["disposition"] == "materialized_partial"
    assert dispositions["batch"]["disposition"] == "unknown"
    assert dispositions["donor_id"]["disposition"] == "not_applicable"
    assert dispositions["n_counts"]["disposition"] == "materialized_complete"


def test_curate_obs_rejects_source_guide_or_control_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    obs, assignments, qc = _obs_fixture()
    monkeypatch.setattr(curation, "EXPECTED_N_OBS", 3)
    wrong = assignments.copy(deep=True)
    wrong.loc["row-a", "source_guide_top"] = "GENE2_1"
    with pytest.raises(AssertionError, match="guide"):
        curation.curate_obs(obs, wrong, qc)
    obs = obs.copy(deep=True)
    obs.loc["row-b", "is_control"] = False
    with pytest.raises(AssertionError, match="control"):
        curation.curate_obs(obs, assignments, qc)


def test_feature_assignment_reads_exact_10x_sparse_guide_and_hash_top(
    tmp_path: Path,
) -> None:
    path = tmp_path / "feature.h5"
    matrix = sp.csc_matrix(
        np.asarray(
            [
                [5, 4, 3],
                [7, 0, 0],
                [1, 8, 0],
                [9, 2, 0],
                [0, 6, 0],
            ],
            dtype=np.int32,
        )
    )
    with h5py.File(path, "w") as handle:
        group = handle.create_group("matrix")
        group.create_dataset("data", data=matrix.data)
        group.create_dataset("indices", data=matrix.indices)
        group.create_dataset("indptr", data=matrix.indptr)
        group.create_dataset("shape", data=np.asarray(matrix.shape, dtype=np.int64))
        group.create_dataset("barcodes", data=np.asarray([b"AA-1", b"BB-1", b"CC-1"]))
        features = group.create_group("features")
        features.create_dataset(
            "id",
            data=np.asarray(
                [b"ENSG1", b"GENE1_1", b"NO_SITE_1", b"Hashing_1", b"Hashing_3"]
            ),
        )
        features.create_dataset(
            "name",
            data=np.asarray(
                [b"G1", b"GENE1_1", b"NO_SITE_1", b"Hashing_1", b"Hashing_3"]
            ),
        )
        features.create_dataset(
            "feature_type",
            data=np.asarray(
                [
                    b"Gene Expression",
                    b"CRISPR Guide Capture",
                    b"CRISPR Guide Capture",
                    b"Custom",
                    b"Custom",
                ]
            ),
        )
    obs = pd.DataFrame(
        {
            "original_obs_index": ["AA-1", "BB-1", "CC-1"],
            "guide": ["GENE1_1", "NO_SITE_1", pd.NA],
        },
        index=["a", "b", "c"],
    )
    feature_table = pd.DataFrame(
        {"id": ["GENE1_1", "NO_SITE_1"], "sequence": ["A" * 20, "C" * 20]}
    )
    frame, receipt = sources.feature_assignments(path, obs, feature_table)
    assert frame["source_guide_top"].tolist()[:2] == ["GENE1_1", "NO_SITE_1"]
    assert pd.isna(frame["source_guide_top"].iloc[2])
    assert frame["source_hash_top"].tolist()[:2] == ["Hashing_1", "Hashing_3"]
    assert receipt["current_equals_top_guide"] == 2
    assert receipt["current_differs_top_guide"] == 0


def test_successor_description_binds_predecessor_and_exact_replacement() -> None:
    class Item:
        def __init__(self, uid: str, key: str) -> None:
            self.uid = uid
            self.key = key

    predecessor = Item("pred", "pert-gym/additions/pred")
    old = Item(curation.EXPECTED_OBS_UID, curation.OBS_KEY)
    other = Item("other", "other/key")
    new = Item("new", curation.OBS_KEY)
    description = json.loads(
        curation.successor_description(new, predecessor, [old, other], [new, other])
    )
    assert description["predecessor_uid"] == "pred"
    assert description["replaced_obs_uid"] == curation.EXPECTED_OBS_UID
    assert description["added_obs_uid"] == "new"
    assert description["member_count_before"] == description["member_count_after"] == 2


def test_var_symbol_axis_allows_only_stable_id_disambiguation() -> None:
    source = pd.Index(["DUP", "DUP", "UNIQUE"])
    stable = pd.Index(["ENSG00000000001", "ENSG00000000002", "ENSG00000000003"])
    accepted = pd.Index(["DUP", "DUP_ENSG00000000002", "UNIQUE"])
    assert curation.accepted_symbol_axis_matches(source, stable, accepted)
    assert not curation.accepted_symbol_axis_matches(
        source, stable, pd.Index(["DUP", "WRONG", "UNIQUE"])
    )


def test_processing_decision_notebook_executes_postwrite_evidence_assertions() -> None:
    nbformat = pytest.importorskip("nbformat")
    notebook_client = pytest.importorskip("nbclient")
    notebook = nbformat.read(DECISION_NOTEBOOK, as_version=4)
    notebook_client.NotebookClient(
        notebook,
        timeout=60,
        resources={"metadata": {"path": str(SCRIPT.parents[5])}},
    ).execute(cwd=str(SCRIPT.parents[5]))
