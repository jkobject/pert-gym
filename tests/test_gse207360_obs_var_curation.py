from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
from copy import deepcopy
from email.message import Message
from pathlib import Path

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

MODULE_PATH = (
    Path(__file__).parents[1]
    / "artifacts"
    / "schema_audit"
    / "real_dataset_curation_20260722"
    / "geo_GSE207360"
    / "t_a2234c88"
    / "curate_obs_var.py"
)
SPEC = importlib.util.spec_from_file_location("gse207360_curation", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
DECISION_NOTEBOOK = MODULE_PATH.parent / "GSE207360_processing_decisions.ipynb"
FULL_DOD_VERIFIER = MODULE_PATH.parent / "verify_full_dod.py"
FULL_DOD_SPEC = importlib.util.spec_from_file_location(
    "gse207360_full_dod", FULL_DOD_VERIFIER
)
assert FULL_DOD_SPEC is not None and FULL_DOD_SPEC.loader is not None
FULL_DOD_MODULE = importlib.util.module_from_spec(FULL_DOD_SPEC)
FULL_DOD_SPEC.loader.exec_module(FULL_DOD_MODULE)


def source_frame() -> pd.DataFrame:
    index = pd.Index(["cell-a", "cell-b", "cell-c", "cell-d"], dtype="object")
    return pd.DataFrame(
        {
            "orig.ident": ["SeuratProject"] * 4,
            "nCount_RNA": [1000, 2000, 3000, 4000],
            "nFeature_RNA": [300, 400, 500, 600],
            "Sample": ["KO", "WT", "WT", "KO"],
            "Barcode": ["a", "b", "c", "d"],
            "hg19": [900, 0, 2100, 800],
            "mm10": [100, 2000, 900, 3200],
            "frac_hg_genes": [0.9, 0.0, 0.7, 0.2],
            "hdbscan_cluster": [1, 2, 3, 4],
            "percent.mt": [0.05, 0.06, 0.07, 0.08],
            "percent.mt_mouse": [0.01, 0.02, 0.03, 0.04],
            "RNA_snn_res.1": [0, 1, 2, 3],
            "seurat_clusters": [0, 1, 2, 3],
            "sample.name": ["KO", "WT", "WT", "KO"],
            "Cell_type1": ["Neuron", "Macrophage", "Endothelial", "Pericytes"],
        },
        index=index,
    )


def predecessor_frame(source: pd.DataFrame) -> pd.DataFrame:
    obs = source.drop(columns="Sample").copy()
    obs["condition"] = ["test", "control", "test", "control"]
    obs["perturbation"] = ["non-targeting", "EGFR", "non-targeting", "EGFR"]
    obs["perturbation_type"] = "genetic"
    obs["disease"] = "Glioblastoma"
    obs["tissue_type"] = "Mesenchymal glioma stem cell"
    obs["organism"] = "mouse"
    obs["is_control"] = [False, True, False, True]
    obs["cancer"] = True
    obs["cell_line"] = "83"
    obs["dataset"] = "GSE207360"
    obs["modality"] = "scRNA"
    obs["assay"] = "Perturb-seq"
    obs["original_obs_index"] = source.index.astype(str)
    obs["original_obs_index_is_duplicated"] = False
    obs["obs_uuid"] = [f"uuid-{i}" for i in range(len(obs))]
    return obs


def test_frozen_rows_and_source_manifest_are_bound() -> None:
    frozen = MODULE.load_frozen_input_bindings()
    assert frozen["crosswalk_row"]["observations"] == 12_487
    assert frozen["audit_row"]["logical_family_count"] == 1
    manifest = json.loads(MODULE.SOURCE_MANIFEST_PATH.read_text())
    assert manifest["real_dataset_id"] == "geo/GSE207360"
    filtered = next(
        item for item in manifest["files"] if item["name"].endswith("filtered.rds.gz")
    )
    assert filtered["sha256"] == MODULE.SOURCE_SPEC["sha256"]
    assert manifest["geo"]["scrna_samples"][0]["time_days_after_injection"] == 15
    assert manifest["geo"]["scrna_samples"][1]["time_days_after_injection"] == 90

    receipt_manifest = json.loads(
        (MODULE.EVIDENCE_DIR / "receipt_manifest.json").read_text()
    )
    assert receipt_manifest["captured_against_head"] == (
        "cbe143a2a53514246a389ac39051bca14dcb7aa0"
    )
    for name, binding in receipt_manifest["receipts"].items():
        receipt_bytes = (MODULE.EVIDENCE_DIR / name).read_bytes()
        assert hashlib.sha256(receipt_bytes).hexdigest() == binding["sha256"]
    for name, expected_sha256 in receipt_manifest["code_bindings"].items():
        bound_bytes = (MODULE.EVIDENCE_DIR / name).read_bytes()
        assert hashlib.sha256(bound_bytes).hexdigest() == expected_sha256

    live_receipt = json.loads((MODULE.EVIDENCE_DIR / "live_receipt.json").read_text())
    assert live_receipt["helper_sha256"] == receipt_manifest[
        "historical_live_receipt_helper_sha256"
    ]
    assert (
        live_receipt["canonical_sha256"]
        == receipt_manifest["receipts"]["live_receipt.json"]["canonical_sha256"]
    )
    assert live_receipt["replay_noop"] is True
    assert (
        live_receipt["registry_counts"]["before"]
        == live_receipt["registry_counts"]["after"]
    )
    assert live_receipt["writes"] == {
        "artifacts": [],
        "collection_writes": 0,
        "deletions": 0,
        "obs_revisions": 0,
        "var_revisions": 0,
        "x_revisions": 0,
    }


def test_processing_decision_notebook_executes_and_lists_residual_gates() -> None:
    nbformat = pytest.importorskip("nbformat")
    notebook_client = pytest.importorskip("nbclient")
    notebook = nbformat.read(DECISION_NOTEBOOK, as_version=4)
    executed = notebook_client.NotebookClient(
        notebook,
        timeout=60,
        resources={"metadata": {"path": str(MODULE_PATH.parents[5])}},
    ).execute(cwd=str(MODULE_PATH.parents[5]))
    namespace = executed.cells[2]["source"]
    assert "len(full_dod_gates) == 13" in namespace
    assert "reviewed_gcs_decommission_ready" in namespace
    assert "safe_to_remove_gcs'] is False" in namespace


def test_exact_source_join_rejects_row_drift() -> None:
    source = source_frame()
    obs = predecessor_frame(source)
    reordered = source.iloc[::-1]
    with pytest.raises(AssertionError, match="ordered identity drift"):
        MODULE.exact_source_join(obs, reordered)


def test_exact_source_join_rejects_value_drift() -> None:
    source = source_frame()
    obs = predecessor_frame(source)
    obs.loc["cell-a", "Cell_type1"] = "wrong"
    with pytest.raises(AssertionError, match="metadata mismatch"):
        MODULE.exact_source_join(obs, source)


def test_curate_obs_materializes_source_exhaustive_semantics() -> None:
    source = source_frame()
    obs = predecessor_frame(source)
    curated, receipt = MODULE.curate_obs(obs, source)

    assert curated.index.equals(obs.index)
    assert curated["original_obs_index"].tolist() == obs["original_obs_index"].tolist()
    assert curated["obs_uuid"].tolist() == obs["obs_uuid"].tolist()
    assert receipt["join_mismatch_count"] == 0
    assert receipt["wt_rows"] == 2
    assert receipt["ko_rows"] == 2
    assert receipt["source_cell_type_counts"] == {
        "Neuron": 1,
        "Macrophage": 1,
        "Endothelial": 1,
        "Pericytes": 1,
    }

    assert curated["sample"].tolist() == [
        "GSM6284972",
        "GSM6284971",
        "GSM6284971",
        "GSM6284972",
    ]
    assert curated["timepoint"].tolist() == [129_600.0, 21_600.0, 21_600.0, 129_600.0]
    assert curated["perturbation"].tolist() == [
        "EGFR",
        "control tumour exposure",
        "control",
        "EGFR-knockout tumour exposure",
    ]
    assert curated["perturbation_type"].tolist() == [
        "CRISPRko",
        "exposure",
        "none",
        "exposure",
    ]
    assert curated["is_control"].tolist() == [False, True, True, False]
    assert curated["is_baseline"].tolist() == [False, True, True, False]
    assert curated["cell_type"].tolist() == [
        "glioblastoma stem cell",
        "Macrophage",
        "glioblastoma stem cell",
        "Pericytes",
    ]
    assert curated["organism"].tolist() == [
        "Homo sapiens",
        "Mus musculus",
        "Homo sapiens",
        "Mus musculus",
    ]
    assert curated["source_accession"].eq("GSE207360").all()
    assert curated["x_semantics"].eq("raw_counts").all()
    assert curated["pct_mito"].tolist() == pytest.approx([5.0, 2.0, 7.0, 4.0])
    assert (
        curated["source_original_perturbation"].tolist() == obs["perturbation"].tolist()
    )
    assert curated["source_original_organism"].eq("mouse").all()
    assert curated["source_seurat_Sample"].tolist() == source["Sample"].tolist()
    assert curated["source_seurat_Cell_type1"].tolist() == source["Cell_type1"].tolist()

    direct_ko = source["frac_hg_genes"].gt(0.5) & source["sample.name"].eq("KO")
    assert curated.loc[direct_ko, "guide_id_state"].eq("unknown").all()
    assert curated.loc[~direct_ko, "guide_id_state"].eq("not_applicable").all()
    assert curated["ethnicity_state"].eq("not_applicable").all()
    assert curated["donor_id_state"].eq("unknown").all()

    dispositions = MODULE.field_dispositions(curated)
    assert set(dispositions) == set(MODULE.CANONICAL_OBS_FIELDS)
    assert dispositions["cell_type"]["disposition"] == "materialized_complete"
    assert dispositions["guide_id"]["disposition"] == "mixed_unknown_not_applicable"
    assert dispositions["ethnicity"]["disposition"] == "not_applicable"
    assert dispositions["donor_id"]["disposition"] == "unknown"


def test_curate_obs_assigns_cell_identity_from_row_level_species_evidence() -> None:
    source = source_frame()
    curated, receipt = MODULE.curate_obs(predecessor_frame(source), source)

    human = source["frac_hg_genes"].gt(0.5)
    mouse = ~human
    assert curated.loc[human, "organism"].eq("Homo sapiens").all()
    assert curated.loc[human, "cell_line"].eq("GSC83").all()
    assert curated.loc[human, "cell_type"].eq("glioblastoma stem cell").all()
    assert curated.loc[mouse, "organism"].eq("Mus musculus").all()
    assert curated.loc[mouse, "cell_line"].isna().all()
    assert curated.loc[mouse, "cell_line_state"].eq("not_applicable").all()
    assert receipt["human_dominant_rows"] == 2
    assert receipt["mouse_dominant_rows"] == 2


def test_curate_obs_reserves_direct_egfr_edit_for_human_ko_cells() -> None:
    source = source_frame()
    curated, _ = MODULE.curate_obs(predecessor_frame(source), source)

    human_ko = source["frac_hg_genes"].gt(0.5) & source["sample.name"].eq("KO")
    mouse_ko = source["frac_hg_genes"].le(0.5) & source["sample.name"].eq("KO")
    assert curated.loc[human_ko, "perturbation"].eq("EGFR").all()
    assert curated.loc[human_ko, "perturbation_type"].eq("CRISPRko").all()
    assert curated.loc[human_ko, "perturbation_target"].eq("EGFR").all()
    assert (
        curated.loc[mouse_ko, "perturbation"].eq("EGFR-knockout tumour exposure").all()
    )
    assert curated.loc[mouse_ko, "perturbation_type"].eq("exposure").all()
    assert curated.loc[mouse_ko, "perturbation_target"].isna().all()
    assert curated.loc[mouse_ko, "perturbation_target_state"].eq("not_applicable").all()


def test_curate_obs_uses_species_appropriate_mitochondrial_qc() -> None:
    source = source_frame()
    curated, _ = MODULE.curate_obs(predecessor_frame(source), source)

    human = source["frac_hg_genes"].gt(0.5)
    mouse = ~human
    assert curated.loc[human, "pct_mito"].tolist() == pytest.approx([5.0, 7.0])
    assert curated.loc[mouse, "pct_mito"].tolist() == pytest.approx([2.0, 4.0])


def test_curate_obs_is_idempotent() -> None:
    source = source_frame()
    first, _ = MODULE.curate_obs(predecessor_frame(source), source)
    second, _ = MODULE.curate_obs(first, source)
    MODULE.verify_obs_semantics(second, first)


def test_x_and_var_identities_are_fixed() -> None:
    assert MODULE.EXPECTED_X == {
        "uid": "4IOEQEw4ylx0Zx4c0000",
        "hash": "rLTZFYwmtPyrsHhVQ6_kp-",
    }
    assert MODULE.EXPECTED_VAR == {
        "uid": "U8OeHI58YG9Y9Nsb0002",
        "hash": "wv2BwlQShhowaM7AYyu4uQ",
    }
    var = pd.DataFrame(
        {
            "stable_feature_id": [
                *[f"ENSG{i}" for i in range(32_738)],
                *[f"ENSMUSG{i}" for i in range(27_998)],
            ]
        }
    )
    verdict = MODULE.verify_var(var)
    assert verdict["needs_revision"] is False
    assert verdict["mismatch_count"] == 0


def test_main_refuses_mac(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(MODULE.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(MODULE.sys, "argv", [str(MODULE_PATH), "plan"])
    with pytest.raises(RuntimeError, match="refusing Mac execution"):
        MODULE.main()


def test_verify_mode_uses_bounded_verify_only_capacity_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(MODULE.platform, "system", lambda: "Linux")
    monkeypatch.setattr(MODULE.sys, "argv", [str(MODULE_PATH), "verify"])

    def verify_gate() -> None:
        raise RuntimeError("verify gate reached")

    monkeypatch.setattr(MODULE, "verify_only_preflight", verify_gate)
    monkeypatch.setattr(
        MODULE,
        "preflight",
        lambda: (_ for _ in ()).throw(AssertionError("writer gate must not run")),
    )
    with pytest.raises(RuntimeError, match="verify gate reached"):
        MODULE.main()


def test_gcs_probe_accepts_only_successful_structured_empty_listing(monkeypatch) -> None:
    monkeypatch.setattr(
        FULL_DOD_MODULE,
        "gcloud_access_token",
        lambda: "bounded-test-token",
    )

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self) -> bytes:
            return b"{}"

    monkeypatch.setattr(FULL_DOD_MODULE.urllib.request, "urlopen", lambda *_a, **_k: Response())
    result = FULL_DOD_MODULE.gcs_prefix_probe(
        "gs://scperturb/data/cleaned/GSE207360/"
    )
    assert result["exists"] is False
    assert result["object_count_lower_bound"] == 0
    assert result["http_status"] == 200


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_gcs_probe_fails_closed_on_access_and_transport_errors(
    monkeypatch, status: int
) -> None:
    import urllib.error

    monkeypatch.setattr(FULL_DOD_MODULE, "gcloud_access_token", lambda: "token")

    def denied(*_args, **_kwargs):
        raise urllib.error.HTTPError("url", status, "denied", Message(), None)

    monkeypatch.setattr(FULL_DOD_MODULE.urllib.request, "urlopen", denied)
    with pytest.raises(RuntimeError, match=f"HTTP {status}"):
        FULL_DOD_MODULE.gcs_prefix_probe(FULL_DOD_MODULE.LEGACY_STAGING_URI)


def test_gcloud_access_token_failure_is_not_absence(monkeypatch) -> None:
    monkeypatch.setattr(
        FULL_DOD_MODULE.subprocess,
        "run",
        lambda *_a, **_k: subprocess.CompletedProcess(
            ["gcloud"], 1, "", "Requester Pays billing project denied"
        ),
    )
    with pytest.raises(RuntimeError, match="access token"):
        FULL_DOD_MODULE.gcloud_access_token()


def _valid_full_dod_receipt() -> dict:
    receipt = {
        "format": FULL_DOD_MODULE.RECEIPT_FORMAT,
        "task_id": "t_f2593783",
        "run_id": "4662",
        "code": {
            "head": "a" * 40,
            "branch": "pert-gym/t_f2593783-complete-gse207360-live-dod-after-pr-117",
            "pr": 138,
            "verifier_sha256": "b" * 64,
        },
        "command": {"exit_code": 0},
        "source": {"sha256": FULL_DOD_MODULE.SOURCE_SHA256, "size": 4_174_159_639},
        "artifacts": deepcopy(FULL_DOD_MODULE.EXPECTED_ARTIFACTS),
        "snapshots": {
            "before": {"canonical_sha256": "c" * 64},
            "after": {"canonical_sha256": "c" * 64},
        },
        "registry_drift": 0,
        "replay_noop": True,
        "gcs_decommission": {"eligible": False, "action": "preserved_no_deletion"},
        "lifecycle": {
            "payload_exit_code": 0,
            "terminal_vm_status": "TERMINATED",
            "task_scoped_labels_cleared": True,
            "local_lease_absent": True,
        },
    }
    receipt["canonical_sha256"] = FULL_DOD_MODULE.receipt_sha256(receipt)
    return receipt


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda r: r["code"].update(head="wrong"), "code head"),
        (lambda r: r["artifacts"]["x"].update(hash="wrong"), "artifact identity"),
        (lambda r: r["artifacts"]["var"].update(key="wrong"), "artifact identity"),
        (lambda r: r["snapshots"]["after"].update(canonical_sha256="d" * 64), "registry drift"),
        (lambda r: r.update(replay_noop=False), "replay"),
        (lambda r: r["lifecycle"].update(terminal_vm_status="RUNNING"), "lifecycle"),
    ],
)
def test_full_dod_receipt_validation_rejects_tampering(mutation, message) -> None:
    receipt = _valid_full_dod_receipt()
    mutation(receipt)
    receipt["canonical_sha256"] = FULL_DOD_MODULE.receipt_sha256(receipt)
    with pytest.raises(AssertionError, match=message):
        FULL_DOD_MODULE.validate_receipt(
            receipt,
            expected_head="a" * 40,
            expected_run_id="4662",
            expected_verifier_sha256="b" * 64,
        )


def test_full_dod_receipt_validation_rejects_bad_canonical_digest() -> None:
    receipt = _valid_full_dod_receipt()
    receipt["canonical_sha256"] = "0" * 64
    with pytest.raises(AssertionError, match="canonical digest"):
        FULL_DOD_MODULE.validate_receipt(
            receipt,
            expected_head="a" * 40,
            expected_run_id="4662",
            expected_verifier_sha256="b" * 64,
        )
