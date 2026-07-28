#!/usr/bin/env python3
"""Materialize local OBS/VAR corrections for GSE196799 cohort C.

This script never connects to LaminDB and never writes GCS. The downstream writer
must wrap its local outputs in the project's append-only publication state machine.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import anndata as ad
import h5py
import pandas as pd

from pert_gym.obs_identity import add_obs_identity, validate_obs_identity

TASK_ID = "t_2b3e6d5e"
ROOT = Path(__file__).resolve().parent
EXPECTED_OBS_COLUMNS = [
    "cell_id",
    "barcode",
    "source_accession",
    "sample_accession",
    "sample_title",
    "source_name",
    "experiment",
    "timepoint",
    "timepoint_unit",
    "culture_method",
    "ascorbic_acid_from_day_12",
    "biosample",
    "sra_experiment",
    "organism",
    "assay",
    "n_genes_by_counts",
    "total_counts",
    "pct_counts_mt",
    "source_cell_call_flag_missing",
]
COMMON_VAR_SHA256 = "1d74fc35a8ed61c7f487b5b3b955fd91e9208e6e42e7943ebf0e8442ec83c134"
CONFIG: dict[str, dict[str, Any]] = {
    "GSM5901230": {
        "rows": 4807,
        "day": 6.0,
        "sample_title": "day_6_3D_suspension_culture",
        "biosample": "SAMN25978134",
        "sra_experiment": "SRX14194757",
        "obs_sha256": "f35de6ba53b5ead8e65ee8ca7cd1419f61dbd8b476b2ab2c6ee4017dc96b0be6",
        "x_sha256": "ae9c968e0c7ceb09a2ac80c1b3518ba4626dc13106aece29214a330e5f049047",
        "obs_uid": "770vKgYITsWgwsQ50000",
        "x_uid": "vn537Bt2uFhZncXo0000",
        "var_uid": "NCuDtw4vtWZpliBU0000",
        "obs_generation": "1785154234703221",
        "x_generation": "1785154234743869",
        "var_generation": "1785154234729370",
        "index_sha256": "0183ccf12a871b94c22676fd6768c1e9f7b33277c85952b86ac3e7c55b281df2",
        "x_nnz": 17517151,
        "x_sum": 58069115,
        "x_maximum": 2358,
    },
    "GSM5901231": {
        "rows": 6430,
        "day": 9.0,
        "sample_title": "day_9_1_3D_suspension_culture_",
        "biosample": "SAMN25978133",
        "sra_experiment": "SRX14194758",
        "obs_sha256": "471a5f6bdbc6430a46b882f7fd38642d3d8680575f69bbdd059e44c785679352",
        "x_sha256": "45c310d2d01c2670078ea9deffbe60ee2b46e4b72fbe520b8e6fdb0ac170a212",
        "obs_uid": "HAjNzeHiAFVj18Bl0000",
        "x_uid": "DMOKXzr7fGa2BzFx0000",
        "var_uid": "2L5w9PF2KfG568jw0000",
        "obs_generation": "1785154234957959",
        "x_generation": "1785154234986151",
        "var_generation": "1785154234984134",
        "index_sha256": "cb4fa0cbd90407a4d00c3f479140ce55dac9867634ac385677dc27aca998c0ee",
        "x_nnz": 26039746,
        "x_sum": 97683665,
        "x_maximum": 3816,
    },
    "GSM5901232": {
        "rows": 4396,
        "day": 18.0,
        "sample_title": "day_18_reaggregated_3D_suspension_culture",
        "biosample": "SAMN25978132",
        "sra_experiment": "SRX14194759",
        "obs_sha256": "e35f168b08062df9d4344b340f00a6729a59719168f46b0d22004b66b5c601e1",
        "x_sha256": "eba53c3e02ab36573dd0fddbbe55d3f957c1f40d47195ef2f48c4dc20a4d81c0",
        "obs_uid": "GbF6GoN62lv1gHph0000",
        "x_uid": "ZX3O7cVwL7bFPZYv0000",
        "var_uid": "v6fulA6ihI0nfyDF0000",
        "obs_generation": "1785154235025834",
        "x_generation": "1785154234960939",
        "var_generation": "1785154234999745",
        "index_sha256": "c51eb6ad44d45d5777f6a0fbc1ddd75cd4eaffd80b86f3e7e26a51d99b64a47f",
        "x_nnz": 17949355,
        "x_sum": 56570353,
        "x_maximum": 1722,
    },
}
EXPECTED_VARS = 20631
CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
DROP_TO_DATASET_METADATA = {
    "source_accession",
    "sample_accession",
    "sample_title",
    "biosample",
    "sra_experiment",
    "timepoint",
    "timepoint_unit",
    "source_cell_call_flag_missing",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_frozen_packet(dataset: str) -> tuple[dict[str, Any], str]:
    config = CONFIG[dataset]
    packet_path = ROOT / f"{dataset}_audit.json"
    packet_sha256 = sha256_file(packet_path)
    packet = json.loads(packet_path.read_text())
    if (
        packet["task_id"] != TASK_ID
        or packet["dataset"] != dataset
        or packet["writes"] != 0
    ):
        raise AssertionError("audit packet task/dataset/write boundary mismatch")
    expected_sha = {
        "obs": config["obs_sha256"],
        "x": config["x_sha256"],
        "var": COMMON_VAR_SHA256,
    }
    if packet["current_payload_sha256"] != expected_sha:
        raise AssertionError("audit packet payload hash binding mismatch")
    for role in ("obs", "x", "var"):
        artifact = packet["current_artifacts"][role]
        if artifact["uid"] != config[f"{role}_uid"]:
            raise AssertionError(f"audit packet {role} UID mismatch")
        if (
            artifact["key"]
            != f"data/cleaned/{dataset}/{role if role != 'x' else 'X'}.{'h5ad' if role == 'x' else 'parquet'}"
        ):
            raise AssertionError(f"audit packet {role} key mismatch")
        if (
            packet["current_object_metadata"][role]["generation"]
            != config[f"{role}_generation"]
        ):
            raise AssertionError(f"audit packet {role} generation mismatch")
    if packet["feature_links"] != {"obs_to_x": True, "x_to_var": True}:
        raise AssertionError("audit packet feature-link verdict mismatch")
    if packet["feature_link_targets"] != {
        "obs.X": config["x_uid"],
        "X.var": config["var_uid"],
    }:
        raise AssertionError("audit packet feature-link target mismatch")
    layout = packet["layout_audit"]
    if not (
        layout["canonical_prefix"] == f"data/cleaned/{dataset}"
        and layout["chunking"] == "not_chunked"
        and layout["same_prefix_var"] is True
        and layout["var_members_for_dataset"] == 1
    ):
        raise AssertionError("audit packet chunk/shared-VAR contract mismatch")
    for filename, binding_key in (
        ("live_audit.json", "live_audit_sha256"),
        ("source_evidence.json", "source_evidence_sha256"),
        ("var_all_rows_evidence.json", "var_all_rows_evidence_sha256"),
        ("generation_recheck.json", "generation_recheck_sha256"),
    ):
        if sha256_file(ROOT / filename) != packet["evidence_binding"][binding_key]:
            raise AssertionError(f"audit packet evidence binding mismatch: {filename}")
    return packet, packet_sha256


def validate_input_hashes(
    dataset: str, obs_path: Path, x_path: Path, var_path: Path
) -> dict[str, str]:
    config = CONFIG[dataset]
    actual = {
        "obs": sha256_file(obs_path),
        "x": sha256_file(x_path),
        "var": sha256_file(var_path),
    }
    expected = {
        "obs": config["obs_sha256"],
        "x": config["x_sha256"],
        "var": COMMON_VAR_SHA256,
    }
    if actual != expected:
        raise AssertionError(
            f"input payload hash mismatch: expected={expected!r}, actual={actual!r}"
        )
    return actual


def assert_constant(frame: pd.DataFrame, column: str, expected: Any) -> None:
    if column not in frame:
        raise AssertionError(f"missing required source OBS column: {column}")
    values = frame[column].drop_duplicates().tolist()
    if values != [expected]:
        raise AssertionError(
            f"{column} drift: expected={[expected]!r}, actual={values!r}"
        )


def validate_source_obs(obs: pd.DataFrame, dataset: str) -> None:
    config = CONFIG[dataset]
    if list(obs.columns) != EXPECTED_OBS_COLUMNS:
        raise AssertionError(
            f"source OBS schema drift: expected={EXPECTED_OBS_COLUMNS!r}, actual={list(obs.columns)!r}"
        )
    if len(obs) != config["rows"] or not obs.index.is_unique:
        raise AssertionError("source OBS row identity/denominator drift")
    assert_constant(obs, "source_accession", "GSE196799")
    assert_constant(obs, "sample_accession", dataset)
    assert_constant(obs, "sample_title", config["sample_title"])
    assert_constant(obs, "source_name", "hiPSCs")
    assert_constant(obs, "experiment", 1)
    assert_constant(obs, "timepoint", config["day"])
    assert_constant(obs, "timepoint_unit", "day")
    assert_constant(obs, "culture_method", "3D suspension culture")
    assert_constant(obs, "ascorbic_acid_from_day_12", False)
    assert_constant(obs, "biosample", config["biosample"])
    assert_constant(obs, "sra_experiment", config["sra_experiment"])
    assert_constant(obs, "organism", "Homo sapiens")
    assert_constant(obs, "assay", "10x Genomics 3' scRNA-seq")
    assert_constant(obs, "source_cell_call_flag_missing", True)
    if (
        not obs["cell_id"]
        .astype(str)
        .equals(pd.Series(obs.index.astype(str), index=obs.index))
    ):
        raise AssertionError("cell_id does not exactly equal the source OBS index")
    if not obs["cell_id"].is_unique or not obs["barcode"].is_unique:
        raise AssertionError("cell_id/barcode uniqueness drift")
    index_material = "\n".join(obs.index.astype(str)).encode()
    if hashlib.sha256(index_material).hexdigest() != config["index_sha256"]:
        raise AssertionError("source OBS ordered-index digest drift")


def build_revised_obs(obs: pd.DataFrame, dataset: str) -> pd.DataFrame:
    validate_source_obs(obs, dataset)
    out = obs.drop(columns=sorted(DROP_TO_DATASET_METADATA)).copy()
    out = out.rename(
        columns={
            "source_name": "source_material",
            "experiment": "source_experiment",
            "n_genes_by_counts": "n_genes",
            "total_counts": "n_counts",
            "pct_counts_mt": "pct_mito",
        }
    )
    out.insert(0, "sample", dataset)
    out.insert(0, "dataset", dataset)
    out["batch"] = "experiment_1"
    out["source_material_ontology_id"] = "EFO:0004905"
    out["organism_ontology_id"] = "NCBITaxon:9606"
    out["technology"] = "10x 3' v3.1"
    out["technology_ontology_id"] = "EFO:0022980"
    out["assay"] = "10x 3' v3.1"
    out["assay_ontology_id"] = "EFO:0022980"
    out["modality"] = "scRNA-seq"
    out["modality_ontology_id"] = "EFO:0008913"
    out["sequencer"] = "Illumina NovaSeq 6000"
    out["is_bulk"] = False
    out["is_pseudobulk"] = False
    out["perturbation"] = "none"
    out["perturbation_type"] = "none"
    out["is_control"] = True
    out["cell_type"] = "unknown"
    out["cell_type_state"] = "missing"
    out["cell_type_source"] = (
        "GEO has source material and series-level differentiated populations, not cell-level labels"
    )
    out["cell_line"] = "unknown"
    out["cell_line_state"] = "missing"
    out["donor_id"] = "unknown"
    out["donor_id_state"] = "missing"
    out["disease"] = "unknown"
    out["disease_state"] = "missing"
    out["tissue_type"] = "not_applicable"
    out["tissue_type_state"] = "not_applicable"
    out["sex"] = "unknown"
    out["sex_state"] = "missing"
    out["age"] = "unknown"
    out["age_state"] = "missing"
    out["ethnicity"] = "unknown"
    out["ethnicity_state"] = "missing"
    out = add_obs_identity(
        out,
        dataset_id=dataset,
        prefix=f"data/cleaned/{dataset}",
    )
    validate_obs_identity(out)
    if len(out) != len(obs) or not out.index.equals(obs.index):
        raise AssertionError("OBS row order/denominator changed")
    if any(CONTROL_RE.search(str(value)) for value in out.index):
        raise AssertionError("control character in revised OBS index")
    return out


def build_revised_var(var: pd.DataFrame) -> pd.DataFrame:
    required = ["feature_id", "gene_symbol", "feature_type"]
    if list(var.columns) != required or len(var) != EXPECTED_VARS:
        raise AssertionError("source VAR schema/denominator drift")
    feature_id = var["feature_id"].astype(str)
    if not var.index.is_unique or not feature_id.is_unique:
        raise AssertionError("source VAR feature identity is not unique")
    if not feature_id.equals(pd.Series(var.index.astype(str), index=var.index)):
        raise AssertionError("feature_id/index order drift")
    if not feature_id.str.fullmatch(r"ENSG\d{11}").all():
        raise AssertionError("non-human-Ensembl feature ID present")
    if feature_id.str.contains(CONTROL_RE).any():
        raise AssertionError("control character in feature ID")
    out = var.copy()
    out["ensembl_id"] = feature_id
    out["gene_id"] = feature_id
    out["organism"] = "Homo sapiens"
    out["organism_ontology_id"] = "NCBITaxon:9606"
    out["author_gene_id"] = feature_id
    out["author_gene_symbol"] = out["gene_symbol"].astype(str)
    if len(out) != len(var) or not out.index.equals(var.index):
        raise AssertionError("VAR order/denominator changed")
    return out


def validate_x(
    x_path: Path, obs: pd.DataFrame, var: pd.DataFrame, dataset: str
) -> dict[str, Any]:
    config = CONFIG[dataset]
    x_sha256 = sha256_file(x_path)
    if x_sha256 != config["x_sha256"]:
        raise AssertionError("X payload hash mismatch")
    with h5py.File(x_path, "r") as handle:
        group = handle["X"]
        encoding = group.attrs.get("encoding-type", "")
        if isinstance(encoding, bytes):
            encoding = encoding.decode()
        data = group["data"]
        data_dtype = str(data.dtype)
        indices = group["indices"]
        indptr = group["indptr"]
        shape = [int(value) for value in group.attrs["shape"]]
        if encoding != "csr_matrix" or data_dtype != "int32":
            raise AssertionError("X encoding/dtype drift")
        if shape != [len(obs), len(var)]:
            raise AssertionError("X shape does not match revised OBS/VAR")
        if len(data) != config["x_nnz"] or len(indices) != len(data):
            raise AssertionError("X CSR nnz/index length drift")
        ptr = indptr[:]
        if len(ptr) != len(obs) + 1 or int(ptr[0]) != 0 or int(ptr[-1]) != len(data):
            raise AssertionError("X CSR indptr boundary drift")
        if (ptr[1:] < ptr[:-1]).any():
            raise AssertionError("X CSR indptr is not monotone")
        total = 0
        minimum = None
        maximum = None
        all_integer = True
        block_size = 1_000_000
        for start in range(0, len(data), block_size):
            block = data[start : start + block_size]
            if len(block) == 0:
                continue
            total += int(block.astype("int64").sum())
            block_min = int(block.min())
            block_max = int(block.max())
            minimum = block_min if minimum is None else min(minimum, block_min)
            maximum = block_max if maximum is None else max(maximum, block_max)
            all_integer = all_integer and bool((block == block.astype("int64")).all())
        if (
            total != config["x_sum"]
            or minimum != 1
            or maximum != config["x_maximum"]
            or not all_integer
        ):
            raise AssertionError("X stored-value invariant drift")
    backed = ad.read_h5ad(x_path, backed="r")
    try:
        shape = [int(backed.n_obs), int(backed.n_vars)]
        if shape != [len(obs), len(var)]:
            raise AssertionError("X shape does not match revised OBS/VAR")
        if not obs.index.astype(str).equals(backed.obs_names.astype(str)):
            raise AssertionError("OBS/X observation axis mismatch")
        if not var.index.astype(str).equals(backed.var_names.astype(str)):
            raise AssertionError("VAR/X feature axis mismatch")
    finally:
        backed.file.close()
    return {
        "shape": shape,
        "sha256": x_sha256,
        "encoding": encoding,
        "dtype": data_dtype,
        "nnz": config["x_nnz"],
        "sum": total,
        "minimum": minimum,
        "maximum": maximum,
        "all_values_integer": all_integer,
        "decision": "retain_exact_bytes",
    }


def dataset_metadata(dataset: str) -> dict[str, Any]:
    config = CONFIG[dataset]
    return {
        "dataset_id": dataset,
        "sample_accession": dataset,
        "source_accession": "GSE196799",
        "source_relation": "GEO series child sample",
        "source_relation_confidence": "exact",
        "sample_title": config["sample_title"],
        "biosample": config["biosample"],
        "sra_experiment": config["sra_experiment"],
        "timepoint_days": config["day"],
        "timepoint_minutes": int(config["day"] * 24 * 60),
        "child_temporal_status": "non_temporal_single_snapshot",
        "family_temporal_status": "temporal_time_course",
        "temporal_family": "GSE196799",
        "source_cell_call_flag_missing": True,
        "x_semantics": "raw_counts_after_deterministic_cell_qc",
        "cell_qc": {
            "n_genes_strictly_greater_than": 2000,
            "mitochondrial_fraction_strictly_less_than": 0.1,
        },
        "doublet_filter_applied": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=sorted(CONFIG), required=True)
    parser.add_argument("--obs", type=Path, required=True)
    parser.add_argument("--var", type=Path, required=True)
    parser.add_argument("--x", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    packet, packet_sha256 = validate_frozen_packet(args.dataset)
    input_hashes = validate_input_hashes(args.dataset, args.obs, args.x, args.var)
    source_obs = pd.read_parquet(args.obs)
    source_var = pd.read_parquet(args.var)
    revised_obs = build_revised_obs(source_obs, args.dataset)
    revised_var = build_revised_var(source_var)
    x_validation = validate_x(args.x, revised_obs, revised_var, args.dataset)

    args.output_dir.mkdir(parents=True, exist_ok=False)
    obs_out = args.output_dir / "obs.parquet"
    var_out = args.output_dir / "var.parquet"
    metadata_out = args.output_dir / "dataset_metadata.json"
    revised_obs.to_parquet(obs_out)
    revised_var.to_parquet(var_out)
    metadata_out.write_text(
        json.dumps(dataset_metadata(args.dataset), indent=2, sort_keys=True) + "\n"
    )
    receipt = {
        "format": "pert-gym.first10-cohort-c-local-remediation/v1",
        "task_id": TASK_ID,
        "dataset": args.dataset,
        "audit_packet_sha256": packet_sha256,
        "evidence_binding": packet["evidence_binding"],
        "source": input_hashes,
        "predecessor_artifacts": packet["current_artifacts"],
        "predecessor_generations": {
            role: packet["current_object_metadata"][role]["generation"]
            for role in ("obs", "x", "var")
        },
        "predecessor_feature_link_targets": packet["feature_link_targets"],
        "layout_contract": packet["layout_audit"],
        "output": {
            "obs_sha256": sha256_file(obs_out),
            "var_sha256": sha256_file(var_out),
            "rows": len(revised_obs),
            "vars": len(revised_var),
            "x": x_validation,
        },
        "publication_writes": 0,
    }
    (args.output_dir / "receipt.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
