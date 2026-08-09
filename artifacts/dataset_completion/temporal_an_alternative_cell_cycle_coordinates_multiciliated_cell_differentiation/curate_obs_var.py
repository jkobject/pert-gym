#!/usr/bin/env python3
"""Source-exhaustive OBS curation and immutable Lamin readback for GSE228110."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import anndata as ad
import pandas as pd
from pandas.testing import assert_frame_equal

from pert_gym.obs_identity import add_obs_identity, validate_obs_identity
from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_b626108c"
LOGICAL_DATASET = (
    "temporal/an_alternative_cell_cycle_coordinates_multiciliated_cell_differentiation"
)
COLLECTION_UUID = "c26ca66a-63ea-4059-a24e-0e0be0a2a173"
PREFIX_ROOT = f"pert-gym/logical/{LOGICAL_DATASET}"
HERE = Path(__file__).resolve().parent
MANIFEST_PATH = HERE / "source_manifest.json"
JOURNAL_PATH = Path.home() / ".cache/pert-gym/curation_journal" / f"{TASK_ID}.json"
SUCCESSOR_COLLECTION_KEY = (
    "pert-gym/additions/20260730-alternative-cell-cycle-multiciliated-e2e-v2"
)

CANONICAL_OBS_FIELDS = (
    "dataset",
    "sample",
    "cell_id",
    "donor_id",
    "batch",
    "cell_type",
    "cell_line",
    "disease",
    "tissue_type",
    "organism",
    "sex",
    "age",
    "ethnicity",
    "sequencer",
    "technology",
    "assay",
    "modality",
    "media",
    "is_bulk",
    "is_pseudobulk",
    "perturbation",
    "perturbation_type",
    "perturbation_technology",
    "perturbation_library",
    "guide_sequence",
    "molecule_sequence",
    "is_control",
    "dose",
    "dose_unit",
    "timepoint",
    "trajectory_id",
    "pseudotime",
    "is_baseline",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
    "n_counts",
    "n_genes",
    "pct_mito",
    "pct_ribo",
    "is_low_quality",
)
STATE_VOCABULARY = {"present", "missing", "not_applicable"}
RIBOCICLIB_SMILES = "CN(C)C(=O)C1=CC2=CN=C(N=C2N1C3CCCC3)NC4=NC=C(C=C4)N5CCNCC5"
DMSO_SMILES = "CS(=O)C"
COMMON_SOURCE = (
    "CELLxGENE collection c26ca66a-63ea-4059-a24e-0e0be0a2a173; "
    "Choksi et al. Nature 2024 doi:10.1038/s41586-024-07476-z; GSE228110"
)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def frame_sha256(frame: pd.DataFrame) -> str:
    payload = pd.util.hash_pandas_object(frame, index=True).values.tobytes()
    schema = canonical(
        [(str(column), str(dtype)) for column, dtype in frame.dtypes.items()]
    )
    return sha256_bytes(schema.encode() + payload)


def ordered_sha256(values: pd.Index | pd.Series | list[Any]) -> str:
    return sha256_bytes("\n".join(str(value) for value in values).encode())


def write_journal(phase: str, **extra: Any) -> None:
    JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "format": "pert-gym.dataset-completion-journal/v1",
        "task_id": TASK_ID,
        "phase": phase,
        "updated_at": int(time.time()),
        **extra,
    }
    temp = JOURNAL_PATH.with_suffix(".tmp")
    temp.write_text(canonical(payload) + "\n", encoding="utf-8")
    temp.replace(JOURNAL_PATH)


def set_field(
    frame: pd.DataFrame,
    field: str,
    values: Any,
    state: Any,
    source: Any,
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def constant(index: pd.Index, value: Any, *, dtype: str | None = None) -> pd.Series:
    return pd.Series(value, index=index, dtype=dtype)


def missing(index: pd.Index, *, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def artifact_by_uid(ln: Any, uid: str) -> Any:
    matches = [
        item for item in ln.Artifact.filter(uid=uid).all() if str(item.uid) == uid
    ]
    if len(matches) != 1:
        raise AssertionError(f"artifact identity drift: {uid}")
    return matches[0]


def collection_by_uid(ln: Any, uid: str) -> Any:
    matches = [
        item for item in ln.Collection.filter(uid=uid).all() if str(item.uid) == uid
    ]
    if len(matches) != 1:
        raise AssertionError(f"Collection identity drift: {uid}")
    return matches[0]


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"artifact absent: {key}")
    return records[-1], records


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size) if artifact.size is not None else None,
        "created_at": str(artifact.created_at),
    }


def member_identity(members: list[Any]) -> list[dict[str, str]]:
    return sorted(
        ({"uid": str(item.uid), "key": str(item.key)} for item in members),
        key=lambda item: (item["key"], item["uid"]),
    )


def membership_sha256(members: list[Any]) -> str:
    return sha256_bytes(canonical(member_identity(members)).encode())


def verify_manifest() -> dict[str, Any]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if (
        manifest["task_id"] != TASK_ID
        or manifest["logical_dataset"] != LOGICAL_DATASET
        or manifest["collection_uuid"] != COLLECTION_UUID
        or len(manifest["members"]) != 8
    ):
        raise AssertionError("source manifest identity drift")
    if sum(int(item["n_obs"]) for item in manifest["members"]) != 151_275:
        raise AssertionError("source manifest observation denominator drift")
    return manifest


def source_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[
        :,
        [
            column
            for column in frame.columns
            if not any(
                column == f"{field}_state" or column == f"{field}_source"
                for field in CANONICAL_OBS_FIELDS
            )
            and column
            not in {
                "obs_uuid",
                "original_obs_index",
                "original_obs_index_is_duplicated",
            }
        ],
    ].copy()


def treatment_series(obs: pd.DataFrame, experiment: str) -> tuple[pd.Series, pd.Series]:
    if experiment.startswith("e2f7"):
        genotype = obs["genotype"].astype("string")
        perturbation = genotype.map(
            {
                "E2f7_wildtype": "E2f7 wild-type control",
                "E2f7_homozygous_knockout": "E2f7 homozygous knockout",
            }
        ).astype("string")
        is_control = genotype.eq("E2f7_wildtype")
        return perturbation, is_control
    if experiment.startswith("ribociclib"):
        sample = obs["orig.ident"].astype("string")
        is_control = sample.str.startswith("DMSO")
        perturbation = sample.map(
            lambda value: (
                "DMSO vehicle control"
                if str(value).startswith("DMSO")
                else "ribociclib"
            )
        ).astype("string")
        return perturbation, is_control
    sample = obs["orig.ident"].astype("string")
    is_control = sample.eq("air_liquid_interface_day1")
    return constant(obs.index, pd.NA, dtype="string"), is_control


def timepoint_minutes(obs: pd.DataFrame, experiment: str) -> pd.Series:
    if experiment.startswith("e2f7"):
        return constant(obs.index, 7 * 24 * 60, dtype="Int64")
    if experiment.startswith("ribociclib"):
        return constant(obs.index, 3 * 24 * 60, dtype="Int64")
    day = obs["orig.ident"].astype("string").str.extract(r"day(\d+)$", expand=False)
    if day.isna().any():
        raise AssertionError("unmapped time-course sample")
    return (day.astype("Int64") * 24 * 60).astype("Int64")


def frequency_table(values: pd.Series) -> dict[str, int]:
    rendered = values.astype("string").fillna("<missing>")
    return {
        str(value): int(count)
        for value, count in rendered.value_counts(dropna=False).sort_index().items()
    }


def pseudotime_source(experiment: str, obs: pd.DataFrame) -> str | None:
    candidates = {
        "timecourse_multiciliated_subset": ("pseudotime",),
        "timecourse_proliferating_stem_subset": ("pseudo_cycling",),
        "e2f7_multiciliated_subset": ("pseudotime",),
        "e2f7_full": ("multiciliated_pseudotime",),
        "ribociclib_full": ("pseudotime",),
        "ribociclib_multiciliated_subset": ("pseudotime",),
    }.get(experiment, ())
    return next((column for column in candidates if column in obs.columns), None)


def curate_obs(
    baseline: pd.DataFrame, member: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    experiment = str(member["experiment"])
    prefix = f"{PREFIX_ROOT}/cellxgene-{member['dataset_id']}"
    out = source_columns(baseline)
    out.index = baseline.index.copy()
    index = out.index
    if len(out) != int(member["n_obs"]) or not out.index.equals(baseline.index):
        raise AssertionError("baseline identity drift")

    out = add_obs_identity(out, dataset_id=LOGICAL_DATASET, prefix=prefix)
    set_field(
        out, "dataset", LOGICAL_DATASET, "present", "frozen logical dataset inventory"
    )
    set_field(
        out,
        "sample",
        out["orig.ident"].astype("string"),
        "present",
        "CELLxGENE obs.orig.ident",
    )
    set_field(
        out,
        "cell_id",
        out["original_obs_index"].astype("string"),
        "present",
        "CELLxGENE obs index",
    )
    set_field(
        out,
        "donor_id",
        out["donor_id"].astype("string"),
        "present",
        "CELLxGENE obs.donor_id; pooled biological material label",
    )
    set_field(
        out,
        "batch",
        out["orig.ident"].astype("string"),
        "present",
        "GSE228110 biological replicate/sample label",
    )
    set_field(
        out,
        "cell_type",
        out["cell_type"].astype("string"),
        "present",
        "CELLxGENE curated obs.cell_type",
    )
    set_field(
        out,
        "cell_line",
        missing(index),
        "not_applicable",
        "primary mouse tracheal epithelial culture, not a cell line",
    )
    set_field(
        out,
        "disease",
        out["disease"].astype("string"),
        "present",
        "CELLxGENE obs.disease",
    )
    set_field(
        out,
        "tissue_type",
        out["tissue_type"].astype("string"),
        "present",
        "CELLxGENE obs.tissue_type",
    )
    set_field(
        out,
        "organism",
        constant(index, "Mus musculus", dtype="string"),
        "present",
        "CELLxGENE obs.organism_ontology_term_id=NCBITaxon:10090",
    )
    set_field(
        out,
        "sex",
        missing(index),
        "missing",
        "source-exhaustive review: mouse sex not reported",
    )
    set_field(
        out,
        "age",
        constant(index, "adult", dtype="string"),
        "present",
        "paper Methods: adult mice",
    )
    set_field(
        out, "ethnicity", missing(index), "not_applicable", "non-human mouse study"
    )
    set_field(
        out,
        "sequencer",
        constant(index, "Illumina NovaSeq 6000", dtype="string"),
        "present",
        "GSE228110 GPL24247 scRNA-seq sample metadata",
    )
    set_field(
        out,
        "technology",
        constant(index, "10x Chromium Single Cell 3' v3", dtype="string"),
        "present",
        "paper Methods: Chromium Single Cell 3-prime Reagent kit v3",
    )
    set_field(
        out,
        "assay",
        constant(index, "10x 3' v3", dtype="string"),
        "present",
        "CELLxGENE assay ontology and paper Methods",
    )
    set_field(
        out,
        "modality",
        constant(index, "scRNA-seq", dtype="string"),
        "present",
        COMMON_SOURCE,
    )
    set_field(
        out,
        "media",
        constant(
            index, "mTEC air-liquid-interface differentiation medium", dtype="string"
        ),
        "present",
        "paper Methods: mTEC basic medium, 2% Nu-Serum, 50 nM retinoic acid",
    )
    set_field(
        out,
        "is_bulk",
        constant(index, False, dtype="boolean"),
        "present",
        "single-cell RNA sequencing",
    )
    set_field(
        out,
        "is_pseudobulk",
        constant(index, False, dtype="boolean"),
        "present",
        "one row per CELLxGENE cell",
    )

    perturbation, is_control = treatment_series(out, experiment)
    if experiment.startswith("timecourse"):
        set_field(
            out,
            "perturbation",
            perturbation,
            "not_applicable",
            "observational differentiation time course without exogenous perturbation",
        )
        set_field(
            out,
            "perturbation_type",
            missing(index),
            "not_applicable",
            "observational differentiation time course",
        )
        set_field(
            out,
            "perturbation_technology",
            missing(index),
            "not_applicable",
            "observational differentiation time course",
        )
        set_field(
            out,
            "molecule_sequence",
            missing(index),
            "not_applicable",
            "no molecule perturbation",
        )
    elif experiment.startswith("e2f7"):
        set_field(
            out,
            "perturbation",
            perturbation,
            "present",
            "CELLxGENE obs.genotype and paper E2f7 experiment",
        )
        set_field(
            out,
            "perturbation_type",
            constant(index, "genetic knockout", dtype="string"),
            "present",
            "paper Methods: E2f7 homozygous knockout mice and wild-type controls",
        )
        set_field(
            out,
            "perturbation_technology",
            constant(index, "germline CRISPR-Cas9 mouse knockout", dtype="string"),
            "present",
            "paper Methods: E2f7 mutant mouse generation",
        )
        set_field(
            out,
            "molecule_sequence",
            missing(index),
            "not_applicable",
            "genetic genotype contrast, not a molecule perturbation",
        )
    else:
        sample = out["orig.ident"].astype("string")
        ribo = sample.str.startswith("Ribociclib")
        set_field(
            out,
            "perturbation",
            perturbation,
            "present",
            "CELLxGENE obs.orig.ident and paper ribociclib experiment",
        )
        set_field(
            out,
            "perturbation_type",
            constant(index, "small-molecule treatment", dtype="string"),
            "present",
            "paper Methods: CDK4/6 inhibition",
        )
        set_field(
            out,
            "perturbation_technology",
            constant(index, "pharmacological CDK4/6 inhibition", dtype="string"),
            "present",
            "paper Methods",
        )
        molecule = constant(index, DMSO_SMILES, dtype="string")
        molecule.loc[ribo] = RIBOCICLIB_SMILES
        set_field(
            out,
            "molecule_sequence",
            molecule,
            "present",
            "PubChem canonical SMILES: CID 679 and CID 44631912",
        )
    set_field(
        out,
        "perturbation_library",
        missing(index),
        "not_applicable",
        "no pooled perturbation library",
    )
    set_field(
        out,
        "guide_sequence",
        missing(index),
        "not_applicable",
        "no per-cell guide capture; inherited E2f7 genotype where applicable",
    )
    set_field(
        out,
        "is_control",
        is_control.astype("boolean"),
        "present",
        "day-1 baseline, wild-type littermate, or DMSO vehicle according to experiment",
    )

    if experiment.startswith("ribociclib"):
        ribo = out["orig.ident"].astype("string").str.startswith("Ribociclib")
        dose = constant(index, pd.NA, dtype="Float64")
        dose.loc[ribo] = 10.0
        dose_state = constant(index, "missing", dtype="string")
        dose_state.loc[ribo] = "present"
        dose_source = constant(
            index,
            "paper reports DMSO vehicle but not its concentration",
            dtype="string",
        )
        dose_source.loc[ribo] = (
            "paper Methods: ribociclib final concentration 10 micromolar"
        )
        set_field(out, "dose", dose, dose_state, dose_source)
        dose_unit = constant(index, pd.NA, dtype="string")
        dose_unit.loc[ribo] = "micromolar"
        set_field(out, "dose_unit", dose_unit, dose_state, dose_source)
    else:
        set_field(
            out,
            "dose",
            missing(index, dtype="Float64"),
            "not_applicable",
            "no administered small molecule",
        )
        set_field(
            out,
            "dose_unit",
            missing(index),
            "not_applicable",
            "no administered small molecule",
        )

    collection_timepoint = timepoint_minutes(out, experiment)
    out["source_collection_timepoint_minutes"] = collection_timepoint
    out["source_collection_timepoint_unit"] = constant(index, "minute", dtype="string")
    out["source_collection_timepoint_source"] = constant(
        index,
        "paper Methods; elapsed time after air-liquid-interface initiation",
        dtype="string",
    )
    time_levels = sorted(int(value) for value in collection_timepoint.unique())
    if len(time_levels) > 1:
        set_field(
            out,
            "timepoint",
            collection_timepoint,
            "present",
            "paper Methods; canonical unit minutes after air-liquid-interface initiation",
        )
        temporal_verdict = "multitimepoint_biological_axis"
        canonical_timepoint_exposed = True
    else:
        set_field(
            out,
            "timepoint",
            missing(index, dtype="Int64"),
            "not_applicable",
            "single collection time in this physical member; exact sourced timing retained only in source_collection_timepoint_minutes",
        )
        temporal_verdict = "single_timepoint_non_temporal"
        canonical_timepoint_exposed = False
    ptime_column = pseudotime_source(experiment, out)
    if ptime_column is None:
        set_field(
            out,
            "trajectory_id",
            missing(index),
            "not_applicable",
            "no row-level trajectory value in this authored CELLxGENE member",
        )
        set_field(
            out,
            "pseudotime",
            missing(index, dtype="Float64"),
            "not_applicable",
            "no row-level trajectory value in this authored CELLxGENE member",
        )
    else:
        ptime = pd.to_numeric(out[ptime_column], errors="coerce").astype("Float64")
        present = ptime.notna()
        trajectory = constant(index, pd.NA, dtype="string")
        trajectory.loc[present] = f"{experiment}:{ptime_column}"
        state = constant(index, "not_applicable", dtype="string")
        state.loc[present] = "present"
        source = constant(
            index,
            f"CELLxGENE obs.{ptime_column}; absent where source authors excluded the cell from the trajectory",
            dtype="string",
        )
        set_field(out, "trajectory_id", trajectory, state, source)
        set_field(out, "pseudotime", ptime, state, source)
    set_field(
        out,
        "is_baseline",
        is_control.astype("boolean"),
        "present",
        "experiment-specific baseline/control mapping",
    )
    for field in (
        "sensitivity",
        "response_metric",
        "response_value",
        "response_source",
    ):
        set_field(
            out,
            field,
            missing(index),
            "not_applicable",
            "single-cell expression/trajectory dataset without row-level response endpoint",
        )

    set_field(
        out,
        "n_counts",
        pd.to_numeric(out["nCount_RNA"], errors="raise"),
        "present",
        "CELLxGENE source obs.nCount_RNA",
    )
    set_field(
        out,
        "n_genes",
        pd.to_numeric(out["nFeature_RNA"], errors="raise"),
        "present",
        "CELLxGENE source obs.nFeature_RNA",
    )
    set_field(
        out,
        "pct_mito",
        pd.to_numeric(out["percent.mt"], errors="raise"),
        "present",
        "CELLxGENE source obs.percent.mt",
    )
    set_field(
        out,
        "pct_ribo",
        missing(index, dtype="Float64"),
        "missing",
        "source-exhaustive review found no published row-level ribosomal fraction",
    )
    set_field(
        out,
        "is_low_quality",
        constant(index, False, dtype="boolean"),
        "present",
        "CELLxGENE Discover accepted-cell payload after published filtering/doublet removal",
    )

    validate_obs_identity(out)
    if not out.index.equals(baseline.index) or len(out) != len(baseline):
        raise AssertionError("OBS row identity changed")
    if any(field not in out.columns for field in CANONICAL_OBS_FIELDS):
        raise AssertionError("canonical OBS field missing")
    for field in CANONICAL_OBS_FIELDS:
        states = set(out[f"{field}_state"].dropna().astype(str))
        if not states or not states <= STATE_VOCABULARY:
            raise AssertionError(f"invalid field state: {field}={states}")
        source = out[f"{field}_source"].astype("string")
        if source.isna().any() or source.str.strip().eq("").any():
            raise AssertionError(f"missing field source: {field}")
        present = out[f"{field}_state"].astype(str).eq("present")
        if out.loc[present, field].isna().any():
            raise AssertionError(f"present field has null values: {field}")
    receipt = {
        "rows": len(out),
        "row_order_preserved": True,
        "original_obs_index_sha256": ordered_sha256(out["original_obs_index"]),
        "obs_uuid_sha256": ordered_sha256(out["obs_uuid"]),
        "frame_sha256": frame_sha256(out),
        "field_states": {
            field: out[f"{field}_state"]
            .astype(str)
            .value_counts(dropna=False)
            .to_dict()
            for field in CANONICAL_OBS_FIELDS
        },
        "residual_unknowns": {
            field: int(out[f"{field}_state"].astype(str).eq("missing").sum())
            for field in CANONICAL_OBS_FIELDS
            if out[f"{field}_state"].astype(str).eq("missing").any()
        },
        "scientific_modality": (
            "single-cell RNA expression during unperturbed air-liquid-interface differentiation"
            if experiment.startswith("timecourse")
            else "single-cell RNA expression after inherited E2f7 knockout contrast"
            if experiment.startswith("e2f7")
            else "single-cell RNA expression after ribociclib versus DMSO treatment"
        ),
        "experimental_unit": {
            "observation": "single CELLxGENE cell",
            "sample_assignment": "source orig.ident projected to cells",
            "intervention_level": (
                "not_applicable"
                if experiment.startswith("timecourse")
                else "mouse genotype/sample"
                if experiment.startswith("e2f7")
                else "culture sample"
            ),
        },
        "experimental_axes": {
            "biological_time": {
                "verdict": temporal_verdict,
                "source_levels_minutes": time_levels,
                "row_frequencies": frequency_table(collection_timepoint),
                "canonical_timepoint_exposed": canonical_timepoint_exposed,
                "level": "sample projected to cell",
            },
            "perturbation": {
                "verdict": (
                    "not_applicable"
                    if experiment.startswith("timecourse")
                    else "inherited_genotype_contrast"
                    if experiment.startswith("e2f7")
                    else "pharmacological_treatment_contrast"
                ),
                "row_frequencies": frequency_table(out["perturbation"]),
                "level": (
                    "not_applicable"
                    if experiment.startswith("timecourse")
                    else "mouse/sample"
                    if experiment.startswith("e2f7")
                    else "culture sample"
                ),
            },
            "dose": {
                "verdict": (
                    "treatment_dose_with_vehicle_concentration_unknown"
                    if experiment.startswith("ribociclib")
                    else "not_applicable"
                ),
                "row_frequencies": frequency_table(out["dose"]),
                "unit_frequencies": frequency_table(out["dose_unit"]),
            },
            "pseudotime": {
                "verdict": (
                    "computed_trajectory_coordinate"
                    if ptime_column is not None
                    else "not_available_for_member"
                ),
                "present_rows": int(out["pseudotime"].notna().sum()),
                "level": "cell",
            },
        },
        "outcomes_endpoints": {
            "expression": {
                "level": "cell",
                "semantics": "accepted source X expression matrix",
            },
            "scalar_response_or_viability": {
                "verdict": "not_applicable",
                "level": "none",
            },
        },
    }
    return out, receipt


def verify_var(var: pd.DataFrame, expected_n_vars: int) -> dict[str, Any]:
    stable = pd.Index(var.index.astype(str))
    feature_reference = var["feature_reference"].astype("string")
    checks = {
        "rows": len(var) == expected_n_vars,
        "unique": stable.is_unique,
        "ensembl_mouse_syntax": stable.str.fullmatch(r"ENSMUSG\d{11}", na=False).all(),
        "mouse_reference_every_row": feature_reference.eq("NCBITaxon:10090").all(),
    }
    if not all(checks.values()):
        raise AssertionError(f"VAR Ensembl/species validation failed: {checks}")
    return {
        "status": "PASS",
        "biological_features_total": len(var),
        "stable_ensembl_id_features": int(
            stable.str.fullmatch(r"ENSMUSG\d{11}", na=False).sum()
        ),
        "correct_species_features": int(feature_reference.eq("NCBITaxon:10090").sum()),
        "stable_id_axis_sha256": ordered_sha256(stable),
        "species": "Mus musculus",
        "namespace": "Ensembl mouse gene (ENSMUSG)",
        "revision_needed": False,
    }


def requester_pays_cache(artifact: Any) -> Path:
    cache_root = Path.home() / ".cache/pert-gym/requester-pays" / TASK_ID
    cache_root.mkdir(parents=True, exist_ok=True)
    suffix = Path(str(artifact.key)).suffix
    path = cache_root / f"{artifact.uid}{suffix}"
    expected_size = int(artifact.size)
    if path.exists() and path.stat().st_size != expected_size:
        path.unlink()
    if not path.exists():
        temp = path.with_suffix(".partial")
        temp.unlink(missing_ok=True)
        subprocess.run(
            [
                "gcloud",
                "storage",
                "cp",
                "--billing-project=jkobject-1549353370965",
                str(artifact.path),
                str(temp),
            ],
            check=True,
        )
        if temp.stat().st_size != expected_size:
            raise AssertionError("requester-pays X download size drift")
        temp.replace(path)
    return path


def load_dataframe_requester_pays(artifact: Any) -> pd.DataFrame:
    uri = str(artifact.path)
    if uri.startswith("gs://"):
        return pd.read_parquet(requester_pays_cache(artifact))
    return artifact.load()


def verify_x(
    x_artifact: Any, baseline: pd.DataFrame, var: pd.DataFrame
) -> dict[str, Any]:
    path = requester_pays_cache(x_artifact)
    data = ad.read_h5ad(path, backed="r")
    try:
        checks = {
            "shape": tuple(data.shape) == (len(baseline), len(var)),
            "obs_axis": pd.Index(data.obs_names.astype(str)).equals(
                pd.Index(baseline.index.astype(str))
            ),
            "var_axis": pd.Index(data.var_names.astype(str)).equals(
                pd.Index(var.index.astype(str))
            ),
        }
        if not all(checks.values()):
            raise AssertionError(f"X axis/dimension drift: {checks}")
        return {
            "status": "PASS",
            "shape": [int(data.n_obs), int(data.n_vars)],
            "obs_axis_sha256": ordered_sha256(data.obs_names.astype(str)),
            "var_axis_sha256": ordered_sha256(data.var_names.astype(str)),
            "artifact_hash": str(x_artifact.hash),
            "artifact_size": int(x_artifact.size),
            "revision_needed": False,
        }
    finally:
        data.file.close()


def feature_link_x(obs_artifact: Any) -> str:
    values = obs_artifact.features.get_values()
    x_value = values.get("X")
    if x_value is None:
        raise AssertionError(f"OBS lacks explicit X feature link: {obs_artifact.uid}")
    if isinstance(x_value, (list, tuple)):
        if len(x_value) != 1:
            raise AssertionError("OBS X feature link is not singular")
        x_value = x_value[0]
    return str(getattr(x_value, "uid", x_value))


def latest_global_successor(ln: Any) -> Any:
    candidates = []
    for collection in ln.Collection.filter().all():
        description = str(collection.description or "")
        if "pert-gym.append-only-dataset-e2e-successor/v1" in description:
            candidates.append(collection)
    if not candidates:
        raise AssertionError("global append-only Collection chain absent")
    candidates.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    return candidates[-1]


def duplicate_gate_main(ln: Any, manifest: dict[str, Any]) -> dict[str, Any]:
    previous = str(ln.setup.settings.branch.name)
    ln.setup.switch("main")
    try:
        exact = []
        scientific = []
        for member in manifest["members"]:
            dataset_id = member["dataset_id"]
            prefix = f"data/cleaned/cellxgene-{dataset_id}"
            for suffix in ("obs.parquet", "X.h5ad", "var.parquet"):
                exact.extend(ln.Artifact.filter(key=f"{prefix}/{suffix}").all())
        terms = manifest["main_duplicate_gate"]["terms"]
        for term in terms:
            scientific.extend(ln.Artifact.filter(description__icontains=term).all())
            scientific.extend(ln.Collection.filter(description__icontains=term).all())
            scientific.extend(ln.Collection.filter(key__icontains=term).all())
        exact_ids = sorted({str(item.uid) for item in exact})
        scientific_ids = sorted({str(item.uid) for item in scientific})
        if exact_ids or scientific_ids:
            raise AssertionError(
                f"main duplicate gate failed: exact={exact_ids}, scientific={scientific_ids}"
            )
        return {
            "status": "PASS",
            "branch": "main",
            "exact_key_matches": 0,
            "scientific_equivalence_matches": 0,
            "terms": terms,
        }
    finally:
        ln.setup.switch(previous)


def prepare(
    ln: Any,
    manifest: dict[str, Any],
    *,
    verify_payloads: bool,
    allow_task_revision: bool = False,
) -> dict[str, Any]:
    prepared_members = []
    all_uuids: list[str] = []
    var_axes = set()
    for member in manifest["members"]:
        dataset_id = member["dataset_id"]
        logical_prefix = f"{PREFIX_ROOT}/cellxgene-{dataset_id}"
        artifact_prefix = f"data/cleaned/cellxgene-{dataset_id}"
        keys = {
            "obs": f"{artifact_prefix}/obs.parquet",
            "x": f"{artifact_prefix}/X.h5ad",
            "var": f"{artifact_prefix}/var.parquet",
        }
        baseline_obs_artifact = artifact_by_uid(ln, member["baseline_obs"]["uid"])
        x_artifact = artifact_by_uid(ln, member["x"]["uid"])
        var_artifact = artifact_by_uid(ln, member["var"]["uid"])
        for role, artifact in (
            ("obs", baseline_obs_artifact),
            ("x", x_artifact),
            ("var", var_artifact),
        ):
            expected_hash = member["baseline_obs" if role == "obs" else role]["hash"]
            if str(artifact.key) != keys[role] or str(artifact.hash) != expected_hash:
                raise AssertionError(f"frozen {role} artifact drift: {dataset_id}")
        baseline = load_dataframe_requester_pays(baseline_obs_artifact)
        var = load_dataframe_requester_pays(var_artifact)
        expected_obs, obs_receipt = curate_obs(baseline, member)
        expected_hash = frame_sha256(expected_obs)
        latest, history = latest_artifact(ln, keys["obs"])
        if str(latest.uid) == str(baseline_obs_artifact.uid):
            obs_curated = False
        elif str(latest.description or "").startswith(
            f"{TASK_ID}: source-exhaustive GSE228110 OBS"
        ):
            actual = load_dataframe_requester_pays(latest)
            try:
                assert_frame_equal(actual, expected_obs, check_categorical=True)
            except AssertionError:
                if not allow_task_revision:
                    raise
                obs_curated = False
            else:
                if frame_sha256(actual) != expected_hash:
                    raise AssertionError("curated OBS frame hash drift")
                obs_curated = True
        else:
            raise AssertionError(
                f"foreign OBS revision after frozen baseline: {latest.uid}"
            )
        var_receipt = verify_var(var, int(member["n_vars"]))
        var_axes.add(var_receipt["stable_id_axis_sha256"])
        x_receipt = (
            verify_x(x_artifact, baseline, var)
            if verify_payloads
            else {
                "status": "deferred_to_heavy_verify",
                "artifact_hash": str(x_artifact.hash),
                "artifact_size": int(x_artifact.size),
            }
        )
        current_obs = latest
        if feature_link_x(current_obs) != str(x_artifact.key):
            raise AssertionError("OBS to X feature-link drift")
        all_uuids.extend(expected_obs["obs_uuid"].astype(str).tolist())
        prepared_members.append(
            {
                "member": member,
                "prefix": logical_prefix,
                "artifact_prefix": artifact_prefix,
                "keys": keys,
                "baseline_obs_artifact": baseline_obs_artifact,
                "latest_obs_artifact": latest,
                "x_artifact": x_artifact,
                "var_artifact": var_artifact,
                "expected_obs": expected_obs,
                "expected_obs_sha256": expected_hash,
                "obs_curated": obs_curated,
                "obs_history_count": len(history),
                "obs_receipt": obs_receipt,
                "var_receipt": var_receipt,
                "x_receipt": x_receipt,
            }
        )
    if len(var_axes) != 1:
        raise AssertionError("shared source VAR axes differ across members")
    if len(all_uuids) != len(set(all_uuids)) or len(all_uuids) != 151_275:
        raise AssertionError("global obs_uuid uniqueness failed")
    return {
        "members": prepared_members,
        "obs_uuid_global_unique": True,
        "obs_uuid_total": len(all_uuids),
        "shared_var_axis_sha256": next(iter(var_axes)),
    }


def publish_obs(
    ln: Any, prepared: dict[str, Any], helper_sha256: str
) -> tuple[list[Any], int]:
    published = []
    writes = 0
    for item in prepared["members"]:
        if item["obs_curated"]:
            published.append(item["latest_obs_artifact"])
            continue
        root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-obs-"))
        path = root / "obs.parquet"
        item["expected_obs"].to_parquet(path)
        description = (
            f"{TASK_ID}: source-exhaustive GSE228110 OBS; "
            f"dataset_id={item['member']['dataset_id']}; rows={len(item['expected_obs'])}; "
            f"frame_sha256={item['expected_obs_sha256']}; helper_sha256={helper_sha256}; "
            "row order preserved; explicit state/source evidence for 42 canonical fields"
        )
        artifact = ln.Artifact.from_dataframe(
            path,
            key=item["keys"]["obs"],
            revises=item["latest_obs_artifact"],
            description=description,
        ).save()
        artifact.features.set_values({"X": item["x_artifact"]})
        published.append(artifact)
        writes += 1
        write_journal(
            "obs_saved",
            dataset_id=item["member"]["dataset_id"],
            obs_uid=str(artifact.uid),
            writes=writes,
        )
    return published, writes


def successor_description(
    predecessor: Any,
    before: list[Any],
    after: list[Any],
    replacements: list[dict[str, str]],
) -> str:
    return canonical(
        {
            "format": "pert-gym.append-only-dataset-e2e-successor/v1",
            "task_id": TASK_ID,
            "dataset_id": LOGICAL_DATASET,
            "predecessor_uid": str(predecessor.uid),
            "predecessor_key": str(predecessor.key),
            "predecessor_membership_sha256": membership_sha256(before),
            "replacements": replacements,
            "member_count_before": len(before),
            "member_count_after": len(after),
            "resulting_membership_sha256": membership_sha256(after),
            "membership_rule": "immutable predecessor with eight same-key OBS artifacts replaced by exact source-curated revisions; X and VAR artifacts reused unchanged; no duplicate artifact keys",
            "rollback": f"select immutable predecessor Collection {predecessor.uid}",
        }
    )


def ensure_successor(
    ln: Any,
    manifest: dict[str, Any],
    new_obs: list[Any],
    *,
    allow_create: bool,
) -> tuple[Any, bool, dict[str, Any]]:
    existing = list(ln.Collection.filter(key=SUCCESSOR_COLLECTION_KEY).all())
    if len(existing) > 1:
        raise AssertionError("successor Collection key collision")
    if existing:
        recorded = json.loads(str(existing[0].description))
        if (
            recorded.get("format") != "pert-gym.append-only-dataset-e2e-successor/v1"
            or recorded.get("task_id") != TASK_ID
            or recorded.get("dataset_id") != LOGICAL_DATASET
        ):
            raise AssertionError("successor Collection provenance drift")
        predecessor = collection_by_uid(ln, str(recorded["predecessor_uid"]))
    else:
        predecessor = latest_global_successor(ln)
    before = list(predecessor.artifacts.all())
    replacement_by_key = {str(item.key): item for item in new_obs}
    old_by_key = {
        str(item.key): item for item in before if str(item.key) in replacement_by_key
    }
    if len(old_by_key) != 8 or len(replacement_by_key) != 8:
        raise AssertionError("target OBS replacement set drift")
    after = [item for item in before if str(item.key) not in replacement_by_key]
    after.extend(replacement_by_key.values())
    keys = [str(item.key) for item in after]
    if len(after) != len(before) or len(keys) != len(set(keys)):
        raise AssertionError("successor Collection key uniqueness/count drift")
    replacements = sorted(
        [
            {
                "key": key,
                "replaced_uid": str(old_by_key[key].uid),
                "added_uid": str(replacement_by_key[key].uid),
            }
            for key in replacement_by_key
        ],
        key=lambda item: item["key"],
    )
    description = successor_description(predecessor, before, after, replacements)
    created = False
    if existing:
        successor = existing[0]
    else:
        if not allow_create:
            raise AssertionError("required successor Collection absent")
        successor = ln.Collection(
            after,
            key=SUCCESSOR_COLLECTION_KEY,
            description=description,
            skip_hash_lookup=True,
        ).save()
        created = True
    actual = list(successor.artifacts.all())
    if str(successor.description) != description or member_identity(
        actual
    ) != member_identity(after):
        raise AssertionError("successor Collection readback drift")
    return (
        successor,
        created,
        {
            "status": "PASS",
            "predecessor": {
                "uid": str(predecessor.uid),
                "key": str(predecessor.key),
                "member_count": len(before),
                "membership_sha256": membership_sha256(before),
            },
            "successor": {
                "uid": str(successor.uid),
                "key": str(successor.key),
                "member_count": len(actual),
                "membership_sha256": membership_sha256(actual),
            },
            "replacements": replacements,
        },
    )


def public_member(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_id": item["member"]["dataset_id"],
        "experiment": item["member"]["experiment"],
        "logical_prefix": item["prefix"],
        "artifact_prefix": item["artifact_prefix"],
        "obs": artifact_identity(item["latest_obs_artifact"]),
        "x": artifact_identity(item["x_artifact"]),
        "var": artifact_identity(item["var_artifact"]),
        "obs_history_count": item["obs_history_count"],
        "obs_curation": item["obs_receipt"],
        "x_validation": item["x_receipt"],
        "var_validation": item["var_receipt"],
        "feature_link_x_key": feature_link_x(item["latest_obs_artifact"]),
    }


def emit_product(phase: str, current: int) -> None:
    print(
        "PRODUCT_EXECUTION="
        + canonical(
            {
                "product_execution": {
                    "host": os.uname().nodename,
                    "pid": os.getpid(),
                    "phase": phase,
                    "payload_heartbeat_at": int(time.time()),
                    "metric": "biological_dataset_completion",
                    "current": current,
                    "denominator": 1,
                    "unit": "biological_dataset",
                }
            }
        ),
        flush=True,
    )


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution; use tools/launch_pert_gym_heavy.py")
    mode = sys.argv[1]
    helper_sha256 = sha256_file(Path(__file__))
    manifest = verify_manifest()
    capacity = preflight()
    write_journal("preflight", mode=mode, helper_sha256=helper_sha256)
    emit_product("preflight", 0)
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    duplicate_gate = duplicate_gate_main(ln, manifest)
    if ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("duplicate gate did not restore jkobject branch")
    prepared = prepare(
        ln,
        manifest,
        verify_payloads=True,
        allow_task_revision=mode == "mutate",
    )
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    obs_writes = 0
    collection_created = False
    collection_receipt: dict[str, Any] = {"status": "not_evaluated_before_write"}
    if mode == "mutate":
        metadata = {
            "run_id": TASK_ID,
            "pid": os.getpid(),
            "host": capacity.hostname,
            "project": capacity.project,
            "zone": capacity.zone,
            "branch": str(ln.setup.settings.branch.name),
            "started_at": time.time(),
        }
        with ExitStack() as stack:
            stack.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            stack.enter_context(distributed_lamin_writer_lease(metadata))
            fresh = prepare(
                ln,
                manifest,
                verify_payloads=False,
                allow_task_revision=True,
            )
            ln.track(
                key=f"pert-gym/dataset-completion/{TASK_ID}",
                kind="script",
                params={
                    "task_id": TASK_ID,
                    "logical_dataset": LOGICAL_DATASET,
                    "mode": mode,
                    "helper_sha256": helper_sha256,
                },
                new_run=True,
                pypackages=False,
                stream_tracking=False,
            )
            new_obs, obs_writes = publish_obs(ln, fresh, helper_sha256)
            successor, collection_created, collection_receipt = ensure_successor(
                ln, manifest, new_obs, allow_create=True
            )
            try:
                ln.finish()
            except AttributeError:
                ln.context.finish()
            write_journal(
                "published",
                collection_uid=str(successor.uid),
                obs_writes=obs_writes,
            )
    elif all(item["obs_curated"] for item in prepared["members"]):
        _, _, collection_receipt = ensure_successor(
            ln,
            manifest,
            [item["latest_obs_artifact"] for item in prepared["members"]],
            allow_create=False,
        )
    elif mode == "verify":
        raise AssertionError("verify requested before curated OBS revisions exist")

    final = prepare(ln, manifest, verify_payloads=True)
    if mode in {"mutate", "verify"} and not all(
        item["obs_curated"] for item in final["members"]
    ):
        raise AssertionError("terminal curated OBS readback failed")
    if all(item["obs_curated"] for item in final["members"]):
        _, _, collection_receipt = ensure_successor(
            ln,
            manifest,
            [item["latest_obs_artifact"] for item in final["members"]],
            allow_create=False,
        )
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    receipt = {
        "format": "pert-gym.real-dataset-e2e-curation/v3",
        "task_id": TASK_ID,
        "logical_dataset": LOGICAL_DATASET,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "source_manifest_sha256": sha256_file(MANIFEST_PATH),
        "duplicate_gate_main": duplicate_gate,
        "source_exhaustive": True,
        "scientific_modality": {
            "family": "single-cell RNA expression across one differentiation time course and two intervention contrasts",
            "members": {
                item["member"]["experiment"]: item["obs_receipt"]["scientific_modality"]
                for item in final["members"]
            },
        },
        "experimental_axes": {
            item["member"]["experiment"]: item["obs_receipt"]["experimental_axes"]
            for item in final["members"]
        },
        "outcomes_endpoints": {
            "expression": {
                "level": "cell",
                "semantics": "accepted source X expression matrices",
            },
            "scalar_response_or_viability": {
                "verdict": "not_applicable",
                "level": "none",
            },
        },
        "members": [public_member(item) for item in final["members"]],
        "obs_validation": {
            "status": "PASS",
            "members": 8,
            "rows": final["obs_uuid_total"],
            "obs_uuid_global_unique": final["obs_uuid_global_unique"],
            "canonical_fields": len(CANONICAL_OBS_FIELDS),
            "state_source_evidence": True,
            "fabricated_values": False,
        },
        "var_validation": {
            "status": "PASS",
            "shared_axis_sha256": final["shared_var_axis_sha256"],
            "biological_features_total_per_member": 30146,
            "stable_ensembl_id_features_per_member": 30146,
            "correct_species_features_per_member": 30146,
        },
        "collections": collection_receipt,
        "writes": {
            "obs_revisions": obs_writes,
            "x_revisions": 0,
            "var_revisions": 0,
            "collection_writes": int(collection_created),
            "deletions": 0,
        },
        "registry_counts": {"before": counts_before, "after": counts_after},
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "host": {
            "hostname": capacity.hostname,
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = sha256_bytes(canonical(receipt).encode())
    write_journal(
        "verified" if mode == "verify" else "complete",
        receipt_sha256=receipt["canonical_sha256"],
    )
    emit_product("checkpointing", 1)
    print("ALTERNATIVE_CELL_CYCLE_CURATION_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
