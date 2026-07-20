"""Shared lightweight dataset loaders for perturbation model benchmarks.

The public contract in this module intentionally stays pure-Python: loaders return
small in-memory batches compatible with :mod:`pert_gym.evaluate` and the baseline
model protocol. Production Lamin/AnnData readers should adapt into this contract
without forcing model-specific heavy dependencies into CI.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .evaluate import EvaluationBatch
from .models.base import Matrix

CONTROL_PERTURBATIONS = {"control", "ctrl", "vehicle", "ntc", "non-targeting"}
DEFAULT_CONTEXT_FIELDS = ("cell_line", "cell_type", "tissue", "disease", "assay")
DEFAULT_MODEL_READY_COLLECTION_KEY = "pert-gym/model-ready/20260621"
DEFAULT_MODEL_READY_MEMBER_COUNT = 1
EMPTY_RESPONSE_SCREEN_EXCLUSION_REASON = (
    "x_semantics=empty response_screen is not expression-model-ready"
)
BROAD_PRISM_EXCLUSION_REASON = (
    "broad_prism_repurposing is held out from expression-model-ready loaders"
)
SANGER_SCORE_EXCLUSION_REASON = "sanger_score_crispr is CRISPRko essentiality/dependency response data, not expression X"
NON_EXPRESSION_SCORE_SEMANTICS = {"gene_effect", "fold_change", "dependency_score"}
DIRECT_RESPONSE_SOURCES = {
    "broad prism repurposing",
    "sanger/gdsc drug response",
    "sanger project score/score2",
    "depmap chronos/ceres dependency",
}
IMAGE_MODALITIES = {"image", "microscopy", "high-content imaging"}
MAPPING_ROLES = {
    "mapping_sidecar",
    "guide_target_mapping",
    "tss_mapping",
    "split_mapping",
    "qa_mapping",
}


@dataclass(frozen=True)
class BenchmarkBatch(EvaluationBatch):
    """Model-ready batch plus row-level covariates and target responses.

    Attributes mirror the canonical benchmark loader contract:
    - ``X`` is the feature/target response matrix extracted from expression-like
      payloads as a small in-memory matrix.
    - ``perturbations`` contains one perturbation identity per row.
    - ``controls`` flags control rows selected from ``obs.is_control`` or from a
      conservative perturbation-name fallback.
    - ``obs_covariates`` contains model-agnostic context metadata copied from obs.
    - ``target_response`` is the supervised response format consumed by the
      current evaluation scaffold; for direct expression prediction it equals
      ``X``.
    - ``feature_names`` are the var/index labels for matrix columns when known.
    - ``compound_features`` optionally carries one chemical feature vector per
      row, e.g. a precomputed Morgan fingerprint for chemCPA-style loaders.
    """

    obs_covariates: Sequence[Mapping[str, Any]] = ()
    target_response: Matrix | None = None
    feature_names: Sequence[str] = ()
    compound_features: Matrix | None = None
    compound_feature_names: Sequence[str] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        if len(self.obs_covariates) != len(self.X):
            raise ValueError("obs_covariates must have one mapping per X row.")
        if self.target_response is not None and len(self.target_response) != len(
            self.X
        ):
            raise ValueError("target_response must have one row per X row.")
        if self.feature_names and len(self.feature_names) != _n_features(self.X):
            raise ValueError("feature_names must match the number of X columns.")
        if self.compound_features is not None and len(self.compound_features) != len(
            self.X
        ):
            raise ValueError("compound_features must have one row per X row.")
        if self.compound_feature_names and self.compound_features is not None:
            if len(self.compound_feature_names) != _n_features(self.compound_features):
                raise ValueError(
                    "compound_feature_names must match compound feature width."
                )


@dataclass(frozen=True)
class BenchmarkDataset:
    """Train/validation/test benchmark split for perturbation-response models."""

    train: BenchmarkBatch
    val: BenchmarkBatch
    test: BenchmarkBatch
    split_by: str
    source: str
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.split_by != "perturbation_identity":
            raise ValueError(
                "BenchmarkDataset splits must be by perturbation identity."
            )
        _validate_split_integrity(self.train, self.val, self.test)
        if not any(self.train.controls or []):
            raise ValueError("train split must include at least one control row.")
        if not any(self.val.controls or []):
            raise ValueError("val split must include at least one control row.")
        if not any(self.test.controls or []):
            raise ValueError("test split must include at least one control row.")


@dataclass(frozen=True)
class ExpressionMemberFilterResult:
    """Expression-loader membership after holding out non-expression screens."""

    included: Sequence[str]
    excluded: Sequence[str]
    excluded_reasons: Mapping[str, str]


@dataclass(frozen=True)
class ResponseTableSample:
    """Direct viability/survival/dependency row without fabricated expression X."""

    sample_id: str
    source: str
    perturbation: str
    context: Mapping[str, Any]
    response_metric: str
    response_value: float | None
    response_direction: str
    response_unit: str | None = None
    response_source: str | None = None


@dataclass(frozen=True)
class ExpressionLikeSample:
    """Expression/pretraining row with explicit matrix semantics and role."""

    sample_id: str
    source: str
    artifact_key: str
    perturbation: str
    context: Mapping[str, Any]
    modality: str
    assay: str
    x_semantics: str
    role: str
    target_task: str | None = None


@dataclass(frozen=True)
class ImagePayloadSample:
    """Image-derived payload row, intentionally separate from expression batches."""

    sample_id: str
    source: str
    payload_artifact_keys: Sequence[str]
    perturbation: str
    context: Mapping[str, Any]
    modality: str
    assay: str


@dataclass(frozen=True)
class MappingSidecarJoin:
    """Typed sidecar mapping that may be joined to samples but is not a label."""

    mapping_id: str
    source: str
    artifact_key: str
    mapping_role: str
    join_fields: Sequence[str]
    notes: str = ""


@dataclass(frozen=True)
class ModelReadyV2Adapters:
    """Typed projections from a model_ready_v2 manifest."""

    responses: Sequence[ResponseTableSample]
    expressions: Sequence[ExpressionLikeSample]
    images: Sequence[ImagePayloadSample]
    mappings: Sequence[MappingSidecarJoin]
    skipped: Mapping[str, str]


@dataclass(frozen=True)
class ModelReadyV2Batch:
    """Unified metadata-only batch row for heterogeneous model_ready_v2 smokes.

    ``features`` intentionally carries either a tiny in-memory feature vector or
    typed payload handles; it never bulk-loads expression matrices or image
    payloads. ``target_mask`` is false when no supervised target is available.
    """

    source_dataset: str
    split: str
    modality: str
    features: Mapping[str, Any]
    perturbation: str
    target: str | None
    guide: str | None
    cell_line: str | None
    cell_type: str | None
    cell_state: str | None
    organism: str | None
    target_label: float | None
    target_mask: bool


def load_tiny_benchmark_dataset(
    *,
    source: str = "synthetic",
    obs_rows: Sequence[Mapping[str, Any]] | None = None,
    X: Matrix | None = None,
    feature_names: Sequence[str] | None = None,
    split_fractions: tuple[float, float, float] = (0.5, 0.25, 0.25),
) -> BenchmarkDataset:
    """Load a tiny model benchmark dataset, falling back to deterministic synthetic data.

    ``obs_rows`` + ``X`` can be supplied by a future model-ready-v0 adapter after
    bounded extraction from Lamin/AnnData. When omitted, a 12-row synthetic panel
    is returned for CI. Splits are by perturbation identity; controls are copied
    into every split so control baselines can be fit/evaluated without leakage of
    held-out perturbation identities.
    """

    if obs_rows is None or X is None:
        obs_rows, X, feature_names = _synthetic_rows()
    if feature_names is None:
        feature_names = [f"gene_{idx}" for idx in range(_n_features(X))]

    _validate_source_rows(obs_rows, X)
    split_names = split_perturbation_identities(obs_rows, split_fractions)
    return _build_dataset(
        obs_rows=obs_rows,
        X=X,
        feature_names=feature_names,
        split_names=split_names,
        source=source,
        metadata={"loader": "tiny", "n_obs": len(X), "n_features": _n_features(X)},
    )


def load_model_ready_v0_or_synthetic(
    *,
    manifest_path: Path | str = Path(
        "artifacts/schema_audit/model_ready_subset_20260621.json"
    ),
    source: str = "model-ready-v0",
) -> BenchmarkDataset:
    """Return a benchmark dataset from model-ready-v0 metadata or synthetic fallback.

    The current v0 model-ready artifact is a Lamin collection manifest, not a
    small checked-in matrix. This loader reads only the manifest metadata and then
    returns the CI-safe synthetic batch while preserving the manifest provenance
    in ``dataset.metadata``. A future bounded extractor can pass materialized
    ``obs_rows``/``X`` into :func:`load_tiny_benchmark_dataset` without changing
    the downstream model contract.
    """

    manifest = _read_json_if_exists(Path(manifest_path))
    collection = manifest.get("model_ready_collection", {})
    expression_members = filter_expression_model_ready_members(
        collection.get("member_keys", []),
        member_metadata=collection.get("member_metadata", {}),
    )
    dataset = load_tiny_benchmark_dataset(source=source)
    metadata = dict(dataset.metadata)
    metadata.update(
        {
            "loader": "model_ready_v0_or_synthetic",
            "fallback": "synthetic",
            "manifest_path": str(manifest_path),
            "model_ready_collection_key": collection.get("key")
            or DEFAULT_MODEL_READY_COLLECTION_KEY,
            "model_ready_member_count": collection.get("member_count")
            or DEFAULT_MODEL_READY_MEMBER_COUNT,
            "model_ready_member_keys": list(expression_members.included),
            "excluded_member_keys": list(expression_members.excluded),
            "excluded_member_reasons": dict(expression_members.excluded_reasons),
        }
    )
    return BenchmarkDataset(
        train=dataset.train,
        val=dataset.val,
        test=dataset.test,
        split_by=dataset.split_by,
        source=source,
        metadata=metadata,
    )


def load_model_ready_v2_adapters(
    *,
    manifest_path: Path | str,
) -> ModelReadyV2Adapters:
    """Load typed adapter samples from a model_ready_v2 TSV/CSV manifest.

    This is intentionally metadata-only and pure Python. It separates response
    tables, expression-like matrices, image-derived payloads, and mapping
    sidecars so loaders do not fabricate scRNA ``X`` for non-expression sources
    or accidentally treat guide/TSS/split mappings as labels.
    """

    path = Path(manifest_path)
    rows = _read_manifest_rows(path)
    return adapt_model_ready_v2_rows(rows)


def load_model_ready_v2_batches(
    *,
    manifest_path: Path | str,
) -> tuple[ModelReadyV2Batch, ...]:
    """Load metadata-only smoke batches for every handled model_ready_v2 row."""

    adapters = load_model_ready_v2_adapters(manifest_path=manifest_path)
    if adapters.skipped:
        details = "; ".join(
            f"{row_id}: {reason}" for row_id, reason in sorted(adapters.skipped.items())
        )
        raise ValueError(f"model_ready_v2 manifest contains skipped rows: {details}")
    return model_ready_v2_batches_from_adapters(adapters)


def model_ready_v2_batches_from_adapters(
    adapters: ModelReadyV2Adapters,
) -> tuple[ModelReadyV2Batch, ...]:
    """Expose response, expression, image, and mapping adapters through one API."""

    batches: list[ModelReadyV2Batch] = []
    for sample in adapters.responses:
        batches.append(
            ModelReadyV2Batch(
                source_dataset=sample.source,
                split=_split_from_context(sample.context),
                modality="screen",
                features={
                    "response_table_handle": sample.response_source or sample.source
                },
                perturbation=sample.perturbation or "needs_row_level_projection",
                target=sample.response_metric,
                guide=_context_value(sample.context, "guide_id"),
                cell_line=_context_value(
                    sample.context, "cell_line", "depmap_id", "sanger_model_id"
                ),
                cell_type=_context_value(sample.context, "cell_type"),
                cell_state=_context_value(sample.context, "cell_state"),
                organism=_context_value(sample.context, "organism")
                or "needs_row_level_projection",
                target_label=sample.response_value,
                target_mask=sample.response_value is not None,
            )
        )
    for sample in adapters.expressions:
        batches.append(
            ModelReadyV2Batch(
                source_dataset=sample.source,
                split=_split_from_context(sample.context),
                modality=sample.modality,
                features={
                    "expression_handle": sample.artifact_key,
                    "x_semantics": sample.x_semantics,
                },
                perturbation=sample.perturbation or "needs_row_level_projection",
                target=sample.target_task,
                guide=_context_value(sample.context, "guide_id"),
                cell_line=_context_value(
                    sample.context, "cell_line", "depmap_id", "sanger_model_id"
                ),
                cell_type=_context_value(sample.context, "cell_type"),
                cell_state=_context_value(sample.context, "cell_state"),
                organism=_context_value(sample.context, "organism")
                or "needs_row_level_projection",
                target_label=None,
                target_mask=False,
            )
        )
    for sample in adapters.images:
        batches.append(
            ModelReadyV2Batch(
                source_dataset=sample.source,
                split=_split_from_context(sample.context),
                modality=sample.modality,
                features={"payload_handles": tuple(sample.payload_artifact_keys)},
                perturbation=sample.perturbation or "needs_row_level_projection",
                target="image_phenotype",
                guide=_context_value(sample.context, "guide_id"),
                cell_line=_context_value(
                    sample.context, "cell_line", "depmap_id", "sanger_model_id"
                ),
                cell_type=_context_value(sample.context, "cell_type"),
                cell_state=_context_value(sample.context, "cell_state"),
                organism=_context_value(sample.context, "organism")
                or "needs_row_level_projection",
                target_label=None,
                target_mask=False,
            )
        )
    for mapping in adapters.mappings:
        batches.append(
            ModelReadyV2Batch(
                source_dataset=mapping.source,
                split="not_split",
                modality="mapping",
                features={
                    "mapping_handle": mapping.artifact_key,
                    "join_fields": tuple(mapping.join_fields),
                },
                perturbation="mapping_only",
                target=mapping.mapping_role,
                guide=None,
                cell_line=None,
                cell_type=None,
                cell_state=None,
                organism="not_applicable",
                target_label=None,
                target_mask=False,
            )
        )
    return tuple(batches)


def adapt_model_ready_v2_rows(
    rows: Sequence[Mapping[str, Any]],
) -> ModelReadyV2Adapters:
    """Project model_ready_v2 manifest rows into typed loader adapter objects."""

    responses: list[ResponseTableSample] = []
    expressions: list[ExpressionLikeSample] = []
    images: list[ImagePayloadSample] = []
    mappings: list[MappingSidecarJoin] = []
    skipped: dict[str, str] = {}

    for idx, row in enumerate(rows):
        row_id = _row_id(row, idx)
        role = str(row.get("artifact_role", "")).strip().lower()
        target_kind = str(row.get("target_kind", "")).strip().lower()
        modality = str(row.get("modality", "")).strip()
        x_semantics = str(row.get("x_semantics", "")).strip()
        source = str(row.get("source", row.get("source_family", ""))).strip()
        has_expression = _as_bool(row.get("has_expression_X"))
        has_explicit_expression_semantics = x_semantics.lower() in {
            "raw_counts",
            "normalized_expression",
            "log1p_expression",
            "delta_expression",
            "signature",
        }
        is_expression_row = has_explicit_expression_semantics or (
            has_expression and not _is_direct_response_row(row)
        )

        if role in MAPPING_ROLES or target_kind == "mapping":
            try:
                mappings.append(_mapping_join_from_manifest_row(row, row_id))
            except ValueError as exc:
                skipped[row_id] = str(exc)
            continue
        if _is_image_payload_row(row):
            try:
                images.append(_image_sample_from_manifest_row(row, row_id))
            except ValueError as exc:
                skipped[row_id] = str(exc)
            continue
        # Measured-expression rows with response labels are expression-response
        # examples, not response tables backed by fake expression.
        if is_expression_row:
            if x_semantics.lower() in {"", "empty", "unknown"}:
                skipped[row_id] = "expression-like rows require explicit x_semantics"
                continue
            try:
                expressions.append(_expression_sample_from_manifest_row(row, row_id))
            except ValueError as exc:
                skipped[row_id] = str(exc)
            continue
        if _is_direct_response_row(row):
            try:
                responses.append(_response_sample_from_manifest_row(row, row_id))
            except ValueError as exc:
                skipped[row_id] = str(exc)
            continue
        skipped[row_id] = (
            f"unhandled model_ready_v2 row source={source!r} modality={modality!r} "
            f"role={role!r} target_kind={target_kind!r}"
        )

    return ModelReadyV2Adapters(
        responses=tuple(responses),
        expressions=tuple(expressions),
        images=tuple(images),
        mappings=tuple(mappings),
        skipped=skipped,
    )


def filter_expression_model_ready_members(
    member_keys: Sequence[str],
    *,
    member_metadata: Mapping[str, Mapping[str, Any]] | None = None,
) -> ExpressionMemberFilterResult:
    """Return members safe for expression-only benchmark loaders."""

    metadata_by_key = member_metadata or {}
    included: list[str] = []
    excluded: list[str] = []
    reasons: dict[str, str] = {}
    for key in member_keys:
        key_text = str(key)
        metadata = metadata_by_key.get(key_text, {})
        prefix = _dataset_prefix_from_obs_key(key_text)
        x_semantics = str(metadata.get("x_semantics", "")).lower()
        modality = str(metadata.get("modality", metadata.get("assay", ""))).lower()
        readout_modality = str(metadata.get("readout_modality", "")).lower()
        perturbation_type = str(metadata.get("perturbation_type", "")).lower()
        if prefix == "sanger_score_crispr":
            excluded.append(key_text)
            reasons[key_text] = SANGER_SCORE_EXCLUSION_REASON
            continue
        if x_semantics in NON_EXPRESSION_SCORE_SEMANTICS or (
            "essentiality" in modality
            and "crispr" in (readout_modality or modality)
            and perturbation_type in {"crisprko", "crispr-ko", "ko"}
        ):
            excluded.append(key_text)
            reasons[key_text] = (
                f"x_semantics={x_semantics} is not expression-model-ready"
            )
            continue
        if x_semantics == "empty" and "response" in modality:
            excluded.append(key_text)
            reasons[key_text] = EMPTY_RESPONSE_SCREEN_EXCLUSION_REASON
            continue
        if prefix == "broad_prism_repurposing":
            excluded.append(key_text)
            reasons[key_text] = BROAD_PRISM_EXCLUSION_REASON
            continue
        included.append(key_text)
    return ExpressionMemberFilterResult(
        included=included, excluded=excluded, excluded_reasons=reasons
    )


def load_response_screen_with_baseline(
    *,
    response_rows: Sequence[Mapping[str, Any]],
    baseline_rows: Sequence[Mapping[str, Any]],
    feature_names: Sequence[str],
) -> BenchmarkBatch:
    """Join bounded response-screen rows to separate baseline RNA expression."""

    if not baseline_rows:
        raise ValueError(
            "response-screen loaders require separate baseline RNA expression"
        )
    baseline_by_id: dict[str, tuple[float, ...]] = {}
    for idx, row in enumerate(baseline_rows):
        stable_id = _stable_depmap_id(row.get("depmap_id") or row.get("ach_id"))
        if not stable_id:
            raise ValueError(f"baseline row {idx} is missing depmap_id/ach_id")
        expression = row.get("expression")
        if expression is None:
            raise ValueError(f"baseline row {idx} is missing baseline RNA expression")
        try:
            expression_values = tuple(
                _finite_float(value, "baseline RNA expression") for value in expression
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"baseline row {idx} has malformed baseline RNA expression"
            ) from exc
        existing_expression = baseline_by_id.get(stable_id)
        if existing_expression is not None:
            if expression_values != existing_expression:
                raise ValueError(
                    f"baseline row {idx} has non-identical baseline RNA expression "
                    f"for duplicate {stable_id}"
                )
            continue
        baseline_by_id[stable_id] = expression_values

    X: list[list[float]] = []
    target_response: list[list[float]] = []
    perturbations: list[str] = []
    controls: list[bool] = []
    covariates: list[dict[str, Any]] = []
    for idx, row in enumerate(response_rows):
        stable_id = _stable_depmap_id(row.get("depmap_id") or row.get("ach_id"))
        if not stable_id:
            raise ValueError(f"response row {idx} is missing depmap_id/ach_id")
        if stable_id not in baseline_by_id:
            raise ValueError(
                f"response row {idx} has no baseline RNA expression for {stable_id}"
            )
        if "response_value" in row:
            response_metric = str(row.get("response_metric", "")).strip().lower()
            response_value = row.get("response_value")
        else:
            response_metric = "lfc" if "lfc" in row else ""
            response_value = row.get("lfc")
        if response_value is None:
            raise ValueError(f"response row {idx} is missing response_value")
        try:
            numeric_response = _finite_float(response_value, "response_value")
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"response row {idx} has malformed response_value: {response_value!r}"
            ) from exc
        if response_metric in {"", "missing", "none", "nan"}:
            raise ValueError(f"response row {idx} is missing response_metric")
        perturbation = str(
            row.get("perturbation")
            or row.get("broad_id")
            or row.get("perturbation_id")
            or ""
        ).strip()
        if not perturbation:
            raise ValueError(
                f"response row {idx} is missing perturbation/broad_id/perturbation_id"
            )
        X.append(list(baseline_by_id[stable_id]))
        target_response.append([numeric_response])
        perturbations.append(perturbation)
        controls.append(_is_control_row(row))
        covariates.append({"depmap_id": stable_id, "response_metric": response_metric})

    if not X:
        raise ValueError("response-screen loader requires at least one response row")
    if len(feature_names) != _n_features(X):
        raise ValueError("feature_names must match baseline RNA expression width")
    return BenchmarkBatch(
        X=X,
        perturbations=perturbations,
        controls=controls,
        obs_covariates=tuple(covariates),
        target_response=target_response,
        feature_names=tuple(feature_names),
    )


def load_essentiality_screen_with_baseline(
    *,
    response_rows: Sequence[Mapping[str, Any]],
    baseline_rows: Sequence[Mapping[str, Any]],
    feature_names: Sequence[str],
) -> BenchmarkBatch:
    """Join CRISPRko essentiality/dependency rows to separate baseline RNA.

    Sanger Project SCORE rows are model/cell-line x perturbation-gene response
    observations. The dependency score is a supervised target and never an
    expression ``X`` matrix; ``X`` comes only from matched baseline RNA rows.
    """

    if not baseline_rows:
        raise ValueError(
            "essentiality loaders require separate baseline RNA expression"
        )
    baseline_by_id: dict[str, tuple[float, ...]] = {}
    for idx, row in enumerate(baseline_rows):
        stable_id = _stable_cell_model_id(
            row.get("sanger_model_id") or row.get("model_name")
        )
        if not stable_id:
            raise ValueError(
                f"baseline row {idx} is missing sanger_model_id/model_name"
            )
        expression = row.get("expression")
        if expression is None:
            raise ValueError(f"baseline row {idx} is missing baseline RNA expression")
        try:
            expression_values = tuple(
                _finite_float(value, "baseline RNA expression") for value in expression
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"baseline row {idx} has malformed baseline RNA expression"
            ) from exc
        existing_expression = baseline_by_id.get(stable_id)
        if existing_expression is not None:
            if expression_values != existing_expression:
                raise ValueError(
                    f"baseline row {idx} has non-identical baseline RNA expression "
                    f"for duplicate {stable_id}"
                )
            continue
        baseline_by_id[stable_id] = expression_values

    X: list[list[float]] = []
    target_response: list[list[float]] = []
    perturbations: list[str] = []
    controls: list[bool] = []
    covariates: list[dict[str, Any]] = []
    for idx, row in enumerate(response_rows):
        stable_id = _stable_cell_model_id(
            row.get("sanger_model_id") or row.get("model_name")
        )
        if not stable_id:
            raise ValueError(
                f"essentiality row {idx} is missing sanger_model_id/model_name"
            )
        if stable_id not in baseline_by_id:
            raise ValueError(
                f"essentiality row {idx} has no baseline RNA expression for {stable_id}"
            )
        if str(row.get("perturbation_type", "")).strip().lower() != "crisprko":
            raise ValueError(
                f"essentiality row {idx} must use perturbation_type=CRISPRko"
            )
        response_metric = str(row.get("response_metric", "")).strip().lower()
        if response_metric not in NON_EXPRESSION_SCORE_SEMANTICS:
            raise ValueError(
                f"essentiality row {idx} has unsupported response_metric: {response_metric!r}"
            )
        response_value = row.get("response_value")
        if response_value is None:
            raise ValueError(f"essentiality row {idx} is missing response_value")
        try:
            numeric_response = _finite_float(response_value, "response_value")
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"essentiality row {idx} has malformed response_value: {response_value!r}"
            ) from exc
        perturbation = str(row.get("perturbation_gene", row.get("perturbation", "")))
        X.append([float(value) for value in baseline_by_id[stable_id]])
        target_response.append([numeric_response])
        perturbations.append(perturbation)
        controls.append(_is_control_row(row))
        covariates.append(
            {
                "sanger_model_id": stable_id,
                "response_metric": response_metric,
                "readout_modality": row.get("readout_modality"),
            }
        )

    if not X:
        raise ValueError("essentiality loader requires at least one response row")
    if len(feature_names) != _n_features(X):
        raise ValueError("feature_names must match baseline RNA expression width")
    return BenchmarkBatch(
        X=X,
        perturbations=perturbations,
        controls=controls,
        obs_covariates=tuple(covariates),
        target_response=target_response,
        feature_names=tuple(feature_names),
    )


def load_chemcpa_drugseq_tiny(
    *,
    artifact_path: Path | str = Path(
        "artifacts/model_benchmarks/chemcpa_drugseq_tiny_20260622.json"
    ),
) -> BenchmarkDataset:
    """Load the tiny real molecular DRUG-seq chemCPA-ready export.

    The export is generated by ``tools/build_chemcpa_drugseq_tiny.py`` from the
    72-sample DRUG-seq GSE120222 expression triplet plus a frozen PubChem/RDKit
    compound-structure snapshot. It contains real expression rows, DMSO controls,
    compound identity/dose metadata, and precomputed Morgan fingerprints. No
    Lamin connection or RDKit import is needed at loader time.
    """

    artifact = _read_json_if_exists(Path(artifact_path))
    if not artifact:
        raise FileNotFoundError(
            f"chemCPA DRUG-seq artifact not found: {artifact_path}. "
            "Run `uv run --with rdkit python tools/build_chemcpa_drugseq_tiny.py`."
        )
    rows = list(artifact["rows"])
    X = [list(map(float, row["expression"])) for row in rows]
    feature_names = list(artifact["feature_names"])
    metadata = {
        "loader": "chemcpa_drugseq_tiny",
        "artifact_path": str(artifact_path),
        "source": artifact.get("source", {}),
        "selection": artifact.get("selection", {}),
        "compound_metadata": artifact.get("compound_metadata", {}),
        "fallback": None,
    }
    split_names = split_perturbation_identities(rows)
    return _build_dataset(
        obs_rows=rows,
        X=X,
        feature_names=feature_names,
        split_names=split_names,
        source="DRUG-seq/GSE120222-real-molecular-tiny",
        metadata=metadata,
    )


def load_scgen_viperturb_tiny(
    *,
    artifact_path: Path | str = Path(
        "artifacts/model_benchmarks/scgen_real_viperturb_tiny_20260622.json"
    ),
) -> BenchmarkDataset:
    """Load the tiny real VIPerturb scGEN-ready expression export.

    The export is generated by ``tools/build_scgen_viperturb_tiny.py`` from the
    reviewed ``pert-gym/model-ready/20260621`` VIPerturb member. It contains a
    bounded local AnnData path plus a JSON sidecar with real expression rows,
    ``condition``/``control_value`` semantics, controls, and at least three
    non-control perturbation identities. No Lamin connection is needed here.
    """

    artifact = _read_json_if_exists(Path(artifact_path))
    if not artifact:
        raise FileNotFoundError(
            f"scGEN VIPerturb artifact not found: {artifact_path}. "
            "Run `uv run python tools/build_scgen_viperturb_tiny.py`."
        )
    rows = list(artifact["rows"])
    X = [list(map(float, row["expression"])) for row in rows]
    feature_names = list(artifact["feature_names"])
    metadata = {
        "loader": "scgen_viperturb_tiny",
        "artifact_path": str(artifact_path),
        "adata_path": artifact.get("export", {}).get("adata_path"),
        "source": artifact.get("source", {}),
        "selection": artifact.get("selection", {}),
        "export": artifact.get("export", {}),
        "fallback": None,
    }
    split_names = split_perturbation_identities(rows)
    return _build_dataset(
        obs_rows=rows,
        X=X,
        feature_names=feature_names,
        split_names=split_names,
        source="VIPerturbSeq/vimentin_screen-real-scgen-tiny",
        metadata=metadata,
    )


def split_perturbation_identities(
    obs_rows: Sequence[Mapping[str, Any]],
    fractions: tuple[float, float, float] = (0.5, 0.25, 0.25),
) -> Mapping[str, set[str]]:
    """Assign non-control perturbation identities to train/val/test splits."""

    if len(fractions) != 3 or not 0.99 <= sum(fractions) <= 1.01:
        raise ValueError("fractions must contain train/val/test values summing to 1.")

    perturbations = sorted(
        {
            str(row["perturbation"])
            for row in obs_rows
            if not _as_bool(row.get("is_control"))
        }
    )
    if len(perturbations) < 3:
        raise ValueError("Need at least three non-control perturbation identities.")

    n_train = max(1, round(len(perturbations) * fractions[0]))
    n_val = max(1, round(len(perturbations) * fractions[1]))
    if n_train + n_val >= len(perturbations):
        n_train = max(1, len(perturbations) - 2)
        n_val = 1
    return {
        "train": set(perturbations[:n_train]),
        "val": set(perturbations[n_train : n_train + n_val]),
        "test": set(perturbations[n_train + n_val :]),
    }


def write_benchmark_artifacts(
    dataset: BenchmarkDataset,
    *,
    artifact_dir: Path | str = Path("artifacts/model_benchmarks"),
) -> Path:
    """Write a compact benchmark-loader smoke summary JSON and return its path."""

    artifact_dir = Path(artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    out_path = artifact_dir / "loader_smoke_summary.json"
    payload = {
        "source": dataset.source,
        "split_by": dataset.split_by,
        "metadata": dict(dataset.metadata),
        "splits": {
            "train": _batch_summary(dataset.train),
            "val": _batch_summary(dataset.val),
            "test": _batch_summary(dataset.test),
        },
    }
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return out_path


def _build_dataset(
    *,
    obs_rows: Sequence[Mapping[str, Any]],
    X: Matrix,
    feature_names: Sequence[str],
    split_names: Mapping[str, set[str]],
    source: str,
    metadata: Mapping[str, Any],
) -> BenchmarkDataset:
    _validate_compound_fingerprints(obs_rows)
    controls_idx = [idx for idx, row in enumerate(obs_rows) if _is_control_row(row)]
    if not controls_idx:
        raise ValueError("Benchmark loader requires at least one control row.")

    batches = {}
    for split, perturbation_names in split_names.items():
        split_idx = [
            idx
            for idx, row in enumerate(obs_rows)
            if _is_control_row(row) or str(row["perturbation"]) in perturbation_names
        ]
        batches[split] = _subset_batch(obs_rows, X, split_idx, feature_names)

    return BenchmarkDataset(
        train=batches["train"],
        val=batches["val"],
        test=batches["test"],
        split_by="perturbation_identity",
        source=source,
        metadata=metadata,
    )


def _subset_batch(
    obs_rows: Sequence[Mapping[str, Any]],
    X: Matrix,
    indices: Sequence[int],
    feature_names: Sequence[str],
) -> BenchmarkBatch:
    matrix = [[float(value) for value in X[idx]] for idx in indices]
    perturbations = [str(obs_rows[idx]["perturbation"]) for idx in indices]
    controls = [_is_control_row(obs_rows[idx]) for idx in indices]
    covariates = [
        {
            key: obs_rows[idx].get(key)
            for key in DEFAULT_CONTEXT_FIELDS
            if key in obs_rows[idx]
        }
        for idx in indices
    ]
    compound_features = None
    if any("compound_fingerprint" in obs_rows[idx] for idx in indices):
        compound_features = [
            [float(value) for value in obs_rows[idx].get("compound_fingerprint", [])]
            for idx in indices
        ]
    return BenchmarkBatch(
        X=matrix,
        perturbations=perturbations,
        controls=controls,
        obs_covariates=covariates,
        target_response=matrix,
        feature_names=tuple(feature_names),
        compound_features=compound_features,
        compound_feature_names=(
            tuple(f"morgan_{idx}" for idx in range(len(compound_features[0])))
            if compound_features
            else ()
        ),
    )


def _validate_compound_fingerprints(obs_rows: Sequence[Mapping[str, Any]]) -> None:
    if not any("compound_fingerprint" in row for row in obs_rows):
        return

    width: int | None = None
    for idx, row in enumerate(obs_rows):
        fingerprint = row.get("compound_fingerprint")
        if fingerprint is None:
            raise ValueError(
                "compound_fingerprint must be present for every row when provided"
            )
        try:
            row_width = len(fingerprint)
        except TypeError as exc:
            raise ValueError(
                f"compound_fingerprint row {idx} must be a sequence"
            ) from exc
        if width is None:
            width = row_width
        elif row_width != width:
            raise ValueError("compound_fingerprint rows must have consistent width")


def _synthetic_rows() -> tuple[list[dict[str, Any]], list[list[float]], list[str]]:
    feature_names = ["gene_a", "gene_b", "gene_c"]
    obs_rows: list[dict[str, Any]] = []
    X: list[list[float]] = []
    rows = [
        ("control", True, [1.0, 1.0, 1.0]),
        ("control", True, [1.1, 0.9, 1.0]),
        ("pert_a", False, [2.0, 1.0, 1.0]),
        ("pert_a", False, [2.1, 1.1, 1.0]),
        ("pert_b", False, [1.0, 2.0, 1.0]),
        ("pert_b", False, [1.1, 2.1, 0.9]),
        ("pert_c", False, [1.0, 1.0, 2.0]),
        ("pert_c", False, [0.9, 1.1, 2.1]),
        ("pert_d", False, [2.0, 2.0, 1.0]),
        ("pert_d", False, [2.1, 1.9, 1.1]),
        ("pert_e", False, [1.0, 2.0, 2.0]),
        ("pert_e", False, [1.1, 2.1, 1.9]),
    ]
    for idx, (perturbation, is_control, row) in enumerate(rows):
        obs_rows.append(
            {
                "cell_id": f"cell_{idx:03d}",
                "perturbation": perturbation,
                "perturbation_type": "synthetic" if not is_control else "control",
                "is_control": is_control,
                "cell_line": "synthetic_line",
                "assay": "synthetic_scRNA_smoke",
            }
        )
        X.append(row)
    return obs_rows, X, feature_names


def _validate_source_rows(obs_rows: Sequence[Mapping[str, Any]], X: Matrix) -> None:
    if len(obs_rows) != len(X):
        raise ValueError("obs_rows and X must have matching row counts.")
    for idx, row in enumerate(obs_rows):
        if "perturbation" not in row:
            raise ValueError(f"obs row {idx} is missing perturbation.")
        if "is_control" not in row:
            raise ValueError(f"obs row {idx} is missing is_control.")
    _n_features(X)


def _validate_split_integrity(*batches: BenchmarkBatch) -> None:
    non_control_sets = []
    for batch in batches:
        non_control_sets.append(
            {
                perturbation
                for perturbation, is_control in zip(
                    batch.perturbations, batch.controls or []
                )
                if not is_control
            }
        )
    for left_idx, left in enumerate(non_control_sets):
        for right in non_control_sets[left_idx + 1 :]:
            overlap = left & right
            if overlap:
                raise ValueError(
                    "Non-control perturbation identities must not overlap across splits: "
                    + ", ".join(sorted(overlap))
                )


def _is_control_row(row: Mapping[str, Any]) -> bool:
    explicit = row.get("is_control")
    if explicit is not None:
        return _as_bool(explicit)
    return str(row.get("perturbation", "")).lower() in CONTROL_PERTURBATIONS


def _finite_float(value: Any, field_name: str) -> float:
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field_name} must be finite")
    return numeric


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def _dataset_prefix_from_obs_key(key: str) -> str:
    suffix = "/obs.parquet"
    if key.endswith(suffix):
        return key[: -len(suffix)]
    return key.split("/", 1)[0]


def _stable_depmap_id(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return text.split("::", 1)[0]


def _stable_cell_model_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _n_features(X: Matrix) -> int:
    if not X:
        return 0
    n_features = len(X[0])
    for row in X:
        if len(row) != n_features:
            raise ValueError("All rows in X must have the same number of features.")
    return n_features


def _read_json_if_exists(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _read_manifest_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"model_ready_v2 manifest not found: {path}")
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text())
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, Mapping):
            rows = payload.get("rows", [])
        else:
            raise ValueError("model_ready_v2 JSON manifest must be an array or object")
        if not isinstance(rows, list) or not all(
            isinstance(row, Mapping) for row in rows
        ):
            raise ValueError("model_ready_v2 manifest rows must be JSON objects")
        return [dict(row) for row in rows]
    delimiter = "\t" if path.suffix.lower() in {".tsv", ".tab"} else ","
    with path.open(newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter=delimiter)]


def _row_id(row: Mapping[str, Any], idx: int) -> str:
    return str(row.get("manifest_row_id") or row.get("sample_id") or f"row_{idx:05d}")


def _context_from_manifest_row(row: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "context_id",
        "cell_line",
        "depmap_id",
        "sanger_model_id",
        "cell_type",
        "cell_state",
        "tissue",
        "disease",
        "donor_id",
        "organism",
        "batch",
        "dose",
        "dose_unit",
        "timepoint",
        "split",
        "guide_id",
    )
    return {
        field: row.get(field) for field in fields if row.get(field) not in {None, ""}
    }


def _context_value(context: Mapping[str, Any], *fields: str) -> str | None:
    for field in fields:
        value = context.get(field)
        if value not in {None, ""}:
            return str(value)
    return None


def _split_from_context(context: Mapping[str, Any]) -> str:
    return _context_value(context, "split") or "not_split"


def _is_direct_response_row(row: Mapping[str, Any]) -> bool:
    source = str(row.get("source") or row.get("source_family") or "").strip().lower()
    target_kind = str(row.get("target_kind", "")).strip().lower()
    target_classification = str(row.get("target_classification", "")).strip().lower()
    return (
        _as_bool(row.get("has_response_label"))
        or target_kind in {"viability", "survival", "dependency", "essentiality"}
        or target_classification == "direct"
        or source in DIRECT_RESPONSE_SOURCES
    )


def _is_image_payload_row(row: Mapping[str, Any]) -> bool:
    modality = str(row.get("modality", "")).strip().lower()
    source = str(row.get("source") or row.get("source_family") or "").lower()
    return (
        _as_bool(row.get("has_image_payload"))
        or modality in IMAGE_MODALITIES
        or "rxrx" in source
        or "jump" in source
    )


def _response_sample_from_manifest_row(
    row: Mapping[str, Any], row_id: str
) -> ResponseTableSample:
    if _as_bool(row.get("has_expression_X")):
        raise ValueError("response-table rows must not rely on fake expression X")
    metric = str(row.get("response_metric", "")).strip()
    value = row.get("response_value")
    direction = str(row.get("response_direction", "")).strip()
    if not metric:
        metric = str(
            row.get("target_kind") or row.get("target_task") or "response_unavailable"
        )
    numeric_value = None
    if value not in {None, ""}:
        numeric_value = _finite_float(value, "response_value")
    if not direction:
        direction = "unavailable"
    return ResponseTableSample(
        sample_id=str(row.get("sample_id") or row.get("obs_id") or row_id),
        source=str(row.get("source") or row.get("source_family") or ""),
        perturbation=str(
            row.get("perturbation") or row.get("perturbation_target") or ""
        ),
        context=_context_from_manifest_row(row),
        response_metric=metric,
        response_value=numeric_value,
        response_direction=direction,
        response_unit=str(row.get("response_unit") or "") or None,
        response_source=str(row.get("response_source") or "") or None,
    )


def _expression_sample_from_manifest_row(
    row: Mapping[str, Any], row_id: str
) -> ExpressionLikeSample:
    artifact_key = str(row.get("artifact_key") or "").strip()
    if not artifact_key:
        raise ValueError("expression-like rows require a non-empty artifact_key")
    role = str(row.get("target_task") or "representation_pretraining").strip()
    if str(row.get("has_response_label", "")).strip().lower() == "true":
        role = "expression_response"
    return ExpressionLikeSample(
        sample_id=str(row.get("sample_id") or row.get("obs_id") or row_id),
        source=str(row.get("source") or row.get("source_family") or ""),
        artifact_key=artifact_key,
        perturbation=str(row.get("perturbation") or ""),
        context=_context_from_manifest_row(row),
        modality=str(row.get("modality") or ""),
        assay=str(row.get("assay") or ""),
        x_semantics=str(row.get("x_semantics") or ""),
        role=role,
        target_task=str(row.get("target_task") or "") or None,
    )


def _image_sample_from_manifest_row(
    row: Mapping[str, Any], row_id: str
) -> ImagePayloadSample:
    keys = _parse_payload_keys(
        row.get("payload_artifact_keys") or row.get("artifact_key")
    )
    if not keys:
        raise ValueError("image rows require at least one payload artifact handle")
    return ImagePayloadSample(
        sample_id=str(row.get("sample_id") or row.get("obs_id") or row_id),
        source=str(row.get("source") or row.get("source_family") or ""),
        payload_artifact_keys=tuple(keys),
        perturbation=str(row.get("perturbation") or ""),
        context=_context_from_manifest_row(row),
        modality=str(row.get("modality") or "image"),
        assay=str(row.get("assay") or ""),
    )


def _mapping_join_from_manifest_row(
    row: Mapping[str, Any], row_id: str
) -> MappingSidecarJoin:
    artifact_key = str(row.get("artifact_key") or "").strip()
    if not artifact_key:
        raise ValueError("mapping rows require a non-empty artifact_key")
    join_fields = _mapping_join_fields(row)
    if not join_fields:
        raise ValueError("mapping rows require at least one explicit join field")
    return MappingSidecarJoin(
        mapping_id=row_id,
        source=str(row.get("source") or row.get("source_family") or ""),
        artifact_key=artifact_key,
        mapping_role=str(
            row.get("artifact_role") or row.get("target_kind") or "mapping"
        ),
        join_fields=join_fields,
        notes=str(row.get("notes") or ""),
    )


def _mapping_join_fields(row: Mapping[str, Any]) -> tuple[str, ...]:
    candidates = (
        "guide_id",
        "guide_sequence",
        "perturbation_target",
        "perturbation_target_id",
        "sample_id",
        "split_policy_id",
        "context_id",
    )
    return tuple(field for field in candidates if row.get(field) not in {None, ""})


def _parse_payload_keys(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple)):
        if not value or any(
            not isinstance(item, str) or not item.strip() for item in value
        ):
            raise ValueError(
                "payload_artifact_keys arrays require non-empty string handles"
            )
        return [item.strip() for item in value]
    if not isinstance(value, str):
        raise ValueError("payload_artifact_keys must be a string or string array")
    text = value.strip()
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("payload_artifact_keys contains malformed JSON") from exc
        if not isinstance(parsed, list):
            raise ValueError("payload_artifact_keys JSON must be an array")
        return _parse_payload_keys(parsed)
    return [part.strip() for part in text.split(";") if part.strip()]


def _batch_summary(batch: BenchmarkBatch) -> dict[str, Any]:
    controls = list(batch.controls or [])
    return {
        "n_obs": len(batch.X),
        "n_features": _n_features(batch.X),
        "n_compound_features": _n_features(batch.compound_features or []),
        "n_controls": sum(1 for is_control in controls if is_control),
        "non_control_perturbations": sorted(
            {
                perturbation
                for perturbation, is_control in zip(batch.perturbations, controls)
                if not is_control
            }
        ),
        "covariate_fields": sorted(
            {field for covariates in batch.obs_covariates for field in covariates}
        ),
    }
