"""Validation for metadata-first processing-decisions notebook contracts.

The contract is deliberately local metadata: it records reproducibility evidence
without fetching Lamin artifacts or GCS payloads.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

REQUIRED_TOP_LEVEL_FIELDS = frozenset(
    {
        "identity",
        "delta_vs_main",
        "biological_context",
        "source_payload",
        "processing_decisions",
        "rejected_alternatives",
        "lineage",
        "validation",
        "collection_membership",
        "limitations_and_rollback",
        "temporary_gcs_dependencies",
        "reconstruction",
        "runtime",
    }
)

REQUIRED_IDENTITY_FIELDS = frozenset(
    {"dataset_id", "source", "version", "license", "checksums"}
)

REQUIRED_DELTA_FIELDS = frozenset(
    {
        "branch",
        "artifact_count",
        "logical_dataset_count",
        "collection_count",
        "added_artifacts",
        "added_collections",
    }
)

REQUIRED_PROCESSING_DECISION_FIELDS = frozenset(
    {
        "inclusion",
        "exclusion",
        "conversion",
        "transformations",
        "quality_control",
        "obs_schema",
        "perturbation_mapping",
        "control_mapping",
        "organism_and_gene_normalization",
        "x_semantics",
        "chunk_size_policy",
        "zarr_or_h5ad",
        "shared_var_identity",
        "auxiliary_modalities",
    }
)

ALLOWED_LIVE_LAMIN_HOSTS = frozenset({"pert-gym-worker-eu"})


def _missing_fields(value: Mapping[str, Any], required: frozenset[str]) -> list[str]:
    return sorted(required.difference(value))


def _nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _has_retained_lamin_raw_artifact(reconstruction: Mapping[str, Any]) -> bool:
    raw_artifact = reconstruction.get("retained_lamin_raw_artifact")
    if isinstance(raw_artifact, Mapping):
        return bool(raw_artifact.get("key") or raw_artifact.get("uid"))
    return _nonempty_string(raw_artifact)


def _has_immutable_upstream_source(reconstruction: Mapping[str, Any]) -> bool:
    sources = reconstruction.get("immutable_upstream_sources", [])
    return isinstance(sources, Sequence) and not isinstance(sources, str) and bool(sources)


def validate_processing_decisions_contract(contract: Mapping[str, Any]) -> list[str]:
    """Return human-readable contract violations without any remote access."""
    errors: list[str] = []
    missing_top_level = _missing_fields(contract, REQUIRED_TOP_LEVEL_FIELDS)
    if missing_top_level:
        errors.append(f"missing top-level fields: {', '.join(missing_top_level)}")

    identity = contract.get("identity", {})
    if not isinstance(identity, Mapping):
        errors.append("identity must be a mapping")
    else:
        missing_identity = _missing_fields(identity, REQUIRED_IDENTITY_FIELDS)
        if missing_identity:
            errors.append(f"missing identity fields: {', '.join(missing_identity)}")

    delta = contract.get("delta_vs_main", {})
    if not isinstance(delta, Mapping):
        errors.append("delta_vs_main must be a mapping")
    else:
        missing_delta = _missing_fields(delta, REQUIRED_DELTA_FIELDS)
        if missing_delta:
            errors.append(
                "delta_vs_main must keep artifact_count, logical_dataset_count, "
                f"and collection_count distinct; missing: {', '.join(missing_delta)}"
            )

    decisions = contract.get("processing_decisions", {})
    if not isinstance(decisions, Mapping):
        errors.append("processing_decisions must be a mapping")
    else:
        missing_decisions = _missing_fields(decisions, REQUIRED_PROCESSING_DECISION_FIELDS)
        if missing_decisions:
            errors.append(
                f"missing processing decision fields: {', '.join(missing_decisions)}"
            )

    dependencies = contract.get("temporary_gcs_dependencies", [])
    if not isinstance(dependencies, list):
        errors.append("temporary_gcs_dependencies must be a list, even when empty")
    else:
        for index, dependency in enumerate(dependencies):
            if not isinstance(dependency, Mapping):
                errors.append(f"temporary_gcs_dependencies[{index}] must be a mapping")
                continue
            missing_dependency = _missing_fields(
                dependency,
                frozenset(
                    {
                        "uri",
                        "purpose",
                        "durable_replacement",
                        "safe_to_remove_prerequisites",
                    }
                ),
            )
            if missing_dependency:
                errors.append(
                    f"temporary_gcs_dependencies[{index}] missing: "
                    f"{', '.join(missing_dependency)}"
                )

    reconstruction = contract.get("reconstruction", {})
    if not isinstance(reconstruction, Mapping):
        errors.append("reconstruction must be a mapping")
    else:
        claimed = reconstruction.get("reproducibility_claimed", False)
        if not isinstance(claimed, bool):
            errors.append("reconstruction.reproducibility_claimed must be boolean")
        elif claimed and not (
            _has_immutable_upstream_source(reconstruction)
            or _has_retained_lamin_raw_artifact(reconstruction)
        ):
            errors.append(
                "reproducibility cannot be claimed when only an unretained GCS object "
                "exists; record an immutable upstream source or retained Lamin raw artifact"
            )
        if "safe_to_remove_gcs" not in reconstruction:
            errors.append("reconstruction.safe_to_remove_gcs is required")

    runtime = contract.get("runtime", {})
    if not isinstance(runtime, Mapping):
        errors.append("runtime must be a mapping")
    else:
        if runtime.get("live_lamin_query_enabled", False):
            allowed_hosts = runtime.get("allowed_live_lamin_hosts")
            if allowed_hosts != sorted(ALLOWED_LIVE_LAMIN_HOSTS):
                errors.append(
                    "live Lamin queries must be hard-guarded to pert-gym-worker-eu"
                )
        elif "allowed_live_lamin_hosts" not in runtime:
            errors.append("runtime.allowed_live_lamin_hosts is required")

    return errors
