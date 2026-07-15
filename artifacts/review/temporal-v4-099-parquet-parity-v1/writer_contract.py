"""Strict, versioned configuration binding for the CELLxGENE logical writer."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

CONFIG_VERSION = "cellxgene-logical-component-config/v1"
AUTHORIZATION_VERSION = "cellxgene-logical-component-authorization/v1"
PROTOCOL = "cellxgene-category-safe-logical-sparse-zarr/v1"
MAPPER_VERSION = "declarative-obs-mapper/v1"

_CONFIG_KEYS = {
    "config_version",
    "protocol",
    "task_id",
    "dataset_config_status",
    "source",
    "source_head",
    "api_identity",
    "shape",
    "logical_key",
    "obs",
    "ordered_var",
    "accepted_components",
    "revision",
    "execution",
    "storage",
    "forbidden_actions",
}
_AUTHORIZATION_KEYS = {
    "authorization_version",
    "protocol",
    "config_sha256",
    "writer_sha256",
    "writer_contract_sha256",
    "parquet_frame_parity_sha256",
    "parent_task_id",
    "parent_task_status",
    "approved_parent_protocol",
    "correction_task_id",
    "review_scope",
    "execution_authorized",
}
_SOURCE_KEYS = {
    "url",
    "api_url",
    "collection_id",
    "collection_version_id",
    "dataset_id",
    "dataset_version_id",
    "asset_id",
}
_HEAD_KEYS = {"status", "final_url", "content_length", "etag", "last_modified", "version_id"}
_API_KEYS = {"public", "tombstone", "is_primary_data", "organism", "assays"}
_OBS_KEYS = {"mapper_version", "required_non_null", "predicates", "assignments", "semantic_evidence"}
_ORDERED_VAR_KEYS = {
    "identity_sha256",
    "organism_ontology_id",
    "canonical_feature_namespace",
    "normalization_version",
    "feature_reference_column",
}
_LEDGER_KEYS = {"current", "denominator", "credit"}
_REVISION_KEYS = {"prefix", "failed_candidate_denylist", "fresh_immutable_required"}
_EXECUTION_KEYS = {
    "host",
    "zone",
    "billing_project",
    "lamin_instance",
    "lamin_branch",
    "single_writer_lease",
    "timeout_seconds",
    "max_rss_bytes",
    "min_available_bytes",
    "heartbeat_interval_seconds",
    "heartbeat_metric",
    "heartbeat_denominator",
    "max_internal_block_rows",
    "output_directory",
}
_STORAGE_KEYS = {"gcs_root", "manifest_last", "shared_var_count", "per_block_var_count", "x_logical_object_count"}
_REQUIRED_FORBIDDEN = {
    "cleanup",
    "deletion",
    "promotion",
    "collection_mutation",
    "lamin_main",
    "vm_lifecycle_change",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def _exact_keys(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    actual = set(value)
    if actual != expected:
        raise ValueError(f"{label} keys mismatch: missing={sorted(expected - actual)}, unknown={sorted(actual - expected)}")
    return value


def _nonempty(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _positive_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _validate_config(config: dict[str, Any]) -> None:
    _exact_keys(config, _CONFIG_KEYS, "config")
    if config["config_version"] != CONFIG_VERSION or config["protocol"] != PROTOCOL:
        raise ValueError("unsupported config version or protocol")
    _nonempty(config["task_id"], "task_id")
    if config["dataset_config_status"] not in {"reviewed-executable", "prewrite-fixture"}:
        raise ValueError("invalid dataset_config_status")

    source = _exact_keys(config["source"], _SOURCE_KEYS, "source")
    if not str(source["url"]).startswith("https://datasets.cellxgene.cziscience.com/"):
        raise ValueError("source.url is not an exact CELLxGENE HTTPS URL")
    if not str(source["api_url"]).startswith("https://api.cellxgene.cziscience.com/curation/v1/collections/"):
        raise ValueError("source.api_url is not an exact CELLxGENE collection API URL")
    for key in _SOURCE_KEYS - {"url", "api_url"}:
        if _UUID.fullmatch(str(source[key])) is None:
            raise ValueError(f"source.{key} must be a UUID")
    if not str(source["url"]).endswith(f"/{source['asset_id']}.h5ad"):
        raise ValueError("source URL is not bound to asset_id")
    if not str(source["api_url"]).endswith(f"/{source['collection_id']}"):
        raise ValueError("source API URL is not bound to collection_id")

    head = _exact_keys(config["source_head"], _HEAD_KEYS, "source_head")
    if head["status"] != 200 or head["final_url"] != source["url"]:
        raise ValueError("source_head status/final_url conflicts with source")
    _positive_int(head["content_length"], "source_head.content_length")
    for key in ("etag", "last_modified"):
        _nonempty(head[key], f"source_head.{key}")
    if head["version_id"] is not None:
        _nonempty(head["version_id"], "source_head.version_id")

    api = _exact_keys(config["api_identity"], _API_KEYS, "api_identity")
    if api["public"] is not True or api["tombstone"] is not False or api["is_primary_data"] is not True:
        raise ValueError("API visibility/tombstone/primary-data identity must fail closed")
    organism = _exact_keys(api["organism"], {"label", "ontology_term_id"}, "api_identity.organism")
    if organism != {"label": "Homo sapiens", "ontology_term_id": "NCBITaxon:9606"}:
        raise ValueError("only exact Homo sapiens identity is approved")
    if not isinstance(api["assays"], list) or not api["assays"]:
        raise ValueError("api_identity.assays must be non-empty")
    for index, assay in enumerate(api["assays"]):
        _exact_keys(assay, {"label", "ontology_term_id"}, f"api_identity.assays[{index}]")
        _nonempty(assay["label"], "assay label")
        if config["dataset_config_status"] == "reviewed-executable":
            _nonempty(assay["ontology_term_id"], "assay ontology_term_id")
        elif assay["ontology_term_id"] is not None:
            _nonempty(assay["ontology_term_id"], "assay ontology_term_id")

    shape = config["shape"]
    if not isinstance(shape, list) or len(shape) != 2:
        raise ValueError("shape must contain [n_obs, n_vars]")
    _positive_int(shape[0], "shape[0]")
    _positive_int(shape[1], "shape[1]")
    if not str(config["logical_key"]).startswith("pert-gym/logical/temporal/"):
        raise ValueError("logical_key must be an explicit temporal logical key")

    obs = _exact_keys(config["obs"], _OBS_KEYS, "obs")
    if obs["mapper_version"] != MAPPER_VERSION:
        raise ValueError("unsupported OBS mapper_version")
    if not isinstance(obs["required_non_null"], list) or not obs["required_non_null"]:
        raise ValueError("obs.required_non_null must be non-empty")
    if len(set(obs["required_non_null"])) != len(obs["required_non_null"]):
        raise ValueError("obs.required_non_null contains duplicates")
    if not isinstance(obs["predicates"], list) or not obs["predicates"]:
        raise ValueError("obs.predicates must be non-empty")
    for predicate in obs["predicates"]:
        if set(predicate) not in ({"column", "op", "value"}, {"column", "op", "values"}):
            raise ValueError("OBS predicate has unknown or omitted fields")
        if predicate["op"] not in {"all_contains", "all_equals", "domain_equals"}:
            raise ValueError("unknown OBS predicate operation")
    if not isinstance(obs["assignments"], list) or not obs["assignments"]:
        raise ValueError("obs.assignments must be non-empty")
    targets: set[str] = set()
    for assignment in obs["assignments"]:
        if assignment.get("op") in {"literal", "nullable_float"}:
            expected = {"target", "op", "value"}
            if assignment.get("op") == "nullable_float":
                expected = {"target", "op"}
        elif assignment.get("op") == "index":
            expected = {"target", "op"}
        elif assignment.get("op") == "copy":
            expected = {"target", "op", "source"}
        elif assignment.get("op") == "concat":
            expected = {"target", "op", "sources", "separator"}
        else:
            raise ValueError("unknown OBS assignment operation")
        if set(assignment) != expected:
            raise ValueError("OBS assignment has unknown or omitted fields")
        target = _nonempty(assignment["target"], "OBS assignment target")
        if target in targets:
            raise ValueError("duplicate OBS assignment target")
        targets.add(target)
    _exact_keys(obs["semantic_evidence"], {"verdict", "basis"}, "obs.semantic_evidence")
    expected_verdict = (
        "accepted"
        if config["dataset_config_status"] == "reviewed-executable"
        else "pending-prewrite"
    )
    if obs["semantic_evidence"]["verdict"] != expected_verdict:
        raise ValueError("OBS semantic evidence verdict conflicts with config status")
    _nonempty(obs["semantic_evidence"]["basis"], "obs.semantic_evidence.basis")

    ordered_var = _exact_keys(config["ordered_var"], _ORDERED_VAR_KEYS, "ordered_var")
    if config["dataset_config_status"] == "reviewed-executable":
        _sha(ordered_var["identity_sha256"], "ordered_var.identity_sha256")
    elif ordered_var["identity_sha256"] is not None:
        _sha(ordered_var["identity_sha256"], "ordered_var.identity_sha256")
    if ordered_var["organism_ontology_id"] != "NCBITaxon:9606" or ordered_var["canonical_feature_namespace"] != "Ensembl Gene ID":
        raise ValueError("ordered-var organism or namespace is not approved")
    if ordered_var["normalization_version"] != "source-string/v1":
        raise ValueError("ordered-var normalization version is not approved")
    _nonempty(ordered_var["feature_reference_column"], "ordered_var.feature_reference_column")

    ledger = _exact_keys(config["accepted_components"], _LEDGER_KEYS, "accepted_components")
    current = _positive_int(ledger["current"], "accepted_components.current")
    denominator = _positive_int(ledger["denominator"], "accepted_components.denominator")
    if current >= denominator or ledger["credit"] != 0:
        raise ValueError("accepted-components bounds/credit are invalid")

    revision = _exact_keys(config["revision"], _REVISION_KEYS, "revision")
    if re.fullmatch(r"temporal-v4-[0-9]{3}", str(revision["prefix"])) is None:
        raise ValueError("revision prefix must be explicit temporal-v4 row identity")
    if not isinstance(revision["failed_candidate_denylist"], list) or not all(isinstance(value, str) and value for value in revision["failed_candidate_denylist"]):
        raise ValueError("revision denylist must contain explicit strings")
    if revision["fresh_immutable_required"] is not True:
        raise ValueError("fresh immutable revision is required")

    execution = _exact_keys(config["execution"], _EXECUTION_KEYS, "execution")
    fixed = {
        "host": "pert-gym-worker-eu",
        "zone": "europe-west1-b",
        "billing_project": "jkobject-1549353370965",
        "lamin_instance": "laminlabs/pertdata",
        "lamin_branch": "jkobject",
        "single_writer_lease": "global-plus-legacy-exclusive",
        "timeout_seconds": 7200,
        "max_rss_bytes": 24 * 1024**3,
        "min_available_bytes": 4 * 1024**3,
        "heartbeat_interval_seconds": 50,
        "heartbeat_metric": "accepted_components",
        "heartbeat_denominator": denominator,
        "max_internal_block_rows": 4096,
    }
    for key, expected in fixed.items():
        if execution[key] != expected:
            raise ValueError(f"execution.{key} conflicts with approved bound {expected!r}")
    _nonempty(execution["output_directory"], "execution.output_directory")

    storage = _exact_keys(config["storage"], _STORAGE_KEYS, "storage")
    expected_storage = {
        "gcs_root": "scperturb/pert-gym/staging",
        "manifest_last": True,
        "shared_var_count": 1,
        "per_block_var_count": 0,
        "x_logical_object_count": 1,
    }
    if storage != expected_storage:
        raise ValueError("storage contract conflicts with approved append-only shape")
    if not isinstance(config["forbidden_actions"], list) or set(config["forbidden_actions"]) != _REQUIRED_FORBIDDEN:
        raise ValueError("forbidden_actions must match the exact fail-closed denyset")


def validate_bound_contract(
    config: dict[str, Any],
    authorization: dict[str, Any],
    *,
    config_sha256: str,
    writer_sha256: str,
    helper_sha256: str,
    contract_sha256: str,
) -> SimpleNamespace:
    """Validate all data and hash bindings without performing any I/O."""
    _validate_config(config)
    _exact_keys(authorization, _AUTHORIZATION_KEYS, "authorization")
    if authorization["authorization_version"] != AUTHORIZATION_VERSION:
        raise ValueError("unsupported authorization version")
    if authorization["protocol"] != config["protocol"]:
        raise RuntimeError("authorization protocol is not bound to config protocol")
    if authorization["config_sha256"] != config_sha256:
        raise RuntimeError("authorization config SHA-256 does not match exact config")
    if authorization["writer_sha256"] != writer_sha256:
        raise RuntimeError("authorization writer SHA-256 does not match exact script")
    if authorization["writer_contract_sha256"] != contract_sha256:
        raise RuntimeError("authorization contract SHA-256 does not match exact helper")
    if authorization["parquet_frame_parity_sha256"] != helper_sha256:
        raise RuntimeError("authorization helper SHA-256 does not match exact helper")
    if authorization["parent_task_status"] != "completed":
        raise RuntimeError("authorization parent task is not completed")
    for key in ("parent_task_id", "approved_parent_protocol", "correction_task_id", "review_scope"):
        _nonempty(authorization[key], f"authorization.{key}")
    if authorization["review_scope"] != "exact-head-independent-review-before-any-execution":
        raise RuntimeError("authorization review scope is not exact-head independent review")
    if not isinstance(authorization["execution_authorized"], bool):
        raise ValueError("authorization.execution_authorized must be boolean")
    return SimpleNamespace(config=config, authorization=authorization)


def load_bound_contract(
    config_path: Path,
    authorization_path: Path,
    *,
    writer_path: Path,
    helper_path: Path,
    require_execution: bool,
) -> SimpleNamespace:
    """Load exact bytes and validate config, executable, and helper bindings."""
    config_bytes = config_path.read_bytes()
    authorization_bytes = authorization_path.read_bytes()
    config = json.loads(config_bytes)
    authorization = json.loads(authorization_bytes)
    result = validate_bound_contract(
        config,
        authorization,
        config_sha256=hashlib.sha256(config_bytes).hexdigest(),
        writer_sha256=hashlib.sha256(writer_path.read_bytes()).hexdigest(),
        helper_sha256=hashlib.sha256(helper_path.read_bytes()).hexdigest(),
        contract_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    )
    if require_execution:
        require_execution_authorized(result)
    return result


def require_execution_authorized(contract: SimpleNamespace) -> None:
    if contract.config["dataset_config_status"] != "reviewed-executable" or contract.authorization["execution_authorized"] is not True:
        raise RuntimeError("contract is not execution-authorized")


def preflight_plan(contract: SimpleNamespace) -> dict[str, Any]:
    config = contract.config
    return {
        "source_url": config["source"]["url"],
        "shape": config["shape"],
        "logical_key": config["logical_key"],
        "accepted_components": {
            "current": config["accepted_components"]["current"],
            "denominator": config["accepted_components"]["denominator"],
        },
        "execution_authorized": contract.authorization["execution_authorized"],
    }
