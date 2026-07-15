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
    "authorization_binding",
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
_AUTHORIZATION_BINDING_KEYS = {
    "parent_task_id",
    "approved_parent_protocol",
    "correction_task_id",
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
_APPROVED_IDENTITY_SHA256_BY_REVISION = {
    "temporal-v4-007": {
        "url": "b1234ed992555b240aa5c93e16f04ad1be9c5c7b2e33bae856f6f00dd85fedd8",
        "api_url": "befe8afa49acae4a74b9733e55050811bbd20ab5613b11cb45c1d6da053f2e0e",
        "collection_id": "3aa5229c091757848a8e61bd4f5784da22eabc6ee7b1297b671b540755dfada1",
        "collection_version_id": "f9890243457a0e02c83d696242107af6a4397558376385fce4d720f871a1a510",
        "dataset_id": "bb65314f9ff7939591aa97314bb78ffeb1cd05cf7f965a5d0496df1cd698e126",
        "dataset_version_id": "901e8ab3f0dc6bf6a2aa99357e963644769fb28d863cac7037b9337bdc616d13",
        "asset_id": "901e8ab3f0dc6bf6a2aa99357e963644769fb28d863cac7037b9337bdc616d13",
        "parent_task_id": "8569b471c31d830ff9c67eee4af41d4b37ef05041460aab85aee72ee0d0bcb66",
        "catalogue_record": "1208e25adf57a01d56c0b00a45b3abf90fc5820c0aac27d3401fb853448800c4",
        "logical_key": "65b7a9faa3c057652fe3a088fe2a334764b19c9de1e3d2a88362c58f24e5c938",
        "task_id": "afe61339315b188e3cb52cf2f604461e98b0d7e1cd4a9408146e901e3518c5a4",
        "dataset_config_status": "37b3e8e87620d7677456ce9c6e141224d53138d0434eae66ccd2d3af8c61a80e",
        "source_head": "7feae14a428745e1a8f6a8c2cdc1e395e1d9d1dcfad1f01cd732e31b2678d400",
        "shape": "0d46c9d78835827da45529f2a0b7dec0e1b4b61521aae419e454830295269714",
        "organism": "4ac6e019aa2207b1bbe041031c02191143ed61de9f357eb790708ded9de52145",
        "assays": "8051c05ee8991d6476c5b9526dac01661cd5d6599d4eef55cbc191023696638a",
        "ordered_var": "6e24c720e545edd8d21246fb3976fb89b97e11af64be3bc76600899faa3d7de8",
        "obs": "9b51b2a7a19d58a3d818ff72d4c545c16bbdb86db3ccec1ab09bf242b80359d9",
        "accepted_components": "983b1464cf3972ffffdd4d9c92a0f7c5b4faaa6b0dc03ce08751489ebba9a40b",
        "execution": "047a2c6b9dedd3c4aff5c738cf3e2a9fb249e4f9c0fab3c066a8de5a4c7f9d4a",
        "storage": "7c870891e7c79024b91c2b590dd2638a13884d5a06ee7e79e50a17c65507565d",
        "forbidden_actions": "fb95b232b7d39710c7fc7d03f921b922190986e5a8149d7b426030455ecca847",
    },
    "temporal-v4-099": {
        "url": "149c627d7823db36ab6c432bf1faccb1a98883e90232ae1f4ca0fd9440477efd",
        "api_url": "479a202a2c70d316e47473d3a3566c24ec35b49686f59639837ab9f0be186e07",
        "collection_id": "e61b846f0af210f6c779684d72d8d6fae3e53996fb39b68bda6cfe84d6047fc6",
        "collection_version_id": "bcf4139c398073252873b10388ca765d1e918386690d1dffc410f787f8b734f9",
        "dataset_id": "9ec066750e10998bc049c79a96373a3f012484331e3b7cb522f8b20abb711b16",
        "dataset_version_id": "0d3439c839c4204f6c6770e6484740bf0ed38212dc5a3bec6805533dc80a13a3",
        "asset_id": "0d3439c839c4204f6c6770e6484740bf0ed38212dc5a3bec6805533dc80a13a3",
        "parent_task_id": "cd25835ac1c731f3d0c94f2dcaebeb1921a37286033e159211aff8403da2083a",
    },
    "temporal-v4-013": {
        "url": "2f0d259aecfd4bfeec86673f1a39b3fb6769f9284b665cb4445e3deb15d528d3",
        "api_url": "826846acf2f2ae1cdba6d81c5e6f5738c2bc3b44d387474efec7fe592ac88662",
        "collection_id": "22d9cf6e60a760b2245b0e99f2ec2c6b05e436f0538aec12ec4f26132419c02d",
        "collection_version_id": "9d64257e5a04477f8bc032c90d99f147870774fcbcf573665e3af5b170cbb413",
        "dataset_id": "9e0bccc4c100a3deebc0b755ee1909433479ffe06ececf4586eef7b1aafdb5c3",
        "dataset_version_id": "f372b7b0be24c172c2db73306c6dd4d2ed97ce5eeea9617e8ad5c17ff4fea968",
        "asset_id": "f372b7b0be24c172c2db73306c6dd4d2ed97ce5eeea9617e8ad5c17ff4fea968",
        "parent_task_id": "90bfe728fa4ee245f47ef2af876c728f4f14a37d77d6fdf4e29e800c9111d74e",
    },
}


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


def _json_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _validate_config(config: dict[str, Any]) -> None:
    row_7 = (
        isinstance(config, dict)
        and isinstance(config.get("revision"), dict)
        and config["revision"].get("prefix") == "temporal-v4-007"
    )
    config_keys = _CONFIG_KEYS | ({"catalogue_record"} if row_7 else set())
    _exact_keys(config, config_keys, "config")
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
    if source["dataset_version_id"] != source["asset_id"]:
        raise ValueError("source dataset_version_id conflicts with asset_id")

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
        if predicate["op"] not in {"all_contains", "all_equals", "all_not_in", "domain_equals"}:
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
    dataset_assignments = [
        assignment
        for assignment in obs["assignments"]
        if assignment["target"] == "dataset"
    ]
    if dataset_assignments != [
        {"target": "dataset", "op": "literal", "value": config["logical_key"]}
    ]:
        raise ValueError("logical_key conflicts with OBS dataset assignment")
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
    runtime_ordered_var = (
        row_7
        and ordered_var["identity_sha256"]
        == "runtime-computed-before-candidate-write"
    )
    if config["dataset_config_status"] == "reviewed-executable":
        if not runtime_ordered_var:
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

    execution_keys = _EXECUTION_KEYS | ({"task_timeout_seconds"} if row_7 else set())
    execution = _exact_keys(config["execution"], execution_keys, "execution")
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
    if row_7 and execution["task_timeout_seconds"] != 10800:
        raise ValueError("execution.task_timeout_seconds conflicts with approved bound 10800")
    _nonempty(execution["output_directory"], "execution.output_directory")
    if Path(execution["output_directory"]).name != (
        f"{revision['prefix']}-{config['task_id']}"
    ):
        raise ValueError("revision prefix conflicts with output/task identity")

    approved_identity = _APPROVED_IDENTITY_SHA256_BY_REVISION.get(revision["prefix"])
    if approved_identity is None:
        raise ValueError("revision has no intrinsically approved identity binding")
    for key in sorted(_SOURCE_KEYS):
        observed_sha256 = hashlib.sha256(source[key].encode()).hexdigest()
        if observed_sha256 != approved_identity[key]:
            raise ValueError(f"source {key} conflicts with approved revision identity")

    scalar_identity = {
        "catalogue_record": config.get("catalogue_record"),
        "logical_key": config["logical_key"],
        "task_id": config["task_id"],
        "dataset_config_status": config["dataset_config_status"],
    }
    json_identity = {
        "source_head": config["source_head"],
        "shape": config["shape"],
        "organism": config["api_identity"]["organism"],
        "assays": config["api_identity"]["assays"],
        "ordered_var": config["ordered_var"],
        "obs": config["obs"],
        "accepted_components": config["accepted_components"],
        "execution": config["execution"],
        "storage": config["storage"],
        "forbidden_actions": config["forbidden_actions"],
    }
    for key, value in scalar_identity.items():
        if key in approved_identity:
            observed_sha256 = hashlib.sha256(str(value).encode()).hexdigest()
            if observed_sha256 != approved_identity[key]:
                raise ValueError(f"{key} conflicts with approved revision identity")
    for key, value in json_identity.items():
        if key in approved_identity and _json_sha256(value) != approved_identity[key]:
            raise ValueError(f"{key} conflicts with approved revision identity")

    authorization_binding = _exact_keys(
        config["authorization_binding"],
        _AUTHORIZATION_BINDING_KEYS,
        "authorization_binding",
    )
    for key in _AUTHORIZATION_BINDING_KEYS:
        _nonempty(authorization_binding[key], f"authorization_binding.{key}")
    parent_task_sha256 = hashlib.sha256(
        authorization_binding["parent_task_id"].encode()
    ).hexdigest()
    if parent_task_sha256 != approved_identity["parent_task_id"]:
        raise ValueError(
            "authorization parent task conflicts with approved revision identity"
        )
    if authorization_binding["correction_task_id"] != config["task_id"]:
        raise ValueError("authorization correction task conflicts with config task_id")
    if authorization_binding["approved_parent_protocol"] != (
        f"{revision['prefix']}-parquet-parity-parent/v1"
    ):
        raise ValueError("authorization parent protocol conflicts with revision identity")

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
    binding = config["authorization_binding"]
    if authorization["parent_task_id"] != binding["parent_task_id"]:
        raise RuntimeError("authorization parent task is not bound to config")
    if authorization["approved_parent_protocol"] != binding["approved_parent_protocol"]:
        raise RuntimeError("authorization parent protocol is not bound to config")
    if authorization["correction_task_id"] != binding["correction_task_id"]:
        raise RuntimeError("authorization correction task is not bound to config task")
    _nonempty(authorization["review_scope"], "authorization.review_scope")
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
