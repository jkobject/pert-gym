from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
REVIEW = ROOT / "artifacts/review/temporal-v4-099-parquet-parity-v1"
CONTRACT_PATH = REVIEW / "writer_contract.py"
ROW_99_CONFIG = REVIEW / "row-99-config.json"
ROW_99_AUTHORIZATION = REVIEW / "authorization.json"
ROW_13_CONFIG = REVIEW / "row-13-prewrite-fixture.json"
WRITER = REVIEW / "write_component.py"
HELPER = REVIEW / "parquet_frame_parity.py"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_contract_module():
    spec = importlib.util.spec_from_file_location("cellxgene_writer_contract", CONTRACT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_writer_module():
    sys.path.insert(0, str(REVIEW))
    try:
        spec = importlib.util.spec_from_file_location("cellxgene_writer", WRITER)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(REVIEW))


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def bound_authorization(config: dict[str, object]) -> dict[str, object]:
    authorization = load_json(ROW_99_AUTHORIZATION)
    authorization["config_sha256"] = hashlib.sha256(
        (json.dumps(config, indent=2, sort_keys=True) + "\n").encode()
    ).hexdigest()
    authorization["writer_sha256"] = sha256(WRITER)
    authorization["writer_contract_sha256"] = sha256(CONTRACT_PATH)
    authorization["parquet_frame_parity_sha256"] = sha256(HELPER)
    authorization["protocol"] = config["protocol"]
    return authorization


def validate(config: dict[str, object], authorization: dict[str, object]):
    contract = load_contract_module()
    return contract.validate_bound_contract(
        config,
        authorization,
        config_sha256=hashlib.sha256(
            (json.dumps(config, indent=2, sort_keys=True) + "\n").encode()
        ).hexdigest(),
        writer_sha256=sha256(WRITER),
        helper_sha256=sha256(HELPER),
        contract_sha256=sha256(CONTRACT_PATH),
    )


def test_row_99_reviewed_fixture_validates_and_is_exactly_hash_bound() -> None:
    contract = load_contract_module()
    config = load_json(ROW_99_CONFIG)
    authorization = load_json(ROW_99_AUTHORIZATION)

    validated = contract.load_bound_contract(
        ROW_99_CONFIG,
        ROW_99_AUTHORIZATION,
        writer_path=WRITER,
        helper_path=HELPER,
        require_execution=False,
    )

    assert validated.config["shape"] == [10224, 35552]
    assert validated.config["accepted_components"] == {
        "current": 2,
        "denominator": 153,
        "credit": 0,
    }
    assert authorization["config_sha256"] == sha256(ROW_99_CONFIG)
    assert authorization["writer_sha256"] == sha256(WRITER)
    assert authorization["writer_contract_sha256"] == sha256(CONTRACT_PATH)
    assert authorization["parquet_frame_parity_sha256"] == sha256(HELPER)
    assert authorization["execution_authorized"] is False
    assert str(config["logical_key"]).endswith(
        "cell_culture_differentiation_and_proliferation_conditions_influence_the_in_vitro"
    )


def test_distinct_row_13_fixture_reaches_generic_preflight_boundary() -> None:
    contract = load_contract_module()
    config = load_json(ROW_13_CONFIG)
    authorization = bound_authorization(config)
    authorization["execution_authorized"] = False
    binding = config["authorization_binding"]
    assert isinstance(binding, dict)
    assert binding["parent_task_id"] == "t_0c090cdd"
    assert binding["parent_task_id"] != config["task_id"]
    assert binding["correction_task_id"] == config["task_id"] == "t_b8dacd9d"
    authorization.update(binding)

    validated = validate(config, authorization)
    plan = contract.preflight_plan(validated)

    assert plan["source_url"].endswith("ba404fa2-44a9-4420-b43c-03f863b12d37.h5ad")
    assert plan["shape"] == [14424, 37434]
    assert plan["logical_key"].endswith(
        "cerebellar_organoid_using_microfluidics_and_combinatorial_barcoding_based_techno"
    )
    assert plan["accepted_components"] == {"current": 3, "denominator": 153}
    assert plan["execution_authorized"] is False
    with pytest.raises(RuntimeError, match="not execution-authorized"):
        contract.require_execution_authorized(validated)


@pytest.mark.parametrize(
    ("mutator", "rebind_config", "match"),
    [
        (lambda c, a: a.__setitem__("config_sha256", "0" * 64), False, "config SHA-256"),
        (lambda c, a: a.__setitem__("writer_sha256", "0" * 64), False, "writer SHA-256"),
        (
            lambda c, a: a.__setitem__("writer_contract_sha256", "0" * 64),
            False,
            "contract SHA-256",
        ),
        (
            lambda c, a: a.__setitem__("parquet_frame_parity_sha256", "0" * 64),
            False,
            "helper SHA-256",
        ),
        (lambda c, a: c.__setitem__("shape", [1, 2]), False, "config SHA-256"),
        (
            lambda c, a: c["source"].__setitem__("asset_id", "00000000-0000-0000-0000-000000000000"),
            True,
            "source URL",
        ),
        (
            lambda c, a: c.__setitem__("logical_key", "pert-gym/logical/temporal/wrong"),
            True,
            "logical_key conflicts with OBS dataset assignment",
        ),
        (
            lambda c, a: c["accepted_components"].__setitem__("current", 999),
            True,
            "accepted-components",
        ),
        (
            lambda c, a: c["obs"].__setitem__("mapper_version", "unknown/v1"),
            True,
            "mapper_version",
        ),
        (
            lambda c, a: c["ordered_var"].__setitem__("identity_sha256", "f" * 64),
            False,
            "config SHA-256",
        ),
        (lambda c, a: a.__setitem__("protocol", "wrong/v1"), False, "protocol"),
    ],
)
def test_every_bound_mismatch_aborts_before_any_write(mutator, rebind_config: bool, match: str) -> None:
    config = load_json(ROW_99_CONFIG)
    authorization = bound_authorization(config)
    mutator(config, authorization)
    if rebind_config:
        authorization = bound_authorization(config)

    with pytest.raises((RuntimeError, ValueError), match=match):
        validate(config, authorization)


@pytest.mark.parametrize(
    ("target", "value", "match"),
    [
        ("parent_task_id", "t_fake", "parent task"),
        ("approved_parent_protocol", "anything/v999", "parent protocol"),
        ("correction_task_id", "t_other", "correction task"),
    ],
)
def test_authorization_identity_is_exactly_bound_to_config(
    target: str, value: str, match: str
) -> None:
    config = load_json(ROW_99_CONFIG)
    authorization = bound_authorization(config)
    authorization[target] = value

    with pytest.raises(RuntimeError, match=match):
        validate(config, authorization)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda c: c["obs"]["assignments"][0].__setitem__(
                "value", "pert-gym/logical/temporal/conflict"
            ),
            "logical_key conflicts with OBS dataset assignment",
        ),
        (
            lambda c: c["source"].__setitem__(
                "dataset_version_id", "00000000-0000-0000-0000-000000000000"
            ),
            "dataset_version_id conflicts with asset_id",
        ),
        (
            lambda c: c["source"].__setitem__(
                "dataset_id", "00000000-0000-0000-0000-000000000000"
            ),
            "source dataset_id conflicts with approved revision identity",
        ),
        (
            lambda c: c["source"].__setitem__(
                "collection_version_id", "00000000-0000-0000-0000-000000000000"
            ),
            "source collection_version_id conflicts with approved revision identity",
        ),
        (
            lambda c: c["revision"].__setitem__("prefix", "temporal-v4-013"),
            "revision prefix conflicts with output/task identity",
        ),
    ],
)
def test_freshly_hash_bound_cross_field_conflicts_fail_intrinsically(
    mutation, match: str
) -> None:
    config = load_json(ROW_99_CONFIG)
    mutation(config)

    with pytest.raises(ValueError, match=match):
        validate(config, bound_authorization(config))


def instrument_external_boundaries(monkeypatch, writer) -> list[str]:
    calls: list[str] = []

    def reject(label: str):
        def boundary(*args, **kwargs):
            calls.append(label)
            raise AssertionError(f"unexpected boundary call: {label}")

        return boundary

    monkeypatch.setattr(Path, "mkdir", reject("mkdir"))
    monkeypatch.setattr(writer.urllib.request, "urlopen", reject("urlopen"))
    monkeypatch.setattr(writer, "source_api", reject("source_api"))
    monkeypatch.setattr(writer, "source_head", reject("source_head"))
    monkeypatch.setattr(writer, "hash_source", reject("source_body"))
    monkeypatch.setattr(writer, "connect_pertdata", reject("lamin"))
    monkeypatch.setattr(writer.fsspec, "filesystem", reject("filesystem"))
    monkeypatch.setattr(writer, "exclusive_bytes", reject("product_write"))
    return calls


def write_contract_files(
    tmp_path: Path, config: dict[str, object], authorization: dict[str, object]
) -> tuple[Path, Path]:
    config_path = tmp_path / "config.json"
    authorization_path = tmp_path / "authorization.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    authorization["config_sha256"] = sha256(config_path)
    authorization_path.write_text(json.dumps(authorization, indent=2, sort_keys=True) + "\n")
    return config_path, authorization_path


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda c, a: c["obs"]["assignments"][0].__setitem__(
                "value", "pert-gym/logical/temporal/conflict"
            ),
            "logical_key conflicts with OBS dataset assignment",
        ),
        (
            lambda c, a: c["source"].__setitem__(
                "dataset_version_id", "00000000-0000-0000-0000-000000000000"
            ),
            "dataset_version_id conflicts with asset_id",
        ),
        (
            lambda c, a: c["source"].__setitem__(
                "dataset_id", "00000000-0000-0000-0000-000000000000"
            ),
            "source dataset_id conflicts with approved revision identity",
        ),
        (
            lambda c, a: c["source"].__setitem__(
                "collection_version_id", "00000000-0000-0000-0000-000000000000"
            ),
            "source collection_version_id conflicts with approved revision identity",
        ),
        (
            lambda c, a: c["revision"].__setitem__("prefix", "temporal-v4-013"),
            "revision prefix conflicts with output/task identity",
        ),
        (
            lambda c, a: (
                c["authorization_binding"].__setitem__("parent_task_id", "t_fake"),
                a.__setitem__("parent_task_id", "t_fake"),
            ),
            "authorization parent task conflicts with approved revision identity",
        ),
        (lambda c, a: a.__setitem__("parent_task_id", "t_fake"), "parent task"),
        (
            lambda c, a: a.__setitem__("approved_parent_protocol", "anything/v999"),
            "parent protocol",
        ),
        (lambda c, a: a.__setitem__("correction_task_id", "t_other"), "correction task"),
    ],
)
def test_contract_conflicts_reject_before_instrumented_boundaries(
    monkeypatch, tmp_path: Path, mutation, match: str
) -> None:
    writer = load_writer_module()
    config = load_json(ROW_99_CONFIG)
    authorization = bound_authorization(config)
    authorization["execution_authorized"] = True
    mutation(config, authorization)
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    calls = instrument_external_boundaries(monkeypatch, writer)
    execution = config["execution"]
    assert isinstance(execution, dict)
    monkeypatch.setattr(
        writer.socket,
        "gethostname",
        lambda: str(execution["host"]),
    )
    monkeypatch.setattr(
        writer,
        "mem_available",
        lambda: int(execution["min_available_bytes"]),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )

    with pytest.raises((RuntimeError, ValueError), match=match):
        writer.main()

    assert calls == []


@pytest.mark.parametrize(
    ("parent_task_id", "parent_task_status", "match"),
    [
        ("t_0c090cdd", "running", "parent task is not completed"),
        ("t_0c090cdd", "reviewed", "parent task is not completed"),
        (
            "t_b8dacd9d",
            "completed",
            "authorization parent task conflicts with approved revision identity",
        ),
        (
            "t_arbitrary",
            "completed",
            "authorization parent task conflicts with approved revision identity",
        ),
    ],
)
def test_row_13_invalid_parent_authorization_rejects_before_instrumented_boundaries(
    monkeypatch,
    tmp_path: Path,
    parent_task_id: str,
    parent_task_status: str,
    match: str,
) -> None:
    writer = load_writer_module()
    config = load_json(ROW_13_CONFIG)
    binding = config["authorization_binding"]
    assert isinstance(binding, dict)
    binding["parent_task_id"] = parent_task_id
    authorization = bound_authorization(config)
    authorization.update(binding)
    authorization["parent_task_status"] = parent_task_status
    authorization["execution_authorized"] = True
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    calls = instrument_external_boundaries(monkeypatch, writer)
    execution = config["execution"]
    assert isinstance(execution, dict)
    monkeypatch.setattr(writer.socket, "gethostname", lambda: str(execution["host"]))
    monkeypatch.setattr(
        writer,
        "mem_available",
        lambda: int(execution["min_available_bytes"]),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )

    with pytest.raises((RuntimeError, ValueError), match=match):
        writer.main()

    assert calls == []


def test_runtime_host_rejects_before_instrumented_boundaries(
    monkeypatch, tmp_path: Path
) -> None:
    writer = load_writer_module()
    config = load_json(ROW_99_CONFIG)
    authorization = bound_authorization(config)
    authorization["execution_authorized"] = True
    config_path, authorization_path = write_contract_files(tmp_path, config, authorization)
    calls = instrument_external_boundaries(monkeypatch, writer)
    monkeypatch.setattr(writer.socket, "gethostname", lambda: "other-host")
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WRITER), "--config", str(config_path), "--authorization", str(authorization_path)],
    )

    with pytest.raises(RuntimeError, match="exact authorized host"):
        writer.main()

    assert calls == []


def test_heartbeat_progress_is_derived_from_configured_n_obs() -> None:
    writer = load_writer_module()

    assert list(writer.heartbeat_progress_rows(10224))[-1] == 9216
    assert list(writer.heartbeat_progress_rows(14424))[-1] == 14336
    assert "10241" not in WRITER.read_text()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda c, a: c.__setitem__("unknown", True),
        lambda c, a: a.__setitem__("unknown", True),
        lambda c, a: c.pop("shape"),
        lambda c, a: a.pop("parent_task_id"),
        lambda c, a: c["execution"].__setitem__("host", "other-host"),
        lambda c, a: c["forbidden_actions"].remove("deletion"),
    ],
)
def test_unknown_omitted_malformed_or_conflicting_values_fail_closed(mutation) -> None:
    config = copy.deepcopy(load_json(ROW_99_CONFIG))
    authorization = copy.deepcopy(load_json(ROW_99_AUTHORIZATION))
    mutation(config, authorization)

    with pytest.raises((RuntimeError, ValueError)):
        validate(config, authorization)


def test_runner_has_no_hidden_row_99_dataset_identity_and_validates_before_io() -> None:
    writer = WRITER.read_text()
    prefix = writer.split("source_api(contract)", maxsplit=1)[0]

    for hidden in (
        "081fef14-662c-430d-888f-b87a701d86b3",
        "73cf6939-3caa-4105-bc57-e073ee885a28",
        "878f431e4a709fb43d0ededbcc35511b16048e369e58984bad77fcf16600db4b",
        "temporal-v4-099-20260715T135852Z-d36b0c6d",
    ):
        assert hidden not in writer
    assert "load_bound_contract(" in prefix
    assert "require_execution_authorized(contract)" in prefix
    assert "OUT.mkdir" in prefix
    assert prefix.index("gethostname") < prefix.index("OUT.mkdir")
    assert prefix.index("mem_available()") < prefix.index("OUT.mkdir")
    assert "time.monotonic() - started > EXECUTION_TIMEOUT_SECONDS" in writer
