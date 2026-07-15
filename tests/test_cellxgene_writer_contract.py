from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
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
    authorization["parent_task_id"] = "t_b8dacd9d"

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
    ("mutator", "match"),
    [
        (lambda c, a: a.__setitem__("config_sha256", "0" * 64), "config SHA-256"),
        (lambda c, a: a.__setitem__("writer_sha256", "0" * 64), "writer SHA-256"),
        (
            lambda c, a: a.__setitem__("writer_contract_sha256", "0" * 64),
            "contract SHA-256",
        ),
        (
            lambda c, a: a.__setitem__("parquet_frame_parity_sha256", "0" * 64),
            "helper SHA-256",
        ),
        (lambda c, a: c.__setitem__("shape", [1, 2]), "config SHA-256"),
        (
            lambda c, a: c["source"].__setitem__("asset_id", "00000000-0000-0000-0000-000000000000"),
            "source URL",
        ),
        (lambda c, a: c.__setitem__("logical_key", "pert-gym/logical/temporal/wrong"), "config SHA-256"),
        (
            lambda c, a: c["accepted_components"].__setitem__("current", 999),
            "accepted-components",
        ),
        (
            lambda c, a: c["obs"].__setitem__("mapper_version", "unknown/v1"),
            "mapper_version",
        ),
        (
            lambda c, a: c["ordered_var"].__setitem__("identity_sha256", "f" * 64),
            "config SHA-256",
        ),
        (lambda c, a: a.__setitem__("protocol", "wrong/v1"), "protocol"),
    ],
)
def test_every_bound_mismatch_aborts_before_any_write(mutator, match: str) -> None:
    config = load_json(ROW_99_CONFIG)
    authorization = bound_authorization(config)
    writes: list[str] = []
    mutator(config, authorization)

    with pytest.raises((RuntimeError, ValueError), match=match):
        validate(config, authorization)

    assert writes == []


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
    assert "time.monotonic() - started > EXECUTION_TIMEOUT_SECONDS" in writer
