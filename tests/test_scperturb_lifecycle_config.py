import json
from pathlib import Path

LIFECYCLE_CONFIG = (
    Path(__file__).resolve().parents[1] / "config/scperturb_lifecycle.v1.json"
)


def test_scperturb_lifecycle_archives_only_raw_prefix() -> None:
    config = json.loads(LIFECYCLE_CONFIG.read_text(encoding="utf-8"))

    assert config == {
        "rule": [
            {
                "action": {
                    "storageClass": "ARCHIVE",
                    "type": "SetStorageClass",
                },
                "condition": {
                    "age": 0,
                    "matchesPrefix": ["data/raw/"],
                    "matchesStorageClass": ["STANDARD"],
                },
            }
        ]
    }
    assert all(rule["action"]["type"] != "Delete" for rule in config["rule"])
