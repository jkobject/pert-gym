import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "tools" / "audit_lamin_triplet_schema.py"


def load_audit_module():
    spec = importlib.util.spec_from_file_location("audit_lamin_triplet_schema", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_lincs_phase_prefixes_have_reviewed_accessions() -> None:
    audit = load_audit_module()

    assert (
        audit.source_accession_for_prefix("LINCS/Phase1/Level2_GEX_delta_n49216x978")
        == "GSE92742"
    )
    assert (
        audit.source_accession_for_prefix("lincs/PHASE2/Level2_GEX_n1000x978")
        == "GSE70138"
    )


def test_lincs_uses_l1000_modality_and_level2_expression_semantics() -> None:
    audit = load_audit_module()
    prefix = "lincs/phase1/Level2_GEX_delta_n49216x978"

    assert (
        audit.infer_modality(prefix, "canonical_triplet", "delta_expression") == "L1000"
    )
    assert audit.infer_x_semantics(prefix, set()) == "normalized_expression"
