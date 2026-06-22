#!/usr/bin/env python3
"""Build the tiny DRUG-seq chemCPA-ready benchmark export.

This is a read-only Lamin export. It loads the small public DRUG-seq GSE120222
triplet (72 samples), resolves compound structures from a curated PubChem snapshot,
derives RDKit Morgan fingerprints, and writes a compact JSON artifact consumed by
``pert_gym.benchmarks.load_chemcpa_drugseq_tiny``.

Run with an environment that has RDKit available, e.g.:

    uv run --with rdkit python tools/build_chemcpa_drugseq_tiny.py
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from rdkit import Chem
from rdkit.Chem import rdFingerprintGenerator

ROOT = Path(__file__).resolve().parents[1]
_lamin_spec = importlib.util.spec_from_file_location(
    "pert_gym_lamin_context", ROOT / "tools" / "lamin_context.py"
)
if _lamin_spec is None or _lamin_spec.loader is None:
    raise RuntimeError("could not load tools/lamin_context.py")
_lamin_module = importlib.util.module_from_spec(_lamin_spec)
_lamin_spec.loader.exec_module(_lamin_module)
connect_pertdata = _lamin_module.connect_pertdata

DEFAULT_OUT = ROOT / "artifacts" / "model_benchmarks" / "chemcpa_drugseq_tiny_20260622.json"
DRUGSEQ_PREFIX = "DRUG-seq/GSE120222"
N_TOP_VARIABLE_GENES = 128
FINGERPRINT_BITS = 256
FINGERPRINT_RADIUS = 2

# PubChem PUG-REST property lookup, retrieved 2026-06-22 by compound name.
# The script intentionally keeps this snapshot local so benchmark generation is
# reproducible and not dependent on a live network call.
PUBCHEM_COMPOUNDS: dict[str, dict[str, Any]] = {
    "pyrazolanthrone": {
        "pubchem_cid": 8515,
        "canonical_name": "anthra(1,9-cd)pyrazol-6(2H)-one",
        "smiles": "C1=CC=C2C(=C1)C3=NNC4=CC=CC(=C43)C2=O",
        "inchi": "InChI=1S/C14H8N2O/c17-14-9-5-2-1-4-8(9)13-12-10(14)6-3-7-11(12)15-16-13/h1-7H,(H,15,16)",
        "inchikey": "ACPOUJIDANTYHO-UHFFFAOYSA-N",
    },
    "nilotinib": {
        "pubchem_cid": 644241,
        "canonical_name": "Nilotinib",
        "smiles": "CC1=C(C=C(C=C1)C(=O)NC2=CC(=CC(=C2)C(F)(F)F)N3C=C(N=C3)C)NC4=NC=CC(=N4)C5=CN=CC=C5",
        "inchi": "InChI=1S/C28H22F3N7O/c1-17-5-6-19(10-25(17)37-27-33-9-7-24(36-27)20-4-3-8-32-14-20)26(39)35-22-11-21(28(29,30)31)12-23(13-22)38-15-18(2)34-16-38/h3-16H,1-2H3,(H,35,39)(H,33,36,37)",
        "inchikey": "HHZIURLSWUIHRB-UHFFFAOYSA-N",
    },
    "miglitol": {
        "pubchem_cid": 441314,
        "canonical_name": "Miglitol",
        "smiles": "C1[C@@H]([C@H]([C@@H]([C@H](N1CCO)CO)O)O)O",
        "inchi": "InChI=1S/C8H17NO5/c10-2-1-9-3-6(12)8(14)7(13)5(9)4-11/h5-8,10-14H,1-4H2/t5-,6+,7-,8-/m1/s1",
        "inchikey": "IBAQFPQHRJAVAV-ULAWRXDQSA-N",
    },
    "dmso": {
        "pubchem_cid": 679,
        "canonical_name": "Dimethyl Sulfoxide",
        "smiles": "CS(=O)C",
        "inchi": "InChI=1S/C2H6OS/c1-4(2)3/h1-2H3",
        "inchikey": "IAZDPXIOMUYVGZ-UHFFFAOYSA-N",
    },
    "triptolide": {
        "pubchem_cid": 107985,
        "canonical_name": "Triptolide",
        "smiles": "CC(C)[C@@]12[C@@H](O1)[C@H]3[C@@]4(O3)[C@]5(CCC6=C([C@@H]5C[C@H]7[C@]4([C@@H]2O)O7)COC6=O)C",
        "inchi": "InChI=1S/C20H24O6/c1-8(2)18-13(25-18)14-20(26-14)17(3)5-4-9-10(7-23-15(9)21)11(17)6-12-19(20,24-12)16(18)22/h8,11-14,16,22H,4-7H2,1-3H3/t11-,12-,13-,14-,16+,17-,18-,19+,20+/m0/s1",
        "inchikey": "DFBIRQPKNDILPW-CIVMWXNOSA-N",
    },
    "homoharringtonine": {
        "pubchem_cid": 285033,
        "canonical_name": "Omacetaxine Mepesuccinate",
        "smiles": "CC(C)(CCC[C@@](CC(=O)OC)(C(=O)O[C@H]1[C@H]2C3=CC4=C(C=C3CCN5[C@@]2(CCC5)C=C1OC)OCO4)O)O",
        "inchi": "InChI=1S/C29H39NO9/c1-27(2,33)8-5-10-29(34,16-23(31)36-4)26(32)39-25-22(35-3)15-28-9-6-11-30(28)12-7-18-13-20-21(38-17-37-20)14-19(18)24(25)28/h13-15,24-25,33-34H,5-12,16-17H2,1-4H3/t24-,25-,28+,29-/m1/s1",
        "inchikey": "HYFHYPWGAURHIV-JFIAXGOJSA-N",
    },
    "mk-2206": {
        "pubchem_cid": 24964624,
        "canonical_name": "Mk-2206",
        "smiles": "C1CC(C1)(C2=CC=C(C=C2)C3=C(C=C4C(=N3)C=CN5C4=NNC5=O)C6=CC=CC=C6)N",
        "inchi": "InChI=1S/C25H21N5O/c26-25(12-4-13-25)18-9-7-17(8-10-18)22-19(16-5-2-1-3-6-16)15-20-21(27-22)11-14-30-23(20)28-29-24(30)31/h1-3,5-11,14-15H,4,12-13,26H2,(H,29,31)",
        "inchikey": "ULDXWLCXEDXJGE-UHFFFAOYSA-N",
    },
}


def _parse_nm(value: Any) -> float:
    text = str(value).strip().lower().replace("nm", "")
    return float(text)


def _dense_matrix(matrix: Any) -> np.ndarray:
    if hasattr(matrix, "toarray"):
        matrix = matrix.toarray()
    return np.asarray(matrix, dtype=np.float32)


def _fingerprint(smiles: str) -> list[int]:
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        raise ValueError(f"RDKit could not parse SMILES: {smiles}")
    generator = rdFingerprintGenerator.GetMorganGenerator(
        radius=FINGERPRINT_RADIUS,
        fpSize=FINGERPRINT_BITS,
    )
    return [int(bit) for bit in generator.GetFingerprint(mol)]


def build_export(out_path: Path) -> Path:
    ln = connect_pertdata()
    try:
        ln.track()
    except Exception as exc:  # pragma: no cover - provenance setup can be offline
        print(f"warning: ln.track() failed for local read-only export: {exc}")
    if ln.setup.settings.instance.slug != "laminlabs/pertdata":
        raise RuntimeError(f"wrong Lamin instance: {ln.setup.settings.instance.slug}")
    if ln.setup.settings.branch.name != "jkobject":
        raise RuntimeError(f"wrong Lamin branch: {ln.setup.settings.branch.name}")

    obs_artifact = ln.Artifact.get(key=f"{DRUGSEQ_PREFIX}/obs.parquet")
    x_artifact = ln.Artifact.get(key=f"{DRUGSEQ_PREFIX}/X.h5ad")
    var_artifact = ln.Artifact.get(key=f"{DRUGSEQ_PREFIX}/var.parquet")

    obs = obs_artifact.load()
    var = var_artifact.load()
    adata = x_artifact.load()
    X = _dense_matrix(adata.X)
    if X.shape[0] != len(obs):
        raise RuntimeError(f"obs/X row mismatch: {len(obs)} vs {X.shape[0]}")
    if X.shape[1] != len(var):
        raise RuntimeError(f"var/X column mismatch: {len(var)} vs {X.shape[1]}")

    variances = np.var(X, axis=0)
    top_idx = np.argsort(variances)[-N_TOP_VARIABLE_GENES:]
    top_idx = top_idx[np.argsort(-variances[top_idx])]
    X_tiny = X[:, top_idx].astype(float)
    var_tiny = var.iloc[top_idx]
    feature_names = [
        str(symbol) if str(symbol) != "nan" else str(index)
        for index, symbol in zip(var_tiny.index, var_tiny["symbol"])
    ]

    compounds = {}
    for key, metadata in sorted(PUBCHEM_COMPOUNDS.items()):
        fp = _fingerprint(metadata["smiles"])
        compounds[key] = {**metadata, "fingerprint": fp, "fingerprint_bits_on": int(sum(fp))}

    rows = []
    for idx, row in obs.reset_index(drop=True).iterrows():
        compound_key = str(row["pert_compound"]).strip().lower()
        if compound_key not in compounds:
            raise RuntimeError(f"missing compound metadata for {compound_key!r}")
        dose_nm = _parse_nm(row["pert_dose"])
        is_control = compound_key == "dmso" or dose_nm == 0.0
        rows.append(
            {
                "sample": str(row["geo_accession"]),
                "title": str(row["title"]),
                "perturbation": compound_key,
                "perturbation_type": "control" if is_control else "drug",
                "is_control": is_control,
                "dose_nM": dose_nm,
                "timepoint": str(row["pert_time"]),
                "cell_line": str(row["cell_line"]),
                "assay": "DRUG-seq",
                "organism": str(row["organism"]),
                "pubchem_cid": compounds[compound_key]["pubchem_cid"],
                "smiles": compounds[compound_key]["smiles"],
                "inchikey": compounds[compound_key]["inchikey"],
                "compound_fingerprint": compounds[compound_key]["fingerprint"],
                "expression": [round(float(value), 6) for value in X_tiny[idx].tolist()],
            }
        )

    payload = {
        "task": "pert-gym MB4F-Mac — real molecular chemCPA-ready tiny subset",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "lamin_instance": ln.setup.settings.instance.slug,
            "lamin_branch": ln.setup.settings.branch.name,
            "dataset_prefix": DRUGSEQ_PREFIX,
            "obs_key": obs_artifact.key,
            "x_key": x_artifact.key,
            "var_key": var_artifact.key,
            "n_obs_source": int(X.shape[0]),
            "n_vars_source": int(X.shape[1]),
            "x_semantics": "stored DRUG-seq expression matrix; no viability/IC50/AUC target used",
        },
        "selection": {
            "n_obs": len(rows),
            "n_features": len(feature_names),
            "feature_selection": f"top {N_TOP_VARIABLE_GENES} variable genes across the 72 source samples",
            "non_control_compounds": sorted(
                {row["perturbation"] for row in rows if not row["is_control"]}
            ),
            "control": "dmso",
            "split_by": "perturbation_identity; controls copied into every split by loader",
        },
        "compound_metadata": {
            "source": "PubChem PUG-REST property lookup by compound name, snapshot embedded in tools/build_chemcpa_drugseq_tiny.py",
            "retrieved_date": "2026-06-22",
            "fingerprint": {
                "type": "RDKit Morgan fingerprint",
                "radius": FINGERPRINT_RADIUS,
                "n_bits": FINGERPRINT_BITS,
            },
            "compounds": compounds,
        },
        "feature_names": feature_names,
        "rows": rows,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    out = build_export(args.out)
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
