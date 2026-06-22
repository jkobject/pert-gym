#!/usr/bin/env python3
"""Update P5F status artifacts after verified GSE247274 ingestion."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATUS_JSON = ROOT / "artifacts/schema_audit/prism_p5f_status_20260622.json"
STATUS_MD = ROOT / "artifacts/schema_audit/prism_p5f_status_20260622.md"

entry = {
    "prefix": "prism_collection/GSE247274",
    "n_obs": 69907,
    "n_vars": 22977,
    "chunk_size": 1000,
    "chunks_verified": 70,
    "verification_json": "artifacts/schema_audit/prism_GSE247274_chunked_verification.json",
}

data = json.loads(STATUS_JSON.read_text())
data["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
data.setdefault("completed_datasets", {})["GSE247274"] = entry
remaining = data.setdefault("remaining", {})
remaining["completed_now"] = 5
remaining["single_staged_smoke_first_candidates_remaining_estimate"] = 26
remaining["duplicate_resolved_candidates_remaining"] = 0
remaining["total_accessible_candidates_remaining_estimate"] = 26
data.setdefault("duplicate_resolution", {})["ingested_after_resolution"] = sorted(
    set(data.get("duplicate_resolution", {}).get("ingested_after_resolution", [])) | {"GSE267982", "GSE247274"}
)
data.get("duplicate_resolution", {})["remaining_duplicate_resolved_candidate"] = []
notes = data.setdefault("notes", [])
notes = [n for n in notes if "GSE247274 remains" not in n and "GSE267982 canonical object was ingested" not in n]
notes.append(
    "GSE247274 canonical object was ingested after duplicate resolution; the redundant `(1)` staged object remains un-ingested and should not be used."
)
data["notes"] = notes
STATUS_JSON.write_text(json.dumps(data, indent=2) + "\n")

md = STATUS_MD.read_text()
repls = {
    "- completed now: `4` datasets": "- completed now: `5` datasets",
    "- accessible candidates remaining estimate: `27` (includes duplicate-resolved GSE247274)": "- accessible candidates remaining estimate: `26`",
    "| GSE267982 | 45808 | 32285 | 46 | `prism_collection/GSE267982` | `artifacts/schema_audit/prism_GSE267982_chunked_verification.json` |": "| GSE267982 | 45808 | 32285 | 46 | `prism_collection/GSE267982` | `artifacts/schema_audit/prism_GSE267982_chunked_verification.json` |\n| GSE247274 | 69907 | 22977 | 70 | `prism_collection/GSE247274` | `artifacts/schema_audit/prism_GSE247274_chunked_verification.json` |",
    "- Duplicate-resolved but not yet ingested: `GSE247274` (canonical and `(1)` objects byte-identical; ingest canonical only).": "- Duplicate-resolved but not yet ingested: none. `GSE247274` canonical object is now ingested; do not ingest the redundant `(1)` copy.",
    "- GSE267982 canonical object was ingested after duplicate resolution; GSE247274 remains an accessible duplicate-resolved candidate, not a hash blocker.": "- GSE267982 and GSE247274 canonical objects were ingested after duplicate resolution; redundant `(1)` staged copies remain un-ingested and should not be used.",
}
for old, new in repls.items():
    if old not in md:
        raise SystemExit(f"missing expected text: {old}")
    md = md.replace(old, new)
STATUS_MD.write_text(md)
print(json.dumps({"updated": [str(STATUS_JSON.relative_to(ROOT)), str(STATUS_MD.relative_to(ROOT))]}, indent=2))
