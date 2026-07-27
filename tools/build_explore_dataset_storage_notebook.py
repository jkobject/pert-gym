#!/usr/bin/env python3
"""Build the live, read-only dataset storage explorer notebook."""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

OUT = Path("notebooks/explore_dataset_storage.ipynb")


def md(text: str, cell_id: str):
    cell = nbf.v4.new_markdown_cell(text.strip())
    cell["id"] = cell_id
    return cell


def code(source: str, cell_id: str):
    cell = nbf.v4.new_code_cell(source.strip())
    cell["id"] = cell_id
    cell["execution_count"] = None
    cell["outputs"] = []
    return cell


def build():
    cells = [
        md(
            """
# Explore the pert-gym storage hierarchy and actual datasets

This notebook explores **data objects**, not Kanban cards or progress reports.
It answers three concrete questions:

1. Which source files live under the canonical `data/raw/<NAME>/` hierarchy?
2. Which processed triplets live under `data/cleaned/<NAME>/`?
3. Which artifacts and Collections already exist in LaminDB?

The canonical bucket contract is:

```text
gs://scperturb/
├── README.md
├── data/
│   ├── raw/
│   │   └── <NAME>/...
│   └── cleaned/
│       └── <NAME>/
│           ├── X.h5ad or X_chunk_<NNNN>.h5ad
│           ├── obs.parquet or obs_chunk_<NNNN>.parquet
│           ├── var.parquet
│           └── README.md
├── other/
│   └── README.md
└── .lamindb/  # hidden technical compatibility prefix
```

The notebook compares that target contract with the **live bucket**. Missing
canonical roots are reported as migration gaps; legacy paths are not silently
relabelled as canonical.
""",
            "title",
        ),
        md(
            """
## The storage layers

| Layer | What is there | Typical location |
|---|---|---|
| Local working/download | Raw downloads, extracted matrices, temporary caches | `data/main/`, `data/gcs_cache/`, `~/Downloads/` |
| GCS raw | Source material grouped by dataset | `gs://scperturb/data/raw/<NAME>/...` |
| GCS cleaned | One H5AD/OBS pair (or numbered chunks), one shared VAR, and one README per dataset | `gs://scperturb/data/cleaned/<NAME>/` |
| GCS other | The explicitly allowlisted auxiliary README only | `gs://scperturb/other/README.md` |
| LaminDB | Registered artifacts, triplets, feature links and versioned Collections | `laminlabs/pertdata`, branch `jkobject` |

GCS cleaned storage is not LaminDB. A cleaned GCS directory can exist without a matching
Lamin artifact. Conversely, a Lamin artifact may point to Lamin-managed S3 storage
and no longer need a project-bucket copy.
""",
            "layers",
        ),
        md(
            """
## Safety

Everything here is read-only:

- local files are opened only for metadata or backed inspection;
- GCS uses bounded read-only JSON API listings, never `cp`, `mv`, `rm`, or upload;
- Lamin uses filters, Collection membership, feature links, and small metadata loads;
- no large remote `X` matrix is materialized on the Mac.

The notebook deliberately refuses recursive GCS listings. Navigate one level at
a time instead. It does not migrate the legacy `pert-gym/` tree into this target
layout; migration requires a separate reviewed operation.
""",
            "safety",
        ),
        md(
            """
## 1. Choose a dataset

Start with one of the presets, then edit or add a dictionary entry. Each preset
contains independent search terms because the same dataset can use different
names in a browser download, GCS path, and Lamin key.

`SCP211` is the default because it demonstrates local source files and published
Lamin triplets. Its canonical GCS raw/cleaned locations are queried directly; if
they are absent, the notebook reports that absence rather than falling back to a
legacy prefix.
""",
            "choose-explain",
        ),
        code(
            """
from pathlib import Path
import json
import os
import re
import subprocess
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen
import zipfile

import pandas as pd
from IPython.display import Markdown, display

pd.set_option("display.max_columns", 50)
pd.set_option("display.max_colwidth", 140)
""",
            "imports",
        ),
        code(
            """
PRESETS = {
    "SCP211": {
        "local_query": "SCP211",
        "gcs_raw": "gs://scperturb/data/raw/SCP211/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/SCP211/",
        "lamin_query": "scp211",
        "note": "Canonical raw/cleaned locations plus the matching Lamin artifacts.",
    },
    "GSE132080": {
        "local_query": "GSE132080",
        "gcs_raw": "gs://scperturb/data/raw/GSE132080/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/GSE132080/",
        "lamin_query": "GSE132080",
        "note": "Canonical raw/cleaned locations plus a published Lamin obs/X/var triplet.",
    },
    "XAtlas HCT116": {
        "local_query": "xatlas",
        "gcs_raw": "gs://scperturb/data/raw/xatlas_orion/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/xatlas_orion/",
        "lamin_query": "xatlas/orion/hct116",
        "note": "Canonical raw source; no conforming canonical H5AD triplet is claimed yet.",
    },
    "GSE216481": {
        "local_query": "GSE216481",
        "gcs_raw": "gs://scperturb/data/raw/GSE216481/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/GSE216481/",
        "lamin_query": "GSE216481",
        "note": "Canonical raw/cleaned locations plus currently visible Lamin chunks.",
    },
    "Artista T37": {
        "local_query": "t37_artista",
        "gcs_raw": "gs://scperturb/data/raw/STDS0000056/",
        "gcs_cleaned": "gs://scperturb/data/cleaned/STDS0000056/",
        "lamin_query": "t37_artista",
        "note": "A useful example of working/staged data with no same-key Lamin match.",
    },
}

SELECTED_DATASET = "SCP211"  # Change this one line first.
selection = PRESETS[SELECTED_DATASET]
selection
""",
            "configuration",
        ),
        md(
            """
The three queries are intentionally visible. If a dataset uses an alias, change
`lamin_query` without changing its GCS URI. A zero same-key Lamin result means
**no matching key was found**, not proof that no aliased dataset exists.
""",
            "alias-warning",
        ),
        code(
            """
def human_bytes(value):
    if value is None:
        return ""
    value = float(value)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if value < 1024 or unit == "PB":
            return f"{value:.2f} {unit}"
        value /= 1024


def as_table(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame()
""",
            "small-helpers",
        ),
        md(
            """
## 2. Local downloads and working files

This scan reads file paths and sizes only. It does not parse entire matrices. The
roots are easy to edit if another download directory is used.
""",
            "local-section",
        ),
        code(
            """
LOCAL_ROOTS = [Path("data/main"), Path("data/gcs_cache"), Path.home() / "Downloads"]
DATA_SUFFIXES = {
    ".h5ad", ".h5", ".hdf5", ".rds", ".rda", ".loom", ".mtx",
    ".parquet", ".csv", ".tsv", ".zip", ".tar", ".gz",
}
MAX_LOCAL_FILES = 10_000
MAX_LOCAL_ENTRIES = 20_000
""",
            "local-options",
        ),
        code(
            """
def scan_local_data(roots, max_files=MAX_LOCAL_FILES, max_entries=MAX_LOCAL_ENTRIES):
    rows = []
    entries_seen = 0
    truncated = False
    for root in roots:
        if not root.exists():
            continue
        pending = [root]
        while pending and not truncated:
            current = pending.pop()
            try:
                entries = os.scandir(current)
            except OSError:
                continue
            with entries:
                for entry in entries:
                    if entries_seen >= max_entries or len(rows) >= max_files:
                        truncated = True
                        break
                    entries_seen += 1
                    path = Path(entry.path)
                    path_lower = str(path).lower()
                    if "openclaw-repair" in path_lower:
                        continue
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(path)
                        elif entry.is_file(follow_symlinks=False) and path.suffix.lower() in DATA_SUFFIXES:
                            size = entry.stat(follow_symlinks=False).st_size
                            rows.append({
                                "root": str(root),
                                "path": str(path),
                                "name": path.name,
                                "suffix": path.suffix.lower(),
                                "bytes": size,
                                "size": human_bytes(size),
                            })
                    except OSError:
                        continue
            if truncated:
                break
    frame = pd.DataFrame(rows)
    frame.attrs.update(entries_seen=entries_seen, scan_truncated=truncated)
    return frame


local_files = scan_local_data(LOCAL_ROOTS)
print(f"Found {len(local_files):,} local data-like files")
print(f"Inspected {local_files.attrs['entries_seen']:,} filesystem entries; truncated={local_files.attrs['scan_truncated']}")
print("Total visible bytes:", human_bytes(local_files["bytes"].sum()) if len(local_files) else "0 B")
local_files.sort_values("bytes", ascending=False).head(20)
""",
            "local-scan",
        ),
        md(
            """
### Local matches for the selected dataset

These are actual files. A local match does not imply that preprocessing is
unfinished; it may be a redundant working copy of data already published in Lamin.
The combined status later makes that distinction explicit.
""",
            "local-filter-explain",
        ),
        code(
            """
local_query = selection["local_query"].lower()
if local_files.empty:
    local_matches = local_files.copy()
else:
    local_matches = local_files[
        local_files["path"].str.lower().str.contains(local_query, regex=False)
    ].sort_values("bytes", ascending=False)
print(f"{len(local_matches):,} local matches for {local_query!r}")
local_matches.head(50)
""",
            "local-filter",
        ),
        md(
            """
### Inspect one local payload without loading it

Set `LOCAL_PREVIEW_PATH` explicitly, or leave it as `None` to inspect the first
match. H5AD/HDF5 files are opened in backed/metadata mode; MatrixMarket only reads
the header; ZIP only lists members. Other formats return file metadata.
""",
            "local-preview-explain",
        ),
        code(
            """
LOCAL_PREVIEW_PATH = None
if LOCAL_PREVIEW_PATH is None and len(local_matches):
    LOCAL_PREVIEW_PATH = local_matches.iloc[0]["path"]
LOCAL_PREVIEW_PATH
""",
            "local-preview-choice",
        ),
        code(
            """
def inspect_local_payload(path_value):
    if not path_value:
        return {"message": "No local match selected"}
    path = Path(path_value)
    result = {"path": str(path), "bytes": path.stat().st_size, "size": human_bytes(path.stat().st_size)}
    lower = path.name.lower()
    if lower.endswith(".h5ad"):
        import anndata as ad
        data = ad.read_h5ad(path, backed="r")
        try:
            result.update({
                "format": "h5ad (backed)",
                "shape": tuple(data.shape),
                "obs_columns": list(data.obs.columns[:30]),
                "var_columns": list(data.var.columns[:30]),
                "x_backing_type": type(data.X).__name__,
            })
        finally:
            if getattr(data, "file", None) is not None:
                data.file.close()
    elif lower.endswith((".h5", ".hdf5")):
        import h5py
        with h5py.File(path, "r") as handle:
            result.update({"format": "HDF5", "top_level_groups": list(handle.keys())[:50]})
    elif lower.endswith(".mtx"):
        with path.open("rt", errors="replace") as handle:
            header = []
            dimensions = None
            for _ in range(100):
                line = handle.readline()
                if not line:
                    break
                header.append(line.rstrip())
                if line and not line.startswith("%"):
                    dimensions = line.split()
                    break
        result.update({"format": "MatrixMarket", "dimensions_line": dimensions, "header": header[:8]})
    elif lower.endswith(".zip"):
        with zipfile.ZipFile(path) as archive:
            result.update({"format": "ZIP", "members": archive.namelist()[:40], "member_count": len(archive.infolist())})
    else:
        result["format"] = "metadata only"
    return result


local_preview = inspect_local_payload(LOCAL_PREVIEW_PATH)
local_preview
""",
            "local-preview",
        ),
        md(
            """
## 3. Compare the canonical hierarchy with the live bucket

The following helper lists exactly one hierarchy level. It never downloads an
object and never performs a recursive walk. This keeps the live comparison
bounded even when a dataset contains many members.
""",
            "gcs-section",
        ),
        code(
            """
GCS_BUCKET_ROOT = "gs://scperturb/"
RAW_GCS_ROOTS = ["gs://scperturb/data/raw/"]
CLEANED_GCS_ROOTS = ["gs://scperturb/data/cleaned/"]
OTHER_GCS_ROOTS = ["gs://scperturb/other/"]
EXPECTED_TOP_LEVEL = {"README.md", "data/", "other/", ".lamindb/"}
EXPECTED_DATA_LEVEL = {"raw/", "cleaned/"}
GCLOUD_TIMEOUT_SECONDS = 120
GCS_BILLING_PROJECT = "jkobject-1549353370965"
MAX_GCS_RESULTS = 500
MAX_GCS_RESPONSE_BYTES = 2 * 1024**2
""",
            "gcs-options",
        ),
        code(
            """
def gcloud_adc_token(timeout=GCLOUD_TIMEOUT_SECONDS):
    completed = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        raise RuntimeError(f"Google ADC unavailable: {completed.stderr.strip()}")
    return completed.stdout.strip()


def list_gcs_level(uri, timeout=GCLOUD_TIMEOUT_SECONDS, max_results=MAX_GCS_RESULTS):
    if not uri.startswith("gs://"):
        raise ValueError("Expected a gs:// URI")
    parsed = urlparse(uri)
    bucket = parsed.netloc
    object_name = parsed.path.lstrip("/")
    token = gcloud_adc_token(timeout)
    if object_name and not uri.endswith("/"):
        query = urlencode({"userProject": GCS_BILLING_PROJECT, "fields": "name,size,updated"})
        endpoint = (
            f"https://storage.googleapis.com/storage/v1/b/{quote(bucket, safe='')}/o/"
            f"{quote(object_name, safe='')}?{query}"
        )
    else:
        query = urlencode({
            "prefix": object_name,
            "delimiter": "/",
            "maxResults": max_results,
            "userProject": GCS_BILLING_PROJECT,
            "fields": "items(name,size,updated),prefixes,nextPageToken",
        })
        endpoint = f"https://storage.googleapis.com/storage/v1/b/{quote(bucket, safe='')}/o?{query}"
    request = Request(endpoint, headers={"Authorization": f"Bearer {token}"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read(MAX_GCS_RESPONSE_BYTES + 1)
    except Exception as error:
        raise RuntimeError(f"GCS listing failed for {uri}: {error}") from error
    if len(payload) > MAX_GCS_RESPONSE_BYTES:
        raise RuntimeError(f"GCS response exceeded {MAX_GCS_RESPONSE_BYTES} bytes for {uri}")
    document = json.loads(payload)
    rows = []
    if "name" in document:
        size = int(document["size"])
        rows.append({"kind": "object", "uri": f"gs://{bucket}/{document['name']}", "bytes": size, "size": human_bytes(size)})
    else:
        for child_prefix in document.get("prefixes", []):
            rows.append({"kind": "prefix", "uri": f"gs://{bucket}/{child_prefix}", "bytes": None, "size": ""})
        for item in document.get("items", []):
            size = int(item["size"])
            rows.append({"kind": "object", "uri": f"gs://{bucket}/{item['name']}", "bytes": size, "size": human_bytes(size)})
    frame = pd.DataFrame(rows)
    frame.attrs.update(result_limit=max_results, listing_truncated=bool(document.get("nextPageToken")))
    return frame
""",
            "gcs-helper",
        ),
        md(
            """
### Validate the bucket shape before inspecting datasets

The expected hierarchy and the observed hierarchy are kept separate. A missing
`data/raw/`, `data/cleaned/`, `other/`, or root README is a visible migration
gap—not an empty successful dataset inventory.
""",
            "gcs-contract-explain",
        ),
        code(
            """
def child_name(uri, parent):
    return uri.removeprefix(parent).rstrip("/") + ("/" if uri.endswith("/") else "")


bucket_top = list_gcs_level(GCS_BUCKET_ROOT)
observed_top = {
    child_name(uri, GCS_BUCKET_ROOT)
    for uri in bucket_top.get("uri", pd.Series(dtype=str))
}
data_top = list_gcs_level("gs://scperturb/data/")
observed_data = {
    child_name(uri, "gs://scperturb/data/")
    for uri in data_top.get("uri", pd.Series(dtype=str))
}
hierarchy_check = pd.DataFrame([
    {
        "level": "gs://scperturb/",
        "expected": sorted(EXPECTED_TOP_LEVEL),
        "observed": sorted(observed_top),
        "missing": sorted(EXPECTED_TOP_LEVEL - observed_top),
        "unexpected_or_legacy": sorted(observed_top - EXPECTED_TOP_LEVEL),
    },
    {
        "level": "gs://scperturb/data/",
        "expected": sorted(EXPECTED_DATA_LEVEL),
        "observed": sorted(observed_data),
        "missing": sorted(EXPECTED_DATA_LEVEL - observed_data),
        "unexpected_or_legacy": sorted(observed_data - EXPECTED_DATA_LEVEL),
    },
])
hierarchy_check
""",
            "gcs-contract-check",
        ),
        md(
            """
### Canonical raw datasets

Each child of `data/raw/` is a dataset `<NAME>`. Its descendants retain the
upstream/source organization needed for reconstruction.
""",
            "gcs-raw-roots-explain",
        ),
        code(
            """
raw_root_tables = []
for root in RAW_GCS_ROOTS:
    table = list_gcs_level(root)
    if len(table):
        table.insert(0, "root", root)
        raw_root_tables.append(table)
raw_root_listing = pd.concat(raw_root_tables, ignore_index=True) if raw_root_tables else pd.DataFrame()
raw_root_listing.head(100)
""",
            "gcs-raw-roots",
        ),
        md(
            """
### Canonical cleaned datasets

Each child of `data/cleaned/` is the same dataset `<NAME>`. Members follow the
explicit `X_*.h5ad`, `var.parquet`, and `obs_*.parquet` convention. Presence here
still does not prove registration in Lamin; we check Lamin separately.
""",
            "gcs-processed-roots-explain",
        ),
        code(
            """
cleaned_root_tables = []
for root in CLEANED_GCS_ROOTS:
    table = list_gcs_level(root)
    if len(table):
        table.insert(0, "root", root)
        cleaned_root_tables.append(table)
cleaned_root_listing = pd.concat(cleaned_root_tables, ignore_index=True) if cleaned_root_tables else pd.DataFrame()
cleaned_root_listing.head(100)
""",
            "gcs-processed-roots",
        ),
        md(
            """
### Build a live inventory of meaningful local and GCS dataset entries

The children of `data/raw/` and `data/cleaned/` are already the meaningful
dataset names. The resulting table contains live paths and prefixes, not a saved
progress export or a reconstruction of the legacy `pert-gym/` tree.
""",
            "layer-inventory-explain",
        ),
        code(
            """
MEANINGFUL_GCS_ROOTS = {
    "GCS raw": RAW_GCS_ROOTS,
    "GCS cleaned": CLEANED_GCS_ROOTS,
}


def dataset_token(uri):
    name = uri.rstrip("/").split("/")[-1]
    for ending in [".tar.gz", ".csv.gz", ".tsv.gz", ".h5ad", ".parquet", ".zip", ".tar"]:
        if name.lower().endswith(ending):
            name = name[: -len(ending)]
            break
    return name


layer_rows = []
# Group local files by the first directory below each configured root.
for root in LOCAL_ROOTS:
    root_string = str(root)
    if local_files.empty:
        continue
    for path_string in local_files.loc[local_files["root"] == root_string, "path"]:
        try:
            relative = Path(path_string).relative_to(root)
        except ValueError:
            continue
        token = relative.parts[0] if len(relative.parts) > 1 else dataset_token(path_string)
        layer_rows.append({"layer": "local download/working", "dataset_token": token, "location": str(root / token)})

for layer, roots in MEANINGFUL_GCS_ROOTS.items():
    for root in roots:
        table = list_gcs_level(root)
        if table.empty or "uri" not in table:
            continue
        for uri in table.loc[table["kind"].isin(["prefix", "object"]), "uri"]:
            layer_rows.append({"layer": layer, "dataset_token": dataset_token(uri), "location": uri})

layer_inventory = pd.DataFrame(layer_rows).drop_duplicates().sort_values(["layer", "dataset_token"])
print("Actual location entries:", len(layer_inventory))
display(layer_inventory.groupby("layer").size().rename("entries"))
layer_inventory.head(200)
""",
            "layer-inventory",
        ),
        md(
            """
This is an inventory of physical/logical **locations**. We classify it against
live Lamin after connecting below. Names such as `SCP211` and `GSE132080` match
well; internal slugs can require an explicit alias and are never silently treated
as definitive absence.
""",
            "layer-inventory-caveat",
        ),
        md(
            """
### Inspect the selected dataset's GCS locations

The preset supplies exact canonical raw and cleaned dataset prefixes.
Listings remain one level deep. Change either URI to descend into a child prefix.
""",
            "gcs-selected-explain",
        ),
        code(
            """
selected_gcs_raw = list_gcs_level(selection["gcs_raw"]) if selection.get("gcs_raw") else pd.DataFrame()
selected_gcs_cleaned = (
    list_gcs_level(selection["gcs_cleaned"])
    if selection.get("gcs_cleaned")
    else pd.DataFrame()
)
print("RAW:", selection.get("gcs_raw"))
display(selected_gcs_raw.head(100))
print("CLEANED:", selection.get("gcs_cleaned"))
display(selected_gcs_cleaned.head(100))
""",
            "gcs-selected",
        ),
        md(
            """
### Navigate to another GCS prefix

Paste any prefix from the tables above. Keep it at a directory level. There is no
recursive option by design.
""",
            "gcs-navigate-explain",
        ),
        code(
            """
GCS_BROWSE_URI = selection.get("gcs_raw")
gcs_browse = list_gcs_level(GCS_BROWSE_URI) if GCS_BROWSE_URI else pd.DataFrame()
gcs_browse.head(200)
""",
            "gcs-navigate",
        ),
        md(
            """
## 4. Query actual LaminDB objects

This connects through the repository helper to `laminlabs/pertdata`, branch
`jkobject`. It does not use a local progress file. Collection counts, artifact
keys, UIDs, sizes, observation counts, paths, and feature links come from Lamin.
""",
            "lamin-section",
        ),
        code(
            """
from tools.lamin_context import connect_pertdata

ln = connect_pertdata()
assert ln.setup.settings.instance.slug == "laminlabs/pertdata"
assert ln.setup.settings.branch.name == "jkobject"
print("Connected:", ln.setup.settings.instance.slug, "branch", ln.setup.settings.branch.name)
""",
            "lamin-connect",
        ),
        md(
            """
### Collections that actually exist

Collection membership is not the same as artifact existence. Canonical
Collections usually contain `obs.parquet` anchors; their linked `X` and `var`
artifacts form the complete triplet.
""",
            "collections-explain",
        ),
        code(
            """
collection_rows = []
for collection in ln.Collection.filter(key__startswith="pert-gym/").order_by("key"):
    collection_rows.append({
        "key": collection.key,
        "uid": collection.uid,
        "members": collection.artifacts.count(),
    })
collections = pd.DataFrame(collection_rows)
collections
""",
            "collections",
        ),
        md(
            """
### Search Lamin for the selected biological dataset

This is a live substring search on artifact keys. The total count is reported
separately from the displayed rows. Raise `MAX_LAMIN_ROWS` cautiously for highly
chunked datasets such as XAtlas.
""",
            "lamin-search-explain",
        ),
        code(
            """
MAX_LAMIN_ROWS = 100
lamin_query = selection["lamin_query"]
lamin_queryset = ln.Artifact.filter(is_latest=True, key__icontains=lamin_query).order_by("key")
lamin_match_count = lamin_queryset.count()
lamin_matches = []
for artifact in lamin_queryset[:MAX_LAMIN_ROWS]:
    lamin_matches.append({
        "key": artifact.key,
        "uid": artifact.uid,
        "suffix": artifact.suffix,
        "bytes": artifact.size,
        "size": human_bytes(artifact.size),
        "n_observations": artifact.n_observations,
        "storage_path": str(artifact.path),
    })
lamin_matches = pd.DataFrame(lamin_matches)
print(f"{lamin_match_count:,} latest Lamin artifacts match {lamin_query!r}; showing {len(lamin_matches):,}")
lamin_matches.head(MAX_LAMIN_ROWS)
""",
            "lamin-search",
        ),
        md(
            """
### Classify the live location inventory into the three requested views

For every meaningful local/GCS entry, the notebook performs a live same-key
Lamin lookup. Common path aliases are explicit below. The result is deliberately
phrased as evidence:

- canonical raw location where preprocessing is not demonstrated;
- canonical cleaned GCS location without a same-key Lamin match;
- a real Lamin artifact match, while preserving any remaining raw/cleaned copy.

A same-key miss is shown as a candidate, not asserted as biological absence.
""",
            "global-classification-explain",
        ),
        code(
            """
LAMIN_ALIASES = {
    "depmap_ccle26q1": "depmap_ccle/26q1",
    "perturbase_gse216481": "GSE216481",
    "sanger_dualguide_crc": "sanger_dual_guide_crc",
    "sanger_gdsc1": "sanger_gdsc/gdsc1",
    "sanger_gdsc2": "sanger_gdsc/gdsc2",
    "virtual_cell_vcpi": "ginkgo-datapoints/vcpi",
    "tcell_d4_rest": "tcell_gwps/D4_Rest",
}
MAX_LAYER_IDENTITIES = 300


def lamin_term(token):
    accession = re.search(r"(GSE\\d+|SCP\\d+|E-MTAB-\\d+|STDS\\d+|STT\\d+)", token, re.IGNORECASE)
    if accession:
        return accession.group(1)
    return LAMIN_ALIASES.get(token.lower(), token)


classified = layer_inventory.copy().head(MAX_LAYER_IDENTITIES)
match_counts = {}
for token in classified["dataset_token"].drop_duplicates():
    term = lamin_term(token)
    if token.lower() in {"temporal", "logical_sparse_zarr"}:
        match_counts[token] = 0
    else:
        match_counts[token] = ln.Artifact.filter(is_latest=True, key__icontains=term).count()
classified["lamin_search_term"] = classified["dataset_token"].map(lamin_term)
classified["lamin_artifact_matches"] = classified["dataset_token"].map(match_counts).fillna(0).astype(int)
cleaned_tokens = set(
    classified.loc[classified["layer"] == "GCS cleaned", "dataset_token"].str.lower()
)


def evidence_category(row):
    if row["lamin_artifact_matches"] > 0:
        return "present in LaminDB (raw/cleaned GCS copy may remain)"
    if row["layer"] == "GCS cleaned" or row["dataset_token"].lower() in cleaned_tokens:
        return "GCS cleaned; no same-key Lamin match"
    return "raw/downloaded candidate; cleaning not demonstrated"


classified["evidence_category"] = classified.apply(evidence_category, axis=1)
classified.groupby("evidence_category").size().rename("location_entries")
""",
            "global-classification",
        ),
        md(
            """
#### A. Downloaded/raw candidates

These real files or GCS prefixes have no same-key processed/Lamin evidence in this
bounded live comparison. Inspect aliases before treating any row as unfinished.
""",
            "raw-candidates-explain",
        ),
        code(
            """
raw_candidates = classified[
    classified["evidence_category"] == "raw/downloaded candidate; cleaning not demonstrated"
]
raw_candidates[["dataset_token", "layer", "location", "lamin_search_term"]].head(200)
""",
            "raw-candidates",
        ),
        md(
            """
#### B. Cleaned GCS data without a same-key Lamin match

These are the closest answer to “cleaned and stored in the canonical GCS
hierarchy, but not yet in LaminDB.” Exact member identity still needs inspection
before publication.
""",
            "processed-candidates-explain",
        ),
        code(
            """
cleaned_not_lamin = classified[
    classified["evidence_category"] == "GCS cleaned; no same-key Lamin match"
]
cleaned_not_lamin[["dataset_token", "location", "lamin_search_term"]].head(200)
""",
            "processed-candidates",
        ),
        md(
            """
#### C. Present in LaminDB

These rows have at least one live latest Artifact key match. The location table
keeps local/GCS copies visible so redundant staging is not mistaken for an
unpublished dataset.
""",
            "lamin-present-explain",
        ),
        code(
            """
locations_with_lamin = classified[classified["lamin_artifact_matches"] > 0]
locations_with_lamin[
    ["dataset_token", "layer", "location", "lamin_search_term", "lamin_artifact_matches"]
].sort_values(["dataset_token", "layer"]).head(300)
""",
            "lamin-present",
        ),
        md(
            """
A Lamin path such as `s3://lamin-us-west-2/.../.lamindb/<uid>.h5ad` is the real
managed payload location. The human-readable artifact key is the stable project
handle; the UID identifies the exact registered object.
""",
            "lamin-path-explain",
        ),
        md(
            """
### Check canonical Collection membership

This tests the matching artifact UIDs against the currently dated canonical
Collection. Artifacts may exist in Lamin while not belonging to this older
Collection version, so both facts are shown.
""",
            "membership-explain",
        ),
        code(
            """
CANONICAL_COLLECTION_KEY = "pert-gym/canonical/20260621"
canonical = ln.Collection.get(key=CANONICAL_COLLECTION_KEY)
canonical_uids = set(canonical.artifacts.all().values_list("uid", flat=True))
if len(lamin_matches):
    lamin_matches["in_canonical_20260621"] = lamin_matches["uid"].isin(canonical_uids)
    display(lamin_matches.head(MAX_LAMIN_ROWS))
""",
            "membership",
        ),
        md(
            """
### Follow a real `obs → X → var` triplet

The notebook selects the first matching `obs.parquet`. Feature links are read from
Lamin rather than guessed by replacing filenames. For non-expression datasets,
links can use typed auxiliary payloads and may differ from this triplet pattern.
""",
            "links-explain",
        ),
        code(
            """
def resolve_linked_artifact(value):
    if value is None:
        return None
    if isinstance(value, str):
        return ln.Artifact.get(key=value)
    return value


selected_obs = None
if len(lamin_matches):
    obs_rows = lamin_matches[lamin_matches["key"].str.endswith("/obs.parquet")]
    if len(obs_rows):
        selected_obs = ln.Artifact.get(uid=obs_rows.iloc[0]["uid"])

triplet = {}
if selected_obs is not None:
    values = selected_obs.features.get_values()
    selected_x = resolve_linked_artifact(values.get("X"))
    selected_var = None
    if selected_x is not None:
        selected_var = resolve_linked_artifact(selected_x.features.get_values().get("var"))
    triplet = {
        "obs_key": selected_obs.key,
        "obs_uid": selected_obs.uid,
        "X_key": getattr(selected_x, "key", None),
        "X_uid": getattr(selected_x, "uid", None),
        "var_key": getattr(selected_var, "key", None),
        "var_uid": getattr(selected_var, "uid", None),
    }
triplet
""",
            "links",
        ),
        md(
            """
### Look inside a small Lamin metadata table

This loads only the selected `obs.parquet` when it is below the explicit byte
limit. It shows actual rows and columns from the dataset. It never loads the
remote `X.h5ad` matrix.
""",
            "lamin-preview-explain",
        ),
        code(
            """
MAX_LAMIN_METADATA_BYTES = 20 * 1024**2
lamin_preview = None
if selected_obs is None:
    print("No matching obs.parquet was found")
elif selected_obs.size is None:
    print("Refusing metadata load with unknown size:", selected_obs.key)
elif selected_obs.size > MAX_LAMIN_METADATA_BYTES:
    print("Refusing metadata load:", selected_obs.key, human_bytes(selected_obs.size))
else:
    lamin_preview = selected_obs.load()
    print("Loaded actual obs table:", selected_obs.key, lamin_preview.shape)
    display(lamin_preview.head(10))
""",
            "lamin-preview",
        ),
        md(
            """
### Matrix location without matrix materialization

For the linked `X`, we display identity, remote path, registered byte size and
`n_observations`. Loading the matrix is intentionally not part of this general
Mac notebook.
""",
            "matrix-metadata-explain",
        ),
        code(
            """
if triplet.get("X_uid"):
    x_artifact = ln.Artifact.get(uid=triplet["X_uid"])
    matrix_location = {
        "key": x_artifact.key,
        "uid": x_artifact.uid,
        "path": str(x_artifact.path),
        "bytes": x_artifact.size,
        "size": human_bytes(x_artifact.size),
        "n_observations": x_artifact.n_observations,
    }
else:
    matrix_location = {"message": "No linked X artifact resolved"}
matrix_location
""",
            "matrix-metadata",
        ),
        md(
            """
## 5. Combined location/status view

This summary is deliberately multi-valued. It does not erase raw copies merely
because Lamin exists, and it does not call a raw download "processed".
""",
            "combined-section",
        ),
        code(
            """
local_present = bool(len(local_matches))
gcs_raw_present = bool(len(selected_gcs_raw))
gcs_cleaned_present = bool(len(selected_gcs_cleaned))
lamin_present = lamin_match_count > 0

if lamin_present:
    primary_state = "present in LaminDB"
elif gcs_cleaned_present:
    primary_state = "cleaned on canonical GCS; no matching Lamin key found"
elif local_present or gcs_raw_present:
    primary_state = "raw/local only; no canonical cleaned GCS or matching Lamin key found"
else:
    primary_state = "not found with the configured canonical locations/aliases"

location_summary = pd.DataFrame([
    {"layer": "local working/download", "present": local_present, "location": ", ".join(str(p) for p in LOCAL_ROOTS), "matches": len(local_matches)},
    {"layer": "GCS raw", "present": gcs_raw_present, "location": selection.get("gcs_raw"), "matches": len(selected_gcs_raw)},
    {"layer": "GCS cleaned", "present": gcs_cleaned_present, "location": selection.get("gcs_cleaned"), "matches": len(selected_gcs_cleaned)},
    {"layer": "LaminDB latest artifacts", "present": lamin_present, "location": "laminlabs/pertdata / jkobject", "matches": lamin_match_count},
])
print(SELECTED_DATASET, "→", primary_state)
display(location_summary)
""",
            "combined-status",
        ),
        md(
            """
### How to interpret overlap

- **Local + GCS raw + Lamin**: data are published, but redundant source/working
  copies may still exist.
- **GCS cleaned + no Lamin match**: a canonical cleaned-data candidate, subject
  to checking aliases and exact member identity.
- **Local/GCS raw only**: downloaded/source data exist, but cleaning/publication
  is not demonstrated by these locations.
- **Lamin only**: durable managed data exist; canonical GCS paths may not yet be
  populated or may no longer be needed.

A substring miss is not a deletion claim. Adjust aliases before concluding that a
dataset is absent. The hierarchy check above is the authority for migration gaps.
""",
            "overlap-interpretation",
        ),
        md(
            """
## 6. Try the useful examples

Change only `SELECTED_DATASET` near the top and rerun all cells:

- `SCP211`: canonical raw/cleaned paths plus Lamin triplets;
- `GSE132080`: canonical dataset paths plus a Lamin triplet;
- `XAtlas HCT116`: canonical dataset paths plus many Lamin chunks;
- `GSE216481`: canonical dataset paths plus Lamin chunks under a different key shape;
- `Artista T37`: canonical paths and an example with no same-key Lamin match.

Then add the dataset you care about to `PRESETS`. Keeping aliases explicit makes
the result auditable and easy to correct.
""",
            "examples",
        ),
        md(
            """
## 7. What this notebook does not claim

- Presence in Lamin is not the same as acceptance into the latest versioned
  Collection or full `DATASET_E2E_V3` completion.
- A GCS `logical/` prefix can hold an incomplete, superseded, candidate, or
  rollback revision; inspect its immediate children and exact revision.
- Local files can be stale duplicates.
- No matrix-scale computation is performed here.
- This notebook locates and safely opens datasets; it does not mutate, clean, or
  delete any storage layer.
""",
            "limitations",
        ),
        md(
            """
## Recap

You now have direct handles to the data:

- local file paths and backed/header inspection;
- live GCS source and processed prefixes with object sizes;
- live Lamin Collection keys, Artifact keys/UIDs, remote paths and feature links;
- a combined view that preserves every physical layer.

The next question is no longer “which progress file says done?” but “for this
biological dataset, which real payloads exist in each storage system?”
""",
            "recap",
        ),
    ]

    notebook = nbf.v4.new_notebook(
        cells=cells,
        metadata={
            "kernelspec": {
                "display_name": "pert-gym (project uv)",
                "language": "python",
                "name": "pert-gym",
            },
            "language_info": {"name": "python", "version": "3.11"},
        },
    )
    nbf.validate(notebook)
    return notebook


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(build(), OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
