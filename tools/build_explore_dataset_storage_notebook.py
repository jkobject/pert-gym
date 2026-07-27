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
# Explore the actual pert-gym datasets and where they live

This notebook explores **data objects**, not Kanban cards or progress reports.
It answers three concrete questions:

1. Which source files have been downloaded but still live as raw/working data?
2. Which processed outputs exist in project GCS staging?
3. Which artifacts and Collections already exist in LaminDB?

The same biological dataset may appear in several layers at once. That is useful:
a raw copy can coexist with processed staging and a published Lamin triplet. The
notebook shows every matching location instead of forcing one misleading status.
""",
            "title",
        ),
        md(
            """
## The storage layers

| Layer | What is there | Typical location |
|---|---|---|
| Local working/download | Raw downloads, extracted matrices, temporary caches | `data/main/`, `data/gcs_cache/`, `~/Downloads/` |
| GCS source staging | Durable temporary copy of downloaded sources | `gs://scperturb/pert-gym/staging/data/main/`, `manual_downloads/`, `sources/` |
| GCS processed staging | Converted/chunked/Zarr outputs awaiting or supporting publication | `gs://scperturb/pert-gym/staging/pert-gym/logical/` and source-specific logical prefixes |
| LaminDB | Registered artifacts, triplets, feature links and versioned Collections | `laminlabs/pertdata`, branch `jkobject` |

GCS staging is not LaminDB. A processed GCS directory can exist without a matching
Lamin artifact. Conversely, a Lamin artifact may point to Lamin-managed S3 storage
and no longer need its old project staging copy.
""",
            "layers",
        ),
        md(
            """
## Safety

Everything here is read-only:

- local files are opened only for metadata or backed inspection;
- GCS uses `gcloud storage ls`, never `cp`, `mv`, `rm`, or upload;
- Lamin uses filters, Collection membership, feature links, and small metadata loads;
- no large remote `X` matrix is materialized on the Mac.

The notebook deliberately refuses recursive GCS listings. Some logical sparse-Zarr
prefixes contain hundreds of thousands of physical objects. Navigate one level at
a time instead.
""",
            "safety",
        ),
        md(
            """
## 1. Choose a dataset

Start with one of the presets, then edit or add a dictionary entry. Each preset
contains independent search terms because the same dataset can use different
names in a browser download, GCS path, and Lamin key.

`SCP211` is the default because it currently demonstrates all three layers:
large local MatrixMarket downloads, a GCS source copy, and published Lamin triplets.
""",
            "choose-explain",
        ),
        code(
            """
from pathlib import Path
import re
import subprocess
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
        "gcs_raw": "gs://scperturb/pert-gym/staging/manual_downloads/2026-06-23/downloads_cleanup/SCP211/",
        "gcs_processed": None,
        "lamin_query": "scp211",
        "note": "Raw copies remain local and on GCS; seven triplets are visible in Lamin.",
    },
    "GSE132080": {
        "local_query": "GSE132080",
        "gcs_raw": "gs://scperturb/pert-gym/staging/data/main/prism_collection/GSE132080.h5ad",
        "gcs_processed": None,
        "lamin_query": "GSE132080",
        "note": "A staged source H5AD and a published Lamin obs/X/var triplet.",
    },
    "XAtlas HCT116": {
        "local_query": "xatlas",
        "gcs_raw": "gs://scperturb/pert-gym/staging/sources/xatlas/orion/hct116/",
        "gcs_processed": "gs://scperturb/pert-gym/staging/xatlas/orion/hct116_filtered_dual_guide_cells/logical_sparse_zarr/",
        "lamin_query": "xatlas/orion/hct116",
        "note": "Huge raw source, processed sparse-Zarr staging, and many Lamin chunks.",
    },
    "GSE216481": {
        "local_query": "GSE216481",
        "gcs_raw": "gs://scperturb/pert-gym/staging/data/main/temporal_pretraining/perturbase_t29/",
        "gcs_processed": "gs://scperturb/pert-gym/staging/pert-gym/logical/perturbase_gse216481/",
        "lamin_query": "GSE216481",
        "note": "Raw and processed staging plus currently visible Lamin chunks.",
    },
    "Artista T37": {
        "local_query": "t37_artista",
        "gcs_raw": "gs://scperturb/pert-gym/staging/data/main/t37_artista/",
        "gcs_processed": None,
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
""",
            "local-options",
        ),
        code(
            """
def scan_local_data(roots, max_files=MAX_LOCAL_FILES):
    rows = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if len(rows) >= max_files:
                break
            try:
                path_lower = str(path).lower()
                if "openclaw-repair" in path_lower:
                    continue
                if path.is_file() and path.suffix.lower() in DATA_SUFFIXES:
                    rows.append({
                        "root": str(root),
                        "path": str(path),
                        "name": path.name,
                        "suffix": path.suffix.lower(),
                        "bytes": path.stat().st_size,
                        "size": human_bytes(path.stat().st_size),
                    })
            except OSError:
                continue
    return pd.DataFrame(rows)


local_files = scan_local_data(LOCAL_ROOTS)
print(f"Found {len(local_files):,} local data-like files")
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
## 3. Browse actual GCS staging

The following helper lists exactly one hierarchy level. It never downloads an
object and never performs a recursive walk. This matters because the staging
bucket currently contains very large raw files and sparse-Zarr trees with huge
physical object counts.
""",
            "gcs-section",
        ),
        code(
            """
RAW_GCS_ROOTS = [
    "gs://scperturb/pert-gym/staging/data/main/",
    "gs://scperturb/pert-gym/staging/manual_downloads/",
    "gs://scperturb/pert-gym/staging/manual_temporal/",
    "gs://scperturb/pert-gym/staging/sources/",
]
PROCESSED_GCS_ROOTS = [
    "gs://scperturb/pert-gym/staging/pert-gym/logical/",
    "gs://scperturb/pert-gym/staging/xatlas/orion/",
]
GCLOUD_TIMEOUT_SECONDS = 120
""",
            "gcs-options",
        ),
        code(
            """
def list_gcs_level(uri, timeout=GCLOUD_TIMEOUT_SECONDS):
    if not uri.startswith("gs://"):
        raise ValueError("Expected a gs:// URI")
    command = ["gcloud", "storage", "ls", "--long", uri]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    if completed.returncode != 0:
        return pd.DataFrame([{"kind": "error", "uri": uri, "message": completed.stderr.strip()}])
    rows = []
    for line in completed.stdout.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("TOTAL:"):
            continue
        parts = stripped.split()
        if stripped.startswith("gs://") and stripped.endswith("/"):
            rows.append({"kind": "prefix", "uri": stripped, "bytes": None, "size": ""})
        elif len(parts) >= 3 and parts[0].isdigit() and parts[-1].startswith("gs://"):
            size = int(parts[0])
            rows.append({"kind": "object", "uri": parts[-1], "bytes": size, "size": human_bytes(size)})
        elif stripped.startswith("gs://"):
            rows.append({"kind": "object", "uri": stripped, "bytes": None, "size": ""})
    return pd.DataFrame(rows)
""",
            "gcs-helper",
        ),
        md(
            """
### Raw/source staging roots

These are live directory-level listings. Open one by copying its URI into
`GCS_BROWSE_URI` below. The command does not infer processing status from a report;
it shows what GCS currently contains.
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
### Processed/logical staging roots

A prefix under `pert-gym/logical/` generally contains processed candidate data,
revisions, chunks, manifests or Zarr arrays. Its existence still does not prove
that a matching artifact was registered in Lamin; we check Lamin separately.
""",
            "gcs-processed-roots-explain",
        ),
        code(
            """
processed_root_tables = []
for root in PROCESSED_GCS_ROOTS:
    table = list_gcs_level(root)
    if len(table):
        table.insert(0, "root", root)
        processed_root_tables.append(table)
processed_root_listing = pd.concat(processed_root_tables, ignore_index=True) if processed_root_tables else pd.DataFrame()
processed_root_listing.head(100)
""",
            "gcs-processed-roots",
        ),
        md(
            """
### Build a live inventory of meaningful local and GCS dataset entries

The top of `manual_downloads/` contains dates rather than datasets, while
`pert-gym/logical/temporal/` contains another dataset level. These roots descend
to the first meaningful dataset/family name without recursively walking payloads.
The resulting table contains real paths and prefixes, not a saved progress export.
""",
            "layer-inventory-explain",
        ),
        code(
            """
MEANINGFUL_GCS_ROOTS = {
    "raw/source staged": [
        "gs://scperturb/pert-gym/staging/data/main/",
        "gs://scperturb/pert-gym/staging/manual_downloads/2026-06-23/downloads_cleanup/",
        "gs://scperturb/pert-gym/staging/manual_temporal/2026-06-23/",
        "gs://scperturb/pert-gym/staging/sources/xatlas/orion/",
    ],
    "processed/logical staged": [
        "gs://scperturb/pert-gym/staging/pert-gym/logical/",
        "gs://scperturb/pert-gym/staging/pert-gym/logical/temporal/",
    ],
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

The preset supplies an exact raw URI and, when known, an exact processed URI.
Listings remain one level deep. Change either URI to descend into a child prefix.
""",
            "gcs-selected-explain",
        ),
        code(
            """
selected_gcs_raw = list_gcs_level(selection["gcs_raw"]) if selection.get("gcs_raw") else pd.DataFrame()
selected_gcs_processed = (
    list_gcs_level(selection["gcs_processed"])
    if selection.get("gcs_processed")
    else pd.DataFrame()
)
print("RAW/STAGED SOURCE:", selection.get("gcs_raw"))
display(selected_gcs_raw.head(100))
print("PROCESSED/LOGICAL STAGING:", selection.get("gcs_processed"))
display(selected_gcs_processed.head(100))
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

- raw/source location where preprocessing is not demonstrated;
- processed/logical GCS location without a same-key Lamin match;
- a real Lamin artifact match, while preserving any remaining source/staging copy.

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
processed_tokens = set(
    classified.loc[classified["layer"] == "processed/logical staged", "dataset_token"].str.lower()
)


def evidence_category(row):
    if row["lamin_artifact_matches"] > 0:
        return "present in LaminDB (source/staging copy may remain)"
    if row["layer"] == "processed/logical staged" or row["dataset_token"].lower() in processed_tokens:
        return "processed/logical staged; no same-key Lamin match"
    return "raw/downloaded candidate; preprocessing not demonstrated"


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
    classified["evidence_category"] == "raw/downloaded candidate; preprocessing not demonstrated"
]
raw_candidates[["dataset_token", "layer", "location", "lamin_search_term"]].head(200)
""",
            "raw-candidates",
        ),
        md(
            """
#### B. Processed/logical staging without a same-key Lamin match

These are the closest answer to “preprocessed and staged, but not yet in
LaminDB.” The exact revision beneath each prefix still needs inspection before
publication.
""",
            "processed-candidates-explain",
        ),
        code(
            """
processed_not_lamin = classified[
    classified["evidence_category"] == "processed/logical staged; no same-key Lamin match"
]
processed_not_lamin[["dataset_token", "location", "lamin_search_term"]].head(200)
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
elif (selected_obs.size or 0) > MAX_LAMIN_METADATA_BYTES:
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
gcs_raw_present = bool(len(selected_gcs_raw)) and not (
    len(selected_gcs_raw) == 1 and selected_gcs_raw.iloc[0].get("kind") == "error"
)
gcs_processed_present = bool(len(selected_gcs_processed)) and not (
    len(selected_gcs_processed) == 1 and selected_gcs_processed.iloc[0].get("kind") == "error"
)
lamin_present = lamin_match_count > 0

if lamin_present:
    primary_state = "present in LaminDB"
elif gcs_processed_present:
    primary_state = "processed/staged on GCS; no matching Lamin key found"
elif local_present or gcs_raw_present:
    primary_state = "downloaded/raw staged; no processed GCS or matching Lamin key found"
else:
    primary_state = "not found with the configured locations/aliases"

location_summary = pd.DataFrame([
    {"layer": "local working/download", "present": local_present, "location": ", ".join(str(p) for p in LOCAL_ROOTS), "matches": len(local_matches)},
    {"layer": "GCS raw/source staging", "present": gcs_raw_present, "location": selection.get("gcs_raw"), "matches": len(selected_gcs_raw)},
    {"layer": "GCS processed/logical", "present": gcs_processed_present, "location": selection.get("gcs_processed"), "matches": len(selected_gcs_processed)},
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
  copies still exist. This is the current default `SCP211` example.
- **GCS processed + no Lamin match**: a genuine staged-publication candidate,
  subject to checking aliases and the exact intended revision.
- **Local/GCS raw only**: downloaded or source-staged, but preprocessing/publication
  is not demonstrated by these locations.
- **Lamin only**: durable managed data exist; old project staging may have been
  cleaned or may use a different prefix.

A substring miss is not a deletion claim. Adjust aliases before concluding that a
dataset is absent.
""",
            "overlap-interpretation",
        ),
        md(
            """
## 6. Try the useful examples

Change only `SELECTED_DATASET` near the top and rerun all cells:

- `SCP211`: raw copies plus Lamin triplets;
- `GSE132080`: a staged H5AD plus a Lamin triplet;
- `XAtlas HCT116`: huge raw source, processed sparse-Zarr, and many Lamin chunks;
- `GSE216481`: raw and logical staging plus Lamin chunks under a different key shape;
- `Artista T37`: local/staged working data with no same-key Lamin match.

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
