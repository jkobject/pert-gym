#!/usr/bin/env Rscript

# Export a VIPerturbSeq Seurat RDS into chunked sparse components.
#
# This intentionally avoids SeuratDisk/h5ad conversion for large genome-wide
# objects. Seurat matrices are feature x cell; chunks are written as feature x
# selected-cells MatrixMarket files and transposed later in Python.
#
# Usage:
#   Rscript tools/export_viperturb_rds_chunks.R input.rds output_dir \
#     --chunk-size 25000 --max-chunks 1 --assay RNA --overwrite

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: Rscript tools/export_viperturb_rds_chunks.R input.rds output_dir [--chunk-size N] [--max-chunks N] [--assay RNA] [--overwrite]", call. = FALSE)
}

input <- normalizePath(args[[1]], mustWork = TRUE)
output_dir <- args[[2]]
chunk_size <- 25000L
max_chunks <- NA_integer_
assay_name <- "RNA"
overwrite <- FALSE

i <- 3L
while (i <= length(args)) {
  arg <- args[[i]]
  if (arg == "--chunk-size") {
    i <- i + 1L
    chunk_size <- as.integer(args[[i]])
  } else if (arg == "--max-chunks") {
    i <- i + 1L
    max_chunks <- as.integer(args[[i]])
  } else if (arg == "--assay") {
    i <- i + 1L
    assay_name <- args[[i]]
  } else if (arg == "--overwrite") {
    overwrite <- TRUE
  } else {
    stop(paste("Unknown argument:", arg), call. = FALSE)
  }
  i <- i + 1L
}

if (is.na(chunk_size) || chunk_size <= 0L) {
  stop("--chunk-size must be a positive integer", call. = FALSE)
}

required <- c("Seurat", "SeuratObject", "Matrix")
missing <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) {
  stop(paste("Missing R packages:", paste(missing, collapse = ", ")), call. = FALSE)
}

if (dir.exists(output_dir)) {
  if (!overwrite) {
    stop(paste("Output dir exists; pass --overwrite:", output_dir), call. = FALSE)
  }
  unlink(output_dir, recursive = TRUE, force = TRUE)
}
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "chunks"), recursive = TRUE, showWarnings = FALSE)

message("READ_RDS ", input)
obj <- readRDS(input)
if (!inherits(obj, "Seurat")) {
  stop("Expected a Seurat object", call. = FALSE)
}
assays <- Seurat::Assays(obj)
if (!assay_name %in% assays) {
  stop(paste("Assay", assay_name, "not found. Available:", paste(assays, collapse = ",")), call. = FALSE)
}
Seurat::DefaultAssay(obj) <- assay_name
assay <- obj[[assay_name]]

mat <- tryCatch(
  SeuratObject::GetAssayData(obj, assay = assay_name, layer = "counts"),
  error = function(e) NULL
)
source_slot <- "counts_layer"
if (is.null(mat) || nrow(mat) == 0 || ncol(mat) == 0) {
  mat <- tryCatch(
    SeuratObject::GetAssayData(obj, assay = assay_name, layer = "data"),
    error = function(e) NULL
  )
  source_slot <- "data_layer"
}
if (is.null(mat) || nrow(mat) == 0 || ncol(mat) == 0) {
  mat <- tryCatch(
    SeuratObject::GetAssayData(obj, assay = assay_name, slot = "counts"),
    error = function(e) NULL
  )
  source_slot <- "counts_slot"
}
if (is.null(mat) || nrow(mat) == 0 || ncol(mat) == 0) {
  stop("Could not extract a non-empty assay matrix", call. = FALSE)
}
mat <- methods::as(mat, "dgCMatrix")

obs <- obj@meta.data
obs$cell_id <- rownames(obs)
utils::write.csv(obs, file.path(output_dir, "obs.csv"), row.names = FALSE, quote = TRUE)

var <- assay@meta.data
if (is.null(var) || nrow(var) == 0) {
  var <- data.frame(row.names = rownames(mat))
}
var$gene_id <- rownames(mat)
utils::write.csv(var, file.path(output_dir, "var.csv"), row.names = FALSE, quote = TRUE)

n_cells <- ncol(mat)
n_features <- nrow(mat)
n_chunks_total <- ceiling(n_cells / chunk_size)
n_chunks_export <- n_chunks_total
if (!is.na(max_chunks)) {
  n_chunks_export <- min(n_chunks_total, max_chunks)
}

manifest_chunks <- list()
for (chunk_idx in seq_len(n_chunks_export)) {
  start_1 <- ((chunk_idx - 1L) * chunk_size) + 1L
  end_1 <- min(chunk_idx * chunk_size, n_cells)
  chunk_name <- sprintf("chunk_%04d", chunk_idx - 1L)
  chunk_dir <- file.path(output_dir, "chunks", chunk_name)
  dir.create(chunk_dir, recursive = TRUE, showWarnings = FALSE)
  matrix_path <- file.path(chunk_dir, "matrix_features_by_cells.mtx")
  cell_ids_path <- file.path(chunk_dir, "cell_ids.txt")

  message("WRITE_CHUNK ", chunk_name, " start=", start_1 - 1L, " end=", end_1)
  chunk_mat <- mat[, start_1:end_1, drop = FALSE]
  Matrix::writeMM(chunk_mat, matrix_path)
  writeLines(colnames(mat)[start_1:end_1], cell_ids_path)

  manifest_chunks[[chunk_idx]] <- list(
    name = chunk_name,
    start = start_1 - 1L,
    end = end_1,
    n_cells = end_1 - start_1 + 1L,
    matrix = file.path("chunks", chunk_name, "matrix_features_by_cells.mtx"),
    cell_ids = file.path("chunks", chunk_name, "cell_ids.txt")
  )
  rm(chunk_mat)
  invisible(gc())
}

manifest <- list(
  input = input,
  assay = assay_name,
  source_slot = source_slot,
  n_cells_total = n_cells,
  n_features = n_features,
  chunk_size = chunk_size,
  n_chunks_total = n_chunks_total,
  n_chunks_exported = n_chunks_export,
  obs = "obs.csv",
  var = "var.csv",
  chunks = manifest_chunks
)

if (requireNamespace("jsonlite", quietly = TRUE)) {
  jsonlite::write_json(manifest, file.path(output_dir, "manifest.json"), auto_unbox = TRUE, pretty = TRUE)
} else {
  # Minimal JSON writer fallback for this manifest shape.
  esc <- function(x) gsub('"', '\\"', x, fixed = TRUE)
  con <- file(file.path(output_dir, "manifest.json"), open = "wt")
  on.exit(close(con), add = TRUE)
  writeLines("{", con)
  writeLines(sprintf('  "input": "%s",', esc(input)), con)
  writeLines(sprintf('  "assay": "%s",', esc(assay_name)), con)
  writeLines(sprintf('  "source_slot": "%s",', esc(source_slot)), con)
  writeLines(sprintf('  "n_cells_total": %d,', n_cells), con)
  writeLines(sprintf('  "n_features": %d,', n_features), con)
  writeLines(sprintf('  "chunk_size": %d,', chunk_size), con)
  writeLines(sprintf('  "n_chunks_total": %d,', n_chunks_total), con)
  writeLines(sprintf('  "n_chunks_exported": %d,', n_chunks_export), con)
  writeLines('  "obs": "obs.csv",', con)
  writeLines('  "var": "var.csv",', con)
  writeLines('  "chunks": [', con)
  for (j in seq_along(manifest_chunks)) {
    ch <- manifest_chunks[[j]]
    comma <- if (j < length(manifest_chunks)) "," else ""
    writeLines(sprintf('    {"name":"%s","start":%d,"end":%d,"n_cells":%d,"matrix":"%s","cell_ids":"%s"}%s', ch$name, ch$start, ch$end, ch$n_cells, ch$matrix, ch$cell_ids, comma), con)
  }
  writeLines("  ]", con)
  writeLines("}", con)
}

writeLines(capture.output(sessionInfo()), file.path(output_dir, "r_session_info.txt"))
cat("EXPORTED_CHUNKS", output_dir, "features", n_features, "cells", n_cells, "chunks", n_chunks_export, "\n")
