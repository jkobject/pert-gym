#!/usr/bin/env Rscript

# Convert VIPerturbSeq Seurat/RDS files to h5ad for downstream triplet ingestion.
#
# Usage:
#   Rscript tools/convert_viperturb_rds.R input.rds output.h5ad
#
# Requires R packages: Seurat, SeuratDisk.

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2) {
  stop("Usage: Rscript tools/convert_viperturb_rds.R input.rds output.h5ad", call. = FALSE)
}

required <- c("Seurat", "SeuratDisk")
missing <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) {
  stop(paste("Missing R packages:", paste(missing, collapse = ", ")), call. = FALSE)
}

input <- normalizePath(args[[1]], mustWork = TRUE)
output <- normalizePath(args[[2]], mustWork = FALSE)
h5seurat <- sub("\\.h5ad$", ".h5seurat", output, ignore.case = TRUE)

obj <- readRDS(input)
SeuratDisk::SaveH5Seurat(obj, filename = h5seurat, overwrite = TRUE)
SeuratDisk::Convert(h5seurat, dest = "h5ad", overwrite = TRUE)

if (!file.exists(output)) {
  generated <- sub("\\.h5seurat$", ".h5ad", h5seurat)
  if (file.exists(generated)) {
    file.rename(generated, output)
  }
}

if (!file.exists(output)) {
  stop(paste("Conversion did not produce", output), call. = FALSE)
}
