#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2) {
  stop("Usage: Rscript tools/export_viperturb_rds_components.R input.rds output_dir", call. = FALSE)
}

required <- c("Seurat", "Matrix")
missing <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) {
  stop(paste("Missing R packages:", paste(missing, collapse = ", ")), call. = FALSE)
}

input <- normalizePath(args[[1]], mustWork = TRUE)
output_dir <- args[[2]]
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

obj <- readRDS(input)
if (!inherits(obj, "Seurat")) {
  stop("Expected a Seurat object", call. = FALSE)
}
if (!"RNA" %in% Seurat::Assays(obj)) {
  stop("Seurat object has no RNA assay", call. = FALSE)
}

Seurat::DefaultAssay(obj) <- "RNA"
rna <- obj[["RNA"]]

mat <- tryCatch(
  SeuratObject::GetAssayData(obj, assay = "RNA", layer = "counts"),
  error = function(e) NULL
)
if (is.null(mat) || nrow(mat) == 0 || ncol(mat) == 0) {
  mat <- tryCatch(
    SeuratObject::GetAssayData(obj, assay = "RNA", layer = "data"),
    error = function(e) NULL
  )
}
if (is.null(mat) || nrow(mat) == 0 || ncol(mat) == 0) {
  mat <- tryCatch(
    SeuratObject::GetAssayData(obj, assay = "RNA", slot = "counts"),
    error = function(e) NULL
  )
}
if (is.null(mat) || nrow(mat) == 0 || ncol(mat) == 0) {
  stop("Could not extract a non-empty RNA counts/data matrix", call. = FALSE)
}

mat <- methods::as(mat, "dgCMatrix")
Matrix::writeMM(mat, file.path(output_dir, "matrix_features_by_cells.mtx"))

obs <- obj@meta.data
obs$cell_id <- rownames(obs)
utils::write.csv(obs, file.path(output_dir, "obs.csv"), row.names = FALSE, quote = TRUE)

var <- rna@meta.data
if (is.null(var) || nrow(var) == 0) {
  var <- data.frame(row.names = rownames(mat))
}
var$gene_id <- rownames(mat)
utils::write.csv(var, file.path(output_dir, "var.csv"), row.names = FALSE, quote = TRUE)

writeLines(capture.output(sessionInfo()), file.path(output_dir, "r_session_info.txt"))
cat("EXPORTED", output_dir, nrow(mat), ncol(mat), "\n")
