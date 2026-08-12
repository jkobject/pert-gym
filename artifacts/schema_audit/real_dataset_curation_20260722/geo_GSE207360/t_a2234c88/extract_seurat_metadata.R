suppressPackageStartupMessages(library(SeuratObject))

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2) {
  stop("usage: extract_seurat_metadata.R OUTPUT_TSV OUTPUT_SUMMARY")
}
input <- file("stdin", "rb")
on.exit(close(input), add = TRUE)
object <- readRDS(input)
metadata <- object@meta.data
write.table(
  metadata,
  file = args[[1]],
  sep = "\t",
  quote = TRUE,
  row.names = TRUE,
  col.names = NA,
  na = ""
)
counts <- object@assays$RNA@counts
summary <- list(
  object_class = class(object),
  seurat_version = as.character(object@version),
  metadata_rows = nrow(metadata),
  metadata_columns = colnames(metadata),
  counts_rows = nrow(counts),
  counts_columns = ncol(counts),
  counts_nnz = length(counts@x),
  counts_min = min(counts@x),
  counts_max = max(counts@x),
  counts_integral = all(counts@x == floor(counts@x)),
  counts_colnames_equal_metadata = identical(colnames(counts), rownames(metadata)),
  active_ident_levels = levels(Idents(object)),
  active_ident_counts = as.list(table(Idents(object)))
)
jsonlite::write_json(summary, path = args[[2]], auto_unbox = TRUE, pretty = TRUE)
