# ============================================================
# Fast + Parallel + data.table step_agg_encoding
# ============================================================
# Purpose:
#   For each selected predictor:
#     - Compute mean(target | level)
#     - Compute count(level)
#   using fast data.table aggregation
#   Parallelized across columns using future.apply::future_lapply
#
# NOTE:
#   Load BEFORE sourcing this file:
#     library(recipes)
#     library(data.table)
#     library(future.apply)
# ============================================================


# ------------------------------------------------------------
# User-facing constructor
# ------------------------------------------------------------
step_agg_encoding <- function(recipe, ..., role = "predictor", trained = FALSE,
                              skip = FALSE, id = rand_id("agg_encoding"),
                              columns = NULL, mappings = NULL, target = NULL) {

  add_step(
    recipe,
    step_agg_encoding_new(
      terms    = enquos(...),
      role     = role,
      trained  = trained,
      skip     = skip,
      id       = id,
      columns  = columns,
      mappings = mappings,
      target   = target
    )
  )
}

# ------------------------------------------------------------
# Internal constructor
# ------------------------------------------------------------
step_agg_encoding_new <- function(terms, role, trained, skip, id,
                                  columns, mappings, target) {

  step(
    subclass = "agg_encoding",
    terms    = terms,
    role     = role,
    trained  = trained,
    skip     = skip,
    id       = id,
    columns  = columns,
    mappings = mappings,
    target   = target
  )
}

# ------------------------------------------------------------
# PREP (FAST): Compute mapping tables using data.table + parallel
# ------------------------------------------------------------
prep.step_agg_encoding <- function(x, training, info = NULL, ...) {

  # Resolve selected columns
  cols <- recipes::recipes_eval_select(x$terms, training, info)

  # Identify target column
  target_col <- info$variable[info$role == "outcome"]

  # Convert training to data.table
  DT <- data.table::as.data.table(training)

  # Convert factor target → numeric for aggregation
  if (is.factor(DT[[target_col]])) {
  DT[, (target_col) := as.numeric(as.character(get(target_col)))]
  }

  # Parallel processing across columns
  mappings_list <- future.apply::future_lapply(
    cols,
    function(col) {
      # Coerce key column to character for consistent joining
      DT[, (col) := as.character(get(col))]

      # FAST mean aggregation
      mean_dt <- DT[, .(val = mean(get(target_col), na.rm = TRUE)), by = col]
      data.table::setnames(mean_dt, "val", paste0("agg_mean_", col))

      # FAST count aggregation
      count_dt <- DT[, .N, by = col]
      data.table::setnames(count_dt, "N", paste0("agg_count_", col))

      # Return mapping list for this column
      list(
        mean_map  = data.table::copy(mean_dt),
        count_map = data.table::copy(count_dt)
      )
    },
    future.seed = TRUE
  )

  names(mappings_list) <- cols

  # Return trained step
  step_agg_encoding_new(
    terms    = x$terms,
    role     = x$role,
    trained  = TRUE,
    skip     = x$skip,
    id       = x$id,
    columns  = cols,
    mappings = mappings_list,
    target   = target_col
  )
}

# ------------------------------------------------------------
# BAKE (FAST): Join encoding tables to new_data using data.table
# ------------------------------------------------------------
bake.step_agg_encoding <- function(object, new_data, ...) {

  DT_new <- data.table::as.data.table(new_data)

  for (col in object$columns) {

    # Convert join key to character
    DT_new[, (col) := as.character(get(col))]

    # Extract mapping tables
    mean_map  <- object$mappings[[col]]$mean_map
    count_map <- object$mappings[[col]]$count_map

    # Ensure keys
    data.table::setkeyv(mean_map, col)
    data.table::setkeyv(count_map, col)
    data.table::setkeyv(DT_new, col)

    # Join mean encoding
    DT_new <- mean_map[DT_new]

    # Join count encoding
    DT_new <- count_map[DT_new]
  }

  tibble::as_tibble(DT_new)

}

# ------------------------------------------------------------
# PRINT METHOD
# ------------------------------------------------------------
print.step_agg_encoding <- function(x, ...) {
  cat("step_agg_encoding (FAST): mean + count encoding for ",
      paste(x$columns, collapse = ", "), "\n", sep = "")
  invisible(x)
}
