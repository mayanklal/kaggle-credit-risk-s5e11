fe_orig <- function(dt_orig, dt_list, cols, target, verbose = TRUE) {
  # ensure data.table
  data.table::setDT(dt_orig)

  # ensure target numeric
  dt_orig[, (target) := as.numeric(get(target))]

  for (col in cols) {
    
    if (verbose) message("Processing column: ", col)

    # ----------------------
    # 1. MEAN MAP (same dtype as original)
    # ----------------------
    mean_map <- dt_orig[, .(orig_mean = mean(get(target), na.rm = TRUE)), by = col]

    # ----------------------
    # 2. COUNT MAP
    # ----------------------
    count_map <- dt_orig[, .(orig_count = .N), by = col]

    # ----------------------
    # Apply to all datasets using LEFT JOIN (exact pandas merge)
    # ----------------------
    for (i in seq_along(dt_list)) {
      data.table::setDT(dt_list[[i]])

      # merge mean
      dt_list[[i]] <- merge(
        dt_list[[i]], mean_map,
        by = col, all.x = TRUE, sort = FALSE
      )
      data.table::setnames(dt_list[[i]], "orig_mean", paste0("orig_mean_", col))

      # merge count
      dt_list[[i]] <- merge(
        dt_list[[i]], count_map,
        by = col, all.x = TRUE, sort = FALSE
      )
      data.table::setnames(dt_list[[i]], "orig_count", paste0("orig_count_", col))
    }
  }

  return(dt_list)
}
