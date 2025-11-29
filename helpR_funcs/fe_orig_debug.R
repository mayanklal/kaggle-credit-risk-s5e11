# fe_orig_debug: ORIG mean + count encoding with detailed diagnostics
# - dt_orig: original (real) dataset (data.frame or data.table)
# - dt_list: list of target data.tables to enrich (train, test, ...). Will be returned enriched.
# - cols:   vector of candidate columns (BASE). Only intersection with dt_orig will be used.
# - target: name of target column in dt_orig (string)
# - verbose: if TRUE, message() lines are printed (default TRUE)
#
# Returns: list(enriched = dt_list, diagnostics = diag_list)
fe_orig_debug <- function(dt_orig, dt_list, cols, target, verbose = TRUE) {
  library(data.table)

  diag_list <- list()
  # enforce data.tables
  setDT(dt_orig)

  # diag: original names
  diag_list$orig_names <- names(dt_orig)

  # 0. basic checks
  if (!target %in% names(dt_orig)) {
    stop("Target column '", target, "' not found in dt_orig. Found: ", paste(names(dt_orig), collapse = ", "))
  }

  # 1. restrict columns to intersection with orig
  common_cols <- intersect(cols, names(dt_orig))
  missing_in_orig <- setdiff(cols, common_cols)
  diag_list$requested_cols <- cols
  diag_list$common_cols <- common_cols
  diag_list$missing_in_orig <- missing_in_orig

  if (verbose) {
    message("Requested cols: ", length(cols), " | Present in orig: ", length(common_cols),
            " | Missing in orig: ", length(missing_in_orig))
    if (length(missing_in_orig) > 0) {
      message("Missing in orig (first 10): ", paste(head(missing_in_orig, 10), collapse = ", "))
    }
  }

  if (length(common_cols) == 0) {
    warning("No requested columns found in dt_orig – nothing to do.")
    return(list(enriched = dt_list, diagnostics = diag_list))
  }

  # 2. ensure target numeric (for mean)
  if (is.factor(dt_orig[[target]])) {
    if (verbose) message("Converting target factor -> numeric in dt_orig")
    dt_orig[, (target) := as.numeric(as.character(get(target)))]
  } else if (!is.numeric(dt_orig[[target]])) {
    if (verbose) message("Coercing target to numeric in dt_orig")
    dt_orig[, (target) := as.numeric(get(target))]
  }

  # 3. ensure columns are character for stable joins/merges
  dt_orig[, (common_cols) := lapply(.SD, as.character), .SDcols = common_cols]

  # 4. build mapping tables (mean + count) and collect diagnostics
  mapping_list <- vector("list", length(common_cols))
  names(mapping_list) <- common_cols
  diag_list$maps <- list()

  for (col in common_cols) {
    # compute map
    map_dt <- dt_orig[, .(orig_mean = mean(get(target), na.rm = TRUE),
                          orig_count = .N),
                      by = col]
    # rename to exact naming
    mean_name  <- paste0("orig_mean_", col)
    count_name <- paste0("orig_count_", col)
    setnames(map_dt, c("orig_mean", "orig_count"), c(mean_name, count_name))

    mapping_list[[col]] <- map_dt

    # diagnostics for this mapping
    diag_list$maps[[col]] <- list(
      n_map_rows = nrow(map_dt),
      sample_map_head = head(map_dt, 5),
      mean_col_name = mean_name,
      count_col_name = count_name,
      unique_keys_in_orig = uniqueN(dt_orig[[col]])
    )

    if (verbose) {
      message("Built map for '", col, "': map_rows=", nrow(map_dt),
              " | unique_keys_in_orig=", diag_list$maps[[col]]$unique_keys_in_orig)
      message(" map sample: ")
      print(diag_list$maps[[col]]$sample_map_head)
    }
  }

  # 5. Apply mappings to each dataset in dt_list with diagnostics
  diag_list$applies <- list()
  for (i in seq_along(dt_list)) {
    # ensure dt_list[[i]] is data.table and clean
    dt <- as.data.table(dt_list[[i]])
    # convert keys to character
    dt[, (common_cols) := lapply(.SD, as.character), .SDcols = common_cols]

    per_dataset_diag <- list(dataset_index = i,
                             original_rows = nrow(dt),
                             original_cols = names(dt),
                             before_na_summary = list())

    if (verbose) message("Applying maps to dataset index ", i, " (nrows=", nrow(dt), ")")

    # for each col: merge and record how many matched
    for (col in common_cols) {
      map_dt <- mapping_list[[col]]
      mean_name  <- paste0("orig_mean_", col)
      count_name <- paste0("orig_count_", col)

      # record how many distinct keys exist in dt and in map
      keys_in_dt   <- unique(dt[[col]])
      keys_in_map  <- unique(map_dt[[col]])
      n_keys_dt    <- length(keys_in_dt)
      n_keys_map   <- length(keys_in_map)
      keys_intersect <- intersect(keys_in_dt, keys_in_map)
      n_keys_intersect <- length(keys_intersect)

      if (verbose) {
        message(sprintf("  col='%s': keys_dt=%d | keys_map=%d | intersect=%d",
                        col, n_keys_dt, n_keys_map, n_keys_intersect))
      }

      # DEBUG: show sample of values that are in dt but not in map (up to 10)
      missing_keys_sample <- setdiff(keys_in_dt, keys_in_map)
      if (length(missing_keys_sample) > 0 && verbose) {
        message("    sample keys in dt but not in orig (up to 10): ", paste(head(missing_keys_sample, 10), collapse = ", "))
      }

      # Perform left merge of map onto dt (keep all dt rows)
      # Use base data.table merge which returns new dt
      merged_dt <- merge(dt, map_dt, by = col, all.x = TRUE, sort = FALSE)

      # After merge: check presence & NA proportion
      mean_na_prop  <- if (mean_name %in% names(merged_dt)) {
        mean(is.na(merged_dt[[mean_name]]))
      } else NA_real_
      count_na_prop <- if (count_name %in% names(merged_dt)) {
        mean(is.na(merged_dt[[count_name]]))
      } else NA_real_

      if (verbose) {
        message(sprintf("    after merge -> %s NA%%=%.2f | %s NA%%=%.2f",
                        mean_name, mean_na_prop * 100,
                        count_name, count_na_prop * 100))
      }

      # attach merged_dt back to dt for next iteration
      dt <- merged_dt

      # store per-column diagnostics for this dataset
      per_dataset_diag[[col]] <- list(
        keys_dt = n_keys_dt,
        keys_map = n_keys_map,
        keys_intersect = n_keys_intersect,
        mean_col = mean_name,
        count_col = count_name,
        mean_na_prop = mean_na_prop,
        count_na_prop = count_na_prop
      )
    } # end per-col loop

    # final dataset-level diagnostics
    per_dataset_diag$final_ncols <- ncol(dt)
    per_dataset_diag$final_cols <- names(dt)

    # store
    diag_list$applies[[paste0("dataset_", i)]] <- per_dataset_diag

    # assign back
    dt_list[[i]] <- dt
  }

  # 6. return enriched data.tables + diagnostics
  return(list(enriched = dt_list, diagnostics = diag_list))
}
