# =================================================================================================
# HIGH-SIGNAL AGGREGATE ENCODING (FE3) – AUTO VERSION
# -------------------------------------------------------------------------------------------------
# PURPOSE:
#   For each categorical variable and each numeric high-signal variable:
#       compute group-level statistics:
#           - mean(signal | category)
#           - sd(signal | category)
#   This creates highly predictive group-based features without target leakage.
#
#   Signal variable types are NOT required.
#   All signal variables receive mean + sd, and pruning will remove weak ones later.
#
# INPUT:
#   dt_list     : list(train, test)
#   cat_vars    : character vector of categorical variables
#   signal_vars : vector of strong-signal variable names (numeric variables)
#
# OUTPUT:
#   $train, $test, $new_features
# =================================================================================================

high_signal_agg_encode <- function(dt_list, cat_vars, signal_vars) {

  message("\n[INFO] Starting HIGH-SIGNAL aggregate encoding (FE3-AUTO)...")

  # --- Validate dt_list ---------------------------------------------------------
  if (!is.list(dt_list) || length(dt_list) != 2) {
    stop("[ERROR] dt_list must be list(train, test)")
  }

  train <- dt_list[[1]]
  test  <- dt_list[[2]]

  # Ensure both are data.table
  if (!inherits(train, "data.table")) train <- data.table::as.data.table(train)
  if (!inherits(test,  "data.table")) test  <- data.table::as.data.table(test)

  message("[INFO] Train and test are ready as data.table.")

  n_train <- nrow(train)

  # --- Combine datasets ---------------------------------------------------------
  message("[STEP 1] Combining train + test for consistent encoding...")
  dt <- data.table::rbindlist(list(train, test), use.names = TRUE, fill = TRUE)

  # --- Validate categorical variables -------------------------------------------
  message("[STEP 2] Validating categorical variables...")

  missing_cat <- setdiff(cat_vars, names(dt))
  if (length(missing_cat) > 0)
    stop(sprintf("[ERROR] Missing categorical variables: %s",
                 paste(missing_cat, collapse = ", ")))

  # --- Validate high-signal variables -------------------------------------------
  message("[STEP 3] Validating high-signal variables...")

  missing_sig <- setdiff(signal_vars, names(dt))
  if (length(missing_sig) > 0)
    stop(sprintf("[ERROR] Missing high-signal variables: %s",
                 paste(missing_sig, collapse = ", ")))

  # Ensure numeric
  for (s in signal_vars) {
    if (!is.numeric(dt[[s]])) {
      message(sprintf("[WARN] Converting '%s' to numeric...", s))
      dt[[s]] <- suppressWarnings(as.numeric(dt[[s]]))
    }
  }

  message("[INFO] High-signal variables validated.")

  # --- Begin encoding ------------------------------------------------------------
  message("[STEP 4] Generating aggregate encodings for all categories × signals...")

  new_features <- c()

  for (cat_col in cat_vars) {

    message(sprintf("[INFO] Processing category: %s", cat_col))

    for (sig in signal_vars) {

      message(sprintf("   • Aggregating signal: %s", sig))

      # Compute group-wise mean and sd of the signal
      tmp <- dt[, .(
        mean = mean(get(sig), na.rm = TRUE),
        sd   = stats::sd(get(sig), na.rm = TRUE)
      ), by = cat_col]

      # Rename columns
      new_cols <- paste0("fe3_", cat_col, "_", sig, "_", c("mean","sd"))
      setnames(tmp, old = c("mean","sd"), new = new_cols)

      # Merge into original dataset
      dt <- merge(dt, tmp, by = cat_col, all.x = TRUE, sort = FALSE)

      new_features <- c(new_features, new_cols)
    }
  }

  message(sprintf("[SUCCESS] Total FE3 aggregate features created: %d", length(new_features)))

  # --- Split back into train + test --------------------------------------------------------------
  message("[STEP 5] Splitting updated dataset back into train & test...")

  train_out <- dt[1:n_train]
  test_out  <- dt[(n_train + 1):nrow(dt)]

  message("[COMPLETE] High-signal aggregate encoding finished!\n")

  return(list(
    train = train_out,
    test  = test_out,
    new_features = new_features
  ))
}
