# =================================================================================================
# QUANTILE BINNING + NUMERIC INTERACTION GENERATOR
# -------------------------------------------------------------------------------------------------
# PURPOSE:
#   This function takes a dataset and a list of numeric variables, then:
#     1. Ensures dataset is a data.table (no library() inside functions)
#     2. Creates quantile-binned versions of each numeric variable
#     3. Creates interaction features between ALL pairs of the numeric variables
#     4. Returns updated dataset + list of new features generated
#
# WHY THIS IS USEFUL:
#   ✔ Quantile bins replace continuous distributions with category-like buckets
#       → good for non-linear models
#   ✔ Tree models learn splits faster and more robustly with binned versions
#   ✔ Numeric × numeric interactions inject non-linear structure
#   ✔ Reduces model variance on extreme values
#
# OUTPUT:
#   A list containing:
#       $dt            → updated data.table
#       $bin_features  → list of quantile binned columns
#       $int_features  → list of interaction columns
#
# -------------------------------------------------------------------------------------------------
# Author: Your optimized reusable feature engineering module
# =================================================================================================

quantile_binning_numeric <- function(dt, num_vars, probs = seq(0, 1, 0.1)) {

  message("\n[INFO] Starting quantile binning + numeric interaction generation...")

  # --- Ensure dt is a data.table -----------------------------------------------------------------
  if (!inherits(dt, "data.table")) {
    message("[INFO] Converting input dataset to data.table...")
    dt <- data.table::as.data.table(dt)
  } else {
    message("[INFO] Input dataset is already a data.table.")
  }

  # --- Validate numeric variables ----------------------------------------------------------------
  message("[STEP 1] Validating numeric variables...")

  missing_vars <- setdiff(num_vars, names(dt))
  if (length(missing_vars) > 0) {
    stop(sprintf("[ERROR] These numeric variables are missing: %s",
                 paste(missing_vars, collapse = ", ")))
  }

  # Ensure variables are numeric
  for (v in num_vars) {
    if (!is.numeric(dt[[v]])) {
      message(sprintf("[WARN] Variable '%s' is not numeric. Attempting to convert...", v))
      dt[[v]] <- suppressWarnings(as.numeric(dt[[v]]))
    }
  }

  message("[INFO] Numeric validation complete.")

  # --- Start generating quantile bins -------------------------------------------------------------
  message("[STEP 2] Creating quantile bins...")

  bin_features <- c()

  for (v in num_vars) {
    new_col <- paste0("qb_", v)

    # Handle NA safely by computing quantiles only on non-NA rows
    qts <- unique(stats::quantile(dt[[v]], probs = probs, na.rm = TRUE))

    # Create quantile bin variable
    dt[[new_col]] <- as.integer(cut(dt[[v]], breaks = qts, include.lowest = TRUE))

    bin_features <- c(bin_features, new_col)

    message(sprintf("[INFO] Binned '%s' → '%s' (quantiles = %d)",
                    v, new_col, length(qts)))
  }

  message(sprintf("[SUCCESS] Total quantile-binned features created: %d", length(bin_features)))

  # --- Generate numeric-numeric interaction features ---------------------------------------------
  message("[STEP 3] Creating numeric interaction features...")

  int_features <- c()

  # Create all pairwise combinations
  if (length(num_vars) >= 2) {
    combos <- combn(num_vars, 2, simplify = FALSE)

    for (pair in combos) {
      v1 <- pair[[1]]
      v2 <- pair[[2]]

      new_col <- paste0("qb_int_", v1, "_", v2)

      dt[[new_col]] <- dt[[v1]] * dt[[v2]]  # simple and effective interaction

      int_features <- c(int_features, new_col)

      message(sprintf("[INFO] Created interaction '%s' × '%s' → '%s'",
                      v1, v2, new_col))
    }
  } else {
    message("[WARN] Only one numeric variable provided. No interactions generated.")
  }

  message(sprintf("[SUCCESS] Total numeric interaction features created: %d",
                  length(int_features)))

  message("[COMPLETE] Quantile binning + interactions successfully generated!\n")

  # --- Return updated dataset + metadata ----------------------------------------------------------
  return(list(
    dt = dt,
    bin_features = bin_features,
    int_features = int_features
  ))
}
