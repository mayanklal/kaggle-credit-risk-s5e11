# =================================================================================================
# HIGH-SIGNAL CROSS FEATURE GENERATOR (TRAIN + TEST VERSION)
# -------------------------------------------------------------------------------------------------
# This function:
#   ✔ Accepts TRAIN and TEST as a list (or two separate objects)
#   ✔ Combines them to guarantee consistent factor levels
#   ✔ Factorizes categorical variables into integer IDs
#   ✔ Detects scaling for high-signal variables automatically
#   ✔ Creates cross-features:
#         hs_cat_signal
#         hs_cat_signal1_signal2
#   ✔ Splits the data back into train and test
#   ✔ Returns updated datasets + list of new features + scaling map
#
# This implementation is fully reusable for future ML competitions.
# =================================================================================================

high_signal_encode <- function(dt_list, cat_vars, signal_vars) {

  message("\n[INFO] Starting HIGH-SIGNAL encoding for TRAIN + TEST...")

  # --- Validate input ---------------------------------------------------------------------------
  if (!is.list(dt_list) || length(dt_list) != 2) {
    stop("dt_list must be a list of length 2: list(train, test)")
  }

  train <- dt_list[[1]]
  test  <- dt_list[[2]]

  # Convert to data.table if necessary
  if (!inherits(train, "data.table")) train <- data.table::as.data.table(train)
  if (!inherits(test, "data.table"))  test  <- data.table::as.data.table(test)

  message("[INFO] Both train and test converted to data.table.")

  n_train <- nrow(train)

  # --- Combine train and test -------------------------------------------------------------------
  message("[STEP 1] Combining train + test for consistent encoding...")
  dt <- data.table::rbindlist(list(train, test), use.names = TRUE, fill = TRUE)

  # --- Helper: Detect appropriate scaling -------------------------------------------------------
  detect_scale <- function(x) {

    if (all(x %in% c(0,1), na.rm = TRUE)) return(1)         # Binary
    if (all(abs(x - round(x)) < 1e-6, na.rm = TRUE)) return(100) # Integer

    decimal_places <- max(
      nchar(sub("^[^.]*\\.?","", sprintf("%.10f", x))),
      na.rm = TRUE
    )
    return(10^decimal_places)
  }

  # --- Factorize categorical variables -----------------------------------------------------------
  message("[STEP 2] Factorizing categorical variables globally...")

  factorize_column <- function(v) as.integer(factor(v))

  for (c in cat_vars) {
    dt[[c]] <- factorize_column(dt[[c]])
  }

  message("[INFO] Factorization done.")

  # --- Detect scaling for each high-signal variable ---------------------------------------------
  message("[STEP 3] Detecting scale for each signal variable...")

  scale_map <- list()
  for (s in signal_vars) {
    scale_map[[s]] <- detect_scale(dt[[s]])
    message(sprintf("[INFO] Scale for %s → %s", s, scale_map[[s]]))
  }

  # --- Generate high-signal cross-features ------------------------------------------------------
  message("[STEP 4] Creating high-signal interaction features...")

  new_features <- c()

  # 4A. cat × signal
  message("[4A] Generating cat × signal features...")
  for (c in cat_vars) {
    for (s in signal_vars) {
      sc <- scale_map[[s]]
      new_col <- paste0("hs_", c, "_", s)

      dt[[new_col]] <- dt[[c]] * (sc * 10) + round(dt[[s]] * sc)

      new_features <- c(new_features, new_col)
    }
  }

  # 4B. cat × signal1 × signal2
  if (length(signal_vars) >= 2) {

    message("[4B] Generating cat × signal1 × signal2 features...")

    combos <- combn(signal_vars, 2, simplify = FALSE)

    for (pair in combos) {
      s1 <- pair[[1]]
      s2 <- pair[[2]]
      sc1 <- scale_map[[s1]]
      sc2 <- scale_map[[s2]]

      for (c in cat_vars) {

        new_col <- paste0("hs_", c, "_", s1, "_", s2)

        dt[[new_col]] <-
          dt[[c]] * (sc1 * sc2 * 10) +
          round(dt[[s1]] * sc1) * 100 +
          round(dt[[s2]] * sc2)

        new_features <- c(new_features, new_col)
      }
    }
  }

  message(sprintf("[SUCCESS] Total new features created: %d", length(new_features)))

  # --- Split back into train and test -----------------------------------------------------------
  message("[STEP 5] Splitting combined dataset back into TRAIN and TEST...")

  train_out <- dt[1:n_train]
  test_out  <- dt[(n_train+1):nrow(dt)]

  message("[COMPLETE] High-signal encoding finished successfully!\n")

  return(list(
    train = train_out,
    test  = test_out,
    new_features = new_features,
    scale_map = scale_map
  ))
}
