# =================================================================================================
# NUMERIC–NUMERIC INTERACTION GENERATOR
# -------------------------------------------------------------------------------------------------
# PURPOSE:
#   Given a dataset and a list of numeric variables, this function generates:
#     1. Multiplicative interactions: x * y
#     2. Ratio interactions: x / y and y / x
#     3. Quadratic features: x^2 and y^2
#     4. Scaled multiplicative interactions (useful for tree models)
#     5. Interactions with previously created quantile-binned numeric variables (qb_x)
#
# WHY THIS IS USEFUL:
#   ✔ Captures non-linear relationships
#   ✔ Adds expressive power to credit-risk models (DTI × Credit Score, etc.)
#   ✔ Interactions with quantile bins help tree models find stepwise risk patterns
#   ✔ Fully reusable across ML competitions
#
# INPUTS:
#   dt        : data.table or data.frame
#   num_vars  : vector of numeric column names
#
# RETURNS:
#   A list containing:
#       $dt             – updated data.table
#       $interaction_features – list of all new features created
#
# =================================================================================================

numeric_interactions <- function(dt, num_vars) {

  message("\n[INFO] Starting NUMERIC × NUMERIC interaction generation...")

  # --- Ensure dt is a data.table ---------------------------------------------------------------
  if (!inherits(dt, "data.table")) {
    message("[INFO] Converting input dataset to data.table...")
    dt <- data.table::as.data.table(dt)
  } else {
    message("[INFO] Input dataset is already a data.table.")
  }

  # --- Validate numeric variables --------------------------------------------------------------
  message("[STEP 1] Validating numeric variable list...")

  missing_vars <- setdiff(num_vars, names(dt))
  if (length(missing_vars) > 0) {
    stop(sprintf("[ERROR] Missing numeric variables: %s",
                 paste(missing_vars, collapse = ", ")))
  }

  # Ensure all numeric variables are numeric
  for (v in num_vars) {
    if (!is.numeric(dt[[v]])) {
      message(sprintf("[WARN] '%s' is not numeric. Attempting conversion...", v))
      dt[[v]] <- suppressWarnings(as.numeric(dt[[v]]))
    }
  }

  message("[INFO] Numeric validation complete.")

  # --- Detect quantile binned variables automatically -------------------------------------------
  qb_vars <- grep("^qb_", names(dt), value = TRUE)
  if (length(qb_vars) > 0) {
    message(sprintf("[INFO] Found %d quantile-binned variables: %s",
                    length(qb_vars), paste(qb_vars, collapse = ", ")))
  } else {
    message("[INFO] No quantile-binned variables found (qb_*). Skipping step 5.")
  }

  # --- Generate interactions --------------------------------------------------------------------
  message("[STEP 2] Generating numeric interactions...")

  interaction_features <- c()

  # 2A. Pairwise numeric combinations -------------------------------------------------------------
  if (length(num_vars) >= 2) {

    combos <- combn(num_vars, 2, simplify = FALSE)

    for (pair in combos) {

      v1 <- pair[[1]]
      v2 <- pair[[2]]

      message(sprintf("[INFO] Processing interaction pair: %s × %s", v1, v2))

      # --- (1) Multiplicative Interaction -------------------------------------------------------
      col_mult <- paste0("num_int_", v1, "_x_", v2)
      dt[[col_mult]] <- dt[[v1]] * dt[[v2]]
      interaction_features <- c(interaction_features, col_mult)

      # --- (2) Ratio Interactions ---------------------------------------------------------------
      col_ratio1 <- paste0("num_int_", v1, "_over_", v2)
      col_ratio2 <- paste0("num_int_", v2, "_over_", v1)

      dt[[col_ratio1]] <- dt[[v1]] / (dt[[v2]] + 1e-6)
      dt[[col_ratio2]] <- dt[[v2]] / (dt[[v1]] + 1e-6)

      interaction_features <- c(interaction_features, col_ratio1, col_ratio2)

      # --- (3) Quadratic (x^2 and y^2) ----------------------------------------------------------
      col_sq1 <- paste0("num_int_", v1, "_sq")
      col_sq2 <- paste0("num_int_", v2, "_sq")

      dt[[col_sq1]] <- dt[[v1]]^2
      dt[[col_sq2]] <- dt[[v2]]^2

      interaction_features <- c(interaction_features, col_sq1, col_sq2)

      # --- (4) Scaled multiplicative interaction ------------------------------------------------
      col_scaled <- paste0("num_int_scaled_", v1, "_x_", v2)
      dt[[col_scaled]] <- round(dt[[v1]] * 1000) * dt[[v2]]

      interaction_features <- c(interaction_features, col_scaled)

      # --- (5) Interaction with quantile bins ---------------------------------------------------
      if (length(qb_vars) > 0) {
        for (qb in qb_vars) {

          col_qb <- paste0("num_int_", v1, "_x_", qb)
          dt[[col_qb]] <- dt[[v1]] * dt[[qb]]

          col_qb2 <- paste0("num_int_", v2, "_x_", qb)
          dt[[col_qb2]] <- dt[[v2]] * dt[[qb]]

          interaction_features <- c(interaction_features, col_qb, col_qb2)
        }
      }
    }
  } else {
    message("[WARN] Only one numeric variable provided → no interactions generated.")
  }

  message(sprintf("[SUCCESS] Total new numeric interaction features created: %d",
                  length(interaction_features)))

  message("[COMPLETE] Numeric × Numeric interactions generated successfully!\n")

  # --- Return updated dataset -------------------------------------------------------------------
  return(list(
    dt = dt,
    interaction_features = interaction_features
  ))
}
