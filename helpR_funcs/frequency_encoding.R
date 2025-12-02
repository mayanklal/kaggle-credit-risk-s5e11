# =================================================================================================
# FREQUENCY ENCODING FOR CATEGORICAL VARIABLES
# -------------------------------------------------------------------------------------------------
# PURPOSE:
#   Given a dataset and a list of categorical variables, this function:
#       1. Ensures dt is a data.table
#       2. Validates that supplied categorical variables exist
#       3. Computes frequency encoding for each categorical variable:
#              freq_<col> = number of occurrences of each category
#       4. Adds all new freq_* variables to the dataset
#
# WHY THIS IS USEFUL:
#   ✔ Simple, fast encoding for categorical variables
#   ✔ No leakage since applied per dataset
#   ✔ Works extremely well with tree models
#   ✔ Fully reusable across ML competitions
#
# INPUTS:
#   dt        : data.table or data.frame
#   cat_vars  : vector of categorical column names
#
# RETURNS:
#   A list containing:
#       $dt             – updated data.table with freq_* columns
#       $freq_features  – vector of all freq_* feature names created
#
# =================================================================================================

frequency_encoding <- function(dt, cat_vars) {

  message("\n[INFO] Starting FREQUENCY ENCODING for categorical variables...")

  # --- Ensure dt is a data.table ---------------------------------------------------------------
  if (!inherits(dt, "data.table")) {
    message("[INFO] Converting input dataset to data.table...")
    dt <- data.table::as.data.table(dt)
  } else {
    message("[INFO] Input dataset is already a data.table.")
  }

  # --- Validate categorical variables -----------------------------------------------------------
  message("[STEP 1] Validating provided categorical variables...")

  missing_vars <- setdiff(cat_vars, names(dt))
  if (length(missing_vars) > 0) {
    stop(sprintf("[ERROR] These categorical variables are missing: %s",
                 paste(missing_vars, collapse = ", ")))
  }

  message("[INFO] Categorical variable validation complete.")

  # --- Begin frequency encoding -----------------------------------------------------------------
  message(sprintf("[STEP 2] Creating frequency features for %d categorical variables...",
                  length(cat_vars)))

  freq_features <- c()

  for (cc in cat_vars) {

    message(sprintf("\n[INFO] Processing categorical variable: '%s'", cc))

    # ------------------------------------------------------
    # Count occurrences of each category (like value_counts)
    # ------------------------------------------------------
    freq_tbl <- table(dt[[cc]])
    freq_map <- as.numeric(freq_tbl)
    names(freq_map) <- names(freq_tbl)

    # ------------------------------------------------------
    # Map frequencies back to the full dataset
    # ------------------------------------------------------
    new_col <- paste0("freq_", cc)
    dt[[new_col]] <- freq_map[as.character(dt[[cc]])]

    # Handle categories that did not appear in freq_tbl
    na_idx <- which(is.na(dt[[new_col]]))
    if (length(na_idx) > 0) {
      replacement_val <- mean(freq_map)
      dt[[new_col]][na_idx] <- replacement_val
      message(sprintf("[WARN] '%s' had unseen categories (%d rows). Replaced with mean freq %0.3f.",
                      cc, length(na_idx), replacement_val))
    }

    freq_features <- c(freq_features, new_col)

    message(sprintf("[SUCCESS] Created frequency feature: '%s'", new_col))
  }

  message("\n[COMPLETE] Frequency encoding successfully completed!\n")

  # --- Return updated dataset + freq_* feature list -------------------------------------------
  return(list(
    dt = dt,
    freq_features = freq_features
  ))
}
