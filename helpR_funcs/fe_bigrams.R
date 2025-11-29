# ============================================================
# Feature Engineering: Bigram Creation (Categorical Interactions)
# ------------------------------------------------------------
# This function creates "bigram" categorical features by 
# concatenating all pairwise combinations of the selected columns.
#
# Equivalent to the Python logic:
#   new_col = col1 + "_" + col2
#   df[new_col] = df[col1].astype(str) + "_" + df[col2].astype(str)
#
# Purpose:
#   - To introduce interaction features between categorical variables
#   - These reveal joint category patterns that single columns cannot capture
#   - Safe to run globally (no target used, no CV needed)
#
# Inputs:
#   - dt_list: list of data.tables, typically list(train, test, orig)
#   - cols: character vector of column names to combine (e.g., TE_BASE)
#
# Output:
#   - The function modifies the data.tables inside dt_list *in place*
#   - Returns dt_list invisibly for functional style if desired
#
# Example:
#   dt_list <- fe_bigrams(list(train_dt, test_dt), cols = c("grade","purpose"))
# ============================================================

fe_bigrams <- function(dt_list, cols) {

  # ---- Generate all pairwise column combinations ----
  # utils::combn() returns a list of length-2 vectors like c("col1", "col2")
  combos <- utils::combn(cols, 2, simplify = FALSE)

  # ---- Loop over all combinations ----
  for (pair in combos) {
    col1 <- pair[1]
    col2 <- pair[2]

    # Construct bigram feature name with "inter_" prefix
    # Example: inter_grade_purpose
    new_col <- paste0("inter_", col1, "_", col2)

    # ---- Apply to each data.table in the list ----
    # We create the concatenated string feature:
    #   "value_of_col1" + "_" + "value_of_col2"
    for (dt in dt_list) {
      dt[, (new_col) :=
            paste0(
              as.character(get(col1)),
              "_",
              as.character(get(col2))
            )]
      # NOTE:
      # - data.table := modifies in place (fast, memory efficient)
      # - get(col) retrieves column dynamically
      # - as.character() ensures safe concatenation even if column is factor
    }
  }

  # Return the dt_list invisibly (side-effect based; also functional if needed)
  invisible(dt_list)
}
