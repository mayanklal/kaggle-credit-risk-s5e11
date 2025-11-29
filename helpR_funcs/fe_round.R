# ============================================================
# Feature Engineering: Rounded Numeric Features (Python equivalent)
# ------------------------------------------------------------
# Creates new numeric features by rounding selected numeric columns
# at user-defined digit levels. Mirrors Python logic:
#
#   df[new_col] = df[col].round(level).astype(int)
#
# Example:
#   annual_income → annual_income_ROUND_1s   (round to nearest 1)
#   annual_income → annual_income_ROUND_10s  (round to nearest 10)
#
# Safe globally (no target used), deterministic, no CV needed.
#
# Inputs:
#   dt_list : list of data.tables (e.g., list(train, test, orig))
#   cols    : character vector of numeric column names to round
#   round_levels : named list of suffix → digits
#       e.g. list("1s" = 0, "10s" = -1)
#
# Output:
#   - Modifies dt_list in-place (fast via data.table :=)
#   - Returns dt_list invisibly
# ============================================================

fe_round <- function(dt_list,
                     cols,
                     round_levels = list(`1s` = 0, `10s` = -1)) {

  # ---- Loop through each selected numeric column ----
  for (col in cols) {

    # Loop through each rounding level
    for (suffix in names(round_levels)) {

      digits <- round_levels[[suffix]]

      # Column name exactly matching Python:
      #   annual_income_ROUND_1s
      new_col <- paste0("round_", col, "_" ,suffix)

      # ---- Apply to each data.table in the list ----
      for (dt in dt_list) {
        # round(x, digits) then convert to integer
        dt[, (new_col) := as.integer(round(get(col), digits))]
      }
    }
  }

  invisible(dt_list)
}
