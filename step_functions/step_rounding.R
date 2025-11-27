# Custom tidymodels recipe step: step_round_feats
# Purpose:
#   For each selected numeric column, create new features by rounding the value
#   at specified digit levels (e.g., 1s → 0 decimals, 10s → -1 decimals).
#   Output column names match the Python logic: col + "_ROUND_" + suffix.

# -------------------------------------------------------------------
# User-facing constructor: adds the step to a recipe
# -------------------------------------------------------------------
step_rounding <- function(recipe, ..., role = "predictor", trained = FALSE,
                             skip = FALSE, id = rand_id("round_feats"),
                             round_levels = list(`1s` = 0, `10s` = -1),
                             columns = NULL) {

  add_step(
    recipe,
    step_rounding_new(
      terms        = enquos(...),       # tidyselect expressions (e.g., all_numeric())
      role         = role,
      trained      = trained,
      skip         = skip,
      id           = id,
      round_levels = round_levels,      # list of suffix → rounding digits
      columns      = columns            # resolved during prep()
    )
  )
}

# -------------------------------------------------------------------
# Internal constructor: creates step object
# -------------------------------------------------------------------
step_rounding_new <- function(terms, role, trained, skip, id,
                                 round_levels, columns) {

  step(
    subclass     = "rounding",
    terms        = terms,
    role         = role,
    trained      = trained,
    skip         = skip,
    id           = id,
    round_levels = round_levels,
    columns      = columns
  )
}

# -------------------------------------------------------------------
# PREP: resolve selected columns (tidyselect → actual names)
# -------------------------------------------------------------------
prep.step_rounding <- function(x, training, info = NULL, ...) {

  # Resolve columns selected by user in recipe
  vars <- recipes::recipes_eval_select(x$terms, training, info)

  # Return trained step with column names filled in
  step_rounding_new(
    terms        = x$terms,
    role         = x$role,
    trained      = TRUE,
    skip         = x$skip,
    id           = x$id,
    round_levels = x$round_levels,
    columns      = vars
  )
}

# -------------------------------------------------------------------
# BAKE: create rounded feature columns for new_data
# -------------------------------------------------------------------
bake.step_rounding <- function(object, new_data, ...) {

  # Loop through each selected column
  for (col in object$columns) {

    # Loop through all rounding suffix → digits mapping
    for (suffix in names(object$round_levels)) {
      digits <- object$round_levels[[suffix]]

      # Create new column name following Python pattern
      new_col <- paste0("round_", col, "_", suffix)

      # Apply rounding: round(x, digits), then cast to integer like Python
      new_data[[new_col]] <- as.integer(round(new_data[[col]], digits))
    }
  }

  new_data
}

# -------------------------------------------------------------------
# PRINT: simple summary for recipe printing
# -------------------------------------------------------------------
print.step_rounding <- function(x, ...) {
  cat("step_rounding: rounding numeric features created from ",
      paste(x$columns, collapse = ", "), "\n", sep = "")
  invisible(x)
}
