# Custom tidymodels recipe step: step_bigrams
# Purpose: create pairwise concatenated "bigram" features
#          from selected predictor columns, excluding
#          any columns passed via the `exclude` argument.

# Constructor exposed to users: add the step to a recipe
step_bigrams <- function(recipe, ..., role = "predictor", trained = FALSE,
                         skip = FALSE, id = rand_id("bigrams"),
                         exclude = NULL, columns = NULL) {
  # terms: tidyselect expressions captured with enquos(...)
  add_step(
    recipe,
    step_bigrams_new(
      terms   = enquos(...),   # captured selectors for vars to consider
      role    = role,
      trained = trained,
      skip    = skip,
      id      = id,
      exclude = exclude,       # user-provided vector of names to exclude
      columns = columns        # will hold resolved column names after prep()
    )
  )
}

# Internal helper: calls recipes::step() to build the step object
step_bigrams_new <- function(terms, role, trained, skip, id, exclude, columns) {
  step(
    subclass = "bigrams",
    terms    = terms,
    role     = role,
    trained  = trained,
    skip     = skip,
    id       = id,
    exclude  = exclude,
    columns  = columns
  )
}

# prep method: resolve selectors to concrete column names and remove excluded names
prep.step_bigrams <- function(x, training, info = NULL, ...) {
  # Resolve tidyselect terms into concrete column names using recipes helper
  vars <- recipes::recipes_eval_select(x$terms, training, info)

  # If an exclude list is provided, remove those names
  if (!is.null(x$exclude)) {
    vars <- setdiff(vars, x$exclude)
  }

  # Return a trained step with columns populated
  step_bigrams_new(
    terms   = x$terms,
    role    = x$role,
    trained = TRUE,
    skip    = x$skip,
    id      = x$id,
    exclude = x$exclude,
    columns = vars
  )
}

# bake method: apply the transformation to new_data (create concatenated bigram columns)
bake.step_bigrams <- function(object, new_data, ...) {
  # Generate all 2-combinations of the resolved column names
  combos <- utils::combn(object$columns, 2, simplify = FALSE)

  # For each pair, create a new column named "col1_col2" with values "as.character(col1)_as.character(col2)"
  for (pair in combos) {
    new_col <- paste0("inter_",pair[1], "_", pair[2])
    new_data[[new_col]] <- paste0(
      as.character(new_data[[pair[1]]]), "_",
      as.character(new_data[[pair[2]]])
    )
  }

  # Return the augmented data.frame/tibble
  new_data
}

# print method: concise description when printed
print.step_bigrams <- function(x, ...) {
  cat("step_bigrams: create concatenated pairwise features from:\n")
  cat("  ", paste0(x$columns, collapse = ", "), "\n")
  invisible(x)
}
