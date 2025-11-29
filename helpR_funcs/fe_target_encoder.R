# =====================================================================
# TargetEncoder (full, corrected, production-grade)
# =====================================================================

if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required. Install with install.packages('data.table')")
}

# ---------------------------------------------------------------------
# Helper: aggregation evaluator
# ---------------------------------------------------------------------
.agg_eval <- function(x, agg) {
  x <- x[!is.na(x)]
  if (length(x) == 0L) return(NA_real_)
  if (agg == "mean")   return(mean(x))
  if (agg == "median") return(stats::median(x))
  if (agg == "min")    return(min(x))
  if (agg == "max")    return(max(x))
  if (agg == "sd")     return(stats::sd(x))
  if (agg == "var")    return(stats::var(x))
  if (agg == "count")  return(length(x))
  if (agg == "sum")    return(sum(x))
  if (grepl("^q[0-9]{2}$", agg)) {
    q <- as.numeric(substr(agg, 2, 3)) / 100
    return(as.numeric(stats::quantile(x, probs = q, names = FALSE)))
  }
  if (agg == "nunique") return(length(unique(x)))
  stop("Unknown aggregation: ", agg)
}

# ---------------------------------------------------------------------
# Helper: fold creation (sklearn-style)
# ---------------------------------------------------------------------
create_folds <- function(n, cv = 5L, seed = 42L) {
  set.seed(seed)
  perm <- sample.int(n)
  base <- floor(n / cv)
  remainder <- n %% cv
  sizes <- rep(base, cv)
  if (remainder > 0) sizes[1:remainder] <- sizes[1:remainder] + 1L
  folds <- integer(n)
  start <- 1L
  for (i in seq_len(cv)) {
    end <- start + sizes[i] - 1L
    folds[perm[start:end]] <- i
    start <- end + 1L
  }
  folds
}

# ---------------------------------------------------------------------
# Helper: smoothing calculation
# ---------------------------------------------------------------------
.calculate_smoothing <- function(train_targets, train_keys, category_means, smooth) {
  if (!identical(smooth, "auto")) {
    m <- max(0, as.numeric(smooth))
    return(m)
  }

  # Compute within-group variances safely
  dt_temp <- data.table::data.table()
  dt_temp[, key := train_keys]
  dt_temp[, target := train_targets]

  vw <- dt_temp[, .(v = if (.N > 1) stats::var(target) else 0), by = key]
  avg_vw <- mean(vw$v, na.rm = TRUE)

  var_btw <- if (length(category_means) <= 1L) NA_real_ else stats::var(category_means, na.rm = TRUE)
  if (!is.finite(var_btw) || var_btw <= 0) return(0)

  m <- avg_vw / var_btw
  if (!is.finite(m) || m < 0) m <- 0
  return(m)
}

# ---------------------------------------------------------------------
# Helper: build mapping (single agg)
# ---------------------------------------------------------------------
.build_mapping <- function(keys_train, targets_train, agg, smooth_param = NULL, fold_global_mean = NULL) {

  temp <- data.table::data.table()
  temp[, key := keys_train]
  temp[, target := targets_train]

  if (agg == "count") {
    map_dt <- temp[, .(val = .N), by = key]
    return(setNames(as.numeric(map_dt$val), map_dt$key))
  }

  map_dt <- temp[, .(val = .agg_eval(target, agg)), by = key]

  if (agg == "mean" && !is.null(smooth_param)) {
    raw <- temp[, .(n = .N, raw_mean = mean(target)), by = key]
    raw[, val := (n * raw_mean + smooth_param * fold_global_mean) / (n + smooth_param)]
    return(setNames(as.numeric(raw$val), raw$key))
  }

  setNames(as.numeric(map_dt$val), map_dt$key)
}

# ---------------------------------------------------------------------
# Helper: apply mapping to keys
# ---------------------------------------------------------------------
.apply_map_to_keys <- function(keys, mapping_vec, fallback) {
  result <- mapping_vec[keys]
  na_idx <- which(is.na(result))
  if (length(na_idx) > 0) result[na_idx] <- fallback
  as.numeric(result)
}

# ---------------------------------------------------------------------
# Main Constructor: TargetEncoder
# ---------------------------------------------------------------------
TargetEncoder <- function(
  cols_to_encode,
  aggs = c("mean"),
  cv = 5L,
  smooth = "auto",
  drop_original = FALSE,
  verbose = FALSE,
  in_place = FALSE,
  debug = FALSE
) {

  if (!is.character(cols_to_encode))
    stop("cols_to_encode must be character vector")

  mappings_ <- list()
  global_stats_ <- list()

  # Logging helper
  logv <- function(...) {
    if (verbose) message(paste(..., collapse=" "))
  }
  logd <- function(...) {
    if (debug) message("[DEBUG]", paste(..., collapse=" "))
  }

  # --------------------------------------------------------------
  # FIT
  # --------------------------------------------------------------
  fit <- function(X, y) {
    dt <- data.table::as.data.table(if (in_place) X else data.table::copy(X))
    y_vec <- as.numeric(y)

    # Compute global stats
    for (agg in aggs) global_stats_[[agg]] <<- .agg_eval(y_vec, agg)

    # Build full mappings
    for (col in cols_to_encode) {

      if (!(col %in% names(dt))) {
        mappings_[[col]] <<- setNames(lapply(aggs, function(x) numeric()), aggs)
        next
      }

      keys <- as.character(dt[[col]])

      temp_dt <- data.table::data.table()
      temp_dt[, key := keys]
      temp_dt[, target := y_vec]

      mappings_[[col]] <<- list()

      for (agg in aggs) {
        if (agg == "count") {
          map_dt <- temp_dt[, .(val = .N), by = key]
          mappings_[[col]][[agg]] <<- setNames(as.numeric(map_dt$val), map_dt$key)
        } else {
          map_dt <- temp_dt[, .(val = .agg_eval(target, agg)), by = key]
          mappings_[[col]][[agg]] <<- setNames(as.numeric(map_dt$val), map_dt$key)
        }
      }
    }

    invisible(NULL)
  }

  # --------------------------------------------------------------
  # TRANSFORM
  # --------------------------------------------------------------
  transform <- function(X) {
    dt <- data.table::as.data.table(if (in_place) X else data.table::copy(X))

    for (col in cols_to_encode) {
      for (agg in aggs) {
        new_col <- paste0("TE_", col, "_", agg)
        mapping_vec <- mappings_[[col]][[agg]]
        fallback <- global_stats_[[agg]]

        if (!(col %in% names(dt)) || length(mapping_vec) == 0) {
          dt[, (new_col) := fallback]
          next
        }

        keys <- as.character(dt[[col]])
        mapped <- .apply_map_to_keys(keys, mapping_vec, fallback)
        dt[, (new_col) := mapped]
      }
    }

    if (drop_original) dt[, (intersect(cols_to_encode, names(dt))) := NULL]

    dt[]
  }

  # --------------------------------------------------------------
  # FIT_TRANSFORM (OOF)
  # --------------------------------------------------------------
  fit_transform <- function(X, y, seed = 42L) {
    dt <- data.table::as.data.table(data.table::copy(X))
    y_vec <- as.numeric(y)
    n <- nrow(dt)

    # Step A: full fit for test-time mappings
    fit(X, y)

    # Preallocate OOF table
    oof <- data.table::data.table(idx = seq_len(n))
    for (col in cols_to_encode)
      for (agg in aggs)
        oof[, (paste0("TE_", col, "_", agg)) := NA_real_]

    folds <- create_folds(n, cv, seed)

    # OOF encoding
    for (f in seq_len(cv)) {
      is_val <- folds == f
      val_idx <- which(is_val)
      train_idx <- which(!is_val)

      fold_global <- lapply(aggs, function(agg) .agg_eval(y_vec[train_idx], agg))
      names(fold_global) <- aggs

      for (col in cols_to_encode) {

        keys_train <- as.character(dt[[col]][train_idx])
        keys_val   <- as.character(dt[[col]][val_idx])
        t_train    <- y_vec[train_idx]

        for (agg in aggs) {
          te_name <- paste0("TE_", col, "_", agg)

          if (length(keys_train) == 0) {
            oof[val_idx, (te_name) := fold_global[[agg]]]
            next
          }

          if (agg == "mean") {
            raw_dt <- data.table::data.table()
            raw_dt[, key := keys_train]
            raw_dt[, target := t_train]

            raw <- raw_dt[, .(n = .N, raw_mean = mean(target)), by = key]
            category_means <- setNames(raw$raw_mean, raw$key)

            m <- .calculate_smoothing(
              t_train, keys_train,
              category_means,
              smooth
            )

            mapping_vec <- .build_mapping(
              keys_train, t_train,
              "mean", m,
              fold_global[["mean"]]
            )

          } else {
            mapping_vec <- .build_mapping(keys_train, t_train, agg)
          }

          mapped_vals <- .apply_map_to_keys(keys_val, mapping_vec, fold_global[[agg]])
          oof[val_idx, (te_name) := mapped_vals]
        }
      }
    }

    data.table::setorder(oof, "idx")
    oof[, idx := NULL]

    # attach OOF columns
    for (nm in names(oof)) dt[, (nm) := oof[[nm]]]

    if (drop_original) dt[, (cols_to_encode) := NULL]

    return(dt[])
  }

  # Return encoder object
  structure(
    list(
      cols_to_encode = cols_to_encode,
      aggs = aggs,
      cv = cv,
      smooth = smooth,
      drop_original = drop_original,
      fit = fit,
      transform = transform,
      fit_transform = fit_transform
    ),
    class = "TargetEncoder"
  )
}

# =====================================================================
# End of TargetEncoder
# =====================================================================
