# =====================================================================
# LogisticRegressionEncoder (LR-OHE embeddings, production-grade)
# =====================================================================
# Produces lr_cat_<col> features via ridge logistic regression on OHE.
# API mirrors your TargetEncoder: fit, transform, fit_transform.
# =====================================================================

if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table required")
if (!requireNamespace("glmnet", quietly = TRUE)) stop("glmnet required")
if (!requireNamespace("Matrix", quietly = TRUE)) stop("Matrix required")

# helper: create folds (same as TargetEncoder)
.create_folds <- function(n, cv = 5L, seed = 42L) {
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

# helper: make sparse OHE from integer codes (0-based)
.make_ohe_sparse <- function(vec_int0, nlev) {
  if (length(vec_int0) == 0) return(Matrix::sparseMatrix(i = integer(0), j = integer(0), x = numeric(0),
                                                         dims = c(0, nlev)))
  i <- seq_along(vec_int0)
  j <- vec_int0 + 1L
  # handle any NA coded as -1 -> set j to NA and then remove
  valid_idx <- which(!is.na(j) & j >= 1 & j <= nlev)
  if (length(valid_idx) == 0) {
    return(Matrix::sparseMatrix(i = integer(0), j = integer(0), x = numeric(0),
                               dims = c(length(vec_int0), nlev)))
  }
  Matrix::sparseMatrix(i = i[valid_idx], j = j[valid_idx], x = 1,
                       dims = c(length(vec_int0), nlev))
}

# Constructor
LogisticRegressionEncoder <- function(
  cols_to_encode,
  cv = 5L,
  lambda = 1e-6,
  drop_original = FALSE,
  verbose = FALSE,
  in_place = FALSE,
  debug = FALSE
) {
  if (!is.character(cols_to_encode)) stop("cols_to_encode must be character vector")

  # internal storage
  models_ <- list()          # list of glmnet full-fit objects per column
  levels_ <- list()          # list of level vectors per column (character)
  global_mean_ <- NULL       # global target mean (for fallback)
  cv_ <- as.integer(cv)
  lambda_ <- as.numeric(lambda)

  logv <- function(...) if (verbose) message(...)
  logd <- function(...) if (debug) message("[DEBUG]", ...)

  # -----------------------
  # fit: train full models (store for transform)
  # -----------------------
  fit <- function(X, y) {
    dt <- data.table::as.data.table(if (in_place) X else data.table::copy(X))
    y_vec <- as.numeric(y)
    global_mean_ <<- mean(y_vec, na.rm = TRUE)
    for (col in cols_to_encode) {
      logv(sprintf("[FIT] Processing column: %s", col))
      if (!(col %in% names(dt))) {
        logv(sprintf("[FIT] Column '%s' not in X — storing NULL model.", col))
        models_[[col]] <<- NULL
        levels_[[col]] <<- character(0)
        next
      }
      # determine levels from data
      lv <- unique(as.character(dt[[col]]))
      levels_[[col]] <<- lv
      # integer codes 0-based
      codes <- as.integer(factor(as.character(dt[[col]]), levels = lv)) - 1L
      nlev <- length(lv)
      X_ohe <- .make_ohe_sparse(codes, nlev)
      # glmnet expects matrix class, sparse is ok
      fit_glm <- glmnet::glmnet(
        x = X_ohe, y = y_vec, family = "binomial",
        alpha = 0, lambda = lambda_
      )
      models_[[col]] <<- fit_glm
      logv(sprintf("[FIT] Stored full glmnet model for '%s' (nlev=%d)", col, nlev))
    }
    invisible(NULL)
  }

  # -----------------------
  # transform: apply stored full models to produce lr_cat_<col>
  # -----------------------
  transform <- function(X) {
    dt <- data.table::as.data.table(if (in_place) X else data.table::copy(X))
    n <- nrow(dt)
    for (col in cols_to_encode) {
      new_col <- paste0("lr_cat_", col)
      logv(sprintf("[TRANSFORM] Column: %s", col))
      if (!(col %in% names(dt))) {
        logv(sprintf("[TRANSFORM] Column '%s' missing — filling with global mean.", col))
        dt[, (new_col) := global_mean_]
        next
      }
      lv <- levels_[[col]]
      if (is.null(lv) || length(lv) == 0) {
        logv(sprintf("[TRANSFORM] No levels stored for '%s' — using global mean.", col))
        dt[, (new_col) := global_mean_]
        next
      }
      # convert to integer codes based on stored levels; unseen -> NA (code = NA)
      codes <- as.integer(factor(as.character(dt[[col]]), levels = lv)) - 1L
      nlev <- length(lv)
      X_ohe <- .make_ohe_sparse(codes, nlev)
      model_full <- models_[[col]]
      if (is.null(model_full)) {
        dt[, (new_col) := global_mean_]
        next
      }
      predp <- as.numeric(predict(model_full, X_ohe, type = "response"))
      # glmnet::predict with sparse input might return matrix with lambda dims; ensure vector
      if (is.matrix(predp)) predp <- as.numeric(predp[, 1])
      # replace any NA with global mean
      predp[is.na(predp)] <- global_mean_
      dt[, (new_col) := predp]
    }
    if (drop_original) dt[, (intersect(cols_to_encode, names(dt))) := NULL]
    return(dt[])
  }

  # -----------------------
  # fit_transform: produce OOF LR embeddings for training rows, and store full models
  # -----------------------
  fit_transform <- function(X, y, seed = 42L) {
    dt <- data.table::as.data.table(data.table::copy(X))
    y_vec <- as.numeric(y)
    n <- nrow(dt)
    global_mean_ <<- mean(y_vec, na.rm = TRUE)

    # prepare OOF storage
    oof <- data.table::data.table(idx = seq_len(n))
    for (col in cols_to_encode) oof[, (paste0("lr_cat_", col)) := NA_real_]

    folds <- .create_folds(n, cv = cv_, seed = seed)

    # full-fit mappings will be created at the end
    for (col in cols_to_encode) {
      logv(sprintf("[OOF] Starting column: %s", col))
      if (!(col %in% names(dt))) {
        logv(sprintf("[OOF] '%s' not found — filling with global mean.", col))
        oof[[paste0("lr_cat_", col)]] <- global_mean_
        models_[[col]] <<- NULL
        levels_[[col]] <<- character(0)
        next
      }
      # compute levels across full train (so final model can use same mapping)
      lv_full <- unique(as.character(dt[[col]]))
      levels_[[col]] <<- lv_full
      nlev <- length(lv_full)
      # build full OHE for train so we can subset
      codes_full <- as.integer(factor(as.character(dt[[col]]), levels = lv_full)) - 1L
      X_ohe_full <- .make_ohe_sparse(codes_full, nlev)

      # OOF loop
      for (f in seq_len(cv_)) {
        val_idx <- which(folds == f)
        tr_idx  <- which(folds != f)
        if (length(tr_idx) == 0 || length(val_idx) == 0) next

        X_tr_ohe <- X_ohe_full[tr_idx, , drop = FALSE]
        y_tr <- y_vec[tr_idx]

        # fit ridge logistic
        fit_glm <- glmnet::glmnet(x = X_tr_ohe, y = y_tr, family = "binomial",
                                 alpha = 0, lambda = lambda_)

        # predict validation
        pred_val <- as.numeric(predict(fit_glm, X_ohe_full[val_idx, , drop = FALSE], type = "response"))
        if (is.matrix(pred_val)) pred_val <- as.numeric(pred_val[, 1])
        pred_val[is.na(pred_val)] <- global_mean_

        oof[[paste0("lr_cat_", col)]][val_idx] <- pred_val
      } # end folds

      # After OOF, fit full model on all rows and store
      full_fit <- glmnet::glmnet(x = X_ohe_full, y = y_vec, family = "binomial",
                                alpha = 0, lambda = lambda_)
      models_[[col]] <<- full_fit

      # If any remaining NA in OOF (e.g., tiny folds), replace with global mean
      na_idx <- which(is.na(oof[[paste0("lr_cat_", col)]]))
      if (length(na_idx) > 0) {
        oof[[paste0("lr_cat_", col)]][na_idx] <- global_mean_
        logv(sprintf("[OOF] Replaced %d NA OOF preds for '%s' with global mean", length(na_idx), col))
      }

      logv(sprintf("[OOF] Completed column: %s (levels=%d)", col, nlev))
    } # end columns

    # attach OOF columns to dt
    for (nm in names(oof)) dt[, (nm) := oof[[nm]]]
    if (drop_original) dt[, (intersect(cols_to_encode, names(dt))) := NULL]
    return(dt[])
  }

  # return the object mirroring TargetEncoder API
  structure(
    list(
      cols_to_encode = cols_to_encode,
      cv = cv_,
      lambda = lambda_,
      drop_original = drop_original,
      verbose = verbose,
      in_place = in_place,
      fit = fit,
      transform = transform,
      fit_transform = fit_transform
    ),
    class = "LogisticRegressionEncoder"
  )
}
# =====================================================================
# End LogisticRegressionEncoder
# =====================================================================
