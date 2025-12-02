# =================================================================================================
# ensemble_fold_cv_lgb  (FIXED, optimized, production)
# -------------------------------------------------------------------------------------------------
# - Single LightGBM model per fold
# - LR embeddings for RAW_CATS (nested OOF) and optional TargetEncoding for INTER/ROUND
# - Test transformed once (no leakage). Test memory copied minimally.
# - Extracted helpers: apply_te(), prepare_fold_data(), align_columns(), fit_lgb_fold()
# - Proper input validation, logging, debug snapshots, best_iter capture, model saving
# - Returns: oof_auc, oof_preds, test_preds, fold_aucs, X_full, y_full, best_iters, model_paths
# -------------------------------------------------------------------------------------------------
# Usage: similar API to your earlier function. LogisticRegressionEncoder & TargetEncoder must exist.
# =================================================================================================

# =================================================================================================
# Helpers: reusable utilities for TE, LR-embeddings, folds, alignment, snapshots
# These live in the global namespace so XGB and LGB runners can both call them.
# =================================================================================================

# Create stratified-ish shuffled folds (sklearn.KFold shuffle-like)
kfold_ids <- function(n, v = 5L, seed = 123L) {
  set.seed(seed)
  vk <- rep_len(seq_len(v), n)
  sample(vk, n)
}

# Create balanced-ish folds (deterministic sizes)
create_folds <- function(n, cv = 5L, seed = 42L) {
  set.seed(seed)
  perm <- sample.int(n)
  base <- floor(n / cv); remainder <- n %% cv
  sizes <- rep(base, cv)
  if (remainder > 0) sizes[1:remainder] <- sizes[1:remainder] + 1L
  folds <- integer(n); start <- 1L
  for (i in seq_len(cv)) {
    end <- start + sizes[i] - 1L
    folds[perm[start:end]] <- i; start <- end + 1L
  }
  folds
}

# Apply target encoding stage using your TargetEncoder class
# Returns list(DT_tr, DT_val, DT_test)
apply_te <- function(DT_tr, DT_val, DT_test, cols, te_cv, te_smooth, drop_original, verbose = TRUE) {
  if (is.null(cols) || length(cols) == 0) return(list(DT_tr = DT_tr, DT_val = DT_val, DT_test = DT_test))
  te <- TargetEncoder(
    cols_to_encode = cols,
    aggs = c("mean"),
    cv = te_cv,
    smooth = te_smooth,
    drop_original = drop_original,
    verbose = verbose
  )
  DT_tr2  <- te$fit_transform(DT_tr, DT_tr[[get_target_var(DT_tr)]])
  DT_val2 <- te$transform(DT_val)
  DT_test2 <- te$transform(DT_test)
  list(DT_tr = DT_tr2, DT_val = DT_val2, DT_test = DT_test2)
}

# Helper to get target var safely from environment; this helper assumes target is passed explicitly to main fn
# Kept here to avoid accidental global dependency; main function will override via closure.
get_target_var <- function(dt) { stop("get_target_var must be overridden in caller") }

# Align columns (train -> val/test), fill small missing sets with zeros, return common ordered tables
align_columns <- function(DT_tr, DT_val, DT_test, missing_threshold = 2000, log = function(...) {}) {
  train_cols <- colnames(DT_tr)
  DT_val2 <- DT_val[, intersect(train_cols, colnames(DT_val)), with = FALSE]
  DT_test2 <- DT_test[, intersect(train_cols, colnames(DT_test)), with = FALSE]

  missing_val <- setdiff(train_cols, colnames(DT_val2))
  missing_test <- setdiff(train_cols, colnames(DT_test2))

  if (length(missing_val) <= missing_threshold) {
    for (mc in missing_val) DT_val2[, (mc) := 0]
  } else {
    log("[align_columns] large missing_val -> using intersection only")
    DT_val2 <- DT_val2[, intersect(train_cols, colnames(DT_val2)), with = FALSE]
  }

  if (length(missing_test) <= missing_threshold) {
    for (mc in missing_test) DT_test2[, (mc) := 0]
  } else {
    log("[align_columns] large missing_test -> using intersection only")
    DT_test2 <- DT_test2[, intersect(train_cols, colnames(DT_test2)), with = FALSE]
  }

  common_final <- intersect(train_cols, intersect(colnames(DT_val2), colnames(DT_test2)))
  list(
    DT_tr = DT_tr[, ..common_final],
    DT_val = DT_val2[, ..common_final],
    DT_test = DT_test2[, ..common_final]
  )
}

# Prepare fold data: extract y and drop target column
prepare_fold_data <- function(DT_tr, DT_val, target) {
  y_tr <- DT_tr[[target]]; y_val <- DT_val[[target]]
  DT_tr[, (target) := NULL]; DT_val[, (target) := NULL]
  list(DT_tr = DT_tr, y_tr = y_tr, DT_val = DT_val, y_val = y_val)
}

# Snapshot saver (debug)
save_snapshots <- function(dt1, dt2, dt3, snapshot_dir, fold, prefix = "snap") {
  if (is.null(snapshot_dir)) return()
  try({
    saveRDS(dt1, file.path(snapshot_dir, sprintf("%s_tr_fold%02d.rds", prefix, fold)))
    saveRDS(dt2, file.path(snapshot_dir, sprintf("%s_val_fold%02d.rds", prefix, fold)))
    saveRDS(dt3, file.path(snapshot_dir, sprintf("%s_test_fold%02d.rds", prefix, fold)))
  }, silent = TRUE)
}

# =================================================================================================
# Main: ensemble_fold_cv_lgb (helpers are global and reusable)
# - NOTE: target and lgb_params are REQUIRED (no defaults) to keep this function reusable.
# =================================================================================================

ensemble_fold_cv_lgb <- function(
  train_dt,
  test_dt,
  target,                    # REQUIRED (no default)
  lgb_params,                # REQUIRED (no default)
  RAW_CATS = NULL,
  INTER = NULL,
  ROUND = NULL,
  enable_te = TRUE,
  folds = 5L,
  te_cv = 5L,
  te_smooth = "auto",
  lambda_lr = 1e-6,
  drop_original_te1 = TRUE,
  drop_original_te2 = FALSE,
  parallel = FALSE,
  future_workers = NULL,
  seed = 2025,
  verbose = TRUE,
  debug = FALSE,
  log_file = "ensemble_lgb_debug_log.txt",
  snapshot_dir = NULL,
  nrounds_local = 10000,
  early_stopping_rounds = 200,
  model_dir = NULL
) {

  # -----------------------------
  # Input validation
  # -----------------------------
  if (!inherits(train_dt, "data.table")) stop("train_dt must be a data.table")
  if (!inherits(test_dt, "data.table")) stop("test_dt must be a data.table")
  if (!is.character(target) || length(target) != 1) stop("target must be a single column name")
  if (!(target %in% names(train_dt))) stop("target not found in train_dt")
  if (!is.list(lgb_params)) stop("lgb_params must be provided as a list (no defaults here)")

  required_pkgs <- c("data.table", "lightgbm", "pROC", "Matrix", "glmnet")
  for (p in required_pkgs) if (!requireNamespace(p, quietly = TRUE))
    stop(sprintf("Package '%s' required. install.packages('%s')", p, p))

  if (debug) {
    if (file.exists(log_file)) file.remove(log_file)
    if (!is.null(snapshot_dir)) dir.create(snapshot_dir, showWarnings = FALSE, recursive = TRUE)
  }

  # local logging
  log <- function(...) {
    msg <- paste0(Sys.time(), " | ", paste(..., collapse = " "))
    if (verbose) message(msg)
    if (debug) write(msg, file = log_file, append = TRUE)
  }

  # prepare data.table
  data.table::setDT(train_dt); data.table::setDT(test_dt)
  if (!is.numeric(train_dt[[target]])) train_dt[, (target) := as.numeric(as.character(get(target)))]

  n <- nrow(train_dt); nt <- nrow(test_dt)
  log("[start] rows train=", n, " test=", nt, " folds=", folds)

  # Prepare folds vector
  folds_vec <- kfold_ids(n, v = folds, seed = seed)

  # ------------------------------------------------------------------
  # Precompute full-fit transforms for test (no leakage)
  #  - TE: fit on full train once to transform test
  #  - LR: fit full LR encoder on full train to transform test once
  # ------------------------------------------------------------------
  DT_train_full <- data.table::copy(train_dt)
  DT_test_full <- data.table::copy(test_dt)  # single copy only

  # Set a safe binding for get_target_var inside apply_te (used by helper)
  assign("get_target_var", function(dt) target, envir = .GlobalEnv)

  te1_full <- NULL; te2_full <- NULL; lre_full <- NULL

  if (enable_te && !is.null(INTER) && length(INTER) > 0) {
    log("[precompute] fitting TE (INTER) on full train for test transform")
    te1_full <- TargetEncoder(cols_to_encode = INTER, aggs = c("mean"), cv = te_cv,
                              smooth = te_smooth, drop_original = drop_original_te1, verbose = FALSE)
    te1_full$fit(DT_train_full, DT_train_full[[target]])
    DT_test_full <- te1_full$transform(DT_test_full)
  }

  if (enable_te && !is.null(ROUND) && length(ROUND) > 0) {
    log("[precompute] fitting TE (ROUND) on full train for test transform")
    te2_full <- TargetEncoder(cols_to_encode = ROUND, aggs = c("mean"), cv = te_cv,
                              smooth = te_smooth, drop_original = drop_original_te2, verbose = FALSE)
    te2_full$fit(DT_train_full, DT_train_full[[target]])
    DT_test_full <- te2_full$transform(DT_test_full)
  }

  if (!is.null(RAW_CATS) && length(RAW_CATS) > 0) {
    log("[precompute] fitting LR encoder on full train for test transform")
    lre_full <- LogisticRegressionEncoder(cols_to_encode = RAW_CATS, cv = 5L, lambda = lambda_lr,
                                          drop_original = FALSE, verbose = FALSE, in_place = FALSE)
    lre_full$fit(DT_train_full, DT_train_full[[target]])
    DT_test_full <- lre_full$transform(DT_test_full)
  }

  # If test has character columns after pre-transform, log warning
  ch_test_full <- names(DT_test_full)[sapply(DT_test_full, is.character)]
  if (length(ch_test_full) > 0) log("[warn] characters remain in test after pre-transform:", paste(ch_test_full, collapse = ", "))

  # ------------------------------------------------------------------
  # Per-fold worker (keeps scope small). Returns a list of results.
  # ------------------------------------------------------------------
  run_fold_worker <- function(fold_id) {
    tryCatch({
      log(sprintf("=== fold %d start ===", fold_id))
      tr_idx <- which(folds_vec != fold_id); val_idx <- which(folds_vec == fold_id)
      DT_tr <- data.table::copy(train_dt[tr_idx]); DT_val <- data.table::copy(train_dt[val_idx])
      DT_test_local <- data.table::copy(DT_test_full)  # lightweight copy reference to pretransformed test

      # Apply TE OOF on DT_tr and transform DT_val, DT_test_local using full-fit mapping for test
      if (enable_te && !is.null(INTER) && length(INTER) > 0) {
        log("[fold] TE stage INTER")
        te1 <- TargetEncoder(cols_to_encode = INTER, aggs = c("mean"), cv = te_cv, smooth = te_smooth,
                             drop_original = drop_original_te1, verbose = FALSE)
        DT_tr <- te1$fit_transform(DT_tr, DT_tr[[target]])
        DT_val <- te1$transform(DT_val)
        # DT_test_local already transformed via te1_full earlier
      }

      if (enable_te && !is.null(ROUND) && length(ROUND) > 0) {
        log("[fold] TE stage ROUND")
        te2 <- TargetEncoder(cols_to_encode = ROUND, aggs = c("mean"), cv = te_cv, smooth = te_smooth,
                             drop_original = drop_original_te2, verbose = FALSE)
        DT_tr <- te2$fit_transform(DT_tr, DT_tr[[target]])
        DT_val <- te2$transform(DT_val)
      }

      # Nested LR embeddings (OOF for DT_tr; transform DT_val and DT_test_local using either local full-fit or lre_full)
      if (!is.null(RAW_CATS) && length(RAW_CATS) > 0) {
        log("[fold] LR embedding stage")
        lre_local <- LogisticRegressionEncoder(cols_to_encode = RAW_CATS, cv = 5L, lambda = lambda_lr,
                                              drop_original = TRUE, verbose = FALSE, in_place = FALSE)
        DT_tr <- lre_local$fit_transform(DT_tr, DT_tr[[target]], seed = seed)
        DT_val <- lre_local$transform(DT_val)
        # transform test: prefer lre_full (pre-fit), fallback to lre_local if missing
        if (!is.null(lre_full)) {
          DT_test_local <- lre_full$transform(DT_test_local)
          missing_lr_cols <- setdiff(paste0("lr_cat_", RAW_CATS), names(DT_test_local))
          if (length(missing_lr_cols) > 0) DT_test_local <- lre_local$transform(DT_test_local)
        } else {
          DT_test_local <- lre_local$transform(DT_test_local)
        }
      }

      # Safety: if any character columns remain -> snapshot & error
      ch_tr <- names(DT_tr)[sapply(DT_tr, is.character)]
      ch_val <- names(DT_val)[sapply(DT_val, is.character)]
      ch_tst <- names(DT_test_local)[sapply(DT_test_local, is.character)]
      if (length(ch_tr) + length(ch_val) + length(ch_tst) > 0) {
        save_snapshots(DT_tr, DT_val, DT_test_local, snapshot_dir, fold_id, prefix = "err")
        stop(sprintf("Character columns remain (train:%d val:%d test:%d)", length(ch_tr), length(ch_val), length(ch_tst)))
      }

      # Prepare fold data (extract y and drop target)
      prep <- prepare_fold_data(DT_tr, DT_val, target)
      DT_tr2 <- prep$DT_tr; y_tr <- prep$y_tr; DT_val2 <- prep$DT_val; y_val <- prep$y_val

      # Align columns and reorder
      aligned <- align_columns(DT_tr2, DT_val2, DT_test_local, missing_threshold = 2000, log = log)
      DT_tr3 <- aligned$DT_tr; DT_val3 <- aligned$DT_val; DT_test3 <- aligned$DT_test

      # Force numeric conversion
      for (nm in names(DT_tr3)) DT_tr3[, (nm) := as.numeric(get(nm))]
      for (nm in names(DT_val3)) DT_val3[, (nm) := as.numeric(get(nm))]
      for (nm in names(DT_test3)) DT_test3[, (nm) := as.numeric(get(nm))]

      # Convert to matrices (once)
      X_tr <- as.matrix(DT_tr3); X_val <- as.matrix(DT_val3); X_tst <- as.matrix(DT_test3)

      # Build datasets and train LightGBM
      dtrain <- lightgbm::lgb.Dataset(data = X_tr, label = y_tr)
      dval <- lightgbm::lgb.Dataset(data = X_val, label = y_val, reference = dtrain)

      log("[fold] training LightGBM fold", fold_id)
      model <- suppressWarnings(
                        lightgbm::lgb.train(
                          params = lgb_params,
                          data = dtrain,
                          valids = list(train = dtrain, valid = dval),
                          nrounds = nrounds_local,
                          early_stopping_rounds = early_stopping_rounds,
                          verbose = 1L
                        )
                      )



      best_it <- model$best_iter
      val_pred <- predict(model, X_val, num_iteration = best_it)
      test_pred <- predict(model, X_tst, num_iteration = best_it)
      fold_auc <- as.numeric(pROC::auc(y_val, val_pred))
      log(sprintf("[fold] Fold %d AUC: %.6f", fold_id, fold_auc))

      model_path <- NA_character_
      if (!is.null(model_dir)) {
        model_path <- file.path(model_dir, sprintf("lgb_fold%02d.txt", fold_id))
        lightgbm::lgb.save(model, model_path)
        log("[fold] saved model ->", model_path)
      }

      save_snapshots(DT_tr3, DT_val3, DT_test3, snapshot_dir, fold_id, prefix = "ok")

      list(fold = fold_id, val_idx = val_idx, val_pred = val_pred, test_pred = test_pred,
           fold_auc = fold_auc, X_val = X_val, y_val = y_val, best_iter = best_it, model_path = model_path)

    }, error = function(e) {
      # capture fold-level error with enough info
      save_snapshots(train_dt, test_dt, test_dt, snapshot_dir, fold_id, prefix = "error")
      stop(sprintf("Error in fold %d: %s", fold_id, e$message))
    })
  } # end run_fold_worker

  # ------------------------------------------------------------------
  # Execute folds (parallel or sequential)
  # ------------------------------------------------------------------
  if (parallel) {
    if (is.null(future_workers)) future_workers <- max(1, parallel::detectCores() - 1)
    future::plan(future::multisession, workers = future_workers)
    log("[parallel] running with workers:", future_workers)
    results <- future.apply::future_lapply(seq_len(folds), run_fold_worker, future.seed = TRUE)
    future::plan("sequential")
  } else {
    results <- vector("list", folds)
    for (f in seq_len(folds)) results[[f]] <- run_fold_worker(f)
  }

  # ------------------------------------------------------------------
  # Aggregate fold outputs
  # ------------------------------------------------------------------
  oof_preds <- numeric(n); test_preds_sum <- numeric(nt); fold_aucs <- numeric(folds)
  best_iters <- integer(folds); model_paths <- character(folds)

  # Preallocate X_full efficiently using dims from first fold
  ncols <- ncol(results[[1]]$X_val)
  X_full <- matrix(NA_real_, nrow = n, ncol = ncols)
  colnames(X_full) <- colnames(results[[1]]$X_val)
  y_full <- numeric(n)

  for (res in results) {
    oof_preds[res$val_idx] <- res$val_pred
    test_preds_sum <- test_preds_sum + res$test_pred / folds
    fold_aucs[res$fold] <- res$fold_auc
    best_iters[res$fold] <- res$best_iter
    model_paths[res$fold] <- res$model_path
    X_full[res$val_idx, ] <- res$X_val
    y_full[res$val_idx] <- res$y_val
  }

  final_auc <- as.numeric(pROC::auc(train_dt[[target]], oof_preds))
  log("[final] OOF AUC =", round(final_auc, 6))

  list(
    oof_auc = final_auc,
    oof_preds = oof_preds,
    test_preds = test_preds_sum,
    fold_aucs = fold_aucs,
    X_full = X_full,
    y_full = y_full,
    best_iters = best_iters,
    model_paths = model_paths
  )
}
