
# Simple shuffled K-fold assignment (like sklearn.KFold shuffle=TRUE)
kfold_ids <- function(n, v = 5, seed = 123) {
  set.seed(seed)
  vk <- rep_len(seq_len(v), n)         # repeated 1..v
  sample(vk, n)                        # shuffle order
}

# ------------------------------------------------------
# Main Ensemble Function (v3, final)
# ------------------------------------------------------
ensemble_fold_cv_model <- function(
  train_dt,
  test_dt,
  target = "loan_paid_back",
  INTER = NULL,
  ROUND = NULL,
  CATS  = NULL,
  xgb_params = list(
    objective = "binary:logistic",
    eval_metric = "auc",
    nrounds = 200
  ),
  folds = 5L,
  te_cv = 5L,
  te_smooth = 1.0,
  drop_original_te1 = TRUE,
  drop_original_te2 = FALSE,
  parallel = FALSE,
  future_workers = NULL,
  seed = 2025,
  verbose = TRUE,
  debug = FALSE,
  log_file = "ensemble_debug_log.txt",
  snapshot_dir = "snapshots"
) {

  # -----------------------------
  # Logging helper
  # -----------------------------
  if (debug) {
    if (file.exists(log_file)) file.remove(log_file)
    dir.create(snapshot_dir, showWarnings = FALSE, recursive = TRUE)
  }
  log <- function(...) {
    msg <- paste0(Sys.time(), " | ", paste(..., collapse = " "))
    if (verbose) message(msg)
    if (debug) write(msg, file = log_file, append = TRUE)
  }

  # -----------------------------
  # Setup
  # -----------------------------
  setDT(train_dt)
  setDT(test_dt)
  if (!target %in% names(train_dt)) stop("Target not found in train_dt")

  # convert target numeric
  if (!is.numeric(train_dt[[target]])) {
    train_dt[, (target) := as.numeric(as.character(get(target)))]
  }

  N  <- nrow(train_dt)
  Nt <- nrow(test_dt)

  test_master <- copy(test_dt)

  # parallel plan
  if (parallel) {
    if (is.null(future_workers)) future_workers <- max(1, parallel::detectCores() - 1)
    future::plan(multisession, workers = future_workers)
    log("Parallel enabled, workers =", future_workers)
  } else {
    future::plan("sequential")
    log("Parallel disabled (sequential)")
  }

  # stratified fold ids
  fold_ids <- kfold_ids(nrow(train_dt), v = folds, seed = seed)

  oof_preds <- numeric(N)
  test_preds_sum <- numeric(Nt)
  fold_aucs <- numeric(folds)

  save_snapshots <- function(dt1, dt2, dt3, fold) {
    if (!debug) return()
    try({
      saveRDS(dt1, file.path(snapshot_dir, sprintf("DT_tr_fold%02d.rds", fold)))
      saveRDS(dt2, file.path(snapshot_dir, sprintf("DT_val_fold%02d.rds", fold)))
      saveRDS(dt3, file.path(snapshot_dir, sprintf("DT_test_fold%02d.rds", fold)))
      log("Saved fold snapshots for fold", fold)
    }, silent = TRUE)
  }

  # ------------------------------------------------------
  # SINGLE FOLD EXECUTOR
  # ------------------------------------------------------
  run_fold <- function(fold) {
    tryCatch({

      log(sprintf("=== Fold %d/%d START ===", fold, folds))

      tr_idx <- which(fold_ids != fold)
      val_idx <- which(fold_ids == fold)

      DT_tr <- copy(train_dt[tr_idx])
      DT_val <- copy(train_dt[val_idx])
      DT_test <- copy(test_master)

      log("Row counts: train=", nrow(DT_tr), " val=", nrow(DT_val), " test=", nrow(DT_test))

      # ------------------------------------------------------
      # TE Stage 1: INTER
      # ------------------------------------------------------
      if (!is.null(INTER) && length(INTER) > 0) {
        log("TE on INTER:", paste(INTER, collapse = ", "))

        te1 <- TargetEncoder(
          cols_to_encode = INTER,
          aggs = c("mean"),
          cv = te_cv,
          smooth = te_smooth,
          drop_original = drop_original_te1,
          verbose = TRUE
        )

        # OOF encode train
        DT_tr  <- te1$fit_transform(DT_tr, DT_tr[[target]])

        # Transform val & test using global mappings
        DT_val <- te1$transform(DT_val)
        DT_test <- te1$transform(DT_test)
      }

      # ------------------------------------------------------
      # TE Stage 2: ROUND columns
      # ------------------------------------------------------
      if (!is.null(ROUND) && length(ROUND) > 0) {
        log("TE on ROUND:", paste(ROUND, collapse = ", "))
      te2 <- TargetEncoder(
          cols_to_encode = ROUND,
          aggs = c("mean"),
          cv = te_cv,
          smooth = te_smooth,
          drop_original = drop_original_te2,
          verbose = TRUE
        )

        # OOF encode train
        DT_tr  <- te2$fit_transform(DT_tr, DT_tr[[target]])

        # Transform validation and test
        DT_val <- te2$transform(DT_val)
        DT_test <- te2$transform(DT_test)
      }

      # ------------------------------------------------------------------
      # EDITED BLOCK — dummy encoding for categorical vars
      # Date: 2025-01-30  | Time: 10:54 IST
      # ------------------------------------------------------------------
      # After TE is completed, convert the remaining categorical variables
      # into dummy/one-hot encoded numeric columns. This prevents xgboost
      # from seeing integer-encoded categories as ordinal.
      # ------------------------------------------------------------------

      if (!is.null(CATS) && length(CATS) > 0) {

        if (verbose) message("Dummy encoding categorical columns: ",
                            paste(CATS, collapse = ", "))

        dummy_encode_dt <- function(dt, cat_cols) {
          for (col in cat_cols) {
            if (col %in% names(dt)) {
              uvals <- unique(dt[[col]])
              if (length(uvals) > 1) {
                for (lvl in uvals) {
                  new_col <- paste0(col, "_", lvl)
                  dt[, (new_col) := as.integer(get(col) == lvl)]
                }
              }
              dt[, (col) := NULL]   # drop original
            }
          }
          return(dt)
        }

        DT_tr  <- dummy_encode_dt(DT_tr,  CATS)
        DT_val <- dummy_encode_dt(DT_val, CATS)
        DT_test <- dummy_encode_dt(DT_test, CATS)

        if (verbose) message("Dummy encoding completed.")
      }

      # ------------------------------------------------------------------
      # END OF EDIT
      # ------------------------------------------------------------------


      # ------------------------------------------------------
      # Character column check
      # ------------------------------------------------------
      ch_tr  <- names(DT_tr)[sapply(DT_tr, is.character)]
      ch_val <- names(DT_val)[sapply(DT_val, is.character)]
      ch_tst <- names(DT_test)[sapply(DT_test, is.character)]
      if (length(ch_tr) + length(ch_val) + length(ch_tst) > 0) {
        save_snapshots(DT_tr, DT_val, DT_test, fold)
        stop(sprintf("Character columns remain after TE (train:%d val:%d test:%d)",
                     length(ch_tr), length(ch_val), length(ch_tst)))
      }

      # ------------------------------------------------------
      # Extract target and drop from predictors
      # ------------------------------------------------------
      y_tr  <- DT_tr[[target]]
      y_val <- DT_val[[target]]
      DT_tr[,  (target) := NULL]
      DT_val[, (target) := NULL]

      # ------------------------------------------------------
      # TRAIN-COLUMNS ONLY alignment (memory-safe)
      # ------------------------------------------------------
      train_cols <- colnames(DT_tr)

      # ensure val/test only contain columns in train
      DT_val  <- DT_val[, intersect(train_cols, colnames(DT_val)), with = FALSE]
      DT_test <- DT_test[, intersect(train_cols, colnames(DT_test)), with = FALSE]

      # fill missing small sets with zeros
      missing_val  <- setdiff(train_cols, colnames(DT_val))
      missing_test <- setdiff(train_cols, colnames(DT_test))

      if (length(missing_val) <= 2000) {
        for (mc in missing_val) DT_val[, (mc) := 0]
      } else {
        log("Large missing_val set → using intersection only.")
      }

      if (length(missing_test) <= 2000) {
        for (mc in missing_test) DT_test[, (mc) := 0]
      } else {
        log("Large missing_test set → using intersection only.")
      }

      # reorder columns
      common_final <- intersect(train_cols, intersect(colnames(DT_val), colnames(DT_test)))
      DT_tr   <- DT_tr[, ..common_final]
      DT_val  <- DT_val[, ..common_final]
      DT_test <- DT_test[, ..common_final]

      # ------------------------------------------------------
      # Force all numeric
      # ------------------------------------------------------
      for (nm in names(DT_tr))  DT_tr[,  (nm) := as.numeric(get(nm))]
      for (nm in names(DT_val)) DT_val[, (nm) := as.numeric(get(nm))]
      for (nm in names(DT_test)) DT_test[, (nm) := as.numeric(get(nm))]

      # ------------------------------------------------------
      # Convert to matrices
      # ------------------------------------------------------
      X_tr <- as.matrix(DT_tr)
      X_val <- as.matrix(DT_val)
      X_tst <- as.matrix(DT_test)

      # ------------------------------------------------------
      # Build DMatrices
      # ------------------------------------------------------
      dtrain <- xgb.DMatrix(data = X_tr, label = y_tr)
      dval   <- xgb.DMatrix(data = X_val, label = y_val)
      dtest  <- xgb.DMatrix(data = X_tst)

      # ------------------------------------------------------
      # XGBoost parameter fix
      # ------------------------------------------------------
      params_clean <- xgb_params

      # Remove forbidden params
      forbidden <- c("nrounds", "niter")
      params_clean <- params_clean[!names(params_clean) %in% forbidden]

      # extract nrounds separately
      nrounds_local <- xgb_params$nrounds %||% xgb_params$niter %||% 200

      log("Training XGBoost with nrounds =", nrounds_local)

      model <- xgboost::xgb.train(
        params = params_clean,    # cleaned params (GPU params go here)
        data = dtrain,
        nrounds = nrounds_local,  # ONLY here
        watchlist = list(train = dtrain, eval = dval),
        verbose = if (verbose) 1L else 0L,
        early_stopping_rounds = 200
      )

      # ------------------------------------------------------
      # Predictions
      # ------------------------------------------------------
      val_pred  <- predict(model, dval)
      test_pred <- predict(model, dtest)

      fold_auc <- as.numeric(pROC::auc(y_val, val_pred))
      log(sprintf("Fold %d AUC: %.6f", fold, fold_auc))

      if (debug) save_snapshots(DT_tr, DT_val, DT_test, fold)

      return(list(
        fold = fold,
        val_idx = val_idx,
        val_pred = val_pred,
        test_pred = test_pred,
        fold_auc = fold_auc
      ))

    }, error = function(e) {
      if (debug) save_snapshots(DT_tr, DT_val, DT_test, fold)
      stop(sprintf("Error in fold %d: %s", fold, e$message))
    })
  }

  # ------------------------------------------------------
  # Execute folds (parallel or sequential)
  # ------------------------------------------------------
  if (parallel) {
    results <- future.apply::future_lapply(seq_len(folds), run_fold, future.seed = TRUE)
  } else {
    results <- lapply(seq_len(folds), run_fold)
  }

  # ------------------------------------------------------
  # Aggregate results
  # ------------------------------------------------------
  for (res in results) {
    oof_preds[res$val_idx] <- res$val_pred
    test_preds_sum <- test_preds_sum + res$test_pred / folds
    fold_aucs[res$fold] <- res$fold_auc
  }

  final_auc <- as.numeric(pROC::auc(train_dt[[target]], oof_preds))

  log("====== FINAL RESULTS ======")
  log("Final OOF AUC =", round(final_auc, 6))

  future::plan("sequential")

  return(list(
    oof_auc = final_auc,
    oof_preds = oof_preds,
    test_preds = test_preds_sum,
    fold_aucs = fold_aucs
  ))
}
