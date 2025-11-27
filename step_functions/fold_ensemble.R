#-----------------------------------------------
# Inputs required:
#   train_df  = training data with target
#   test_df   = competition test set
#   rec       = your feature engineering recipe
#   mod       = your xgboost model spec
#   v = number of folds
#-----------------------------------------------

fold_ensemble <- function(train_df, test_df, rec, mod, v = 5, target) {

  # Convert test to tibble for tidymodels
  test_df <- tibble::as_tibble(test_df)

  # Prepare outer folds
  set.seed(123)
  folds <- vfold_cv(train_df, v = v, strata = !!sym(target))

  # Prepare containers
  oof_preds <- numeric(nrow(train_df))
  test_pred_sum <- numeric(nrow(test_df))

  # Row index mapping
  train_indices <- 1:nrow(train_df)

  # Loop over folds
  for (i in seq_along(folds$splits)) {
    cat("---- Fold", i, "of", v, "----\n")

    split  <- folds$splits[[i]]
    tr_idx <- assessment(split, data = FALSE)$in_id      # validation row ids (0/1)
    val_idx <- which(tr_idx == 0)                         # invert (tidymodels encoding)
    tr_idx <- which(tr_idx == 1)

    # Extract data
    train_fold <- train_df[tr_idx, ]
    val_fold   <- train_df[val_idx, ]

    #--------------------------------------
    # Prep recipe on fold-train data only
    # This runs:
    #   - step_bigrams
    #   - step_round
    #   - step_agg_encoding
    #   - step_advanced_target_encoding (internal CV)
    #--------------------------------------
    rec_fold <- prep(rec, training = train_fold, retain = TRUE)

    # Fold-encoded training and validation
    X_train_encoded <- bake(rec_fold, new_data = train_fold)
    X_val_encoded   <- bake(rec_fold, new_data = val_fold)

    # Fold-encoded test
    X_test_encoded  <- bake(rec_fold, new_data = test_df)

    #--------------------------------------
    # Fit model on encoded training fold
    #--------------------------------------
    wf <- workflow() %>%
      add_recipe(rec) %>%
      add_model(mod)

    fitted_fold <- fit(wf, data = train_fold)

    #--------------------------------------
    # Validation predictions (OOF)
    #--------------------------------------
    val_probs <- predict(fitted_fold, X_val_encoded, type = "prob")$.pred_1
    oof_preds[val_idx] <- val_probs

    fold_auc <- roc_auc(
      bind_cols(val_fold, tibble(.pred_1 = val_probs)),
      truth = !!sym(target),
      .pred_1
    )
    cat("Fold AUC:", round(fold_auc$.estimate, 5), "\n")

    #--------------------------------------
    # Test predictions (ensemble)
    #--------------------------------------
    fold_test_probs <- predict(fitted_fold, X_test_encoded, type = "prob")$.pred_1
    test_pred_sum <- test_pred_sum + fold_test_probs / v
  }

  #-------------------------
  # Final OOF AUC
  #-------------------------
  oof_auc <- roc_auc(
    bind_cols(train_df, tibble(.pred_1 = oof_preds)),
    truth = !!sym(target),
    .pred_1
  )

  cat("======================\n")
  cat("Overall OOF AUC:", round(oof_auc$.estimate, 5), "\n")
  cat("======================\n")

  return(list(
    oof_auc = oof_auc$.estimate,
    oof_preds = oof_preds,
    test_preds = test_pred_sum
  ))
}
