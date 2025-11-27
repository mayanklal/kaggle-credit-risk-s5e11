#######################################################################
# DEBUG LOGGER
#######################################################################

log_msg <- function(...) {
  cat(format(Sys.time()), " | ", paste0(...), "\n",
      file = "target_encoding_log.txt", append = TRUE)
}

#######################################################################
# MAIN STEP
#######################################################################

step_advanced_target_encoding <- function(recipe, ..., role = "predictor",
                                          trained = FALSE, skip = FALSE,
                                          id = rand_id("advanced_target_encoding"),
                                          aggs = c("mean"),
                                          cv = 5L,
                                          smooth = "auto",
                                          drop_original = FALSE,
                                          cols_to_encode = NULL,
                                          mappings = NULL,
                                          global_stats = NULL,
                                          cv_encoded_train = NULL,
                                          training_row_names = NULL) {

  log_msg("---- step_advanced_target_encoding() called ----")

  add_step(
    recipe,
    step_advanced_target_encoding_new(
      terms = enquos(...),
      role = role,
      trained = trained,
      skip = skip,
      id = id,
      aggs = aggs,
      cv = as.integer(cv),
      smooth = smooth,
      drop_original = drop_original,
      cols_to_encode = cols_to_encode,
      mappings = mappings,
      global_stats = global_stats,
      cv_encoded_train = cv_encoded_train,
      training_row_names = training_row_names
    )
  )
}

step_advanced_target_encoding_new <- function(terms, role, trained, skip, id,
                                             aggs, cv, smooth, drop_original,
                                             cols_to_encode, mappings,
                                             global_stats,
                                             cv_encoded_train,
                                             training_row_names) {

  step(
    subclass = "advanced_target_encoding",
    terms = terms,
    role = role,
    trained = trained,
    skip = skip,
    id = id,
    aggs = aggs,
    cv = cv,
    smooth = smooth,
    drop_original = drop_original,
    cols_to_encode = cols_to_encode,
    mappings = mappings,
    global_stats = global_stats,
    cv_encoded_train = cv_encoded_train,
    training_row_names = training_row_names
  )
}

#######################################################################
# AGGREGATOR
#######################################################################

.ate_apply_agg <- function(x, agg) {
  switch(
    agg,
    mean = if (length(x)>0) mean(x, na.rm=TRUE) else NA_real_,
    sd   = if (length(x)>0) stats::sd(x, na.rm=TRUE) else NA_real_,
    var  = if (length(x)>0) stats::var(x, na.rm=TRUE) else NA_real_,
    min  = if (length(x)>0) min(x, na.rm=TRUE) else NA_real_,
    max  = if (length(x)>0) max(x, na.rm=TRUE) else NA_real_,
    median = if (length(x)>0) stats::median(x, na.rm=TRUE) else NA_real_,
    sum   = if (length(x)>0) sum(x, na.rm=TRUE) else NA_real_,
    count = length(x),
    nunique = length(unique(x)),
    skew = {
      if (length(x) <= 1) return(0)
      m <- mean(x, na.rm=TRUE)
      s <- stats::sd(x, na.rm=TRUE)
      if (is.na(s) || s==0) 0 else mean((x-m)^3, na.rm=TRUE)/(s^3)
    },
    stop(paste0("Unsupported agg: ", agg))
  )
}

#######################################################################
# PREP (DEBUG-ENABLED)
#######################################################################

prep.step_advanced_target_encoding <- function(x, training, info = NULL, ...) {

  log_msg("---- PREP started ----")

  cols <- recipes::recipes_eval_select(x$terms, training, info)
  cols <- names(cols)
  log_msg("Selected columns for encoding: ", paste(cols, collapse=","))

  outcome_col <- info$variable[info$role=="outcome"]
  log_msg("Outcome column detected: ", outcome_col)

  DT_full <- data.table::as.data.table(training)
  log_msg("Initial DT_full names: ", paste(names(DT_full), collapse=","))

  rn <- rownames(training)
  if (is.null(rn)) rn <- as.character(seq_len(nrow(training)))

  old_names <- names(DT_full)
  new_names <- tolower(old_names)
  if (!identical(old_names, new_names)) {
    data.table::setnames(DT_full, old=old_names, new=new_names)
    cols <- tolower(cols)
    outcome_col <- tolower(outcome_col)
    log_msg("Lowercased DT_full names: ", paste(names(DT_full), collapse=","))
  }

  # Coerce outcome to numeric
  if (!is.numeric(DT_full[[outcome_col]])) {
    log_msg("Outcome is not numeric; coercing.")
    DT_full[, (outcome_col) := as.numeric(as.character(get(outcome_col)))]
  }
  if (any(is.na(DT_full[[outcome_col]]))) stop("Outcome contains NA after coercion.")

  y <- DT_full[[outcome_col]]
  global_stats <- list()
  for (agg in x$aggs) {
    global_stats[[agg]] <- .ate_apply_agg(y, agg)
    log_msg("Global stat[", agg, "] = ", global_stats[[agg]])
  }

  cv_encoded <- data.table::data.table(.row_id__ = rn)

  folds <- rsample::vfold_cv(training, v = x$cv)
  log_msg("Created ", x$cv, " folds.")

  #######################################################################
  # Fold Function (DEBUG)
  #######################################################################

  process_fold <- function(fold_index) {
    tryCatch({

      log_msg("---- Processing fold ", fold_index, " ----")

      split <- folds$splits[[fold_index]]

      #----------------------------------------------------------
      # CRITICAL FINAL FIX: extract integer row indices
      #----------------------------------------------------------
      train_idx <- rsample::analysis(split)$.row
      val_idx   <- rsample::assessment(split)$.row

      # Fallback if .row missing
      if (is.null(train_idx)) train_idx <- as.integer(rownames(rsample::analysis(split)))
      if (is.null(val_idx))   val_idx   <- as.integer(rownames(rsample::assessment(split)))

      log_msg("Fold ", fold_index, " | train rows: ", length(train_idx),
              " | val rows: ", length(val_idx))

      #----------------------------------------------------------
      # Subset using integer vector (now safe)
      #----------------------------------------------------------
      DT_train <- DT_full[train_idx]
      DT_val   <- DT_full[val_idx]

      log_msg("DT_train BEFORE lowercase: ", paste(names(DT_train), collapse=","))
      log_msg("DT_val BEFORE lowercase: "  , paste(names(DT_val), collapse=","))

      # enforce lowercase
      data.table::setnames(DT_train, tolower(names(DT_train)))
      data.table::setnames(DT_val,   tolower(names(DT_val)))

      log_msg("DT_train AFTER lowercase: ", paste(names(DT_train), collapse=","))
      log_msg("DT_val AFTER lowercase: "  , paste(names(DT_val), collapse=","))

      out_dt <- data.table::data.table(.row_id__ = rn[val_idx])

      #----------------------------------------------------------
      # Column Loop
      #----------------------------------------------------------
      for (col in cols) {
        log_msg("Encoding column: ", col)

        DT_train[, (col) := as.character(get(col))]
        DT_val[,   (col) := as.character(get(col))]

        for (agg in x$aggs) {

          log_msg("  Agg: ", agg)
          log_msg("  DT_train has col? ", col %in% names(DT_train))
          log_msg("  DT_val has col? ", col %in% names(DT_val))

          #------------------------------------------------------
          # Aggregation
          #------------------------------------------------------
          if (agg == "count") {
            map_dt <- DT_train[, .(val=.N), by=col]

          } else {
            map_dt <- DT_train[, .(val=.(.ate_apply_agg(get(outcome_col), agg))), by=col]
            map_dt[, val := sapply(val, function(v) if (length(v)==0) NA_real_ else v)]
          }

          # Clean mapping for non-mean
          if (agg != "mean") {
            map_dt <- map_dt[, .SD, .SDcols=c(col,"val")]
          }

          log_msg("  map_dt columns: ", paste(names(map_dt), collapse=","))
          log_msg("  map_dt head: ", paste(capture.output(print(head(map_dt))), collapse=" | "))

          #------------------------------------------------------
          # Mean with smoothing
          #------------------------------------------------------
          if (agg == "mean") {

            raw_stats <- DT_train[, .(n=.N,
                                      raw_mean=mean(get(outcome_col), na.rm=TRUE)), by=col]

            var_within <- DT_train[, .(v={
              tmp <- if (.N>1) stats::var(get(outcome_col), na.rm=TRUE) else 0
              if (is.na(tmp)) 0 else tmp
            }), by=col]

            avg_var_within <- mean(var_within$v, na.rm=TRUE)
            if (is.na(avg_var_within)) avg_var_within <- 0

            var_between <- stats::var(raw_stats$raw_mean, na.rm=TRUE)
            if (is.na(var_between)) var_between <- 0

            m <- x$smooth
            if (identical(x$smooth,"auto"))
              m <- ifelse(var_between>0, avg_var_within/var_between, 0)

            log_msg("  smoothing m = ", m)

            raw_stats[, val := (n * raw_mean + m * global_stats[["mean"]])/(n+m)]
            map_dt <- raw_stats[, .SD, .SDcols=c(col,"val")]
          }

          if (!(col %in% names(map_dt)))
            stop(paste0("Missing join key: ", col))

          log_msg("  Joining on ", col)

          data.table::setkeyv(map_dt, col)
          data.table::setkeyv(DT_val, col)

          joined <- map_dt[DT_val, on=col]

          log_msg("  joined head: ", paste(capture.output(print(head(joined))), collapse=" | "))

          vals <- joined$val
          vals[is.na(vals)] <- global_stats[[agg]]

          out_dt[, (paste0("TE_", col, "_", agg)) := vals]
        }
      }

      out_dt

    }, error=function(e) {
      log_msg("ERROR in fold ", fold_index, ": ", e$message)
      stop(e)
    })
  }

  log_msg("---- Starting fold processing ----")

  fold_results <- lapply(seq_along(folds$splits), process_fold)

  log_msg("---- Fold processing complete ----")

  fold_dt <- data.table::rbindlist(fold_results, use.names=TRUE, fill=TRUE)
  data.table::setkey(fold_dt, .row_id__)

  cv_encoded <- merge(cv_encoded, fold_dt,
                      by=".row_id__", all.x=TRUE, sort=FALSE)

  #######################################################################
  # FULL DATA MAPPINGS FOR BAKE
  #######################################################################

  log_msg("---- Building full-data mappings ----")

  mappings_list <- list()

  for (col in cols) {

    log_msg("Mapping col: ", col)

    DT_full[, (col) := as.character(get(col))]
    mappings_list[[col]] <- list()

    for (agg in x$aggs) {

      log_msg("  Agg: ", agg)

      if (agg=="count") {
        map_dt_full <- DT_full[, .(val=.N), by=col]

      } else if (agg=="mean") {

        raw_stats <- DT_full[, .(n=.N, raw_mean=mean(get(outcome_col),na.rm=TRUE)), by=col]

        var_within <- DT_full[, .(v={
          tmp <- if (.N>1) stats::var(get(outcome_col), na.rm=TRUE) else 0
          if (is.na(tmp)) 0 else tmp
        }),by=col]

        avg_var_within <- mean(var_within$v, na.rm=TRUE)
        if (is.na(avg_var_within)) avg_var_within <- 0

        var_between <- stats::var(raw_stats$raw_mean, na.rm=TRUE)
        if (is.na(var_between)) var_between <- 0

        m <- x$smooth
        if (identical(x$smooth,"auto"))
          m <- ifelse(var_between>0, avg_var_within/var_between, 0)

        raw_stats[, val := (n*raw_mean + m*global_stats[["mean"]])/(n+m)]
        map_dt_full <- raw_stats[, .SD, .SDcols=c(col,"val")]

      } else {

        map_dt_full <- DT_full[, .(val=.(.ate_apply_agg(get(outcome_col),agg))), by=col]
        map_dt_full[, val := sapply(val, function(v) if (length(v)==0) NA_real_ else v)]
      }

      if (agg!="mean")
        map_dt_full <- map_dt_full[, .SD, .SDcols=c(col,"val")]

      data.table::setkeyv(map_dt_full, col)
      mappings_list[[col]][[agg]] <- data.table::copy(map_dt_full)

      log_msg("    map_dt_full rows: ", nrow(map_dt_full))
    }
  }

  log_msg("---- PREP completed ----")

  step_advanced_target_encoding_new(
    terms = x$terms,
    role = x$role,
    trained = TRUE,
    skip = x$skip,
    id = x$id,
    aggs = x$aggs,
    cv = x$cv,
    smooth = x$smooth,
    drop_original = x$drop_original,
    cols_to_encode = cols,
    mappings = mappings_list,
    global_stats = global_stats,
    cv_encoded_train = cv_encoded,
    training_row_names = rn
  )
}

#######################################################################
# BAKE
#######################################################################

bake.step_advanced_target_encoding <- function(object, new_data, ...) {

  log_msg("---- BAKE started ----")

  DT_new <- data.table::as.data.table(new_data)
  log_msg("BAKE input names: ", paste(names(DT_new), collapse=","))

  rn_new <- rownames(new_data)
  if (is.null(rn_new)) rn_new <- as.character(seq_len(nrow(new_data)))

  if (!is.null(object$cv_encoded_train) &&
      identical(rn_new, object$training_row_names)) {

    log_msg("Returning CV-encoded training rows")

    cv_dt <- data.table::copy(object$cv_encoded_train)
    DT_new[, .row_id__ := rn_new]

    merged <- merge(DT_new, cv_dt, by=".row_id__", all.x=TRUE, sort=FALSE)
    merged[, .row_id__ := NULL]

    if (object$drop_original) merged[, (object$cols_to_encode) := NULL]

    log_msg("---- BAKE done (training rows) ----")
    return(tibble::as_tibble(merged))
  }

  log_msg("Applying full-data mappings")

  for (col in object$cols_to_encode) {
    log_msg("Encoding in BAKE: ", col)

    DT_new[, (col) := as.character(get(col))]

    for (agg in object$aggs) {

      map_dt <- object$mappings[[col]][[agg]]
      data.table::setkeyv(map_dt, col)
      data.table::setkeyv(DT_new, col)

      joined <- map_dt[DT_new, on=col]
      vals <- joined$val
      vals[is.na(vals)] <- object$global_stats[[agg]]

      DT_new[, (paste0("TE_", col, "_", agg)) := vals]
    }
  }

  if (object$drop_original)
    DT_new[, (object$cols_to_encode) := NULL]

  log_msg("---- BAKE finished ----")

  tibble::as_tibble(DT_new)
}

#######################################################################
# PRINT
#######################################################################

print.step_advanced_target_encoding <- function(x, ...) {
  cat("step_advanced_target_encoding: ",
      paste(x$cols_to_encode, collapse=","),
      " | aggs=", paste(x$aggs, collapse=","),
      " | cv=", x$cv, " | smooth=", x$smooth,
      " | drop=", x$drop_original, "\n")
  invisible(x)
}
