# title: "00_setup.R"
# author: "Mayank Lal"
# date: "November 9, 2025"

# ================================================================
#  Auto-install and load required R packages (preferring Conda binaries)
#  Works inside a Conda-based R environment
# ================================================================

# --- List of required packages
required_packages <- c(
  "data.table", "plyr", "purrr", "formattable", "tidyr", "flextable",
  "lubridate", "dplyr", "tidyverse", "janitor", "odbc", "DBI", "dbplyr",
  "lubridate", "tidylog", "stringi", "timeDate", "officer",
  "rjson", "crosstalk", "DT", "openxlsx", "reshape2",
  "kableExtra", "knitr", "grid", "readr", "zoo", "TSstudio", "forecast",
  "tseries", "config", "mgcv", "highcharter", "RODBC"
)

# --- Helper function to check installation
is_installed <- function(pkg) {
  suppressWarnings(requireNamespace(pkg, quietly = TRUE))
}

# --- Install via Conda if possible
install_via_conda <- function(pkg) {
  conda_path <- "/home/mayanklal/miniconda/condabin/conda"
  if (!file.exists(conda_path)) {
    message("⚠️ Conda not found at expected path. Skipping Conda install.")
    return(FALSE)
  }
  cmd <- sprintf('%s install -y -c conda-forge r-%s', conda_path, pkg)
  message(sprintf("Trying Conda install for '%s'...", pkg))
  res <- system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
  if (res == 0) {
    message(sprintf("✅ Conda install successful for '%s'.", pkg))
    return(TRUE)
  } else {
    message(sprintf("⚠️ Conda install failed for '%s'. Will try CRAN next.", pkg))
    return(FALSE)
  }
}


# --- Fallback to CRAN if Conda fails
install_via_cran <- function(pkg) {
  message(sprintf("Installing '%s' from CRAN...", pkg))
  install.packages(pkg, repos = "https://cloud.r-project.org")
}

# --- Main loop
for (pkg in required_packages) {
  if (!is_installed(pkg)) {
    message(sprintf("'%s' not found.", pkg))
    success <- install_via_conda(pkg)
    if (!success && !is_installed(pkg)) {
      install_via_cran(pkg)
    }
  } else {
    message(sprintf("'%s' already installed ✅", pkg))
  }
}

message("\n✅ All required packages are now available.\n")


#Load Libraries
lapply(required_packages, require, character.only = TRUE)

# Get Working Directory
getwd()

# Load Configuration Files
#thresholds <- config::get(file = "configs/thresholds.yaml")
# model_params <- config::get(file = "configs/model_params.yaml")

# Create log file
# logfilename <- paste0("logs/log_", format(Sys.Date(), "%d_%m_%Y"), ".log")
# log <- file(logfilename)
# sink(log, append = TRUE)
# sink(log, append = TRUE, type = "message")

# Set seed
set.seed(12)
