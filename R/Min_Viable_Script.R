# Load Libraries

# Data
library(data.table)

# EDA
library(tidyverse)

# Load Data
train <- fread(paste0(getwd(),"/data/raw/train.csv"))
View(train)

# Structure of Data
str(train)

# Summary of Data
summary(train)

# Plot target variable - Right Skewed
train %>% ggplot(aes(x = loan_paid_back)) +
  geom_bar()

# Plot histogram of annual_income - Left Skewed
train %>% ggplot(aes(x = annual_income)) +
  geom_histogram()

# Plot histogram of DTI - Left Skewed
train %>% ggplot(aes(x = debt_to_income_ratio)) +
  geom_histogram()

# Plot histogram of credit_score - Nearly Normal Distributed with slight left skewness
train %>% ggplot(aes(credit_score)) +
  geom_histogram()

# Plot histogram of loan_amount - Nearly normal distributed with outliers towards right
train %>% ggplot(aes(loan_amount)) +
  geom_histogram()

# Plot histogram of interest - Normal Distributed
train %>% ggplot(aes(interest_rate)) +
  geom_histogram()

# Plot bar plot of gender - Nearly equally distributed between female and male. There are other genders as well.
train %>% ggplot(aes(gender)) +
  geom_bar()

# Plot bar plot of Marital Status
train %>% ggplot(aes(marital_status)) +
  geom_bar()

# Plot bar plot of educational status
train %>% ggplot(aes(education_level)) +
  geom_bar()

# Plot bar plot of loan purpose
train %>% ggplot(aes(loan_purpose)) +
  geom_bar()

# Plot bar plot of grade subgrade
train %>% ggplot(aes(grade_subgrade)) + 
  geom_bar()



