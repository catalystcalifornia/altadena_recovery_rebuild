
library(tibble)
transform_ain_to_curr <- function(ain_vector, xwalk_df, curr_ain) {
  # Get all AIN columns except the current one
  historical_cols <- setdiff(names(xwalk_df)[grepl("^ain_", names(xwalk_df))], curr_ain)
  
  # Create lookup by pivoting all historical AIns to current AIN
  lookup <- xwalk_df %>%
    select(all_of(c(curr_ain, historical_cols))) %>%
    pivot_longer(
      cols = all_of(historical_cols),
      names_to = "vintage",
      values_to = "historical_ain"
    ) %>%
    filter(!is.na(historical_ain)) %>%
    # Keep first match if duplicates exist
    distinct(historical_ain, .keep_all = TRUE) %>%
    select(historical_ain, current_ain = all_of(curr_ain)) %>%
    deframe()
  
  # Map to current AIN, keep original if not found
  lookup[ain_vector] %>% 
    replace(is.na(.), ain_vector[is.na(.)])
}

# # Usage:
# transformed_ains <- transform_ain_to_2025_12(ains, xwalk_parcels)