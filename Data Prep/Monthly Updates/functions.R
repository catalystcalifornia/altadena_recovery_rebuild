
library(tibble)
transform_ain_to_2025_12 <- function(ain_vector, xwalk_df) {
  lookup <- xwalk_df %>%
    select(starts_with("ain_")) %>%
    pivot_longer(
      cols = -ain_2025_12,
      values_to = "historical_ain"
    ) %>%
    filter(!is.na(historical_ain)) %>%
    distinct(historical_ain, ain_2025_12) %>%
    deframe()  # Creates a named vector for fast lookup
  
  # Use lookup, return original if not found
  result <- ifelse(ain_vector %in% names(lookup), 
                   lookup[ain_vector], 
                   ain_vector)
  
  return(result)
}

# # Usage:
# transformed_ains <- transform_ain_to_2025_12(ains, xwalk_parcels)