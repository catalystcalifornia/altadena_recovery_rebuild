# Get Debris Removal Data from Army Corps of Engineers
# Data visualized here: https://jecop-public.usace.army.mil/portal/apps/experiencebuilder/experience/?id=efbee5617ffa4d17b572d5f312004806
# Will use arcgis rest API services to try and extract data which seems to include EPA status and any steps following Phase 1 debris cleanup
# e.g., ROE submission, HHW removal, etc.

library(httr)
library(jsonlite)
library(dplyr)
library(sf)

source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")

# Base configuration
BASE_URL <- "https://jecop-public.usace.army.mil/arcgis/rest/services/USACE_Debris_Parcels_Southern_California_Public/MapServer/0/query"
BATCH_SIZE <- 2000  # Max records per request, set by API
CSV_FILEPATH <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\USACE Debris Data\\usace_debris_data.csv"

##### Helper Functions #####
#' Fetch all records from ArcGIS service as json, with automatic pagination
# @return List of all features
fetch_all_records <- function(output_format = 'json') {
  all_features <- list()
  offset <- 0
  total_fetched <- 0
  
  cat(sprintf("Starting download in %s format...\n", output_format))
  
  repeat {
    # Build query parameters
    query_params <- list(
      where = '1=1',
      outFields = '*',
      f = output_format,
      resultOffset = offset,
      resultRecordCount = BATCH_SIZE,
      returnGeometry = 'true'
    )
    
    cat(sprintf("Fetching records %d to %d...\n", offset, offset + BATCH_SIZE))
    
    # Make request
    response <- tryCatch({
      GET(BASE_URL, query = query_params, timeout(30))
    }, error = function(e) {
      cat(sprintf("Request failed: %s\n", e$message))
      return(NULL)
    })
    
    if (is.null(response)) break
    
    # Check response status
    if (status_code(response) != 200) {
      cat(sprintf("Error: HTTP %d\n", status_code(response)))
      break
    }
    
    # Parse JSON
    data <- tryCatch({
      content(response, as = "parsed", type = "application/json")
    }, error = function(e) {
      cat(sprintf("Failed to parse JSON: %s\n", e$message))
      return(NULL)
    })
    
    if (is.null(data)) break
    
    # Check for errors in response
    if (!is.null(data$error)) {
      cat(sprintf("Error from server: %s\n", data$error$message))
      break
    }
    
    # Extract features
    features <- data$features
    
    if (is.null(features) || length(features) == 0) {
      cat("No more records found.\n")
      break
    }
    
    all_features <- c(all_features, features)
    total_fetched <- total_fetched + length(features)
    cat(sprintf("  Retrieved %d records. Total so far: %d\n", 
                length(features), total_fetched))
    
    # If we got fewer records than requested, we've reached the end
    if (length(features) < BATCH_SIZE) {
      cat("Reached end of dataset.\n")
      break
    }
    
    offset <- offset + BATCH_SIZE
    
    # Be nice to the server
    Sys.sleep(0.5)
  }
  
  return(all_features)
}

#' Convert features to data frame
#'
#' @param features List of features from ArcGIS
#' @return Data frame with attributes
features_to_df <- function(features) {
  # Extract attributes from each feature
  attrs_list <- lapply(features, function(f) {
    attrs <- f$attributes
    
    # Handle NULL or empty attributes
    if (is.null(attrs) || length(attrs) == 0) {
      return(NULL)
    }
    
    # Convert NULL values to NA for consistency
    attrs <- lapply(attrs, function(x) {
      if (is.null(x)) NA else x
    })
    
    # Try to convert to data frame, handle errors
    tryCatch({
      as.data.frame(attrs, stringsAsFactors = FALSE)
    }, error = function(e) {
      cat(sprintf("Warning: Skipping problematic record\n"))
      return(NULL)
    })
  })
  
  # Remove NULL entries
  attrs_list <- attrs_list[!sapply(attrs_list, is.null)]
  
  # Combine into single data frame
  df <- bind_rows(attrs_list)
  
  return(df)
}

#' Convert features to sf object (spatial data frame)
#'
#' @param features List of features from ArcGIS
#' @return sf object with geometry
features_to_sf <- function(features) {
  # Extract attributes
  df <- features_to_df(features)
  
  # Extract geometries
  geoms <- lapply(features, function(f) {
    if (!is.null(f$geometry) && !is.null(f$geometry$rings)) {
      # Polygon geometry
      coords <- f$geometry$rings[[1]]
      coords_matrix <- do.call(rbind, coords)
      st_polygon(list(coords_matrix))
    } else {
      st_polygon()  # Empty geometry
    }
  })
  
  # Create sf object
  sf_obj <- st_sf(df, geometry = st_sfc(geoms, crs = 4326))
  
  return(sf_obj)
}

##### Extract data from ArcGIS #####

# Fetch all records
features <- fetch_all_records(output_format = 'json')

if (length(features) > 0) {
  cat(sprintf("\nTotal records fetched: %d\n", length(features)))
  
  # Convert to data frame
  cat("\nConverting to data frame...\n")
  debris_df <- features_to_df(features) # 14351
  
  # Save as CSV
  write.csv(debris_df, CSV_FILEPATH, row.names = FALSE)
  cat("Data saved to: usace_debris_data.csv\n")
  
  cat("\nDownload complete!\n")
  cat(sprintf("Dimensions: %d rows x %d columns\n", nrow(debris_df), ncol(debris_df)))
  
  # Print column names
  cat("\nColumn names:\n")
  print(names(debris_df))
  
} else {
  cat("\nNo data retrieved.\n")
}

##### minor clean up and export to postgres #####
eaton_df <- debris_df %>%
  filter(event_sub_name=="Eaton") %>% # 7004
  mutate(ain = gsub("-", "", apn))

table_name <- "usace_debris_removal_parcels_2025"
date_ran <- as.character(Sys.Date())

dbWriteTable(con, Id(schema="data", table_name=table_name), eaton_df,
             overwrite = FALSE, row.names = FALSE)

dbSendQuery(con, paste0("COMMENT ON TABLE data.", table_name, " IS
            'Debris removal data for parcels impacted by the Eaton Fire,
            Data imported on ",date_ran, "
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_permit_typology.docx
            Source: US Army Corps of Engineers'"))

dbDisconnect(con)
