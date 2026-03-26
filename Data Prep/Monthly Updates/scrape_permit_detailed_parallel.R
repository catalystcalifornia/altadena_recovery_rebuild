# Objective: Scrape LA County Permit Portal for detailed permit data, based on 
# general permit data collected via scrape_parcel_permits.R

library(dplyr)
library(furrr)
library(future)

source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\Monthly Updates\\scraping_functions.R")

con <- connect_to_db("altadena_recovery_rebuild")
schema <- "dashboard"

# set some metadata for exporting results
date_ran <- as.character(Sys.Date())
curr_year <- strsplit(date_ran, "-", fixed=TRUE)[[1]][1] # year
curr_month <- strsplit(date_ran, "-", fixed=TRUE)[[1]][2] # month

general_table_name <- paste("scraped_general_permit_data", 
                            curr_year, # year
                            curr_month, # month
                            sep="_") 

detailed_table_name <- paste("scraped_detailed_permit_data", 
                             curr_year, # year
                             curr_month, # month
                             sep="_") 

workflow_table_name <- paste("scraped_workflow_permit_data", 
                             curr_year, # year
                             curr_month, # month
                             sep="_") 

detailed_table_name <- paste0(detailed_table_name, "_parallel")
workflow_table_name <- paste0(workflow_table_name, "_parallel")

detailed_csv_filepath <- paste0("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Permit Data Prepped\\", detailed_table_name, ".csv")
workflow_csv_filepath <- paste0("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Permit Data Prepped\\", workflow_table_name, ".csv")


# Filter for Eaton Fire permits
# keeping a wide net for now could be more restrictive (i.e., specific permit type AND applied in 2025 AND mention eaton (or fire))
# with wide net will get permits other than rebuild like ELEC, Construction and Demolition
# Get this month's permits
con <- connect_to_db("altadena_recovery_rebuild")
general_permits <- dbGetQuery(con, statement=paste0("SELECT * FROM ", schema, ".", general_table_name))
dbDisconnect(con)

permits_filtered <- general_permits %>%
  # filter out permits applied for before Eaton Fire
  filter(as.Date(applied_date, format = "%m/%d/%Y") > as.Date("2025-01-07")) %>%
  filter(!(status %in% c("Void", "Canceled", "Denied")))

base_url <- "https://epicla.lacounty.gov/energov_prod/SelfService/"

url_location_suffix <- "?tab=locations"

permits_to_scrape <- permits_filtered %>%
  select(permit_number, permit_href, ain) %>%
  mutate(id = paste(permit_number, ain, sep = "_")) %>%
  unique()

if (file.exists(detailed_csv_filepath)) {
  prev_data <- read.csv(detailed_csv_filepath,
                        encoding = "UTF-8",
                        colClasses = c("character"))
  
  # we want unsuccessful scrapes to get scraped again so only include 
  # successful scrapes as prev_data
  prev_data_filtered <- prev_data %>%
    filter(response_status == "success")
  
  scraped_permits <- prev_data_filtered %>%
    select(permit_number, ain) %>%
    mutate(id = paste(permit_number, ain, sep = "_")) %>%
    unique()
  
  remaining <- data.frame(id=setdiff(permits_to_scrape$id, scraped_permits$id)) %>%
    left_join(permits_to_scrape, by="id")
  
} else {
  print("there's no csv with that name - starting scrape from the beginning")
  
  remaining <- permits_to_scrape
}

closeAllConnections()
gc() 

if (nrow(remaining)> 0){
  # Set up parallel processing with batches to manage memory
  # Use fewer workers than cores to be respectful to the website
  plan(multisession, workers = 8)
  
  # Process in small batches to write incrementally and manage memory
  batch_size <- 50  # Adjust based on your memory constraints
  batches <- split(remaining, ceiling(seq_len(nrow(remaining)) / batch_size))
  
  first_write_this_session <- !file.exists(detailed_csv_filepath)
  
  # Function to scrape one permit
  scrape_one_permit <- function(row_data) {
    permit_url <- paste0(base_url, row_data$permit_href, "/")
    
    tryCatch({
      message(paste("Scraping:", permit_url))
      
      results <- scrape_permits_detailed(
        url = permit_url, 
        url_suffix = url_location_suffix, 
        ain = row_data$ain, 
        permit_number = row_data$permit_number, 
        wait_time = 30, 
        max_retries = 1, 
        retry_wait_time = 60
      )
      
      Sys.sleep(1)  # Still be polite even in parallel
      
      return(list(
        detailed = data.frame(results$permit_details),
        workflow = data.frame(results$workflow),
        success = TRUE
      ))
      
    }, error = function(e) {
      message(paste("Error scraping", row_data$permit_number, ":", e$message))
      return(list(
        detailed = data.frame(
          permit_number = row_data$permit_number,
          ain = row_data$ain,
          response_status = "error",
          error_message = e$message
        ),
        workflow = data.frame(),
        success = FALSE
      ))
    })
  }
  
  # Process each batch
  for (i in seq_along(batches)) {
    message(paste("\n=== Processing batch", i, "of", length(batches), "==="))
    
    current_batch <- batches[[i]]
    
    # Scrape this batch in parallel
    batch_results <- future_map(
      split(current_batch, seq_len(nrow(current_batch))),
      scrape_one_permit,
      .options = furrr_options(seed = TRUE),
      .progress = TRUE
    )
    
    # Extract results from this batch
    batch_detailed <- bind_rows(lapply(batch_results, function(x) x$detailed))
    batch_workflow <- bind_rows(lapply(batch_results, function(x) x$workflow))
    
    # Write this batch to CSV (append mode after first write)
    write.table(batch_detailed, 
                file = detailed_csv_filepath, 
                sep = ",", 
                row.names = FALSE, 
                col.names = first_write_this_session,  # Headers only on first write
                append = !first_write_this_session,    # Don't append on first write
                fileEncoding = "UTF-8",
                quote = TRUE,
                qmethod = "double")
    
    write.table(batch_workflow, 
                file = workflow_csv_filepath, 
                sep = ",", 
                row.names = FALSE, 
                col.names = first_write_this_session,  # Headers only on first write
                append = !first_write_this_session,    # Don't append on first write
                fileEncoding = "UTF-8",
                quote = TRUE,
                qmethod = "double")
    
    # After first write, switch to append mode
    first_write_this_session <- FALSE
    
    # Clear batch results from memory
    rm(batch_results, batch_detailed, batch_workflow)
    gc()
    
    message(paste("Batch", i, "written to CSV"))
  }
}

# note: there's a chance that the scrape could be interrupted between writes to the detailed and workflow csvs
# should check that all ids are in both csvs
# it's unlikely to have a big impact but still good to resolve when there's more time.

#### Get final data and export to pg

final_detailed_data <- read.csv(detailed_csv_filepath,
                                encoding = "UTF-8",
                                colClasses = c("character"))

final_workflow_data <- read.csv(workflow_csv_filepath,
                                encoding = "UTF-8",
                                colClasses = c("character"))



con <- connect_to_db("altadena_recovery_rebuild")

dbWriteTable(con, Id(schema=schema, table_name=detailed_table_name), final_detailed_data,
             overwrite = FALSE, row.names = FALSE)

dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", detailed_table_name, " IS
            'Detailed permit data for Altadena parcels with some or significant damage,
            Data imported on ",date_ran, "
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_monthly_scrape.docx
            Source: https://epicla.lacounty.gov/energov_prod/SelfService/[permit_href]'"))

dbWriteTable(con, Id(schema=schema, table_name=workflow_table_name), final_workflow_data,
             overwrite = FALSE, row.names = FALSE)

dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", workflow_table_name, " IS
            'Extended detailed permit data that includes workflow items for Altadena parcels with some or significant damage,
            Data imported on ",date_ran, "
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_monthly_scrape.docx
            Source: https://epicla.lacounty.gov/energov_prod/SelfService/[permit_href]'"))

dbDisconnect(con)



##### Combine and export CSV for ECI to review #####
# con <- connect_to_db("altadena_recovery_rebuild")
# general_permits <- dbGetQuery(con, statement=paste0("SELECT * FROM data.", general_table_name))
# detailed <- dbGetQuery(con, statement=paste0("SELECT * FROM data.", detailed_table_name)) %>%
#   mutate(eaton_fire="Y")
# workflow <- dbGetQuery(con, statement=paste0("SELECT * FROM data.", workflow_table_name))
# dbDisconnect(con)
# 
# combined <- general_permits %>% 
#   left_join(detailed, by=c("permit_number", "ain"), suffix=c(".general", ".detailed")) %>%
#   mutate(eaton_fire = replace_na(eaton_fire, "N")) %>%
#   left_join(workflow, by=c("permit_number", "ain"), suffix=c("", ".workflow")) %>% 
#   select(ain, record_id, eaton_fire, permit_number, ends_with(".general"), permit_href, expiration_date, ends_with(".detailed"), ends_with(".workflow"), everything())
# 
# write.csv(combined, file="W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Permit Data Prepped\\ECI to review\\scraped_permit_data.csv",
#           row.names=FALSE, fileEncoding="UTF-8")
# 
# colnames(combined)