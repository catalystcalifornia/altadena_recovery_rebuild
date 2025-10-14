# Objective: Scrape LA County Permit Portal for detailed permit data, based on 
# general permit data collected via scrape_parcel_permits.R

library(dplyr) 

source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\scraping_functions.R")

# set some metadata for exporting results
general_table_name <- paste("general_permit_data", 
                    strsplit(as.character(Sys.Date()), "-", fixed=TRUE)[[1]][1], # year
                    strsplit(as.character(Sys.Date()), "-", fixed=TRUE)[[1]][2], # month
                    sep="_") 

detailed_table_name <- paste("detailed_permit_data", 
                             strsplit(as.character(Sys.Date()), "-", fixed=TRUE)[[1]][1], # year
                             strsplit(as.character(Sys.Date()), "-", fixed=TRUE)[[1]][2], # month
                             sep="_") 

workflow_table_name <- paste("workflow_permit_data", 
                             strsplit(as.character(Sys.Date()), "-", fixed=TRUE)[[1]][1], # year
                             strsplit(as.character(Sys.Date()), "-", fixed=TRUE)[[1]][2], # month
                             sep="_") 

date_ran <- as.character(Sys.Date())

detailed_csv_filepath <- paste0("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Permit Data Prepped\\", detailed_table_name, ".csv")
workflow_csv_filepath <- paste0("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Permit Data Prepped\\", workflow_table_name, ".csv")


# Filter for Eaton Fire permits
# keeping a wide net for now could be more restrictive (i.e., specific permit type AND applied in 2025 AND mention eaton (or fire))
# with wide net will get permits other than rebuild like ELEC, Construction and Demolition
# Get this month's permits
con <- connect_to_db("altadena_recovery_rebuild")
general_permits <- dbGetQuery(con, statement=paste0("SELECT * FROM data.", general_table_name))
dbDisconnect(con)

permits_filtered <- general_permits %>%
  filter(grepl("2025$", applied_date)) %>%
  filter(grepl("eaton|fire", project_name, ignore.case = TRUE) | 
           grepl("eaton|fire", description, ignore.case = TRUE) |
           grepl("UNC-BLDR|CREB", permit_number, ignore.case=TRUE))

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
  
  scraped_permits <- prev_data %>%
    select(permit_number, ain) %>%
    mutate(id = paste(permit_number, ain, sep = "_")) %>%
    unique()
  
  remaining <- data.frame(id=setdiff(permits_to_scrape$id, scraped_permits$id)) %>%
    left_join(permits_to_scrape, by="id")
  
  include_headers <- FALSE
  append_value <- TRUE
  
} else {
  print("boo")
  
  remaining <- permits_to_scrape
  include_headers <- TRUE
  append_value <- FALSE
}

closeAllConnections()
gc() 

for (row_ in 1:nrow(remaining)) {   
  row_ain <- remaining[row_, "ain"]
  row_permit <- remaining[row_, "permit_number"]
  permit_href <- remaining[row_, "permit_href"]
  permit_url <- paste0(base_url, permit_href,"/")

  # note: should add flag to know when page is loaded (<div class="form-group" id="focusText" tabindex="1"><label>Permit Number:</label><span class="ng-binding">UNC-BLDR250826010738</span></div>)
  message(paste(row_, ":", permit_url))
  results <- scrape_permits_detailed(
    url=permit_url, 
    url_suffix=url_location_suffix, 
    ain = row_ain, 
    permit_number=row_permit, 
    wait_time = 30, 
    max_retries = 1, 
    retry_wait_time = 60)
  
  result_detailed <- data.frame(results$permit_details)
  result_workflow <- data.frame(results$workflow)
  
  
  # Write initial data (with header)
  write.table(result_detailed, 
              file = detailed_csv_filepath, 
              sep = ",", 
              row.names = FALSE, 
              col.names = include_headers,
              append = append_value,
              fileEncoding = "UTF-8",
              quote = TRUE,
              qmethod = "double")
  
  write.table(result_workflow, 
              file = workflow_csv_filepath, 
              sep = ",", 
              row.names = FALSE, 
              col.names = include_headers,
              append = append_value,
              fileEncoding = "UTF-8",
              quote = TRUE,
              qmethod = "double")
  
  include_headers <- FALSE
  append_value <- TRUE
  
  Sys.sleep(1)
}

#### Get final data and export to pg

final_detailed_data <- read.csv(detailed_csv_filepath,
                       encoding = "UTF-8",
                       colClasses = c("character"))

final_workflow_data <- read.csv(workflow_csv_filepath,
                                encoding = "UTF-8",
                                colClasses = c("character"))



con <- connect_to_db("altadena_recovery_rebuild")

dbWriteTable(con, Id(schema="data", table_name=detailed_table_name), final_detailed_data,
             overwrite = FALSE, row.names = FALSE)

dbSendQuery(con, paste0("COMMENT ON TABLE data.", detailed_table_name, " IS
            'Detailed permit data for Altadena parcels with some or significant damage,
            Data imported on ",date_ran, "
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_scrape_permit_data.docx
            Source: https://epicla.lacounty.gov/energov_prod/SelfService/[permit_href]'"))

dbWriteTable(con, Id(schema="data", table_name=workflow_table_name), final_workflow_data,
             overwrite = FALSE, row.names = FALSE)

dbSendQuery(con, paste0("COMMENT ON TABLE data.", workflow_table_name, " IS
            'Extended detailed permit data that includes workflow items for Altadena parcels with some or significant damage,
            Data imported on ",date_ran, "
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_scrape_permit_data.docx
            Source: https://epicla.lacounty.gov/energov_prod/SelfService/[permit_href]'"))

dbDisconnect(con)

