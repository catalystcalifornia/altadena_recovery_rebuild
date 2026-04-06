# Objective: Scrape LA County Permit Portal for information on building permits
# for thousands of addresses/parcels, and assess possibility of regularly 
# scraping these sites.

library(dplyr) 

source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\Monthly Updates\\scraping_functions.R")

con <- connect_to_db("altadena_recovery_rebuild")
schema <- "dashboard"

# set some metadata for exporting results
date_ran <- as.character(Sys.Date())
curr_year <- "2026" # strsplit(date_ran, "-", fixed=TRUE)[[1]][1] # year
curr_month <- "04" # strsplit(date_ran, "-", fixed=TRUE)[[1]][2] # month
table_name <- paste("scraped_general_permit_data", 
                    curr_year, # year
                    curr_month, # month
                    sep="_") 

prev_xwalk_month <- "12"
curr_xwalk_month <- "04"
curr_xwalk_year <- "2026"

curr_xwalk_table <- paste0("dashboard.crosswalk_assessor_", curr_xwalk_year, "_", prev_xwalk_month, "_", curr_xwalk_month) ### MUST UPDATE


# EPIC LA - LA County building permits (unincorporated cities only)
lac_permits_url <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st="


xwalk <- dbGetQuery(con, paste("SELECT * FROM", curr_xwalk_table, ";"))
ains <- xwalk %>% select(starts_with("ain_"))
ains_list <- unlist(ains) %>% unique() 
cat(paste("Unique number of AINS:", length(ains_list))) #5685
dbDisconnect(con)

##### Establish scraping process #####
# 1. Confirm chromote is working properly with simple site like google.com
test_chromote()

# 2. If above works, try to extract data from one test url to get general data fields
url_ <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st=2204%20Grand%20Oaks%20Avenue"
message(paste("Current search URL:", url_))
permits <- scrape_permits_chromote(url=url_, wait_time = 30)

# 3. If above works, scale it up to loop through a list of AINS and return all permits (with general data fields)
csv_filepath <- paste0("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Permit Data Prepped\\", table_name, ".csv")

##### Start scraping #####
# # if csv already  exists for this table see where it left off, pull in results and pick up on next row

if (file.exists(csv_filepath)) {
  prev_data <- read.csv(csv_filepath,
                        encoding = "UTF-8",
                        colClasses = c("character"))
  
  # Only count successful scrapes as "done"
  prev_data_filtered <- prev_data %>%
    filter(response_status == "success")
  
  scraped_ains <- prev_data_filtered %>%  
    pull(ain) %>%
    unique()
  
  remaining <- data.frame(ain=setdiff(ains_list, scraped_ains))
  
  
} else {
  print("there's no csv with that name - starting scrape from the beginning")
  
  remaining <- data.frame(ain=ains_list)
  
}

closeAllConnections()
gc() 

first_write_this_session <- !file.exists(csv_filepath)

if (nrow(remaining)> 0){
  for (row_ in 1:nrow(remaining)) { 
    
    row_ain <- remaining[row_, "ain"]
    portal_url <- paste0(lac_permits_url, row_ain)
    
    message(paste(row_, ":", portal_url))
    result <- scrape_permits_chromote(
      url=portal_url, 
      wait_time = 30,
      ain = row_ain, 
      max_retries = 1, 
      retry_wait_time = 60)
    
    
    # Write initial data (with header)
    write.table(result,
                file = csv_filepath,
                sep = ",",
                row.names = FALSE,
                col.names = first_write_this_session,  # Headers only on first write
                append = !first_write_this_session,    # Don't append on first write
                fileEncoding = "UTF-8",
                quote = TRUE,
                qmethod = "double")
    
    # After first write, switch to append mode
    first_write_this_session <- FALSE
    
    Sys.sleep(1)
  }
}


final_data <- read.csv(csv_filepath,
                       encoding = "UTF-8",
                       colClasses = c("character"))



con <- connect_to_db("altadena_recovery_rebuild")

dbWriteTable(con, Id(schema=schema, table_name=table_name), final_data,
             overwrite = FALSE, row.names = FALSE)

dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", table_name, " IS
            'General permit data for Altadena parcels with some or significant damage,
            Data imported on ",date_ran, "
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_monthly_scrape.docx
            Source: https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st=[ain]'"))

dbDisconnect(con)

# # QA Checks
# # read in pg table and check
# con <- connect_to_db("altadena_recovery_rebuild")
# check_df <- dbGetQuery(con, sprintf("SELECT * FROM %s.%s;", schema, table_name ))
# 
# 
# # 1. if any retried failed (error, timeout) - if not too many manually confirm if these had no results (fine) or we missed their permits (bad)
# # ## Check the response_status to see if we got any "timeout" or "error" for a given request
# # ## will rerun each once more with a longer wait time to see if we get better results
# unsuccessful_requests <- check_df %>%
#   filter(response_status != "success")
# 
# # use EPIC LA to spot check a few of these
# spot_check_5 <- unsuccessful_requests %>% slice_sample(n=5)
# # https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st=5847021016 - true error, should have 5 permits
# # https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st=5847020010 - true error, should have 1 permit
# # https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st=5841029013 - true error, should have 8 permits
# # https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st=5846020018 - true error, should have 8 permits
# # https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st=5846021002 - true error, should have 16 permits
# 
# # if issues are true errors, run the next code to get the permitting data and update the postgres table
# 
# if (nrow(unsuccessful_requests)>0) {
#   for (row_ in 1:nrow(unsuccessful_requests)) {
# 
#     row_ain <- unsuccessful_requests[row_, "ain"]
#     portal_url <- paste0(lac_permits_url, row_ain)
# 
#     message(paste(row_, ":", portal_url))
#     result <- scrape_permits_chromote(
#       url=portal_url,
#       wait_time = 30,
#       ain = row_ain,
#       max_retries = 1,
#       retry_wait_time = 60)
# 
# 
#     # Write initial data (with header)
#     write.table(result,
#                 file = csv_filepath,
#                 sep = ",",
#                 row.names = FALSE,
#                 col.names = first_write_this_session,  # Headers only on first write
#                 append = !first_write_this_session,    # Don't append on first write
#                 fileEncoding = "UTF-8",
#                 quote = TRUE,
#                 qmethod = "double")
# 
#     Sys.sleep(3)
#   }
# }
# 
# # clean up
# final_data_clean <- read.csv(csv_filepath,
#                              encoding = "UTF-8",
#                              colClasses = c("character"))
# retried <- final_data_clean %>% 
#   filter(ain %in% unsuccessful_requests$ain) %>%
#   mutate(qa_add=TRUE) %>%
#   filter(response_status=="success") %>%
#   distinct()
# not_retried <-final_data_clean %>% filter(!ain %in% unsuccessful_requests$ain) %>% mutate(qa_add = FALSE)
# 
# final_data_clean <-  rbind(not_retried, retried)
# csv_filepath_clean <- paste0("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Permit Data Prepped\\", table_name, "_cleaned.csv")
# 
# write.csv(final_data_clean, csv_filepath_clean, row.names=FALSE, fileEncoding = "UTF-8")
# 
# ## read in final data with retried requests and export to pg
# final_data_retried <- final_data <- read.csv(csv_filepath_clean,
#                                              encoding = "UTF-8",
#                                              colClasses = c("character"))
# 
# dbWriteTable(con, Id(schema=schema, table_name=paste0(table_name,"_cleaned")), final_data_retried,
#              overwrite = FALSE, row.names = FALSE)
# 
# dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", table_name, " IS
#             'General permit data for Altadena parcels with some or significant damage,
#             Data imported on ",date_ran, "
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_permit_scrape_and_rebuild_", curr_year, "_", curr_month, ".docx", "
#             Source: https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st=[ain]'"))
# 
# # 2. if any ains are associated with 100 permits
# ## We pulled first 100 permits per parcel, if any have 100 check to see if we missed any
# ## if so, update function to go to additional results pages to get rest of permits
# check_record_counts <- final_data_retried %>% filter(record_id==100)
# 
# if (nrow(check_record_counts)==100) {
#   message(
#     paste("These parcels should be reviewed individually. If the portal shows more than 100 permits for any, we should update the functions. AINS: ",
#           as.list(check_record_counts$ain))
#   )
# }
# 
# dbDisconnect(con)

# # end of script - rest is old code that may be better for scraping permit details

##### Methods to improve
# perhaps put this on a lambda/aws schedule to run monthly without worrying about RDP staying connected

##### next steps:
## export ALL permits to postgres
## import into second script to scrape permit details (filter for relevant permits)
## export permit details to postgres
## in other task, work on permit typology