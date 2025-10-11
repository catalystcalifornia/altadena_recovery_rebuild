# Objective: Scrape two different sites for information on building permits
# for thousands of addresses/parcels, and assess possibility of regularly 
# scraping these sites.

library(dplyr) 

source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\scraping_functions.R")

con <- connect_to_db("altadena_recovery_rebuild")

# EPIC LA - LA County building permits (unincorporated cities only)
lac_permits_url <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st="


jan_parcels <- dbGetQuery(con, 
                          "SELECT DISTINCT dmgs.ain, dmgs.damage_category, xwalk.site_address_parcel FROM data.rel_assessor_damage_level as dmgs LEFT JOIN data.crosswalk_dins_assessor_jan2025 as xwalk ON dmgs.ain = xwalk.ain WHERE dmgs.damage_category = 'Significant Damage' OR dmgs.damage_category = 'Some Damage' ORDER BY dmgs.ain")

##### Establish scraping process #####
# 1. Confirm chromote is working properly with simple site like google.com
test_chromote()

# 2. If above works, try to extract data from one test url to get general data fields
url_ <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st=2204%20Grand%20Oaks%20Avenue"
message(paste("Current search URL:", url_))
permits <- scrape_permits_chromote(url=url_, wait_time = 30)

# 3. If above works, scale it up to loop through a list of addresses and return all permits (with general data fields)
# using 10 Jan parcels (Altadena Only)
altadena_parcels <- jan_parcels %>%
  mutate(city = case_when(grepl("ALTADENA, CA", site_address_parcel)~"Altadena",
                          grepl("PASADENA, CA", site_address_parcel)~"Pasadena",
                          .default="something else!")) %>%
  filter(city=='Altadena') %>%
  mutate(portal_url = paste0(lac_permits_url, ain)) 


final_data <- NULL

for (row_ in 1:nrow(altadena_parcels)) { 
  
  address_url <- altadena_parcels[row_, "portal_url"]
  message(paste(row_, ":", address_url))
  result <- scrape_permits_chromote(url=address_url, wait_time = 30)
  result <- result %>%
    mutate(ain=altadena_parcels[row_, "ain"],
           site_address_parcel=altadena_parcels[row_, "site_address_parcel"],
           damage_category=altadena_parcels[row_, "damage_category"])
  
  if(is.null(final_data)) {
        final_data <- result
  } else {
    final_data <- bind_rows(final_data, result)
  }
  
  Sys.sleep(3)
}


# dbWriteTable(con, Id(schema="data", table_name="general_permit_data_10_2025"), final_data, 
#              overwrite = FALSE, row.names = FALSE) 
# 
# dbSendQuery(con, "COMMENT ON TABLE data.general_permit_data_10_2025 IS
#             'General permit data for Altadena parcels with some or significant damage,
#             Data imported on 10-12-25
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_scrape_permit_data.docx
#             Source: https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st=[ain]'")


##### Methods to improve
# wait_for_spa_load(): need a way to know how many total permits (results) there are and if we need to repeat scrape for subsequent pages 
## - for now we assume all parcels have 100 permits or fewer - can see if any have 100 permits and then look to see if there are more

# if an ain is associated with a timeout/error status, we'll need to add a section to this script that perhaps reruns them with a
# longer max_wait time.

# will want to put this on a lambda/aws schedule to run monthly without worrying about RDP staying connected

# next steps:
## export ALL permits to postgres
## import and clean up for relevant permits
## export clean to postgres
## import clean, scrape permit details