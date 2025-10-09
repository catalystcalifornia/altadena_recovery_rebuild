# Objective: Scrape two different sites for information on building permits
# for thousands of addresses/parcels, and assess possibility of regularly 
# scraping these sites.

library(dplyr) 

source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\scraping_functions.R")

con <- connect_to_db("altadena_recovery_rebuild")

##### Test dataframes: use to confirm we're getting correct data responses before applying to full parcel universe #####
# Focuses on unincorporated/LA County/Altadena data first (Pasadena commented out for now)
# Create two test dataframes to run through each website. Using the same two addresses
# which will return some positive number of permits for one address and zero for
# the other (and vice versa depending on which permit site is used)
unincorporated_parcels <- data.frame(address=c("2204 Grand Oaks Avenue", "1630 Carriage House Rd"),
                                     expected_permits=c(5, 0)) 
# pasadena_parcels <- data.frame(address=c("2204 Grand Oaks Avenue", "1630 Carriage House Rd"),
#                                expected_permits=c(0, 19))
# Website #1: EPIC LA - LA County building permits (unincorporated cities only)
lac_permits_url <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st="
# Website #2:https://mypermits.cityofpasadena.net/EnerGov_Prod/SelfService#/search?m=1&fm=1&ps=10&pn=1&em=true&st=[example:1630%20Carriage%20House]
# Details: Pasadena building permits 
# pasadena_permits_url <- "https://mypermits.cityofpasadena.net/EnerGov_Prod/SelfService#/search?m=2&ps=10&pn=1&em=true&st="


##### January Universe #####
jan_parcels <- dbGetQuery(con, 
                          "SELECT DISTINCT dmgs.ain, dmgs.damage_category, xwalk.site_address_parcel FROM data.rel_assessor_damage_level as dmgs LEFT JOIN data.crosswalk_dins_assessor_jan2025 as xwalk ON dmgs.ain = xwalk.ain WHERE dmgs.damage_category = 'Significant Damage' OR dmgs.damage_category = 'Some Damage' ORDER BY dmgs.ain")

##### Establish scraping process #####
# 1. Confirm chromote is working properly with simple site like google.com
test_chromote()

# 2. If above works, try to extract data from one test url to get general data fields
url_ <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=100&pn=1&em=true&st=2204%20Grand%20Oaks%20Avenue"
message(paste("Current search URL:", url_))
page_response <- wait_for_spa_load(url=url_)
permits <- extract_permit_data_general(url=url_, html_content=page_response)  # Extract the data
Sys.sleep(5)

# 3. If above works, scale it up to loop through a list of addresses and return all permits (with general data fields)
# using 10 Jan parcels (Altadena Only)
test <- jan_parcels %>%
  mutate(city = case_when(grepl("ALTADENA, CA", site_address_parcel)~"Altadena",
                          grepl("PASADENA, CA", site_address_parcel)~"Pasadena",
                          .default="something else!")) %>%
  filter(city=='Altadena') %>%
  mutate(portal_url = paste0(lac_permits_url, ain)) 


final_data <- NULL

for (row_ in 1:nrow(test)) {
  
  address_url <- test[row_, "portal_url"]
  message(paste(row_, ":", address_url))
  result <- scrape_permits_chromote(url=address_url, wait_time = 15)
  result <- result %>%
    mutate(ain=test[row_, "ain"],
           site_address_parcel=test[row_, "site_address_parcel"],
           damage_category=test[row_, "damage_category"])
  
  if(is.null(final_data)) {
        final_data <- result
  } else {
    final_data <- bind_rows(final_data, result)
  }
  
  Sys.sleep(5)
}


##### Methods to improve
# wait_for_spa_load(): may be able to cut wait time short, e.g., if "No results were found" appears, end/break
# wait_for_spa_load(): need a way to know how many total permits (results) there are and if we need to repeat scrape for subsequent pages - for now we assume all parcels have 100 permits or fewer

