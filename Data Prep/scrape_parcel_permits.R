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

# 2. If above works, try to extract data from test url
url_ <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st=2204%20Grand%20Oaks%20Avenue"
message(paste("Current search URL:", url_))
html_response <- wait_for_spa_load(url=url_)
permits <- extract_permit_data_specific(html_response)  # Extract the data
Sys.sleep(5)

# 3. If above works, scale it up with Jan parcels
result <- scrape_permits_chromote(lac_permits_url)

