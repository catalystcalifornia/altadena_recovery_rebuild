# Create the following relational tables for analysis
# 1: Residential properties
## properties with residential use codes
## properties with units (total units)
## total square footage
## Type of residential property
## homeowner/renter

# 2: Damage type for residential properties

# 3: West/East Altadena relational table (residential properties)


# Library and environment set up ----

library(sf)
library(rmapshaper)
library(leaflet)
library(htmlwidgets)
library(stringr)
library(readxl)
library(tidyverse)
library(janitor)
library(mapview)
library(writexl)

options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")

# STEP 1: Load in data/shapes ----
# eaton fire shape
eaton_fire <- st_read(con_alt, query="SELECT * FROM data.eaton_fire_prmtr_3310", geom="geom")

# get xwalk for damage inspection data
dins_xwalk <- st_read(con_alt, query="SELECT * FROM data.crosswalk_dins_assessor_jan2025", geom="geom")

# get assessor parcels
assessor_parcels <- st_read(con_alt, query="Select * from data.assessor_parcels_universe_jan2025", geom="geom")

# get assessor data
assessor_data <- st_read(con_alt, query="Select * from data.assessor_data_universe_jan2025")

# get west and east altadena
east <- st_read(con_alt, query="SELECT * FROM data.east_altadena_3310", geom="geom")

west <- st_read(con_alt, query="SELECT * FROM data.west_altadena_3310", geom="geom")

## Prep parcels in Altadena ------
st_crs(assessor_parcels)
st_crs(east)
st_crs(west)

# parcels in east altadena
# parcels_east <- st_join(assessor_parcels, east %>% select(name), join=st_within, left=FALSE)
parcels_east <- st_join(assessor_parcels, east %>% select(name), join=st_intersects, left=FALSE)

# check
mapview(parcels_east) +
  mapview(east) 
# looks good

parcels_west <- st_join(assessor_parcels, west %>% select(name), join=st_intersects, left=FALSE)

# check
mapview(parcels_west) +
  mapview(west)
# looks good

# 2 assessor parcels overlapping, keep in east not west
# 5862005304 and 5843005901
parcels_west <- parcels_west %>%
  filter(!ain %in% c("5862005304","5843005901"))

# join together
parcels_altadena <- rbind(parcels_west,parcels_east)

# check for duplicates
nrow(parcels_altadena)
length(unique(parcels_altadena$ain))
# looks good 14525
# this is our universe moving forward

# filter assessor data for same ains
data_altadena <- assessor_data %>%
  filter(ain %in% parcels_altadena$ain)

nrow(data_altadena)
length(unique(data_altadena$ain))
# 14519, difference of 6, that's fine for now

# Add total units and total square feet for us to see throughout
data_altadena <- data_altadena %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
       total_sq_ft = rowSums(across(ends_with("_square_feet")))) %>%
         mutate(total_units=total_units-landlord_units) ## fix later

data_altadena %>%
  select(ends_with("_units")) %>%
  View()

# TABLE 1: RESIDENTIAL PROPERTIES INFO ------
# We need to analyze all residential properties in Altadena and know what is single family, multi-family, condos, number of units, square footage, owner/renter
# this is our basis for all other tables
# Use codes: Ws01data\Project\RDA Team\Altadena Recovery and Rebuild\Data\Assessor Data Extract\Use Code 2023.pdf
## Residential properties ----
data_altadena_res <- data_altadena %>%
  filter(str_detect(use_code, "^0")) 

table(data_altadena_res$use_code)

# pull out x and v suffixes which are vacant or vacant parcel with improvements that are non-structural

data_altadena_res_vacant <- data_altadena_res %>%
  filter(str_detect(use_code, "V$") | str_detect(use_code, "X$") ) 

data_altadena_res_vacant %>%
  select(total_units, total_sq_ft, use_code, everything()) %>%
  View()

# one has incorrect units 5841019007

assessor_data_condos <- assessor_data %>%
  filter(str_detect(use_code, "C$") | str_detect(use_code, "E$") ) 


