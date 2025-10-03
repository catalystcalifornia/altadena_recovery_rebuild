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
       total_sq_ft = rowSums(across(ends_with("_square_feet"))),
       total_bedrooms = rowSums(across(ends_with("_bedrooms")))) %>%
         mutate(total_units=total_units-landlord_units) # so we don't double count rental units

data_altadena %>%
  select(ends_with("_units"),ends_with("_bedrooms")) %>%
  View()

# TABLE 1: RESIDENTIAL PROPERTIES INFO ------
# We need to analyze all residential properties in Altadena and know what is single family, multi-family, condos, number of units, square footage, owner/renter
# this is our basis for all other tables
# Use codes: Ws01data\Project\RDA Team\Altadena Recovery and Rebuild\Data\Assessor Data Extract\Use Code 2023.pdf
## Residential properties ----
data_altadena_res_temp <- data_altadena %>%
  filter(str_detect(use_code, "^0")) 

table(data_altadena_res_temp$use_code)

# pull out x and v suffixes which are vacant or vacant parcel with improvements that are non-structural

data_altadena_res_vacant <- data_altadena_res_temp %>%
  filter(str_detect(use_code, "V$") | str_detect(use_code, "X$") ) 

data_altadena_res_vacant %>%
  select(total_units, total_sq_ft, total_bedrooms, use_code, everything()) %>%
  View()
# sort by units descending

# one has incorrect units 5841019007 https://portal.assessor.lacounty.gov/parceldetail/5841019007
# remove from vacant land

# get a list of vacant residential properties we want to exclude
data_altadena_res_vacant <- data_altadena_res_vacant %>%
  filter(ain!="5841019007")

# now get the final list of residential properties, excluding vacant parcels
data_altadena_res <- data_altadena_res_temp %>%
  filter(!ain %in% data_altadena_res_vacant$ain)

## Mixed use properties ----
## Types can be
# STORE COMBINATION (WITH OFFICE OR RESIDENTIAL) - 121
## OFFICE BUILDING (office & residential) 172
data_altadena_mixed_temp <- data_altadena %>%
  filter(str_detect(use_code, "^121") | str_detect(use_code, "^172")) 

# check
table(data_altadena_mixed_temp$use_code)

data_altadena_mixed_temp %>%
  select(total_units, landlord_units, total_sq_ft, total_bedrooms, use_code, everything()) %>%
  View()

# units can include non-living units, see https://portal.assessor.lacounty.gov/parceldetail/5825002062, 1 unit bu 0 beds or baths
# only keep records where there are bedrooms
# some records can include bedrooms and no units https://portal.assessor.lacounty.gov/parceldetail/5853019029
# only keep records where there are bedrooms and units

data_altadena_mixed <- data_altadena_mixed_temp  %>%
  filter(total_bedrooms>0 & total_units>0)
  
# check
data_altadena_mixed %>%
  select(total_units, landlord_units, total_sq_ft, total_bedrooms, use_code, everything()) %>%
  View()

## Add column flags and then merge two property types ----
use_codes <- table(data_altadena_res$use_code) %>% as.data.frame()
# one room/boarding house
data_altadena_res <- data_altadena_res %>%
  mutate(residential=TRUE, # residential column flag
         res_type=case_when( # type of residential property
           # condos first
          str_detect(use_code, "C$|E$") ~ "Condominium",
          str_detect(use_code, "^01") ~ "Single-family",
          str_detect(use_code, "^02|^03|^04|^05") ~ "Multifamily",
          str_detect(use_code,"^08") ~ "Boarding house",
          TRUE ~NA))

table(data_altadena_res$res_type,useNA='always')

# coding for homeowner vs. renter
check <- data_altadena_res %>%
  select(num_howmowner_exemption, homeowner_exemption_val,landlord_units, total_units, use_code, total_bedrooms, everything())

View(check)
# some inconsistencies but let's count at least one homeowner exemptions as homeowner, rental as if landlord units are reported, if homeowners and rental then combined
# one room/boarding house
data_altadena_res <- data_altadena_res %>%
  mutate(owner_renter=case_when(
    num_howmowner_exemption>=1 & landlord_units==0 ~ "Homeowner",
    num_howmowner_exemption>=1 & landlord_units>=1 ~ "Homeowner/Renter",
    num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter",
  TRUE ~ NA))

check <- data_altadena_res %>%
  select(owner_renter,num_howmowner_exemption, homeowner_exemption_val,landlord_units, total_units, use_code, total_bedrooms, everything())

check %>%
  filter(is.na(owner_renter)) %>%
  View()
# owned by trusts, churches, etc. count as other?

data_altadena_res <- data_altadena_res %>%
  mutate(owner_renter=case_when(
    num_howmowner_exemption>=1 & landlord_units==0 ~ "Homeowner",
    num_howmowner_exemption>=1 & landlord_units>=1 ~ "Homeowner/Renter",
    num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter",
    TRUE ~ "Other"))

table(data_altadena_res$owner_renter, useNA='always')
# might want to count these as homeowner
View(check)

# check units and total square footage again
data_altadena_res %>%
  select(ends_with("_units"),ends_with("_square_feet"), ends_with("_bedrooms")) %>%
  View()

# add total units square footage and units with bedrooms to check
unit_cols <- names(data_altadena_res)[str_detect(names(data_altadena_res), "_units")]
unit_cols <- unit_cols[unit_cols != "landlord_units"]
unit_cols <- unit_cols[unit_cols != "total_units"]
bed_cols  <- str_replace(unit_cols, "_units", "_bedrooms")  # match corresponding bedrooms
square_feet_cols <- names(data_altadena_res)[str_detect(names(data_altadena_res), "_square_feet")]
square_feet_cols <- square_feet_cols[square_feet_cols != "total_square_feet"]

data_altadena_res <- data_altadena_res %>%
  rowwise() %>%
  mutate(
    units_with_bedrooms = sum(map2_dbl(
      unit_cols, bed_cols,
      ~ ifelse(get(.y) > 0, get(.x), 0)
    )),
    square_feet_with_bedrooms = sum(map2_dbl(
      square_feet_cols, bed_cols,
      ~ ifelse(get(.y) > 0, get(.x), 0)
    ))
  ) %>%
  ungroup()

# check units and total square footage again
data_altadena_res %>%
  select(units_with_bedrooms, square_feet_with_bedrooms, ends_with("_units"),ends_with("_square_feet"), ends_with("_bedrooms")) %>%
  View()

## properties with units (total units)
## total square footage


