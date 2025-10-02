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
parcels_east <- st_join(assessor_parcels, east %>% select(name), join=st_within, left=FALSE)
parcels_east_test <- st_join(assessor_parcels, east %>% select(name), join=st_intersects, left=FALSE)

# check
mapview(parcels_east_test) +
  mapview(east) 
# looks good

parcels_west <- st_join(assessor_parcels, west %>% select(name), join=st_within, left=FALSE)

# check
mapview(parcels_west) +
  mapview(west)
# looks good

# join together
parcels_altadena <- rbind(parcels_west,parcels_east)

# TABLE 1: RESIDENTIAL PROPERTIES INFO ------
# We need to analyze all residential properties in Altadena and know what is single family, multi-family, condos, number of units, square footage, owner/renter
