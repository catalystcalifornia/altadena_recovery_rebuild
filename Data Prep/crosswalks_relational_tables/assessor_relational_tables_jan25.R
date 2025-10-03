# Create the following relational tables for analysis
# 1: Residential properties
## properties with residential use codes
## properties with units (total units)
## total square footage
## Type of residential property
## homeowner/renter


# 2: West/East Altadena relational table (residential properties)

# 3: Damage type for residential properties

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

# STEP 2: Prep parcels in Altadena ------
## Select parcels in West or East Altadena st_join ----
st_crs(assessor_parcels)
st_crs(east)
st_crs(west)

# parcels within east altadena
parcels_east <- st_join(assessor_parcels, east %>% select(name,label), join=st_within, left=FALSE)
# parcels_east <- st_join(assessor_parcels, east %>% select(name), join=st_intersects, left=FALSE) # tried intersects but I think the st_within will be better for mapping and most applicable for altadena
# for st_intersects -- 2 assessor parcels overlapping 5862005304 and 5843005901

# check
mapview(parcels_east) +
  mapview(east) 
# looks good

# parcels within west altadena
parcels_west <- st_join(assessor_parcels, west %>% select(name,label), join=st_within, left=FALSE)

# check
mapview(parcels_west) +
  mapview(west)
# looks good

# join together
parcels_altadena <- rbind(parcels_west,parcels_east)
table(parcels_altadena$name,useNA='always')

# check for duplicates
nrow(parcels_altadena)
length(unique(parcels_altadena$ain))
# looks good 13940
# this is our universe moving forward until we reduce to residential

# filter assessor data for same ains
data_altadena <- assessor_data %>%
  filter(ain %in% parcels_altadena$ain)

nrow(data_altadena)
length(unique(data_altadena$ain))
# no difference

## Prep additional fields for units and square feet -----
# Add total units and total square feet for us to see throughout
data_altadena <- data_altadena %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
       total_square_feet = rowSums(across(ends_with("_square_feet"))),
       total_bedrooms = rowSums(across(ends_with("_bedrooms")))) %>%
         mutate(total_units=total_units-landlord_units) # so we don't double count rental units

# check
data_altadena %>%
  select(ends_with("_units"),ends_with("_bedrooms")) %>%
  View()
# looks good

# Just to check consistency in units and square footage -- calculate units and square footage with bedrooms
# create units column list--remove totals
unit_cols <- names(data_altadena_res)[str_detect(names(data_altadena_res), "_units")]
unit_cols <- unit_cols[unit_cols != "landlord_units"]
unit_cols <- unit_cols[unit_cols != "total_units"]

# create bedroom columns list
bed_cols  <- str_replace(unit_cols, "_units", "_bedrooms")

# create square feet columns list--remove total
square_feet_cols <- names(data_altadena_res)[str_detect(names(data_altadena_res), "_square_feet")]
square_feet_cols <- square_feet_cols[square_feet_cols != "total_square_feet"]

# sum unit and square feet columns when corresponding building column is greater than 0, e.g., to sum b1_units, b1_bedrooms has to be >0
data_altadena_res <- data_altadena_res %>%
  rowwise() %>%
  mutate(
    units_with_bedrooms = sum(map2_dbl( # sum units if bedroom column is > 0
      unit_cols, bed_cols,
      ~ ifelse(get(.y) > 0, get(.x), 0)
    )),
    square_feet_with_bedrooms = sum(map2_dbl( # sum square feet if bedroom column is > 0
      square_feet_cols, bed_cols,
      ~ ifelse(get(.y) > 0, get(.x), 0)
    ))
  ) %>%
  ungroup()

# check units and total square footage again look at discrepancies to see what to use moving forward
data_altadena_res %>%
  filter(units_with_bedrooms!=total_units)%>%
  select(units_with_bedrooms, square_feet_with_bedrooms, ends_with("_units"),ends_with("_square_feet"), ends_with("_bedrooms"),res_type,everything()) %>%
  View()
# See https://portal.assessor.lacounty.gov/parceldetail/5743003001, seems like total_units and total_square_feet will be a more accurate match

# STEP 3: TABLE 1: RESIDENTIAL PROPERTIES INFO ------
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
  select(total_units, total_square_feet, total_bedrooms, use_code, everything()) %>%
  View()
# sort by units descending

# one has incorrect use code 5841019007 https://portal.assessor.lacounty.gov/parceldetail/5841019007
# remove from vacant land

# get a list of vacant residential properties we want to exclude
data_altadena_res_vacant <- data_altadena_res_vacant %>%
  filter(ain!="5841019007")

# now get the final list of residential properties, excluding vacant parcels
data_altadena_res <- data_altadena_res_temp %>%
  filter(!ain %in% data_altadena_res_vacant$ain) %>%
  # recode use code for one property
  mutate(use_code=ifelse(ain=="5841019007","0100",use_code))

## Mixed use properties ----
## Types can be
# STORE COMBINATION (WITH OFFICE OR RESIDENTIAL) - 121
## OFFICE BUILDING (office & residential) 172
data_altadena_mixed_temp <- data_altadena %>%
  filter(str_detect(use_code, "^121") | str_detect(use_code, "^172")) 

# check
table(data_altadena_mixed_temp$use_code)

data_altadena_mixed_temp %>%
  select(total_units, landlord_units, total_square_feet, total_bedrooms, use_code, everything()) %>%
  View()

# landlord units can include non-living units, e.g., commercial rental units (also see FIELD DEF pdf in Assessor Data Extract folder), see https://portal.assessor.lacounty.gov/parceldetail/5825002062, 1 unit but 0 beds or baths
# only keep records where there are total_units
# no units or bedrooms, but landlord units https://portal.assessor.lacounty.gov/parceldetail/5845010018

data_altadena_mixed <- data_altadena_mixed_temp  %>%
  filter(total_units>0)
  
# check
data_altadena_mixed %>%
  select(total_units, landlord_units, total_square_feet, total_bedrooms, use_code, everything()) %>%
  View()
# https://portal.assessor.lacounty.gov/parceldetail/5845018003 looks okay
# https://portal.assessor.lacounty.gov/parceldetail/5825002062 larger than other properties and doesn't look like there is residential, drop

data_altadena_mixed <- data_altadena_mixed  %>%
  filter(ain!="5825002062")

## Add column flags and then merge two property types ----
use_codes <- table(data_altadena_res$use_code) %>% as.data.frame()
View(use_codes)
# one room/boarding house

use_codes_mixed <- table(data_altadena_mixed$use_code) %>% as.data.frame()
View(use_codes_mixed)

### Flags for mixed use or residential ------
data_altadena_res <- data_altadena_res %>%
  mutate(residential=TRUE, # residential column flag
         mixed_use=FALSE) # mixed use flag

data_altadena_mixed <- data_altadena_mixed %>%
  mutate(residential=FALSE, # residential column flag
         mixed_use=TRUE) # mixed use flag

# bind residential and mixed use         
rel_res_df <- rbind(data_altadena_res, data_altadena_mixed)

### Flags for type of residential property
rel_res_df  <- rel_res_df  %>%
  mutate(
         res_type=case_when( # type of residential property
           # condos first
          str_detect(use_code, "C$|E$") ~ "Condominium",
          str_detect(use_code, "^01") ~ "Single-family",
          str_detect(use_code, "^02|^03|^04|^05") ~ "Multifamily",
          str_detect(use_code,"^08") ~ "Boarding house",
          str_detect(use_code,"^12|^17") ~ "Mixed use", 
          TRUE ~NA))

table(rel_res_df$res_type,useNA='always')

### Flags for homeowner vs. renter -----
check <- rel_res_df  %>%
  select(num_howmowner_exemption, homeowner_exemption_val,landlord_units, total_units, use_code, total_bedrooms, everything())

View(check)
# some inconsistencies but let's count at least one homeowner exemptions as homeowner, rental as if landlord units are reported, if homeowners and rental then combined
rel_res_df <- rel_res_df %>%
  mutate(owner_renter=case_when(
    num_howmowner_exemption>=1 & landlord_units==0 ~ "Homeowner",
    num_howmowner_exemption>=1 & landlord_units>=1 ~ "Homeowner/Renter",
    num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter",
  TRUE ~ NA))

check <- rel_res_df %>%
  select(owner_renter,num_howmowner_exemption, homeowner_exemption_val,landlord_units, total_units, use_code, total_bedrooms, everything())

check %>%
  filter(is.na(owner_renter)) %>%
  View()
# owned by trusts, churches, etc. count as other?

rel_res_df <- rel_res_df %>%
  mutate(owner_renter=case_when(
    num_howmowner_exemption>=1 & landlord_units==0 ~ "Homeowner",
    num_howmowner_exemption>=1 & landlord_units>=1 ~ "Homeowner/Renter",
    num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter",
    TRUE ~ "Other"))

table(rel_res_df$owner_renter, useNA='always')
# might want to count these as homeowner, but let's leave for now as another homeowner type, could be family homes, but not primary residences

## Clean up and export to postgres ----
rel_res_df_final <- rel_res_df %>%
  select(ain,residential,mixed_use,res_type,owner_renter,total_units,landlord_units,total_square_feet,total_bedrooms,use_code)

# check for duplicates
length(unique(rel_res_df_final$ain))
nrow(rel_res_df_final) #same count

table_name <- "rel_assessor_residential_jan2025"
schema <- "data"
indicator <- "Relational data table with summarized information and flags for residential and mixed use properties in Altadena as of January 2025. Only includes properties in either West or East Altadena proper"
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_jan25.R "
qa_filepath<-"  QA_sheet_relational_tables.docx "
dbWriteTable(con_alt, Id(schema, table_name), rel_res_df_final,
             overwrite = FALSE, row.names = FALSE)

# Add metadata
column_names <- colnames(rel_res_df_final) # Get column names
column_names
column_comments <- c('Assessor ID number - use this to match to other relational tables',
                     'Flag for whether property is a residential use (e.g., use code starting with 0)',
                     'Flag for whether property is mixed residential-commercial (use code starting with 1 but with a residential combo indicated in 3rd character)',
                     'Residential type -- either single-family, multifamily, mixed use, condominium, boarding house',
                     'Housing tenure-ownerships -- either homeowner (indicated by homeowner exemption), renter, homeowner-renter combo, or other (no homeowner exemption or rental units indicated',
                     'Total residential units on the property -- use caution when interpreting for mixed use - - can include commercial',
                     'Total rental units on the property -- use caution when interpreting for mixed use - can include commercial',
                     'Total square feet of buildings on property',
                     'Total bedrooms on property',
                     'Original use code for reference')

add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

# STEP 4: TABLE 2: Table to indicate West or East Altadena with geometry ------
rel_area_geom_df <- parcels_altadena %>%
  select(ain,name,label) %>%
  rename(area_name=name,
         area_label=label)

# select just the residential/mixed uses that we found in the assessor data
rel_area_geom_df <- rel_area_geom_df %>%
  filter(ain %in% rel_res_df_final$ain)

table_name <- "rel_assessor_altadena_parcels_jan2025"
schema <- "data"
indicator <- "Relational spatial table with geometries and area flags for residential/mixed use properties in either West or East Altadena proper"
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_jan25.R "
qa_filepath<-"  QA_sheet_relational_tables.docx "

export_shpfile(con=con_alt, df=rel_area_geom_df, schema="data",
               table_name= "rel_assessor_altadena_parcels_jan2025",
               geometry_column = "geom")


# STEP 5: TABLE 3: Damage categories ------
# We need to summarise at assessor ID number the damage categories based on our definitions
# any parcel with at least one building damaged or majorly destroyed >25% is counted as significant damage
# any parcel with maximum of an affecting or minor <25% damage is counted as some damage
# no damage or inaccessible is counted as no damage



# Add metadata
column_names <- colnames(rel_area_geom_df) # Get column names
column_names
column_comments <- c('Assessor ID number - use this to match to other relational tables',
                     'West or East Altadena shortened label',
                     'West or East Altadena long label',
                     'geometry')

add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

