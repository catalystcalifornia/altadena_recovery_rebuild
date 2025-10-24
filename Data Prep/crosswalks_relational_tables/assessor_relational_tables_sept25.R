# Create the following relational tables for analysis
# 1: Residential properties from January with September AINs and corresponding info from Sept:
## use codes
## zoning
## type of residential property (single family, rental)
## homeowner/renter


# 2: Sales data -- when last sold

# 3: Lot size - relational table with custom data

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
library(lwgeom)  # provides st_oriented_envelope()


options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")

# STEP 1: Load in data/shapes ----
# get xwalk for january and september
jan_sept_xwalk <- st_read(con_alt, query="SELECT * FROM data.crosswalk_assessor_jan_sept_2025")

# get assessor data for september
assessor_data <- st_read(con_alt, query="Select * from data.assessor_data_universe_sept2025")

# get assessor parcels for september
assessor_parcels <- st_read(con_alt, query="Select * from data.assessor_parcels_universe_sept2025", geom="geom")

# get relational tables from january
res <- st_read(con_alt, query="Select * from data.rel_assessor_residential_jan2025")

damage <- st_read(con_alt, query="Select * from data.rel_assessor_damage_level")

shapes <- st_read(con_alt, query="Select * from data.rel_assessor_altadena_parcels_jan2025", geom="geom")

# get custom data from september
assessor_custom <- st_read(con_alt, query="Select * from data.assessor_customdata_universe_sept2025")

# STEP 2: Prep residential parcels in Altadena ------
# filter jan sept crosswalk for residential parcels in Altadena
jan_sept_xwalk_alt <- jan_sept_xwalk %>%
  filter(ain_jan %in% res$ain)

nrow(jan_sept_xwalk_alt)
length(unique(jan_sept_xwalk_alt$ain_jan))
length(unique(jan_sept_xwalk_alt$ain_sept))
# one duplicate september ain
jan_sept_xwalk_alt$ain_sept[duplicated(jan_sept_xwalk_alt$ain_sept)]

View(jan_sept_xwalk_alt)
# two parcels merged into one
# ran this in postgres SELECT * FROM data.assessor_data_universe_jan2025 where ain='5841023010' or ain='5841023009'
# doesn't exist in september data old ains or new ain 5841023022

# filter sept assessor data for same ains
sept_data_altadena <- assessor_data %>%
  filter(ain %in% jan_sept_xwalk_alt$ain_sept)

# what's missing
# missing_sept_data <-  jan_sept_xwalk_alt %>% anti_join(sept_data_altadena, by=c("ain_sept"="ain"))
# 
# https://portal.assessor.lacounty.gov/parceldetail/5847020011 - > not sold, active
# https://portal.assessor.lacounty.gov/parceldetail/5830015015 - > not sold active
# 
# https://portal.assessor.lacounty.gov/parceldetail/5841023009 -- > looks like sold and merged to 5841023010
# 
# # september data for all 4 don't exist

# check the types of status for the ains left in the xwalk
parcel_statuses <- as.data.frame(table(jan_sept_xwalk_alt$status))
# very few have changed

## Prep additional fields for units and square feet -----
# Add total units and total square feet for us to see throughout
data_altadena <- sept_data_altadena %>%
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

unit_cols <- names(data_altadena)[str_detect(names(data_altadena), "_units")]
unit_cols <- unit_cols[unit_cols != "landlord_units"]
unit_cols <- unit_cols[unit_cols != "total_units"]

# QA view result
View(as.data.frame(unit_cols))

# create bedroom columns list
bed_cols  <- str_replace(unit_cols, "_units", "_bedrooms")

# QA view result
View(as.data.frame(bed_cols))

# create square feet columns list--remove total
square_feet_cols <- names(data_altadena)[str_detect(names(data_altadena), "_square_feet")]
square_feet_cols <- square_feet_cols[square_feet_cols != "total_square_feet"]

# QA view result
View(as.data.frame(square_feet_cols))

# sum unit and square feet columns when corresponding building column is greater than 0, e.g., to sum b1_units, b1_bedrooms has to be >0
data_altadena <- data_altadena %>%
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
data_altadena %>%
  filter(units_with_bedrooms!=total_units)%>%
  select(units_with_bedrooms, square_feet_with_bedrooms, ends_with("_units"),ends_with("_square_feet"), ends_with("_bedrooms"),
         # res_type,
         everything()) %>%
  View()

# See https://portal.assessor.lacounty.gov/parceldetail/5743003001, seems like total_units and total_square_feet will be a more accurate match

# STEP 3: TABLE 1: RESIDENTIAL PROPERTIES INFO ------
# We want the same residential properties info to track and compare as for january -- single family, multi-family, condos, number of units, square footage, owner/renter
# this is our basis for all other tables
# Use codes: Ws01data\Project\RDA Team\Altadena Recovery and Rebuild\Data\Assessor Data Extract\Use Code 2023.pdf
## Residential properties ----
data_altadena_res_temp <- data_altadena %>%
  filter(str_detect(use_code, "^0")) 

table(data_altadena_res_temp$use_code)

# pull out x and v suffixes which are vacant or vacant parcel with improvements that are non-structural

# QA: Looking at the Use Code 2023.pdf do we also want to include the suffix U:  Vacant parcel under UAIZ contract? I believe UAIZ contract =Urban Agriculture Incentive Zone
# Check if there are any U suffix values
data_altadena_res_temp_u<-data_altadena_res_temp %>%
  filter(str_detect(use_code, "U$") ) # there are none so we can move forward

# Proceed with other residential types:
data_altadena_res_vacant <- data_altadena_res_temp %>%
  filter(str_detect(use_code, "V$") | str_detect(use_code, "X$") ) 

table(data_altadena_res_vacant$use_code) 

# no vacant parcels given that they were filtered out in january -- in future want to consider how we keep these even if now vacant-- did some residential properties just become vacant?
# # code to keep for future iterations in case vacant parcels appear
# data_altadena_res_vacant %>%
#   select(total_units, total_square_feet, total_bedrooms, use_code, everything()) %>%
#   View()
# # sort by units descending
# 
# View(as.data.frame((unique(data_altadena_res_vacant$ain))))

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
# QA: I also double checked AIN 5845018003 because it had 0 bedrooms but looks residental and OK to keep: https://www.zillow.com/homedetails/848-Marcheta-St-Altadena-CA-91001/82875015_zpid/

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
  select(ain,residential,mixed_use,res_type,owner_renter,total_units,landlord_units,total_square_feet,total_bedrooms,use_code,zoning_code) %>%
  rename(ain_sept=ain)

# check for duplicates
length(unique(rel_res_df_final$ain))
nrow(rel_res_df_final) #same count

# table_name <- "rel_assessor_residential_sept2025"
# schema <- "data"
# indicator <- "Relational data table with summarized information and flags for September parcels that were in Altadena in january 2025, selected based on january september crosswalk and keeping only the january 2025 parcels in Altadena. Only includes properties in either West or East Altadena proper"
# source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_jan25.R "
# qa_filepath<-"  QA_sheet_relational_tables.docx "
# dbWriteTable(con_alt, Id(schema, table_name), rel_res_df_final,
#              overwrite = FALSE, row.names = FALSE)
# 
# # Add metadata
# column_names <- colnames(rel_res_df_final) # Get column names
# column_names
# column_comments <- c('Assessor ID number from September 2025- use this to match to other relational tables ',
#                      'Flag for whether property is a residential use (e.g., use code starting with 0)',
#                      'Flag for whether property is mixed residential-commercial (use code starting with 1 but with a residential combo indicated in 3rd character)',
#                      'Residential type -- either single-family, multifamily, mixed use, condominium, boarding house',
#                      'Housing tenure-ownerships -- either homeowner (indicated by homeowner exemption), renter, homeowner-renter combo, or other (no homeowner exemption or rental units indicated',
#                      'Total residential units on the property -- use caution when interpreting for mixed use - - can include commercial',
#                      'Total rental units on the property -- use caution when interpreting for mixed use - can include commercial',
#                      'Total square feet of buildings on property',
#                      'Total bedrooms on property',
#                      'Original use code for reference',
#                      'Zoning code for the property')

# add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

# STEP 4: TABLE 2: Sales status and other parcel changes--change in ownership ------
table(rel_res_df$year_sold_to_state) # go back and run for january

table(rel_res_df$tax_stat_key) # go back and run for january

table(rel_res_df$doc_reason_code) 

table(rel_res_df$land_reason_key) # go back and run for january - why doesn't this have a W value for misfortunate?, there are numbers in the field though there are no numbers in data dictionary go back and ask assessor

table(rel_res_df$impairment_key) # values not in data dictionary

table(rel_res_df$document_key) # not in data dictionary

# clean up sales date
sales <- rel_res_df %>% 
  select(ain, use_code, contains("owner"),
         recording_date,
         year_sold_to_state,
         last_sale_verif_key, last_sale_date, last_sale_amount, 
         sale_two_verif_key, sale_two_date, sale_two_amount, 
         sale_three_verif_key, sale_three_date, sale_three_amount,
         doc_reason_code,
         land_reason_key,
         tax_stat_key,
         impairment_key,
         partial_interest,
         exemption_type
         ) 

sales <- sales %>%
  mutate(last_sale_char=as.character(last_sale_date),
    last_sale_date=as.Date(last_sale_char, format = "%Y%m%d"),
    sale_two_char=as.character(sale_two_date),
    sale_two_date=as.Date(sale_two_char, format = "%Y%m%d"),
    sale_three_char=as.character(sale_three_date),
   sale_three_date=as.Date(sale_three_char, format = "%Y%m%d")
    )

View(sales) # looks good
## extract year and month of most recent sale and flag for if sale took place after Eaton
# what date to use for flag of being sold after Eaton?
# first sale occurred in February according to news sources - https://www.cbsnews.com/losangeles/news/first-altadena-property-with-home-destroyed-by-eaton-fire-hits-market-sells-within-days/
# fire started on January 7th, 2025  https://www.fire.ca.gov/incidents/2025/1/7/eaton-fire
# LASD allowed all residents to at least visit their properties on 1/21-25 -- use thise date https://www.instagram.com/p/DFGeGEOBbw5/?hl=en

sales <- sales %>%
  mutate(last_sale_year=format(last_sale_date,"%Y"),
         last_sale_month=format(last_sale_date,"%m"),
         sold_after_eaton=ifelse(last_sale_date>"2025-01-21", TRUE, FALSE))

sales %>% 
  select(ain,last_sale_year,last_sale_month, last_sale_date, sold_after_eaton,land_reason_key) %>% 
  arrange(desc(last_sale_date)) %>% 
            View()

# # sold 2/7/25 - had listed prior and then relisted 
# https://portal.assessor.lacounty.gov/parceldetail/5827004009
# https://www.redfin.com/CA/Altadena/453-Alberta-St-91001/home/7251344
# 
# # sold 2/10/25 unclear when listed
# https://portal.assessor.lacounty.gov/parceldetail/5841013002
# http://redfin.com/CA/Altadena/3000-Santa-Anita-Ave-91001/home/7258703
# 
# # sold 2025-02-11 listed 1/30/25
# https://www.redfin.com/CA/Altadena/92-E-Harriet-St-91001/home/7255666
# https://portal.assessor.lacounty.gov/parceldetail/5835031011

# based on news article and this data, lets use date of 2-8-25 for sale after fire -- escrow takes 30 days to close anyways typically

sales <- sales %>%
  mutate(sold_after_eaton=ifelse(last_sale_date>="2025-02-08", TRUE, FALSE))

# clean up for postgres
sales_final <- sales %>%
  select(ain,
         sold_after_eaton, last_sale_year, last_sale_month, 
         first_owner_name, first_owner_name_overflow, second_owner_name,
         recording_date, 
         ownership_code, doc_reason_code, land_reason_key, partial_interest,
         tax_stat_key, year_sold_to_state, impairment_key,
         last_sale_date, last_sale_verif_key, last_sale_amount,
         sale_two_date, sale_two_verif_key, sale_two_amount,
         sale_three_date, sale_three_verif_key, sale_three_amount) %>%
  rename(ain_sept=ain)

View(sales_final)

# table_name <- "rel_assessor_sales_sept2025"
# schema <- "data"
# indicator <- "Relational table with information on sales date and ownership/documentation changes. Includes a flag for sales of properties that were likely listed after the fire"
# source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_sept25.R
# See Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Extract\\FIELD DEF -- SBF.pdf for full data dictionary"
# qa_filepath<-"  QA_sheet_relational_tables_sept25.docx "
# 
# dbWriteTable(con_alt, Id(schema, table_name), sales_final,
#              overwrite = FALSE, row.names = FALSE)

# # Add metadata
# column_names <- colnames(sales_final) # Get column names
# column_names
# column_comments <- c('ain for september use to match to other tables',
#         ' true false field for if sale took place after 2-8-25--likely to have been listed after eaton fire', 
#          'year of last sale', 
#          'month of last sale in number format', 
#          'owner name', 
#          "owner name overflow", 
#          "second owner name",
#          " This is the date of last change or correction of ownership", 
#          "This element contains a code that describes the relationships between the recording and valuation dates", 
#          "This element contains a one digit code which identifies the specific reason for a reappraisable or  non reappraisable status", 
#         "Reason key for the last land value change", 
#          "The percentage of property involved in a transfer of ownership. First two digits are percentage of property being transferred rounded to nearest whole. Third digit indicates the specific interest being transferred",
#          "A one-digit code that indicates whether or not property taxes are delinquent", 
#          "If parcel is delinquent, this indicates the four digits of the year in which taxes first became delinquent", 
#         "A key indicating whether the parcel value has been impaired and describing the impairment",
#          "This is the last sale date - Present for both verified and unverified sales", 
#          "Verification key of last sale - only unverified sales appear on the Secured Basic File Abstract", 
#          "Last unverified sale amount - If the sale is a verified sale (non-numeric character as indicated on the verifications key), the sale amount will not show",
#         "This is the second to last sale date - Present for both verified and unverified sales", 
#         "Verification key of second to last sale - Only unverified sales appear on the Secured Basic File Abstract", 
#         "Second to Last unverified sale amount - If the sale is a verified sale (non-numeric character as indicated on the verifications key), the sale amount will not show",
#         "This is the third to last sale date - Present for both verified and unverified sales", 
#         "Verification key of third to last sale - Only unverified sales appear on the Secured Basic File Abstract", 
#         "Third to last unverified sale amount - If the sale is a verified sale (non-numeric character as indicated on the verifications key), the sale amount will not show")
# 
# add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)



# STEP 5: TABLE 3: Table with geometry and lot size and indicator for west/east altadena based on january parcels ------
# select geometries from september in the data we want
sept_shapes <- assessor_parcels %>% 
  filter(ain %in% rel_res_df_final$ain_sept)

nrow(rel_res_df_final)
nrow(sept_shapes)
# looks good

# get the same parcels from the custom data
sept_custom_data <- assessor_custom %>%
  filter(ain %in% sept_shapes$ain)

nrow(sept_custom_data)
# looks good --this data has lot size which we want to test if we can calculate lot size on our own for same results

sept_shapes <- sept_shapes %>%
  left_join(sept_custom_data %>% select(ain,lot_size)) %>%
  mutate(lot_size=as.numeric(lot_size),
         area_calculated=as.numeric(st_area(geom))*10.7639) # area for 3310 is in square meets so convert to square feet

# calculate difference between shape area and lot size
sept_shapes <- sept_shapes %>%
  mutate(diff=lot_size-shape_area,
         diff_calculated=lot_size-area_calculated)

# event lot size isn't accurate with the assessor portal
# https://portal.assessor.lacounty.gov/parceldetail/5832024007
# https://portal.assessor.lacounty.gov/parceldetail/5857034024
# go with lot size, and if not there, then our calculated area

sept_shapes <- sept_shapes %>%
  mutate(lot_area=ifelse(is.na(lot_size),area_calculated,
                         lot_size
                         )) %>%
  select(-c(diff, diff_calculated))

# calculate the width and length of each parcels
# from ChatGPT
# Apply bounding box extraction per feature
bbox_list <- lapply(st_geometry(sept_shapes), st_bbox)

# Convert list of bbox to data frame
bbox_df <- do.call(rbind, lapply(bbox_list, function(b) {
  data.frame(
    xmin = b["xmin"],
    xmax = b["xmax"],
    ymin = b["ymin"],
    ymax = b["ymax"]
  )
}))

# Compute width and length
bbox_df$width <- bbox_df$xmax - bbox_df$xmin
bbox_df$length <- bbox_df$ymax - bbox_df$ymin

sept_shapes_w_l <- cbind(sept_shapes, bbox_df)

sept_shapes_w_l <- sept_shapes_w_l %>%
  mutate(width=width*3.28084, #3310 produces in meters convert to feet
         length=length*3.28084)

View(sept_shapes_w_l)
## too big, see
# https://portal.assessor.lacounty.gov/parceldetail/5831009001
# need the smallest bounding box?

mbrs <- st_oriented_envelope(sept_shapes)

# Function to compute rectangle side lengths
get_sides <- function(rect) {
  coords <- st_coordinates(rect)[, 1:2]   # x, y
  # consecutive points
  dists <- sqrt(diff(coords[,1])^2 + diff(coords[,2])^2)
  # rectangle: two unique side lengths (each repeats twice)
  sort(unique(round(dists, 6)))  # avoid floating precision
}

# Apply to all polygons
sides_list <- lapply(mbrs, get_sides)

# Extract shortest and longest sides
shortest <- sapply(sides_list, min)
longest  <- sapply(sides_list, max)

polygons$shortest_width <- shortest
polygons$longest_length <- longest


rel_area_geom_df <- parcels_altadena %>%
  select(ain,name,label) %>%
  rename(area_name=name,
         area_label=label)

# select just the residential/mixed uses that we found in the assessor data
rel_area_geom_df <- rel_area_geom_df %>%
  filter(ain %in% rel_res_df_final$ain)

# QA quick check---checks out we got the same AINs

# sum(as.numeric(unique(rel_area_geom_df$ain))) # 75678334063382
# sum(as.numeric(unique(rel_res_df_final$ain))) # 75678334063382


# table_name <- "rel_assessor_altadena_parcels_jan2025"
# schema <- "data"
# indicator <- "Relational spatial table with geometries and area flags for residential/mixed use properties in either West or East Altadena proper"
# source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_jan25.R "
# qa_filepath<-"  QA_sheet_relational_tables.docx "
# 
# export_shpfile(con=con_alt, df=rel_area_geom_df, schema="data",
#                table_name= "rel_assessor_altadena_parcels_jan2025",
#                geometry_column = "geom")


# Add metadata
column_names <- colnames(rel_area_geom_df) # Get column names
column_names
column_comments <- c('Assessor ID number - use this to match to other relational tables',
                     'West or East Altadena shortened label',
                     'West or East Altadena long label',
                     'geometry')

# add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)



# STEP 5: TABLE 3: Damage categories ------
# We need to summarise at assessor ID number the damage categories based on our definitions
# any parcel with at least one building damaged or majorly destroyed >25% is counted as significant damage
# any parcel with maximum of an affecting or minor <25% damage is counted as some damage
# no damage or inaccessible is counted as no damage

# I want the following columns: overall damage category based on groups, binary columns with count of structures in each damage level, list of damage types, count of structures assessed

# keep just AINs in our residential or mixed used base
residential_ains <- st_read(con_alt, query="SELECT * FROM data.rel_assessor_residential_jan2025")

dins_xwalk_res <- dins_xwalk %>% 
  filter(ain %in% residential_ains$ain)

length(unique(dins_xwalk_res$ain))
length(unique(residential_ains$ain))
# some parcels aren't assessed if they aren't in the fire perimeter
# mapview(dins_xwalk_res) +
#   mapview(eaton_fire,col.regions="red") +
#   mapview(parcels_altadena)
# looks pretty good, some holes are from commercial, parks, schools, etc.

## Create binary columns for each damage level -----
table(dins_xwalk_res$damage)

dins_xwalk_res <- dins_xwalk_res %>%
  mutate(destroyed_damage=ifelse(damage=="Destroyed (>50%)", 1,0),
         major_damage=ifelse(damage=="Major (26-50%)", 1,0),
         minor_damage=ifelse(damage=="Minor (10-25%)",1,0),
         affected_damage=ifelse(damage=="Affected (1-9%)",1,0),
         no_damage=ifelse(damage=="No Damage",1,0),
         inaccessible_damage=ifelse(damage=="Inaccessible",1,0))

# check
dins_xwalk_res %>%
  select(damage,ends_with("_damage")) %>%
  View()
# looks good

rel_assessor_dins <- dins_xwalk_res %>%
  st_drop_geometry() %>%
  group_by(ain) %>%
  summarise(
    structure_count=n(),
    destroyed_damage_count=sum(destroyed_damage),
    major_damage_count=sum(major_damage),
    minor_damage_count=sum(minor_damage),
    affected_damage_count=sum(affected_damage),
    no_damage_count=sum(no_damage),
    inaccessible_damage_count=sum(inaccessible_damage),
    damage_type_list=list(unique(damage))
  )

# check counts
nrow(dins_xwalk_res)
sum(rel_assessor_dins$structure_count)
# checks out

sum(rel_assessor_dins$destroyed_damage_count)
# checks out

## Create a single damage category based on highest damage level -----
rel_assessor_dins <- rel_assessor_dins %>%
  ungroup() %>%
  mutate(damage_category = case_when(
    destroyed_damage_count>=1 ~ "Significant Damage", # top code so destroyed goes first
    major_damage_count>=1 ~ "Significant Damage", # same coding for highest damage of major
    minor_damage_count>=1 ~ "Some Damage", # next category
    affected_damage_count>=1 ~ "Some Damage",
    no_damage_count>=1 ~ "No Damage",
    inaccessible_damage_count>=1 ~ "No Damage",
    TRUE ~ NA))

# check
rel_assessor_dins %>%
  select(damage_category,everything()) %>%
  View()
# looks good

check <- rel_assessor_dins %>%
  group_by(damage_category,damage_type_list) %>%
  count()
# looks good

# sort list of damage for easier viewing
rel_assessor_dins$damage_type_list <- lapply(rel_assessor_dins$damage_type_list,sort)

check <- rel_assessor_dins %>%
  group_by(damage_category,damage_type_list) %>%
  count()

View(check)
# looks good

# add a column with the count of unique damage types
rel_assessor_dins<- rel_assessor_dins %>%
  group_by(ain) %>%
  mutate(damage_type_count=length(unique(damage_type_list[[1]]))) %>%
  ungroup() %>%
  mutate(mixed_damage=ifelse(damage_type_count<2, "One Damage Type",
                             "Two or More"))

rel_assessor_dins %>%
  select(damage_type_count,damage_type_list,mixed_damage) %>%
  View()
# looks good

## Clean up and push to postgres----
rel_assessor_dins_final <- rel_assessor_dins %>%
  rowwise() %>%
  mutate(damage_type_list=paste(damage_type_list,collapse=', '), # make list column easier to read
         source="CalFire DINS") %>% # add a source column so we know that it came from CalFire
  select(ain, damage_category,source,structure_count,mixed_damage,everything()) 

View(rel_assessor_dins_final)
nrow(rel_assessor_dins_final)
length(unique(rel_assessor_dins_final$ain))
# final check of unique AINs, looks good

# I want to add the unassessed properties too so we can easily loop over this relational table for analysis, so any properties not in the database is labeled as no damage
residential_ains_no_damage <- residential_ains %>%
  filter(!ain %in% rel_assessor_dins_final$ain) %>%
  select(ain) %>%
  mutate(damage_category="No Damage",
         source="Assessor Parcel Outside of Fire Area")

rel_assessor_dins_final_final <- bind_rows(rel_assessor_dins_final,residential_ains_no_damage)


table_name <- "rel_assessor_damage_level"
schema <- "data"
indicator <- "Relational table that contains summarised damage levels for assessor IDS assessed by CalFire post the Eaton Fire, Unlike the damage inspection database, this table is at the unique parcel level rather than building level
Table includes all residential parcels in Altadena. If they werent assessed then No Damage is assumed. The source column indicates if the parcel was assessed or we are assuming No Damage because outside of fire area and/or not assessed"
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_jan25.R "
qa_filepath<-"  QA_sheet_relational_tables.docx "

# dbWriteTable(con_alt, Id(schema, table_name), rel_assessor_dins_final_final,
#              overwrite = FALSE, row.names = FALSE)


# Add metadata
column_names <- colnames(rel_assessor_dins_final_final) # Get column names
column_names
column_comments <- c('Assessor ID number - use this to match to other relational tables',
                     'Overall damage category level -- top coded so highest damage level of a building on the property takes precedent',
                     'Whether damage category is based on CalFire DINs or assumed because not in the database',
                     'Number of structures assessed by CalFire associated with AIN',
                     'Indicator for whether there was a one type of damage or two or more',
                     'total count for destroyed structures on property',
                     'total count for major damage structures on property',
                     'total count for minor damage structures on property',
                     'total count for affected damage structures on property',
                     'total count for no damage structures on property',
                     'total count for inaccessible structures on property',
                     'list of unique damage types assigned to property by CalFire',
                     'Total unique types of damage')

# add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

