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

shapes <- st_read(con_alt, query="Select * from data.rel_assessor_altadena_parcels_jan2025", geom="geom") %>%
  rename(ain_jan=ain)

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
# QA check that worked: 
# print(unit_cols)

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

# QA check
# data_altadena_res%>%select(use_code, residential)%>%View()
# table(data_altadena_res$use_code) # all look residential -good

data_altadena_mixed <- data_altadena_mixed %>%
  mutate(residential=FALSE, # residential column flag
         mixed_use=TRUE) # mixed use flag

# QA check
# table(data_altadena_mixed$use_code) # all mixed use -good

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

### Flags for ownership type ----
##### Homeowner vs. renter occupied coded first -----
check <- rel_res_df  %>%
  select(num_howmowner_exemption, homeowner_exemption_val,landlord_units, total_units, use_code, total_bedrooms, everything())

View(check)
# some inconsistencies but let's count at least one homeowner exemptions as homeowner, rental as if landlord units are reported, if homeowners and rental then combined
rel_res_df <- rel_res_df %>%
  mutate(owner_renter=case_when(
    num_howmowner_exemption>=1 & landlord_units==0 ~ "Owner occupied",
    num_howmowner_exemption>=1 & landlord_units>=1 ~ "Owner & Renter occupied",
    num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter occupied",
    TRUE ~ NA))

check <- rel_res_df %>%
  select(owner_renter,num_howmowner_exemption, homeowner_exemption_val,landlord_units, total_units, use_code, total_bedrooms, everything())

check %>%
  filter(is.na(owner_renter)) %>%
  View()
# owned by trusts, churches, etc. count as other?

rel_res_df <- rel_res_df %>%
  mutate(owner_renter=case_when(
    num_howmowner_exemption>=1 & landlord_units==0 ~ "Owner occupied",
    num_howmowner_exemption>=1 & landlord_units>=1 ~ "Owner & Renter occupied",
    num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter occupied",
    TRUE ~ "Other"))

table(rel_res_df$owner_renter, useNA='always')
## Recode the remaining "OTHER" category

#### Ownership type typology guide ----
## For more details on how we came up with these groups, see the script: Data Prep/resident_exemption_explore.R
# Categories: 
# Veteran exemption: move to 'Owner occupied'
# New category: LLC-owned (LLC, LP, Inc, Investment)
# New category: Trust-owned (TRUST, TRS, TR, TRST)
# New category: Church / Welfare exemption OR Church or services in owner name
# New category: Sold to state (tax delinquent)
# New category: SBE or Government owned
# Other: Properties that are clearly not owner occupied
# likely owner occupied, no exemption--properties that seem to be individually owned but not homeowner exemption

##### Exemption properties - veteran, church, welfare org  -----
# Start with exemption type recoding:
other<-rel_res_df%>%filter(owner_renter=="Other") # separate out 'Other' so I can perform checks throughout

table(other$exemption_type) 

rel_res_df<-rel_res_df%>%
  mutate(
    owner_renter = case_when(
      exemption_type %in% "1" & owner_renter == "Other" ~ "Owner occupied",
      exemption_type %in% c("4", "5", "7") & owner_renter == "Other" ~ "Church/Welfare exemption",
      TRUE ~ owner_renter  # keeps existing value if none of the above conditions are met
    )
  )

# check
table(rel_res_df$owner_renter) # OTHER category went from 4602 to 4568 and we have 20 new observations under-owner occupied

##### Trusts and LLCs  -----
# move on to trusts and LLCs
# test limited recoding
test <- rel_res_df%>%
  mutate(
    owner_renter = ifelse(
      (grepl("TRUST", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Trust owned",owner_renter))%>%
  mutate(
    owner_renter = ifelse(
      (grepl("LLC", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC", first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "LLC owned",owner_renter))%>%
  mutate(owner_renter=ifelse(ain %in% "5829032026", "LLC owned", owner_renter)) %>% # manually recoded after original result returned a property with a LLC and trust
  filter(owner_renter %in% c("LLC owned","Trust owned"))

table(test$owner_renter) 

# test expanding recoding and top coding corporate
test_2 <- rel_res_df %>%
  # top code corporate and add other abbreviations
  mutate(
    owner_renter = ifelse(
      (grepl("LLC| LP| Inc| Investment", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc| Investment", first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc| Investment", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "LLC owned",owner_renter))%>%
  mutate(
    owner_renter = ifelse(
      (grepl("TRUST|TRST| TR | TRS", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR | TRS",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR | TRS", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Trust owned",owner_renter))%>%
  filter(owner_renter %in% c("LLC owned","Trust owned"))

# check counts:
table(test_2$owner_renter) # 1 more trust and more LLCs

# check ain that was originally hard coded
test_2 %>% filter(ain=='5829032026') %>% select(owner_renter) # resolved by top coding

# View expanded results with broader recoding
test_2 %>%
  filter(!ain %in% test$ain) %>%
  select(ain,owner_renter,contains("_owner"),exemption_type, num_howmowner_exemption,landlord_units) %>%
  View()
# all valid inclusions

# test counts without Other owner_renter requirement--how do counts change?
other_trust_llc <- rel_res_df %>%
  mutate(
    flag_llc = ifelse(
      (grepl("LLC| LP| Inc", first_owner_name, ignore.case = TRUE)) |
        (grepl("LLC| LP| Inc", first_owner_name_overflow, ignore.case = TRUE)) |
        (grepl("LLC| LP| Inc", second_owner_name, ignore.case = TRUE)),
      1, 0
    ),
    flag_trust = ifelse(
      (grepl("TRUST|TRST| TR | TRS", first_owner_name, ignore.case = TRUE)) |
        (grepl("TRUST|TRST| TR | TRS",first_owner_name_overflow, ignore.case = TRUE)) |
        (grepl("TRUST|TRST| TR | TRS", second_owner_name, ignore.case = TRUE)),
      1, 0
    )
  )

# check counts
other_trust_llc%>%
  summarise(llc_tot=sum(flag_llc), # this is 406 LLCs disregarding Other requirement vs. 236 LLCs with Other requirement
            trust_to=sum(flag_trust)) # this is 5576 trusts vs the 1532 trusts with the Other requirement 

# trust llc check compared to exemptions and owner types
trust_llc_check <- other_trust_llc %>% group_by(flag_trust, flag_llc, exemption_type, owner_renter) %>% summarise(count=n())

View(trust_llc_check)
# difference is largely due to homeowner properties that are owned in a trust, and some that have clear rental units
# notably some renter occupied properties have a church or welfare exemption too
# llcs are also most frequently renter occupied
# trusts are most frequently also owner occupied
# top coding owner-occupied and renter-occupied, note that trusts are likely owner occupied whereas llcs are likely renter occupied
# 3 more trusts that are also LLC, but decided to top code LLC

# proceed with expanded recoding
rel_res_df <- rel_res_df%>%
  # top code corporate and add other abbreviations
  mutate(
    owner_renter = ifelse(
      (grepl("LLC| LP| Inc | Investment", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc| Investment", first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc| Investment", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "LLC owned",owner_renter))%>%
  mutate(
    owner_renter = ifelse(
      (grepl("TRUST|TRST| TR | TRS", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR | TRS",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR | TRS", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Trust owned",owner_renter))

# check:
table(rel_res_df$owner_renter) # looks good

##### Government owned/tax defaulted properties  -----
## Now lets recode based on tax status:
table(rel_res_df$tax_stat_key)

sold_to_state <- rel_res_df %>% filter(tax_stat_key %in% c("1","2")) %>%
  select(ain, owner_renter, tax_stat_key, year_sold_to_state, first_owner_name, second_owner_name, first_owner_name_overflow, exemption_type, num_howmowner_exemption, landlord_units, owner_renter)
table(sold_to_state$owner_renter)
# some are other owner types, prioritizing tax defaulted status as this could affect rebuilding

rel_res_df<-rel_res_df%>%
  mutate(
    owner_renter = case_when(
      tax_stat_key %in% c("1","2") ~ "Sold to state",
      tax_stat_key %in% "3" ~ "SBE or Government owned",
      TRUE ~ owner_renter  # keeps existing value if none of the above conditions are met
    )
  )

# check
table(rel_res_df$owner_renter) # numbers check out

##### Addtl Church and welfare properties  -----
# explore other
rel_res_df %>%
  filter(owner_renter=="Other") %>%
  select(ain,owner_renter,contains("_owner"),exemption_type, num_howmowner_exemption,total_units,landlord_units) %>%
  View()

test <- rel_res_df%>%
  mutate(
    owner_renter = ifelse(
      (grepl("CHURCH|FRATERNAL|SERVICES", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("CHURCH|FRATERNAL|SERVICES",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("CHURCH|FRATERNAL|SERVICES", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Church/Welfare exemption",owner_renter))%>%
  filter(owner_renter=="Church/Welfare exemption")

# check recoding
test %>%
  filter(owner_renter=="Church/Welfare exemption") %>%
  filter(!exemption_type %in% c("4", "5", "7")) %>%
  select(ain,owner_renter,contains("_owner"),exemption_type, num_howmowner_exemption,total_units,landlord_units) %>%
  View()
# looks good

# apply recoding
rel_res_df <- rel_res_df%>%
  mutate(
    owner_renter = ifelse(
      (grepl("CHURCH|FRATERNAL|SERVICES", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("CHURCH|FRATERNAL|SERVICES",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("CHURCH|FRATERNAL|SERVICES", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Church/Welfare exemption",owner_renter))

##### Explore other types more  -----
check_other_owner <- rel_res_df %>%
  filter(owner_renter=="Other") %>%
  group_by(first_owner_name,first_owner_name_overflow,second_owner_name) %>%
  summarise(count=n())

View(check_other_owner)
# ordered by count in view and found a large number (>40)
# owned by John and Margaret Cameron - https://portal.assessor.lacounty.gov/parceldetail/5840014044
# Reviewed other owner names of 2 or more, flags to change:
# SUBSIDIZED HOUSING CORP, Pasadena Cemetery Association 	PUBLIC WORKS GROUP CORP   -- > welfare
# Keep LA VINA HOMEOWNERS as Other
# keep 	LINCOLN AVENUE WATER CO  as other
# LITTLE,WILLIAM  family who had several homes destroyed - owner-occupied
# https://spectrumnews1.com/ca/southern-california/wildfires/2025/01/12/altadena-siblings-see-their-homes--reduce-to-ashes-

check_other_owner_use_code <- rel_res_df %>%
  filter(owner_renter=="Other") %>%
  group_by(use_code) %>%
  summarise(count=n())
# use code 010E is almost all owned by John and Margaret Cameron
# These are attempts to convert rental properties to condos
# https://library.municode.com/ca/los_angeles_county/codes/code_of_ordinances/349596?nodeId=TIT8COPRBUWARE_DIV3HO_CH8.48COCO
# keep as other
# first name CAMERON,JOHN K,JR AND  or CAMERON,JOHN K AND or CAMERON,JOHN K JR AND        

# used this code to view the dataframes main columns and search for different owner names, get associated AINs and use codes, and look up parcels online
rel_res_df %>%
  filter(owner_renter=="Other") %>%
  select(ain,owner_renter,contains("_owner"),use_code, exemption_type, num_howmowner_exemption,total_units,landlord_units) %>%
  View()

# A final recode of other
test <- rel_res_df%>%
  mutate(
    owner_renter = ifelse(
      grepl("SUBSIDIZED HOUSING CORP|Pasadena Cemetery Association|PUBLIC WORKS GROUP CORP", first_owner_name, ignore.case = TRUE) & owner_renter == "Other",
      "Church/Welfare exemption",owner_renter),
    owner_renter = ifelse(
      grepl("LA VINA HOMEOWNERS|CAMERON,JOHN K,JR AND|CAMERON,JOHN K AND|CAMERON,JOHN K JR AND|LINCOLN AVENUE WATER CO", first_owner_name, ignore.case = TRUE) & owner_renter == "Other",
      "Other",
      ifelse(owner_renter=="Other","Likely owner-occupied, no exemption", owner_renter)))

# check other
remaining_other <-test %>% filter(owner_renter=="Other") %>%
  select(ain, owner_renter, tax_stat_key, year_sold_to_state, first_owner_name, second_owner_name, first_owner_name_overflow, exemption_type, num_howmowner_exemption, total_units, landlord_units, owner_renter)
# looks good

# check likely homeowner occupied
likely_homeowner <-test %>% filter(owner_renter=="Likely owner-occupied, no exemption") %>%
  select(ain, owner_renter, tax_stat_key, year_sold_to_state, first_owner_name, second_owner_name, first_owner_name_overflow, exemption_type, num_howmowner_exemption, total_units, landlord_units, owner_renter)
# sorted and skimmed owner names, looks okay

rel_res_df<-rel_res_df%>%
  mutate(
    owner_renter = ifelse(
      grepl("SUBSIDIZED HOUSING CORP|Pasadena Cemetery Association|PUBLIC WORKS GROUP CORP", first_owner_name, ignore.case = TRUE) & owner_renter == "Other",
      "Church/Welfare exemption",owner_renter),
    owner_renter = ifelse(
      grepl("LA VINA HOMEOWNERS|CAMERON,JOHN K,JR AND|CAMERON,JOHN K AND|CAMERON,JOHN K JR AND|LINCOLN AVENUE WATER CO", first_owner_name, ignore.case = TRUE) & owner_renter == "Other",
      "Other",
      ifelse(owner_renter=="Other","Likely owner-occupied, no exemption", owner_renter)))

# check
table(rel_res_df$owner_renter,useNA='always') # numbers check out


## Clean up and export to postgres ----
# recode values for clarity
rel_res_df <- rel_res_df %>%
  mutate(owner_renter_orig=owner_renter,
         owner_renter=case_when(
           owner_renter_orig=="Church/Welfare exemption" ~ "Church or charity owned",
           owner_renter_orig=="Likely owner-occupied, no exemption" ~ "Likely owner occupied, no exemption",
           owner_renter_orig=="LLC owned" ~ "Corporation owned",
           owner_renter_orig=="Other" ~ "Other ownership",
           owner_renter_orig=="Owner & Renter occupied" ~ "Owner & renter occupied",
           owner_renter_orig=="SBE or Government owned" ~ "Government owned",
           owner_renter_orig=="Owner occupied" ~ "Owner occupied, homeowner exemption",
           TRUE ~ owner_renter_orig
         ))

# check
table(rel_res_df$owner_renter_orig,useNA='always') 
table(rel_res_df$owner_renter,useNA='always') # numbers check out

rel_res_df_final <- rel_res_df %>%
  select(ain,residential,mixed_use,res_type,owner_renter,total_units,landlord_units,total_square_feet,total_bedrooms,use_code,zoning_code) %>%
  rename(ain_sept=ain)

# check for duplicates
length(unique(rel_res_df_final$ain_sept))
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
#                      'Housing tenure-ownerships -- either homeowner (indicated by homeowner exemption), renter, trust owned, corporation owned, sold to state, government owned, other ownership, or owner likely but with no exemption. For more detailed methodology of choices see R script',
#                      'Total rental units on the property -- use caution when interpreting for mixed use - can include commercial',
#                      'Total square feet of buildings on property',
#                      'Total bedrooms on property',
#                      'Original use code for reference',
#                      'Zoning code for the property')
# 
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

# quick check

sales%>%select(last_sale_date, sold_after_eaton)%>%View() # Looks good

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
  filter(ain %in% jan_sept_xwalk_alt$ain_sept)

nrow(rel_res_df_final)
nrow(sept_shapes)
# looks good

# get the same parcels from the custom data
sept_custom_data <- assessor_custom %>%
  filter(ain %in% sept_shapes$ain)

nrow(sept_custom_data)
# looks good missing data for the same 4 parcels --this data has lot size which we want to test if we can calculate lot size on our own for same results

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

# add west and east identifier so we don't need to to match to jan every time we run the analysis
sept_shapes_alt <- sept_shapes %>%
  left_join(jan_sept_xwalk_alt, by=c("ain"="ain_sept")) %>%
  left_join(shapes %>% st_drop_geometry(), by=c("ain_jan"="ain_jan")) 

# clean up
sept_shapes_final <- sept_shapes_alt %>%
  select(ain,area_name,area_label,lot_size, lot_area) %>%
  rename(ain_sept=ain)

# mapview(sept_shapes_final)

# # Export to postgres
# table_name <- "rel_assessor_altadena_parcels_sept2025"
# schema <- "data"
# indicator <- "Relational spatial table with geometries and area flags for residential/mixed use properties in either West or East Altadena proper as of September 2025"
# source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_sept25.R "
# qa_filepath<-"  QA_sheet_relational_tables_sept25.docx "
# 
# export_shpfile(con=con_alt, df=sept_shapes_final, schema="data",
#                table_name="rel_assessor_altadena_parcels_sept2025",
#                geometry_column = "geom")
# 
# 
# # Add metadata
# column_names <- colnames(sept_shapes_final) # Get column names
# column_names
# column_comments <- c('Assessor ID number for September - use this to match to other relational tables',
#                      'West or East Altadena shortened label based on parcel as of January',
#                      'West or East Altadena long label based on parcel as of January',
#                      'lot size provided from assessor - doesnt match portal',
#                      'lot size provided from assessor or calculated by us using st_area when missing from assessor data - doesnt match portal',
#                      'geometry')
# 
# add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


# STEP 6: TABLE 4: Damage categories based on January ------
# So we don't need to join the damage categories to every September parcel in each script, let's push a relational table based on january categories
damage_sept <- jan_sept_xwalk_alt %>%
  left_join(damage %>% select(ain,damage_category),
            by=c("ain_jan"="ain"))

nrow(damage_sept)
length(unique(damage_sept$ain_sept))
# one duplicate

# are there different damage assessments with duplicates?
damage_sept_final <- damage_sept %>%
  group_by(ain_sept,damage_category) %>%
  summarise(count=n())

nrow(damage_sept_final)
# one damage category for the duplicate, now same rows as unique september ains
# what's missing?
missing <- damage_sept_final %>%
  anti_join(rel_res_df_final)
# These are the same we don't have data for so fine for now, but we have shapes so keep

# clean up and push to postgres
damage_sept_final <- damage_sept_final %>%
  select(-count) %>%
  ungroup()
#
# table_name <- "rel_assessor_damage_level_sept2025"
# schema <- "data"
# indicator <- "Relational table that contains summarised damage levels for assessor IDS as of September 2025, based on the assessed damage of their matched January parcels by CalFire post the Eaton Fire, Unlike the damage inspection database, this table is at the unique parcel level rather than building level
# Table includes all residential parcels in Altadena. If they werent assessed then No Damage is assumed. See the january damage table for further detail on the structures and number of damage categories recorded on each parcel, use the jan sept relational table to join"
# source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_sept25.R "
# qa_filepath<-"  QA_sheet_relational_tables_sept25.docx "
# 
# dbWriteTable(con_alt, Id(schema, table_name), damage_sept_final,
#              overwrite = FALSE, row.names = FALSE)
# 
# # Add metadata
# column_names <- colnames(damage_sept_final) # Get column names
# column_names
# column_comments <- c('Assessor ID number from September- use this to match to other relational tables',
#                      'Overall damage category level -- top coded so highest damage level of a building on the property takes precedent')
# 
# add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
# 


##### IN PROGRESS - Need length and width of each parcel--not working to match assessor portal -------
# # calculate the width and length of each parcels
# # from ChatGPT
# # Apply bounding box extraction per feature
# bbox_list <- lapply(st_geometry(sept_shapes), st_bbox)
# 
# # Convert list of bbox to data frame
# bbox_df <- do.call(rbind, lapply(bbox_list, function(b) {
#   data.frame(
#     xmin = b["xmin"],
#     xmax = b["xmax"],
#     ymin = b["ymin"],
#     ymax = b["ymax"]
#   )
# }))
# 
# # Compute width and length
# bbox_df$width <- bbox_df$xmax - bbox_df$xmin
# bbox_df$length <- bbox_df$ymax - bbox_df$ymin
# 
# sept_shapes_w_l <- cbind(sept_shapes, bbox_df)
# 
# sept_shapes_w_l <- sept_shapes_w_l %>%
#   mutate(width=width*3.28084, #3310 produces in meters convert to feet
#          length=length*3.28084)
# 
# View(sept_shapes_w_l)
# ## too big, see
# # https://portal.assessor.lacounty.gov/parceldetail/5831009001
# # need the smallest bounding box?
# 
# # Function to compute minimum rotated rectangle (manual version)
# min_bbox <- function(geom) {
#   coords <- st_coordinates(st_convex_hull(geom))[, 1:2]
#   ch <- coords[grDevices::chull(coords), ]
#   
#   best_area <- Inf
#   best_rect <- NULL
#   
#   for (i in 1:(nrow(ch)-1)) {
#     # Compute angle of each edge
#     dx <- ch[i+1,1] - ch[i,1]
#     dy <- ch[i+1,2] - ch[i,2]
#     angle <- atan2(dy, dx)
#     
#     # Rotate points
#     rot <- matrix(c(cos(-angle), -sin(-angle), sin(-angle), cos(-angle)), 2, 2)
#     rot_pts <- as.matrix(ch) %*% rot
#     
#     # Get bounding box
#     xmin <- min(rot_pts[,1]); xmax <- max(rot_pts[,1])
#     ymin <- min(rot_pts[,2]); ymax <- max(rot_pts[,2])
#     
#     area <- (xmax - xmin) * (ymax - ymin)
#     
#     # Keep smallest-area rectangle
#     if (area < best_area) {
#       best_area <- area
#       best_rect <- list(
#         width  = xmax - xmin,
#         height = ymax - ymin,
#         angle  = angle
#       )
#     }
#   }
#   
#   return(best_rect)
# }
# 
# # Apply to all polygons
# dims <- lapply(st_geometry(sept_shapes), min_bbox)
# sept_shapes$shortest_width <- sapply(dims, function(x) min(x$width, x$height))
# sept_shapes$longest_length <- sapply(dims, function(x) max(x$width, x$height))
# sept_shapes$orientation    <- sapply(dims, function(x) x$angle)
# 
# sept_shapes <- sept_shapes %>%
#   mutate(width=shortest_width*3.28084, #3310 produces in meters convert to feet
#          length=longest_length*3.28084)
# 
# 
# 
# 
# #----------------------------------------------------
# # Function to calculate lot width and depth assuming
# # the FRONT LOT LINE = LONGEST EDGE
# #----------------------------------------------------
# lot_metrics_longest_front <- function(polygon) {
#   # Return NA if geometry is empty
#   if (length(polygon) == 0 || st_is_empty(polygon)) {
#     return(data.frame(lot_depth = NA, lot_width = NA))
#   }
#   
#   # Ensure valid geometry
#   polygon <- st_make_valid(polygon)
#   
#   # Extract boundary (outer + inner rings)
#   boundary <- st_boundary(polygon)           # MULTILINESTRING
#   lines <- st_cast(boundary, "LINESTRING")   # individual LINESTRINGs
#   
#   # Split each LINESTRING into consecutive edges
#   edges <- do.call(rbind, lapply(st_geometry(lines), function(ls) {
#     coords <- st_coordinates(ls)[, 1:2]  # just X,Y
#     n <- nrow(coords)
#     if (n < 2) return(NULL)
#     data.frame(
#       x1 = coords[1:(n-1),1],
#       y1 = coords[1:(n-1),2],
#       x2 = coords[2:n,1],
#       y2 = coords[2:n,2]
#     )
#   }))
#   
#   if (is.null(edges) || nrow(edges) == 0) {
#     return(data.frame(lot_depth = NA, lot_width = NA))
#   }
#   
#   # Edge lengths & midpoints
#   edges$length <- sqrt((edges$x2 - edges$x1)^2 + (edges$y2 - edges$y1)^2)
#   edges$mid_x <- (edges$x1 + edges$x2)/2
#   edges$mid_y <- (edges$y1 + edges$y2)/2
#   
#   # 1️⃣ Front = longest edge
#   front <- edges[which.max(edges$length), ]
#   front_mid <- c(front$mid_x, front$mid_y)
#   
#   # 2️⃣ Rear = farthest edge midpoint
#   edges$dist <- sqrt((edges$mid_x - front_mid[1])^2 + (edges$mid_y - front_mid[2])^2)
#   rear <- edges[which.max(edges$dist), ]
#   rear_mid <- c(rear$mid_x, rear$mid_y)
#   
#   # 3️⃣ Lot depth
#   lot_depth <- sqrt(sum((rear_mid - front_mid)^2))
#   
#   # 4️⃣ Midpoint of depth line
#   mid_depth <- (front_mid + rear_mid)/2
#   depth_vec <- rear_mid - front_mid
#   depth_unit <- depth_vec / sqrt(sum(depth_vec^2))
#   perp_vec <- c(-depth_unit[2], depth_unit[1])
#   
#   # 5️⃣ Perpendicular line for width
#   perp_line <- st_sfc(st_linestring(rbind(
#     mid_depth + 1000 * perp_vec,
#     mid_depth - 1000 * perp_vec
#   )), crs = st_crs(polygon))
#   
#   # 6️⃣ Intersection with polygon boundary
#   poly_line <- st_union(lines)
#   ints <- st_intersection(perp_line, poly_line)
#   coords_int <- st_coordinates(ints)[, 1:2]
#   
#   # Compute width
#   if (nrow(coords_int) >= 2) {
#     lot_width <- sqrt((coords_int[1,1] - coords_int[2,1])^2 +
#                         (coords_int[1,2] - coords_int[2,2])^2)
#   } else {
#     lot_width <- NA
#   }
#   
#   return(data.frame(lot_depth = lot_depth, lot_width = lot_width))
# }
# 
# 
# #----------------------------------------------------
# # Example use
# #----------------------------------------------------
# # Apply the function to all polygons
# # Ensure valid geometries and remove empty ones
# lots <- st_make_valid(sept_shapes)
# lots <- lots[!st_is_empty(lots), ]
# 
# # Compute lot metrics for all polygons
# results <- lapply(st_geometry(lots), lot_metrics_longest_front)
# metrics_df <- do.call(rbind, results)
# 
# # Add to your sf data frame
# lots$lot_depth <- metrics_df$lot_depth*3.28084
# lots$lot_width <- metrics_df$lot_width*3.28084
# 
# lot_metrics_pca <- function(polygon) {
#   if (length(polygon) == 0 || st_is_empty(polygon)) {
#     return(data.frame(lot_depth = NA, lot_width = NA))
#   }
#   
#   # Ensure valid geometry
#   polygon <- st_make_valid(polygon)
#   
#   # Extract all coordinates
#   coords <- st_coordinates(st_boundary(polygon))[, 1:2]
#   
#   # Center the coordinates
#   coords_centered <- scale(coords, center = TRUE, scale = FALSE)
#   
#   # PCA on coordinates
#   pca <- prcomp(coords_centered)
#   
#   # Rotate coordinates along principal axes
#   rotated <- coords_centered %*% pca$rotation
#   
#   # Compute ranges along first and second principal axes
#   range_x <- range(rotated[,1])
#   range_y <- range(rotated[,2])
#   
#   lot_depth <- diff(range_y)  # along minor axis
#   lot_width <- diff(range_x)  # along major axis
#   
#   return(data.frame(lot_depth = lot_depth, lot_width = lot_width))
# }
# 
# results <- lapply(st_geometry(sept_shapes), lot_metrics_pca)
# metrics_df <- do.call(rbind, results)
# 
# lots$lot_depth <- metrics_df$lot_depth*3.28084
# lots$lot_width <- metrics_df$lot_width*3.28084
# 




