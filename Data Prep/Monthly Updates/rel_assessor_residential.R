## PURPOSE: The purpose of this script is to produce the rel_assessor_residential table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_assessor_residential.docx ##
## SCRIPT OUTPUT: rel_assessor_residential_YYYY_MM

#### STEP 1: SET UP (Update year and month) ####
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

year <- "2025"
month <- "09"

#### STEP 2: PULL XWALKS AND DATA (Update to latest data and xwalks) ####
# get xwalk for PREVIOUS MONTH and CURRENT MONTH
xwalk <- st_read(con_alt, query="SELECT * FROM data.crosswalk_assessor_jan_sept_2025")
# get assessor data for CURRENT MONTH
assessor_data <- st_read(con_alt, query="Select * from data.assessor_data_universe_sept2025")

# JZ QA notes: 
# Do we not need the step of looking at which AINs in the most recent month's assessor data were also in the January 2025 assessor data?
# See commented out code beow:

# filter jan sept crosswalk for residential parcels in Altadena
# jan_sept_xwalk_alt <- jan_sept_xwalk %>%
#   filter(ain_jan %in% res$ain)
# 
# nrow(jan_sept_xwalk_alt)
# length(unique(jan_sept_xwalk_alt$ain_jan))
# length(unique(jan_sept_xwalk_alt$ain_sept))
# one duplicate september ain
# jan_sept_xwalk_alt$ain_sept[duplicated(jan_sept_xwalk_alt$ain_sept)]
# 
# View(jan_sept_xwalk_alt)

# filter MOST RECENT MONTH assessor data for same ains
# sept_data_altadena <- assessor_data %>%
#   filter(ain %in% jan_sept_xwalk_alt$ain_sept)


#### STEP 3:GETTING TOTAL UNITS (NO UPDATES) ####

data_total_units <- assessor_data %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
         total_square_feet = rowSums(across(ends_with("_square_feet"))),
         total_bedrooms = rowSums(across(ends_with("_bedrooms")))) %>%
  mutate(total_units=total_units-landlord_units) # so we don't double count rental units

#### STEP 4: GETTING RES_TYPE (NO UPDATES) ####

rel_res_df  <- data_total_units  %>%
  mutate(
    res_type=case_when( # type of residential property
      # condos first
      str_detect(use_code, "C$|E$") ~ "Condominium",
      str_detect(use_code, "^01") ~ "Single-family",
      str_detect(use_code, "^02|^03|^04|^05") ~ "Multifamily",
      str_detect(use_code,"^08") ~ "Boarding house",
      str_detect(use_code,"^12|^17") ~ "Mixed use", 
      TRUE ~NA))

#### STEP 5: GETTING MIXED VS RESIDENTIAL BOOLEAN COLUMNS (Update ??) ####
## Residential properties
data_altadena_res_temp <- rel_res_df %>%
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

### Flags for mixed use or residential
data_altadena_res <- data_altadena_res %>%
  mutate(residential=TRUE, # residential column flag
         mixed_use=FALSE) # mixed use flag


#### STEP 6: CREATING OWNER_RENTER COLUMN (Update ??)) ####
data_altadena_owner <- data_altadena_res %>%
  mutate(owner_renter=case_when(
    num_howmowner_exemption>=1 & landlord_units==0 ~ "Owner occupied",
    num_howmowner_exemption>=1 & landlord_units>=1 ~ "Owner & Renter occupied",
    num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter occupied",
    TRUE ~ "Other")) 

# check
table(data_altadena_owner$owner_renter)


data_altadena_owner<-data_altadena_owner%>%
  mutate(
    owner_renter = case_when(
      exemption_type %in% "1" & owner_renter == "Other" ~ "Owner occupied",
      exemption_type %in% c("4", "5", "7") & owner_renter == "Other" ~ "Church/Welfare exemption",
      TRUE ~ owner_renter )) %>% # keeps existing value if none of the above conditions are met 
  # top code corporate and add other abbreviations
  mutate(
    owner_renter = ifelse(
      (grepl("LLC| LP| Inc | Investment", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc | Investment", first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc | Investment", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "LLC owned",owner_renter))%>%
  mutate(
    owner_renter = ifelse(
      (grepl("TRUST|TRST| TR | TRS", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR | TRS",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR | TRS", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Trust owned",owner_renter)) %>%
  mutate(
    owner_renter = case_when(
      tax_stat_key %in% c("1","2") ~ "Sold to state",
      tax_stat_key %in% "3" ~ "SBE or Government owned",
      TRUE ~ owner_renter  # keeps existing value if none of the above conditions are met
    )
  ) %>%
  mutate(
    owner_renter = ifelse(
      (grepl("CHURCH|FRATERNAL|SERVICES", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("CHURCH|FRATERNAL|SERVICES",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("CHURCH|FRATERNAL|SERVICES", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Church/Welfare exemption",owner_renter)) %>%
  mutate(
    owner_renter = ifelse(
      grepl("SUBSIDIZED HOUSING CORP|Pasadena Cemetery Association|PUBLIC WORKS GROUP CORP", first_owner_name, ignore.case = TRUE) & owner_renter == "Other",
      "Church/Welfare exemption",owner_renter),
    owner_renter = ifelse(
      grepl("LA VINA HOMEOWNERS|CAMERON,JOHN K,JR AND|CAMERON,JOHN K AND|CAMERON,JOHN K JR AND|LINCOLN AVENUE WATER CO", first_owner_name, ignore.case = TRUE) & owner_renter == "Other",
      "Other",
      ifelse(owner_renter=="Other","Likely owner-occupied, no exemption", owner_renter))) %>%
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


#### STEP 7: CLEAN UP DF ####
final_res_data <- data_altadena_owner %>% 
  select(ain, residential, mixed_use, res_type, owner_renter, total_units, landlord_units, total_square_feet, total_bedrooms, use_code, zoning_code)
#### STEP 8: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_residential_", year, "_", month)
schema <- "dashboard"
indicator <- "Relational table table with summarized information and flags for current month parcels that were in Altadena in january 2025, selected based on crosswalk and keeping only the january 2025 parcels in Altadena. Only includes properties in either West or East Altadena proper."
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_residential.R "
qa_filepath<-"  QA_sheet_rel_assessor_residential.docx "

dbWriteTable(con_alt, Id(schema, table_label), final_res_data,
             overwrite = FALSE, row.names = FALSE)


# Add metadata
column_names <- colnames(final_res_data) # Get column names

column_comments <- c('Assessor ID number from September 2025- use this to match to other relational tables ',
                     'Flag for whether property is a residential use (e.g., use code starting with 0)',
                     'Flag for whether property is mixed residential-commercial (use code starting with 1 but with a residential combo indicated in 3rd character)',
                     'Residential type -- either single-family, multifamily, mixed use, condominium, boarding house',
                     'Housing tenure-ownerships -- either homeowner (indicated by homeowner exemption), renter, trust owned, corporation owned, sold to state, government owned, other ownership, or owner likely but with no exemption. For more detailed methodology of choices see R script',
                     'Total rental units on the property -- use caution when interpreting for mixed use - can include commercial',
                     'Total square feet of buildings on property',
                     'Total bedrooms on property',
                     'Original use code for reference',
                     'Zoning code for the property')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 9: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)
