## PURPOSE: The purpose of this script is to produce the rel_assessor_owner table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_assessor_residential.docx ##
## SCRIPT OUTPUT: rel_assessor_owner_YYYY_MM

#### STEP 1: SET UP (UPDATE year and month) ####
library(sf)
library(rmapshaper)
library(stringr)
library(readxl)
library(tidyverse)
library(janitor)
library(writexl)

options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")

year <- "2025"
month <- "12"

#### STEP 2: PULL XWALKS AND DATA (UPDATE to latest data and xwalks) ####
# get for CURRENT MONTH
xwalk <- st_read(con_alt, query="SELECT * FROM dashboard.crosswalk_assessor_2025_09_12")
# get assessor data for CURRENT MONTH and filter with xwalk for just AINs we are evaluating for
assessor_data <- st_read(con_alt, query="SELECT * FROM dashboard.assessor_data_universe_2025_12") %>%
  filter(ain %in% xwalk$ain_2025_12)
# get sales data for current month 
sales_data <- st_read(con_alt, query="select * from dashboard.rel_assessor_sales_2025_12_temp") %>%
  filter(ain %in% xwalk$ain_2025_12)
# get residential data for current month 
residential_data <- st_read(con_alt, query="select * from dashboard.rel_assessor_residential_2025_12")

# join sales data to assessor data keeping columns for owner type
owner_info <- sales_data %>%
  left_join(assessor_data %>% 
              select(ain,exemption_type,landlord_units,tax_stat_key,year_sold_to_state,
                     contains("owner"),mail_house_no,contains("m_")), 
            by=c("ain"="ain")
            )

#### STEP 1: CREATING OWNER_RENTER COLUMN (QA AND UPDATE IF NEEDED) ####
# check exemption type recoding and fix if needed
table(owner_info$exemption_type,useNA='always')

data_altadena_owner <- rel_res_df  %>%
  mutate(exemption_type=ifelse(exemption_type=='ÿ', NA, exemption_type),
         owner_renter=case_when(
           num_howmowner_exemption>=1 & landlord_units==0 ~ "Owner occupied",
           num_howmowner_exemption>=1 & landlord_units>=1 ~ "Owner & Renter occupied",
           num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter occupied",
           TRUE ~ "Other")) 

# check
table(data_altadena_owner$owner_renter,useNA='always')


data_altadena_owner<-data_altadena_owner%>%
  # owner occupied or church/welfare exemption
  mutate(
    owner_renter = case_when(
      exemption_type %in% "1" & owner_renter == "Other" ~ "Owner occupied",
      exemption_type %in% c("4", "5", "7") & owner_renter == "Other" ~ "Church/Welfare exemption",
      TRUE ~ owner_renter )) %>% # keeps existing value if none of the above conditions are met 
  # top code corporate owned
  mutate(
    owner_renter = ifelse(
      (grepl("LLC| LP| Inc| Investment| Corp", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc| Investment| Corp", first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc| Investment| Corp", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "LLC owned",owner_renter))%>%
  # trust owned
  mutate(
    owner_renter = ifelse(
      (grepl("TRUST|TRST| TR | TRS", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR | TRS",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR | TRS", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Trust owned",owner_renter)) %>%
  # sold to state/govt owned
  mutate(
    owner_renter = case_when(
      tax_stat_key %in% c("1","2") ~ "Sold to state",
      tax_stat_key %in% "3" ~ "SBE or Government owned",
      TRUE ~ owner_renter  # keeps existing value if none of the above conditions are met
    )
  ) %>%
  # other church or charity owned
  mutate(
    owner_renter = ifelse(
      (grepl("CHURCH|FRATERNAL|SERVICES", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("CHURCH|FRATERNAL|SERVICES",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("CHURCH|FRATERNAL|SERVICES", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Church/Welfare exemption",owner_renter)) %>%
  # recoding others church or charity owned
  mutate(
    owner_renter = ifelse(
      grepl("SUBSIDIZED HOUSING CORP|PASADENA CEMETRY ASSN CORP|Pasadena Cemetery Association|PUBLIC WORKS GROUP CORP", first_owner_name, ignore.case = TRUE),
      "Church/Welfare exemption",owner_renter),
    # adding land trusts will call church, charity, or nonprofit
    owner_renter = ifelse(
      grepl("GREENLINE HOUSING FOUNDATION|NHS NEIGHBORHOOD REDEVELOPMENT", first_owner_name, ignore.case = TRUE),
      "Church/Welfare exemption",owner_renter),
    owner_renter = ifelse(
      grepl("LA VINA HOMEOWNERS|CAMERON,JOHN K,JR AND|CAMERON,JOHN K AND|CAMERON,JOHN K JR AND|LINCOLN AVENUE WATER CO", first_owner_name, ignore.case = TRUE) & owner_renter == "Other",
      "Other",
      ifelse(owner_renter=="Other","Likely owner-occupied, no exemption", owner_renter)))%>%
  mutate(owner_renter_orig=owner_renter,
         owner_renter=case_when(
           owner_renter_orig=="Church/Welfare exemption" ~ "Church, charity, or nonprofit owned",
           owner_renter_orig=="Likely owner-occupied, no exemption" ~ "Likely owner occupied, no exemption",
           owner_renter_orig=="LLC owned" ~ "Corporation owned",
           owner_renter_orig=="Other" ~ "Other ownership",
           owner_renter_orig=="Owner & Renter occupied" ~ "Owner & renter occupied",
           owner_renter_orig=="SBE or Government owned" ~ "Government owned",
           owner_renter_orig=="Owner occupied" ~ "Owner occupied, homeowner exemption",
           TRUE ~ owner_renter_orig
         ))

# Check owner and renter recoding
table(data_altadena_owner$owner_renter,useNA='always')
# Dec QA
# Church or charity owned(went up)           Corporation owned(went up)         Government owned 
# 16                                          167                                   2 
# Likely owner occupied, no exemption   Other ownership             Owner & renter occupied 
# 1235                                              46                                 387 
# Owner occupied, homeowner exemption(went down)   Renter occupied                       Sold to state(went down) 
# 2570                                              521                                  31 
# Trust owned                                <NA> 
#   694(went up)                                   0 

# Review likely owner occupied
likely_homeowner <-data_altadena_owner %>% filter(owner_renter=="Likely owner occupied, no exemption") %>%
  select(ain, owner_renter, tax_stat_key, year_sold_to_state, first_owner_name, second_owner_name, first_owner_name_overflow, exemption_type, num_howmowner_exemption, total_units, landlord_units, owner_renter)
View(likely_homeowner)

likely_homeowner_table <- likely_homeowner %>%
  group_by(first_owner_name,first_owner_name_overflow,second_owner_name) %>%
  summarise(count=n())
View(likely_homeowner_table) # sort desc by count
# looks okay


#### STEP 7: CLEAN UP DF AND ADD PARCELS WITH MISSING DATA: UPDATE ####
final_res_data <- data_altadena_owner %>% 
  # add address field for dashboard
  mutate(zip=gsub("0000","",zip)) %>%
  mutate(city_state=gsub(" CA", ", CA", city_state)) %>% 
  mutate(address=paste(situs_house_no, direction, street_name, unit, city_state, zip)) %>%
  mutate(address=gsub("\\s+", " ", address)) %>% 
  mutate(address=ifelse(address=="0 0", NA, address)) %>%
  select(ain, residential, mixed_use, res_type, owner_renter, total_units, landlord_units, total_square_feet, total_bedrooms, address, use_code, zoning_code) %>%
  rename(ain_2025_12 = ain)

# Add missing parcels (UPDATE)
final_missing_data <- missing_data %>% select(starts_with("ain"))%>% 
  left_join(xwalk_prev %>% select(starts_with("ain"), starts_with("use_code")), by=c("ain_2025_09"="ain_2025_09"))

final_missing_data <- final_missing_data %>%
  mutate(use_code=ifelse(is.na(use_code_2025_09), use_code_2025_01,
                         use_code_2025_09)) %>%
  distinct(ain_2025_12,use_code) %>%
  # add just some columns we might need, dont assume address, owner, or square feet is the same
  # type of residential property
  mutate(
    res_type=case_when( 
      # condos first
      str_detect(use_code, "C$|E$") ~ "Condominium",
      str_detect(use_code, "^01") ~ "Single-family",
      str_detect(use_code, "^02|^03|^04|^05") ~ "Multifamily",
      str_detect(use_code,"^08") ~ "Boarding house",
      str_detect(use_code,"^12|^17") ~ "Mixed use", 
      TRUE ~NA)) %>%
  # res or mixed use
  mutate(residential=ifelse(str_detect(use_code, "^0"), TRUE, FALSE),
         # Keep mixed use flag in case a parcel changes over time
         mixed_use=ifelse(str_detect(use_code, "^121") | str_detect(use_code, "^172"), TRUE, FALSE))

final_res_data <- bind_rows(final_res_data,final_missing_data)

# check for duplicates
nrow(final_res_data)-length(unique(final_res_data$ain_2025_12)) # should be 0 difference

# check for same number of rows as xwalk
nrow(final_res_data)-length(unique(xwalk$ain_2025_12)) # should be 0 difference

# check for NA res type - should be 0
table(final_res_data$res_type,useNA='always')

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

column_comments <- c('Assessor ID number for current month- use this to match to other relational tables ',
                     'Flag for whether property is a residential use (e.g., use code starting with 0)',
                     'Flag for whether property is mixed residential-commercial (use code starting with 1 but with a residential combo indicated in 3rd character)',
                     'Residential type -- either single-family, multifamily, mixed use, condominium, boarding house',
                     'Housing tenure-ownerships -- either homeowner (indicated by homeowner exemption), renter, trust owned, corporation owned, sold to state, government owned, other ownership, or owner likely but with no exemption. For more detailed methodology of choices see R script',
                     'Total rental units on the property -- use caution when interpreting for mixed use - can include commercial',
                     'Total square feet of buildings on property',
                     'Total bedrooms on property',
                     'site address if address provided in current data',
                     'Current use code if not available in current data prior use code',
                     'Zoning code for the property')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 9: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)
