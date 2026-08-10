## PURPOSE: The purpose of this script is to produce the rel_assessor_residential table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_tables_update_2026_08.docx ##
## SCRIPT OUTPUT: rel_assessor_residential_YYYY_MM

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

year <- "2026"
month <- "08"

#### STEP 2: PULL XWALKS AND DATA (UPDATE to latest data and xwalks) ####
# get for CURRENT MONTH
xwalk <- st_read(con_alt, query="SELECT * FROM dashboard.crosswalk_assessor_2026_04_08")
# get assessor data for CURRENT MONTH and filter with xwalk for just AINs we are evaluating for
assessor_data <- st_read(con_alt, query="SELECT * FROM dashboard.assessor_data_universe_2026_08") %>%
  filter(ain %in% xwalk$ain_2026_08)

# Check difference between new data and xwalk with jan universe in it
nrow(assessor_data)-length(unique(xwalk$ain_2026_08))
# 1 fewer parcel in assessor data

# Check what's missing in current data
missing_data <- xwalk %>% filter(!ain_2026_08 %in% assessor_data$ain)
View(missing_data) # 08/06/2026 5842008017 missing but no usetype or address so fine to drop
# we'll need the old residential type at minimum for filters to work in dashboard, other fields can stay blank
# Update code before final steps
# pull in previous xwalk for reference later
xwalk_prev <- st_read(con_alt, query="SELECT * FROM dashboard.crosswalk_assessor_2026_12_04") %>%
  filter(ain_2026_04 %in% missing_data$ain_2026_04)

#### STEP 2A: CHECK IF PADDING IS NEEDED IN USE CODE ####
unique(nchar(assessor_data$use_code)) # 4 characters which is expected
# don't need to pad

# section for if you need to pad - otherwise leave commented out
# assessor_data <- assessor_data %>%
#   mutate(use_code_orig = use_code,
#          use_code=str_pad(use_code_orig, width = 4, side = "left", pad = "0"))
# 
# # check
# unique(nchar(assessor_data$use_code))
# assessor_data %>% select(use_code, use_code_orig) %>% slice_sample(n=20)


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

# Check - should be 0 NA
table(rel_res_df$res_type,useNA="always")
# 08/06/2026
# Condominium   Multifamily Single-family          <NA> 
#   64           316          5295             0 
# Dec
# Condominium   Multifamily Single-family          <NA> 
#   64           316          5289             0 

#### STEP 5: GETTING MIXED VS RESIDENTIAL BOOLEAN COLUMNS (No Updates) ####
## Residential & mixed use properties flag
rel_res_df <- rel_res_df %>%
  mutate(residential=ifelse(str_detect(use_code, "^0"), TRUE, FALSE),
         # Keep mixed use flag in case a parcel changes over time
         mixed_use=ifelse(str_detect(use_code, "^121") | str_detect(use_code, "^172"), TRUE, FALSE))

# Check - should be 0 NA
table(rel_res_df$residential,useNA="always")
# Aug 2026- TRUE - 5675 0 NA
# Dec 2025- TRUE - 5675 0 NA - went up because of the 6 parcels that were missing in 2025-12 data but are not missing now
table(rel_res_df$mixed_use,useNA="always")
# Aug 2026- FALSE - 5675 0 NA
# Dec 2025- TRUE - 5675 0 NA

# check for vacant parcels, but okay to keep in data if they change to vacant over time
vacant <- rel_res_df %>%
  filter(str_detect(use_code, "V$") | str_detect(use_code, "X$") )

  nrow(vacant)
# Dec 2025 - 1 vacant
# Apr 2026 - 2 vacant
# ain - 5841006015, in dashboard.assessor_data_universe_2025_01 - this parcel was not vacant. important to see how use codes change over time
# ain - 5842013027 was 5842013003 in jan25 (based on current xwalk) and in dashboard.assessor_data_universe_2025_01 was not vacant
# Aug 2026 - 347 vacant  #could be a red flag that something is wrong in the code or a massive outflux of residents from Altadena / new building
# print(unique(vacant$ain)) #still 347; includes the 2 other vacancies from previous years
  # QAer do you mind double checking this? *************
  
  #### STEP 7: CLEAN UP DF AND ADD PARCELS WITH MISSING DATA: UPDATE ####
final_res_data <- rel_res_df %>% 
  # add address field for dashboard
  mutate(zip=gsub("0000","",zip)) %>%
  mutate(city_state=gsub(" CA", ", CA", city_state)) %>% 
  # convert fractional address back to normal character
  mutate(fraction_re=format(as.Date(fraction, format = "%d-%b"), "%m/%d"),
         fraction_re=gsub("0","",fraction_re)) %>%
  mutate(address=paste(situs_house_no, fraction_re,direction, street_name, unit, city_state, zip) %>%
           # remove extra NAs
         str_replace_all("\\bNA\\b", "") %>%
           str_squish()) %>%
  mutate(address=gsub("\\s+", " ", address)) %>% 
  mutate(address=ifelse(address=="0 0", NA, address)) %>%
  select(ain, residential, mixed_use, res_type, total_units, landlord_units, total_square_feet, total_bedrooms, address, use_code, zoning_code) %>%
  rename(ain_2026_08 = ain)
  
# Add missing parcels (UPDATE)
final_missing_data <- missing_data %>% select(starts_with("ain"))%>% 
  left_join(xwalk_prev %>% select(starts_with("ain"), starts_with("use_code")), by=c("ain_2026_04"="ain_2026_04"))

final_missing_data <- final_missing_data %>%
  mutate(use_code=ifelse(is.na(use_code_2026_04), use_code_2025_12,
                         use_code_2026_04)) %>%
  distinct(ain_2026_08,use_code) %>%
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

# individually update ain 5842008017 (08/06/2026 Update)
update_row <- final_res_data %>% # replace df with the correct df name
  filter(ain_2026_08 == '5842008010') %>% 
  mutate(ain_2026_08 = '5842008017') #needs to be single = to replace

final_res_data <- final_res_data %>%  # replace df with the correct df name
  rows_update(update_row, by = "ain_2026_08")

# check for duplicates
nrow(final_res_data)-length(unique(final_res_data$ain_2026_08)) # should be 0 difference

# check for same number of rows as xwalk
nrow(final_res_data)-length(unique(xwalk$ain_2026_08)) # should be 0 difference

# check for NA res type - should be 0
table(final_res_data$res_type,useNA='always')
# April numbers - flag significant changes # Aug 2026 exactly the same
# Condominium   Multifamily Single-family          <NA> 
#   64           316          5296             0 

#### STEP 8: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_residential_", year, "_", month)
schema <- "dashboard"
indicator <- "Relational table with summarized assessor information and flags for current month parcels that were in Altadena in january 2025, selected based on crosswalk and keeping only the january 2025 parcels in Altadena. Only includes properties in either West or East Altadena proper."
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/AB/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_residential.R "
qa_filepath<-"  QA_sheet_rel_tables_update_2026_08.docx "

# dbWriteTable(con_alt, Id(schema, table_label), final_res_data,
#              overwrite = FALSE, row.names = FALSE)


# Add metadata
column_names <- colnames(final_res_data) # Get column names

column_comments <- c('Assessor ID number for current month- use this to match to other relational tables ',
                     'Flag for whether property is a residential use (e.g., use code starting with 0)',
                     'Flag for whether property is mixed residential-commercial (use code starting with 1 but with a residential combo indicated in 3rd character)',
                     'Residential type -- either single-family, multifamily, mixed use, condominium, boarding house',
                     'Total units -- use caution when interpreting for mixed use - can include commercial',
                     'Total rental units on the property -- use caution when interpreting for mixed use - can include commercial',
                     'Total square feet of buildings on property',
                     'Total bedrooms on property',
                     'site address if address provided in current data',
                     'Current use code if not available in current data prior use code',
                     'Zoning code for the property')

# add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 9: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)
