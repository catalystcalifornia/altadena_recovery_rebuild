## PURPOSE: The purpose of this script is to produce the rel_assessor_fhsz table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_assessor_fhsz.docx ##
## SCRIPT OUTPUT: rel_assessor_fhsz_YYYY_MM

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
month <- "12"

#### STEP 2: PULL XWALKS AND DATA (Update to latest data and xwalks) ####
# get xwalk for PREVIOUS MONTH and CURRENT MONTH
xwalk <- st_read(con_alt, query="SELECT ain_2025_09, ain_2025_12 FROM dashboard.crosswalk_assessor_09_12_2025")
# get assessor fhsz from PREVIOUS MONTH
assessor_fhsz <- st_read(con_alt, query="Select * from dashboard.rel_assessor_fhsz_2025_09")

#### STEP 3: FILTER (Update ain column names) ####
# select fhsz for current month via xwalk
curr_fhsz <- xwalk %>%
  left_join(assessor_fhsz,
            by = c("ain_2025_09" = "ain_sept")
            ) %>%
  #drop older column
  select(-ain_jan) %>%
  #remove duplicates, keep only first occurrence of ain parcel 
  distinct(ain_2025_12,local_fhsz_list, local_fhsz, state_fhsz, combined_fhsz) %>%
  # add authority column
  mutate(authority=case_when(state_fhsz=="Very High" & (local_fhsz=="None" | is.na(local_fhsz)) ~ "State",
                             state_fhsz=="Very High" & (local_fhsz!="None" | !is.na(local_fhsz)) ~ "State & Local",
                             (state_fhsz=="None" | is.na(state_fhsz)) & local_fhsz!="None" ~ "Local",
                             TRUE ~ "None"))

# check for duplicates and gaps
curr_fhsz %>%
  group_by(ain_2025_12) %>%
  mutate(count=n()) %>%
  filter(count > 1) %>%
  nrow() # should be 0

nrow(curr_fhsz) - length(unique(curr_fhsz$ain_2025_12)) # should be 0

nrow(curr_fhsz) - length(unique(xwalk$ain_2025_12)) # should be 0

#### STEP 4: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_fhsz_", year, "_", month)
schema <- "dashboard"
indicator <- paste0("Relational table of Fire Hazard State Zone in either West or East Altadena proper as of MONTH:", month, " YEAR:", year)
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_fhsz.R "
qa_filepath<-"  QA_sheet_rel_assessor_fhsz.docx "

dbWriteTable(con_alt, Id(schema, table_label), curr_fhsz,
             overwrite = FALSE, row.names = FALSE)

# Add metadata
column_names <- colnames(curr_fhsz) # Get column names

column_comments <- c(
                     'Assessor ID number for current month - use this to match to other relational tables',
                     'list of local fire hazard zones the parcel fell within',
                     'local fire hazard zone - top coded based on most severe zone--uses this or combined_fhsz',
                     'state fire hazard zone',
                     'main local fire hazard zone - top coded for most severe zone parcel falls within, this is the category to use',
                     'local or state authority identifier')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 5: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)

