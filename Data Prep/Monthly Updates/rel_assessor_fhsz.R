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
xwalk <- st_read(con_alt, query="SELECT ain_2025_09, ain_2025_12 FROM dashboard.crosswalk_assessor_2025_09_12")
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
  distinct(ain_2025_12, .keep_all = TRUE)

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

column_comments <- c('Assessor ID number for previous month',
                     'Assessor ID number for current month - use this to match to other relational tables',
                     "Label for area in altadena, East of West",
                     'list of local fire hazard zones the parcel fell within',
                     'local fire hazard zone - top coded based on most severe zone--uses this or combined_fhsz',
                     'state fire hazard zone',
                     'main local fire hazard zone - top coded for most severe zone parcel falls within, this is the category to use')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 5: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)

