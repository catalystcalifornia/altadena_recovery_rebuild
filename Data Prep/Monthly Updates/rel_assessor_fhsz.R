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
month <- "09"

#### STEP 2: PULL XWALKS AND DATA (Update to latest data and xwalks) ####
# get xwalk for PREVIOUS MONTH and CURRENT MONTH
xwalk <- st_read(con_alt, query="SELECT * FROM data.crosswalk_assessor_jan_sept_2025")
# get assessor fhsz for CURRENT MONTH
assessor_fhsz <- st_read(con_alt, query="Select * from data.rel_assessor_fhsz_sept2025")
# get relational tables from PREVIOUS MONTH
res <- st_read(con_alt, query="Select * from data.rel_assessor_residential_jan2025")

#### STEP 3: FILTER (Update ain column names) ####
# filter crosswalk for residential parcels in Altadena
xwalk_alt <- xwalk %>%
  filter(ain_jan %in% res$ain)

# select from current month in the data we want
curr_fhsz <- assessor_fhsz %>%
  filter(ain_sept %in% xwalk_alt$ain_sept) %>%   # keep only matching ain_sept
  left_join(xwalk_alt %>% select(ain_sept, ain_jan),
            by = "ain_sept")                     # bring in ain_jan

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

column_comments <- c('West or East Altadena shortened label based on parcel as of January',
                     'Assessor ID number for current month - use this to match to other relational tables',
                     'state fire hazard zone',
                     'local fire hazard zone - top coded based on most severe zone--uses this or combined_fhsz',
                     'list of local fire hazard zones the parcel fell within',
                     'main local fire hazard zone - top coded for most severe zone parcel falls within',
                     'Assessor ID number from month of Eaton Fire, January 2025')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 5: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)

