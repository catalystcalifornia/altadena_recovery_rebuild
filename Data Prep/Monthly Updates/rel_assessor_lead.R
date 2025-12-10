## PURPOSE: The purpose of this script is to produce the rel_assessor_lead table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_assessor_lead.docx ##
## SCRIPT OUTPUT: rel_assessor_lead_YYYY_MM

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
# get assessor lead for CURRENT MONTH
assessor_lead <- st_read(con_alt, query="Select * from data.rel_assessor_lead_sept2025")
# get relational tables from PREVIOUS MONTH
res <- st_read(con_alt, query="Select * from data.rel_assessor_residential_jan2025")

#### STEP 3: FILTER (Update ain column names) ####
# filter crosswalk for residential parcels in Altadena
xwalk_alt <- xwalk %>%
  filter(ain_jan %in% res$ain)

# select from current month in the data we want
curr_lead <- assessor_lead %>%
  filter(ain_sept %in% xwalk_alt$ain_sept) %>%   # keep only matching ain_sept
  left_join(xwalk_alt %>% select(ain_sept, ain_jan),
            by = "ain_sept")                     # bring in ain_jan

#### STEP 4: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_lead_", year, "_", month)
schema <- "dashboard"
indicator <- paste0("Relational table of Fire Hazard State Zone in either West or East Altadena proper as of MONTH:", month, " YEAR:", year)
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_lead.R "
qa_filepath<-"  QA_sheet_rel_assessor_lead.docx "

dbWriteTable(con_alt, Id(schema, table_label), curr_lead,
             overwrite = FALSE, row.names = FALSE)


# Add metadata
column_names <- colnames(curr_lead) # Get column names

column_comments <- c('Assessor ID number for current month - use this to match to other relational tables',
                     'lead testing grid',
                     'True-false for if testing grid mead was above 80, NA if not tested',
                     'Label for if area is in high lead area or not, or if not tested',
                     'lead geometric mean for testing grid, NA if not tested',
                     'Assessor ID number from month of Eaton Fire, January 2025')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 5: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)