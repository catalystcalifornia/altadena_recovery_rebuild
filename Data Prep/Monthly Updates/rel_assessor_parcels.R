## PURPOSE: The purpose of this script is to produce the rel_assessor_parcels table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_assessor_parcels.docx ##
## SCRIPT OUTPUT: rel_assessor_parcels_YYYY_MM

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
xwalk <- st_read(con_alt, query="SELECT * FROM dashboard.crosswalk_assessor_2025_09_12")
# get assessor parcels for CURRENT MONTH
assessor_parcels <- st_read(con_alt, query="SELECT * FROM dashboard.assessor_parcels_universe_2025_12", geom="geom")
# get assessor parcels from PREVIOUS MONTH
shapes <- st_read(con_alt, query="SELECT * FROM dashboard.assessor_parcels_universe_2025_09", geom="geom")

#### STEP 3: FILTER (Update ain column names) ####
# select geometries from current month in the data we want
curr_shapes <- assessor_parcels %>% 
  filter(ain %in% xwalk$ain_2025_12)  

#### STEP 4: LEFT JOIN (Update ain column names) ####
# add west and east identifier so we don't need to to match to jan every time we run the analysis
curr_shapess_alt <- curr_shapes %>%
  left_join(xwalk, by=c("ain"="ain_2025_12")) %>%
  left_join(shapes %>% st_drop_geometry(), by=c("ain_2025_09"="ain_2025_09")) 

# clean up
curr_shapes_final <- curr_shapess_alt %>%
  select(ain,area_name,area_label) 

# mapview(curr_shapes_final)

#### STEP 5: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_parcels_", year, "_", month)
schema <- "dashboard"
indicator <- paste0("Relational spatial table with geometries of residential properties in either West or East Altadena proper as of MONTH:", month, " YEAR:", year)
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_parcels.R "
qa_filepath<-"  QA_sheet_rel_assessor_parcels.docx "

export_shpfile(con=con_alt, df=curr_shapes_final, schema="dashboard",
               table_name= table_label,
               geometry_column = "geom")


# Add metadata
column_names <- colnames(curr_shapes_final) # Get column names

column_comments <- c('Assessor ID number for current month - use this to match to other relational tables',
                     'West or East Altadena shortened label based on parcel as of January',
                     'West or East Altadena long label based on parcel as of January',
                     'geometry')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 6: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)
