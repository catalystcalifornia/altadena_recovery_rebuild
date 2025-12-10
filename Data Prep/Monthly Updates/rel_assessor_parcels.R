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

# get west and east altadena
east <- st_read(con_alt, query="SELECT * FROM data.east_altadena_3310", geom="geom")
west <- st_read(con_alt, query="SELECT * FROM data.west_altadena_3310", geom="geom")

st_crs(assessor_parcels)
st_crs(east)
st_crs(west)
#### STEP 3: JOIN FOR EAST/WEST labeling (Update ain column names) ####

# parcels within east altadena
parcels_east <- st_join(assessor_parcels, east %>% select(name,label), join=st_within, left=FALSE)

# check
# mapview(parcels_east) +
#   mapview(east) 
# looks good

# parcels within west altadena
parcels_west <- st_join(assessor_parcels, west %>% select(name,label), join=st_within, left=FALSE)

# check
# mapview(parcels_west) +
#   mapview(west)
# looks good

# join together
parcels_altadena <- rbind(parcels_west,parcels_east)
table(parcels_altadena$name,useNA='always')

# check for duplicates
nrow(parcels_altadena)
length(unique(parcels_altadena$ain))


# select geometries from current month in the data we want with xwalk
parcels_altadena <- parcels_altadena %>% 
  filter(ain %in% xwalk$ain_2025_12)  

# select columns needed and rename
rel_area_geom_df <- parcels_altadena %>%
  select(ain,name,label) %>%
  rename(ain_2025_12 = ain,
         area_name=name,
         area_label=label)

#### STEP 4: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_parcels_", year, "_", month)
schema <- "dashboard"
indicator <- paste0("Relational spatial table with geometries of residential properties in either West or East Altadena proper as of MONTH:", month, " YEAR:", year)
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_parcels.R "
qa_filepath<-"  QA_sheet_rel_assessor_parcels.docx "

export_shpfile(con=con_alt, df=rel_area_geom_df, schema="dashboard",
               table_name= table_label,
               geometry_column = "geom")


# Add metadata
column_names <- colnames(rel_area_geom_df) # Get column names

column_comments <- c('Assessor ID number for current month - use this to match to other relational tables',
                     'West or East Altadena shortened label based on parcel as of January',
                     'West or East Altadena long label based on parcel as of January',
                     'geometry')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 5: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)
