## PURPOSE: The purpose of this script is to produce the rel_assessor_parcels table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_assessor_parcels.docx ##
## SCRIPT OUTPUT: rel_assessor_parcels_YYYY_MM

#### STEP 1: SET UP (Update year and month) ####
library(sf)
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

#### STEP 2: PULL XWALKS AND DATA (Update to latest data and xwalks) ####
# get xwalk for PREVIOUS MONTH and CURRENT MONTH
xwalk <- st_read(con_alt, query="SELECT * FROM dashboard.crosswalk_assessor_09_12_2025")
# get area names from January
assessor_areas<- st_read(con_alt, query="Select * from data.rel_assessor_altadena_parcels_jan2025")
# get curr geoms
geoms <- st_read(con_alt, query="Select * from dashboard.assessor_parcels_universe_2025_12") %>%
  filter(ain %in% xwalk$ain_2025_12)

#### STEP 3: FILTER (Update ain column names) ####
# select area name for current month via xwalk
curr_area <- geoms %>% 
  select(ain) %>%
  rename(ain_2025_12=ain) %>%
  left_join(xwalk, by="ain_2025_12") %>%
  left_join(assessor_areas %>% st_drop_geometry() %>% select(ain,area_name,area_label),
            by = c("ain_2025_01" = "ain")
  ) 

# select distinct records and trim down columns
curr_area <- curr_area %>%
  distinct(ain_2025_12,area_name,area_label,geom)

# check for duplicates and gaps
curr_area %>%
  group_by(ain_2025_12) %>%
  mutate(count=n()) %>%
  filter(count > 1) %>%
  nrow() # should be 0

nrow(curr_area) - length(unique(curr_area$ain_2025_12)) # should be 0

nrow(curr_area) - length(unique(curr_area$ain_2025_12)) # should be 0

# check for NA - should be 0
table(curr_area$area_name,useNA='always')
# Dec QA
# East West <NA> 
#   1664 4012    0 

# check geom still working
st_crs(curr_area)


#### STEP 4: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_parcels_", year, "_", month)
schema <- "dashboard"
indicator <- paste0("Relational spatial table with geometries of residential properties in either West or East Altadena proper as of MONTH:", month, " YEAR:", year)
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_parcels.R "
qa_filepath<-"  QA_sheet_rel_assessor_parcels.docx "

export_shpfile(con=con_alt, df=curr_area, schema="dashboard",
               table_name= table_label,
               geometry_column = "geom")


# Add metadata
column_names <- colnames(curr_area) # Get column names

column_comments <- c('Assessor ID number for current month - use this to match to other relational tables',
                     'West or East Altadena shortened label based on parcel as of January',
                     'West or East Altadena long label based on parcel as of January',
                     'geometry')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 5: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)
