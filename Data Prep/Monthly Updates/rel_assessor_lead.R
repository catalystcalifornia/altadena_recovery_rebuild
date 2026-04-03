## PURPOSE: The purpose of this script is to produce the rel_assessor_lead table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_tables_update_2026_04.docx ##
## SCRIPT OUTPUT: rel_assessor_lead_YYYY_MM

#### STEP 1: SET UP (Update year and month) ####
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
month <- "04"

#### STEP 2: PULL SHAPES AND DATA (Update to latest data and xwalks) ####
# current month parcels
parcels <- st_read(con_alt, query="SELECT ain_2026_04,geom FROM dashboard.rel_assessor_parcels_2026_04")

# get lead data
lead <- st_read(con_alt, query="SELECT * FROM data.lacdph_lead_results_grid_2025", geom = "geom") %>% 
  mutate(hi_lead_flag = ifelse(lead_geometric_mean > 80, TRUE, FALSE)) 

#### Step 3: intersect and compute which grid has the majority of the parcel in it, assign to that one ####
st_crs(lead)
st_crs(parcels)
# mapview(lead) + mapview(parcels)

ain_lead <- st_intersection(parcels %>%
                              mutate(ain_id = row_number()),
                            lead %>% 
                              mutate(grid_id = row_number())) %>% 
  #calculate overlap
  mutate(overlap_area = st_area(.)) %>%
  #only keep more of the parcel in the lead level grid 
  group_by(ain_id) %>%
  slice_max(overlap_area, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  #deleting columns that are not needed
  st_drop_geometry() 

# check for missing
table(ain_lead$hi_lead_flag,useNA='always')

ain_lead_missing <- ain_lead %>% filter(is.na(hi_lead_flag))

# parcels %>% filter(ain_2026_04 %in% ain_lead_missing$ain_2026_04) %>% mapview() + mapview(lead)
#untested grids

# clean up table and for parcels missing from grids add flag for not assessed
curr_lead <- ain_lead %>%
  select(ain_2026_04, grid_name, lead_geometric_mean, hi_lead_flag) %>%
  mutate(hi_lead_label=case_when(
    hi_lead_flag==TRUE ~ "High Lead Grid",
    hi_lead_flag==FALSE ~ "Not High Lead Grid",
    TRUE ~ "Grid Not Tested"
  ))

table(curr_lead$hi_lead_label,useNA='always')

nrow(curr_lead) - nrow(parcels) # same count

#### STEP 4: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_lead_", year, "_", month)
schema <- "dashboard"
indicator <- paste0("Relational table of Lead Grids and Testing in either West or East Altadena proper as of MONTH:", month, " YEAR:", year)
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_lead.R "
qa_filepath<-"  QA_sheet_rel_tables_update_2026_04.docx "

# dbWriteTable(con_alt, Id(schema, table_label), curr_lead,
#              overwrite = FALSE, row.names = FALSE)


# Add metadata
column_names <- colnames(curr_lead) # Get column names

column_comments <- c(
                     'Assessor ID number for current month - use this to match to other relational tables',
                     'lead testing grid',
                     'lead geometric mean for testing grid, NA if not tested',
                     'True-false for if testing grid mead was above 80, NA if not tested',
                     'Label for if area is in high lead area or not, or if not tested')

# add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 5: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)