## PURPOSE: The purpose of this script is to produce the rel_assessor_fhsz table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_tables_update_2026_04.docx ##
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

year <- "2026"
month <- "04"

#### STEP 2: PULL current data and fhsz (Update to latest data and xwalks) ####
# current month parcels
parcels <- st_read(con_alt, query="SELECT ain_2026_04,geom FROM dashboard.rel_assessor_parcels_2026_04") %>%
  rename(ain=ain_2026_04) # rename ain for code simplicity

# get fire hazard zones
fhsz_local <- st_read(con_alt, query = "Select * FROM data.local_fhsz_3310", geom="geom") %>%
  filter(fhsz_descr!="NonWildland")

fhsz_state <- st_read(con_alt, query = "Select * FROM data.state_fhsz_3310", geom="geom")

#### Step 3: Join parcel data to local fire hazard zones data ####
st_crs(fhsz_local)
st_crs(parcels)
# right projections

parcels_local_fhsz<- st_intersection(parcels,fhsz_local)

# #check
# mapview(parcels_local_fhsz) # looks good
# nrow(parcels_local_fhsz)
# length(unique(parcels_local_fhsz$ain))
# duplicates, keep the highest hazard

parcels_local_fhsz_dedup <-parcels_local_fhsz %>%
  st_drop_geometry() %>%
  group_by(ain) %>%
  summarize(count=n(),
            local_fhsz_list=paste(fhsz_descr, collapse = ", ")) %>%
  ungroup()

#check which are duplicates and if they've been assigned correctly compared to parcels_local_fhsz_dedup
# dupes <- parcels_local_fhsz[duplicated(parcels_local_fhsz$ain) | duplicated(parcels_local_fhsz$ain, fromLast = TRUE), ]
# View(dupes[order(dupes$ain), ])


parcels_local_fhsz_dedup <-  parcels_local_fhsz_dedup %>%
  mutate(local_fhsz=
           case_when(grepl("Very High",local_fhsz_list) ~ "Very High",
                     grepl("High",local_fhsz_list) ~ "High",
                     grepl("Moderate",local_fhsz_list) ~ "Moderate",
                     TRUE ~NA))

check <- parcels_local_fhsz_dedup %>%
  group_by(local_fhsz, local_fhsz_list) %>%
  summarise(count=n())

#### Step 4: Join parcel data to state fire hazard zones data ####
st_crs(fhsz_state)
st_crs(parcels)
# right projections

parcels_state_fhsz<- st_intersection(parcels,fhsz_state)

# #check
# mapview(parcels_state_fhsz) # looks good
# nrow(parcels_state_fhsz)
# length(unique(parcels_state_fhsz$ain))
# no duplicates
table(parcels_state_fhsz$fhsz_descr,useNA='always')

parcels_state_fhsz <- parcels_state_fhsz %>%
  st_drop_geometry() %>%
  select(ain,fhsz_descr) %>%
  rename(state_fhsz=fhsz_descr)

#### Step 5: Join together the local and state fhsz joins ####
parcels_fhsz <- parcels_local_fhsz_dedup %>% select(-count) %>% ungroup() %>%
  full_join(parcels_state_fhsz, by=c("ain"))

# check
table(parcels_fhsz$local_fhsz,useNA='always')
table(parcels_local_fhsz_dedup$local_fhsz,useNA='always')

table(parcels_fhsz$state_fhsz,useNA='always')
table(parcels_state_fhsz$state_fhsz,useNA='always')

nrow(parcels_fhsz) - length(unique(parcels_fhsz$ain))
# no duplicates

View(parcels_fhsz)

# create a unified column
check <- parcels_fhsz %>%
  group_by(local_fhsz, state_fhsz) %>%
  summarise(count=n())

parcels_fhsz <- parcels_fhsz %>%
  mutate(combined_fhsz=
           case_when(local_fhsz=="Very High" & state_fhsz=="Very High" ~ "Very High",
                     is.na(local_fhsz) & state_fhsz=="Very High" ~ "Very High",
                     !is.na(local_fhsz) & is.na(state_fhsz) ~ local_fhsz,
                     TRUE ~ NA))

# check
View(parcels_fhsz)
xtabs(~ local_fhsz + state_fhsz + combined_fhsz, data = parcels_fhsz, na.action=na.pass, addNA=TRUE)

#### Step 6: Clean fire hazard join and push to postgres as relational table ####
# add missing parcels that didn't match to any fire hazard
# create a dataframe with no hazard for the parcels that didn't match to a hazard zone
parcels_fhsz_none <- parcels %>%
  filter(!ain %in% parcels_fhsz$ain) %>%
  st_drop_geometry() %>%
  select(ain) %>%
  mutate(local_fhsz_list=NA,
         local_fhsz="None",
         state_fhsz="None",
         combined_fhsz="None")

# combine dataframes together
curr_fhsz  <- rbind(parcels_fhsz_none,parcels_fhsz)

# check for duplicates and gaps
curr_fhsz %>%
  group_by(ain) %>%
  mutate(count=n()) %>%
  filter(count > 1) %>%
  nrow() # should be 0

nrow(curr_fhsz) - length(unique(curr_fhsz$ain)) # should be 0

nrow(curr_fhsz) - length(unique(parcels$ain)) # should be 0

#### STEP 7: Final column authority level ####
# select fhsz for current month via xwalk
curr_fhsz <-curr_fhsz %>%
  # add authority column
  mutate(authority=case_when(state_fhsz=="Very High" & (local_fhsz=="None" | is.na(local_fhsz)) ~ "State",
                             state_fhsz=="Very High" & (local_fhsz!="None" | !is.na(local_fhsz)) ~ "State & Local",
                             (state_fhsz=="None" | is.na(state_fhsz)) & local_fhsz!="None" ~ "Local",
                             TRUE ~ "None"))

xtabs(~ local_fhsz + state_fhsz + authority, data = curr_fhsz , na.action=na.pass, addNA=TRUE)


#### STEP 8: PUSH TO PGADMIN  ####
curr_fhsz <- curr_fhsz %>%
  rename(ain_2026_04=ain)

# final check
table(curr_fhsz$combined_fhsz,useNA='always')
table(curr_fhsz$authority,useNA='always')

# QA that tables add up to same number so 1 authority identified for 1 fhsz
# cat(paste0("Table 1 is summed at [", sum(table(curr_fhsz$combined_fhsz,useNA='always')), "] and Table 2 is summed at [", sum(table(curr_fhsz$authority,useNA='always')), "] and they should match!"))

# Export to postgres
table_label <- paste0("rel_assessor_fhsz_", year, "_", month)
schema <- "dashboard"
indicator <- paste0("Relational table of Fire Hazard State Zone in either West or East Altadena proper as of MONTH:", month, " YEAR:", year)
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_fhsz.R "
qa_filepath<-" QA_sheet_rel_tables_update_2026_04.docx "

# dbWriteTable(con_alt, Id(schema, table_label), curr_fhsz,
#              overwrite = FALSE, row.names = FALSE)

# Add metadata
column_names <- colnames(curr_fhsz) # Get column names

column_comments <- c(
                     'Assessor ID number for current month - use this to match to other relational tables',
                     'list of local fire hazard zones the parcel fell within',
                     'local fire hazard zone - top coded based on most severe zone--uses this or combined_fhsz',
                     'state fire hazard zone',
                     'main local fire hazard zone - top coded for most severe zone parcel falls within, this is the category to use',
                     'local or state authority identifier')

# add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 9: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)

