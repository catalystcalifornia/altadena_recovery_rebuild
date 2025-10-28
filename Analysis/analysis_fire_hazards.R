## Analyze the number of damaged properties in fire zones by area
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_analysis_zoning_fire_zones.docx

#### Step 0: Set Up ####
#load libraries
library(dplyr)
library(purrr)
library(stringr)
library(RPostgres)
library(mapview)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)


#### Step 1: Pull in relational datasets ####
housing_sept <- st_read(con, query="SELECT * FROM data.rel_assessor_residential_sept2025 WHERE residential = 'true'") 

damage <- st_read(con, query="SELECT * FROM data.rel_assessor_damage_level_sept2025")

fhsz_local <- st_read(con, query = "Select * FROM data.local_fhsz_3310", geom="geom") %>%
  filter(fhsz_descr!="NonWildland")

fhsz_state <- st_read(con, query = "Select * FROM data.state_fhsz_3310", geom="geom")

# parcel shapes for september
parcels_sept <-  st_read(con, query="SELECT * FROM data.rel_assessor_altadena_parcels_sept2025", geom="geom")

#### Step 2: Join parcel data to local fire hazard zones data ####
st_crs(fhsz_local)
st_crs(parcels_sept)
# right projections

# select residential parcels only
parcels_sept <- parcels_sept %>%
  filter(ain_sept %in% housing_sept$ain_sept)

parcels_local_fhsz<- st_intersection(parcels_sept,fhsz_local)

# #check
# mapview(parcels_local_fhsz) # looks good
# nrow(parcels_local_fhsz)
# length(unique(parcels_local_fhsz$ain_sept))
# duplicates, keep the highest hazard

parcels_local_fhsz_dedup <-parcels_local_fhsz %>%
  st_drop_geometry() %>%
  group_by(area_name,ain_sept) %>%
  summarize(count=n(),
    local_fhsz_list=paste(fhsz_descr, collapse = ", ")) %>%
 ungroup()


parcels_local_fhsz_dedup <-  parcels_local_fhsz_dedup %>%
  mutate(local_fhsz=
           case_when(grepl("Very High",local_fhsz_list) ~ "Very High",
                     grepl("High",local_fhsz_list) ~ "High",
                     grepl("Moderate",local_fhsz_list) ~ "Moderate",
                     TRUE ~NA))

check <- parcels_local_fhsz_dedup %>%
  group_by(local_fhsz, local_fhsz_list) %>%
  summarise(count=n())


#### Step 3: Join parcel data to state fire hazard zones data ####
st_crs(fhsz_state)
st_crs(parcels_sept)
# right projections

parcels_state_fhsz<- st_intersection(parcels_sept,fhsz_state)

# #check
# mapview(parcels_state_fhsz) # looks good
nrow(parcels_state_fhsz)
length(unique(parcels_state_fhsz$ain_sept))
# no duplicates
table(parcels_state_fhsz$fhsz_descr,useNA='always')

parcels_state_fhsz <- parcels_state_fhsz %>%
  st_drop_geometry() %>%
  select(area_name,ain_sept,fhsz_descr) %>%
  rename(state_fhsz=fhsz_descr)

#### Step 4: Join together the local and state fhsz joins ####
parcels_fhsz <- parcels_local_fhsz_dedup %>% select(-count) %>% ungroup() %>%
  full_join(parcels_state_fhsz, by=c("ain_sept","area_name"))

# check
table(parcels_fhsz$local_fhsz,useNA='always')
table(parcels_local_fhsz_dedup$local_fhsz,useNA='always')

table(parcels_fhsz$state_fhsz,useNA='always')
table(parcels_state_fhsz$state_fhsz,useNA='always')

nrow(parcels_fhsz)
length(unique(parcels_fhsz$ain_sept))
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

View(parcels_fhsz)


#### Step 5: Clean fire hazard join and push to postgres as relational table ####
# add missing parcels that didn't match to any fire hazard
# create a dataframe with no hazard for the parcels that didn't match to a hazard zone
parcels_fhsz_none <- parcels_sept %>%
  filter(!ain_sept %in% parcels_fhsz$ain_sept) %>%
  st_drop_geometry() %>%
  select(area_name, ain_sept) %>%
  mutate(local_fhsz_list=NA,
         local_fhsz="None",
         state_fhsz="None",
         combined_fhsz="None")

# combine dataframes together
parcels_fhsz_final <- rbind(parcels_fhsz_none,parcels_fhsz)

# Upload table to postgres as relational table and add table/column comments
# dbWriteTable(con, name = "rel_assessor_fhsz_sept2025", value = parcels_fhsz_final, overwrite = FALSE)
# schema <- "data"
# table_name <- "rel_assessor_fhsz_sept2025"
# indicator <- "Relational table of september residential parcels that include the local and state responsibility fire hazard zones. If the parcels is not in a fire hazard zone then the category None is used"
# source <- "Source: LA County Assessor Data, September 2025 & Local & state fire hazard zones from Cal office of fire marshall"
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_zones.docx"
# column_names <- colnames(parcels_fhsz_final) # Get column names
# column_names
# column_comments <- c(
#   "area name",
#   "ain september 2025",
#   "list of local fire hazard zones the parcel fell within",
#   "local fire hazard zone - top coded based on most severe zone--uses this or combined_fhsz",
#   "main local fire hazard zone - top coded for most severe zone parcel falls within",
#   "state fire hazard zone",
#   "combined category of fire hazard zone accounting for state and local zones")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


### Step 6: Calculate the fire hazard zones prevalence by damage level for west and east altadena #####
# what percentage of properties are within the fire hazard zones by damage level
# bring in relational table
all_df <- st_read(con, query="SELECT * FROM data.rel_assessor_fhsz_sept2025") %>%
  left_join(damage)

analysis_fhsz_e_w <- all_df %>%
  group_by(area_name,damage_category) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,damage_category,combined_fhsz) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_fhsz_alt <- all_df %>%
  mutate(area_name='Altadena') %>%
  group_by(area_name,damage_category) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,damage_category,combined_fhsz) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_fhsz <- rbind(analysis_fhsz_e_w,
                             analysis_fhsz_alt) %>%
  rename(area_damage_total=total,
         fhsz_count=count,
         fhsz_prc=prc) %>%
  ungroup()

check <- analysis_fhsz %>%
  group_by(area_name,damage_category) %>%
  summarise(sum=sum(fhsz_prc))
# checks out

# Upload tables to postgres and add table/column comments
dbWriteTable(con, name = "analysis_damage_fhsz_sept2025", value = analysis_fhsz, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_damage_fhsz_sept2025"
indicator <- "Analysis of the percentage of properties in West and East Altadena in fire hazard areas by damage category, e.g., the % of significantly damaged properties in very high fire hazard areas in West Altadena"
source <- "Source: LA County Assessor Data, September 2025 & California Office of Fire Marshall state and local responsibility areas"
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_zones.docx"
column_names <- colnames(analysis_fhsz) # Get column names
column_names
column_comments <- c(
  "area name",
  "damage category",
  "combined local and state fire hazard zone - top coded",
  "count of properties in the zone within the area-damage category - numerator",
  "percent of properties in the zone out of all properties in the area-damage category",
  "total properties in the combined area-damage category - denominator")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 7: close connection ####
dbDisconnect(con)
