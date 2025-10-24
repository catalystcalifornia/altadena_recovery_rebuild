## Producing Analysis Tables on Parcels with High Lead Levels
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_analysis_lead.docx

#### Step 0: Set Up ####
#load libraries
library(dplyr)
library(purrr)
library(stringr)
library(RPostgres)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)


#### Step 1: Pull in relational datasets ####
parcels <- st_read(con, query="SELECT * FROM data.rel_assessor_altadena_parcels_jan2025", geom = "geom") 

hi_lead <- st_read(con, query="SELECT * FROM data.lacdph_lead_results_grid_2025", geom = "geom") %>% 
  filter(lead_geometric_mean > 80) 

#### Step 2: intersect and compute which grid has the majority of the parcel in it, assign to that one ####

ain_hi_lead <- st_intersection(parcels %>%
                          mutate(ain_id = row_number()),
                          hi_lead %>% 
                          mutate(grid_id = row_number())
                          ) %>% 
                          #calculate overlap
                          mutate(overlap_area = st_area(.)) %>%
                          #only keep more of the parcel in the lead level grid 
                          group_by(ain_id) %>%
                          slice_max(overlap_area, n = 1, with_ties = FALSE) %>%
                          ungroup() %>%
                          #deleting columns that are not needed
                          st_drop_geometry() %>%
                          select(-c(gid, gid.1))

#### Step 3: combine with damage and residential type database ####
damage_df <- st_read(con, query="SELECT * FROM data.rel_assessor_damage_level")
  
restype_df <- st_read(con, query="SELECT * FROM rel_assessor_residential_jan2025")

lead_damage_df <- ain_hi_lead %>%
  left_join(damage_df, by = "ain")

lead_damage_restype_df <- lead_damage_df %>%
  left_join(restype_df, by = "ain")

#### Step 4: calculate by Altadena, West, East for res_type and for owner_renter ####
final_restype_df <- lead_damage_restype_df %>% 
  group_by(res_type, damage_category) %>% 
  summarise(altadena_count = n(),
            west_count = sum(area_name == "West", na.rm = TRUE),
            east_count = sum(area_name == "East", na.rm = TRUE)) %>% 
  mutate(altadena_prc = altadena_count/sum(altadena_count)*100,
         west_prc = west_count/sum(west_count)*100,
         east_prc = east_count/sum(east_count)*100) %>%
  #cleaning 
  filter(res_type != 'Boarding house') %>%
  pivot_longer(
    cols = -c(damage_category, res_type),
    names_to = c("area", ".value"),
    names_pattern = "(altadena|west|east)_(count|prc)" 
  )

final_homeowner_df <- lead_damage_restype_df %>% 
  group_by(owner_renter, damage_category) %>% 
  summarise(altadena_count = n(),
            west_count = sum(area_name == "West", na.rm = TRUE),
            east_count = sum(area_name == "East", na.rm = TRUE)) %>% 
  mutate(altadena_prc = altadena_count/sum(altadena_count)*100,
         west_prc = west_count/sum(west_count)*100,
         east_prc = east_count/sum(east_count)*100) %>%
  #cleaning 
  pivot_longer(
    cols = -c(damage_category, owner_renter),
    names_to = c("area", ".value"),
    names_pattern = "(altadena|west|east)_(count|prc)" 
  )

#### Step 5: upload table to postgres ####
dbWriteTable(con, name = "analysis_restype_lead_damage", value = final_restype_df, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_restype_lead_damage"
indicator <- "Data on distribution of residential property types (single-family, multifamily, etc.) for all of Altadena, West Altadena, East Altadena by damage type that are in areas at or above the lead safety threshold of >=80mg/kg by damage level."
source <- "Multiple Data Sources"
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_lead.docx"
column_names <- colnames(final_restype_df) # Get column names
column_comments <- c(
  "type of residence",
  "type of damage",
  "Altadena, East/West of Lake Avenue",
  "count",
  "percent"
  )
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

dbWriteTable(con, name = "analysis_owner_renter_lead_damage", value = final_homeowner_df, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_owner_renter_lead_damage"
indicator <- "Data on distribution of residential ownership (homeowner, renter, etc.) for all of Altadena, West Altadena, East Altadena by damage type that are in areas at or above the lead safety threshold of >=80mg/kg by damage level."
source <- "Multiple Data Sources"
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_lead.docx"
column_names <- colnames(final_homeowner_df) # Get column names
column_comments <- c(
  "home owner or renter",
  "type of damage",
  "Altadena, East/West of Lake Avenue",
  "count",
  "percent"
)
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#### Step 6: close database connection ####
dbDisconnect(con)