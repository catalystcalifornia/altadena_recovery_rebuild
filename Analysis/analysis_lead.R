## Producing Analysis Tables on Parcels with High Lead Levels
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_analysis_lead.docx

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
parcels <- st_read(con, query="SELECT * FROM data.rel_assessor_altadena_parcels_sept2025", geom = "geom") 

hi_lead <- st_read(con, query="SELECT * FROM data.lacdph_lead_results_grid_2025", geom = "geom") %>% 
  filter(lead_geometric_mean > 80) 

lead <- st_read(con, query="SELECT * FROM data.lacdph_lead_results_grid_2025", geom = "geom") %>% 
  mutate(hi_lead_flag = ifelse(lead_geometric_mean > 80, TRUE, FALSE)) 

#### Step 2: intersect and compute which grid has the majority of the parcel in it, assign to that one ####
st_crs(lead)
st_crs(parcels)
# mapview(lead) + mapview(parcels)

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
                            st_drop_geometry() %>%
                            select(-c(gid, gid.1))

# clean up table and for parcels missing from grids add flag for not assessed
ain_lead_all <- ain_lead %>%
  select(ain_sept, area_name, area_label, grid_name, lead_geometric_mean, hi_lead_flag) %>%
  mutate(hi_lead_label=case_when(
    hi_lead_flag==TRUE ~ "High Lead Grid",
    TRUE ~ "Not High Lead Grid"
  ))

ain_lead_missing <- parcels %>%
  filter(!ain_sept %in% ain_lead_all$ain_sept) %>%
  select(ain_sept, area_name, area_label) %>%
  mutate(grid_name=NA,
         lead_geometric_mean=NA,
         hi_lead_flag=NA,
         hi_lead_label="Not Tested") %>%
  st_drop_geometry()

ain_lead_all <- rbind(ain_lead_missing, ain_lead_all)
nrow(ain_lead_all)
nrow(parcels)
length(unique(ain_lead_all$ain_sept))
duplicate_rows <- ain_lead_all[(duplicated(ain_lead_all$ain_sept, fromLast = FALSE) | duplicated(ain_lead_all$ain_sept, fromLast = TRUE)), ]
# keep one

ain_lead_all <- distinct(ain_lead_all)
table(ain_lead_all$hi_lead_label,useNA='always')

ain_lead_final_df <- ain_lead_all %>%
  select(-area_name, -area_label)

# export to postgres
# dbWriteTable(con, name = "rel_assessor_lead_sept2025", value = ain_lead_final_df, overwrite = FALSE)
# schema <- "data"
# table_name <- "rel_assessor_lead_sept2025"
# indicator <- "September 2025 assessor parcels matched to high lead grids. Includes all parcels not just residential. Includes flag for if lead geometric mean in test grid was over the recommended lifetime exposure of 80"
# source <- "Multiple Data Sources"
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_lead.docx"
# column_names <- colnames(ain_lead_final_df) # Get column names
# column_names
# column_comments <- c(
#   "AIN for september",
#   "lead testing grid",
#   "lead geometric mean for testing grid, NA if not tested",
#   "True-false for if testing grid mead was above 80, NA if not tested",
#   "Label for if area is in high lead area or not, or if not tested"
# )
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#### Step 3: combine with damage and residential type database ####
damage_df <- st_read(con, query="SELECT * FROM data.rel_assessor_damage_level_sept2025")
  
restype_df <- st_read(con, query="SELECT * FROM data.rel_assessor_residential_sept2025")%>%
  filter(residential==TRUE) # just keep residential properties

lead_damage_df_all <- ain_lead_all  %>%
  left_join(damage_df, by = "ain_sept")

# final df just for residential properties
lead_damage_restype_df_all <- lead_damage_df_all %>%
  right_join(restype_df, by = "ain_sept")

# check
nrow(restype_df)
nrow(lead_damage_restype_df_all)
length(unique(lead_damage_restype_df_all$ain_sept))
# looks good

table(lead_damage_restype_df_all$hi_lead_label,useNA='always')

#### Step 4: calculate by Altadena, West, East for res_type and for owner_renter ####
## Properties with high lead levels by residential type and damage level first
final_restype_df_e_w <- lead_damage_restype_df_all %>% 
  group_by(area_name,res_type, damage_category) %>% 
  mutate(total=n()) %>% # calculate total for denominator of analysis--e.g., single family homes in West Altadena that were significantly damaged
  ungroup() %>%
  group_by(area_name,res_type, damage_category, hi_lead_label) %>%
  summarise(count=n(), # calculate numerator
            prc=count/min(total)*100, # calculate %
            total=min(total)) # carry through the total
            
final_restype_df_alt<- lead_damage_restype_df_all %>% 
  mutate(area_name="Altadena") %>% # dummy for all of altadena to repeat for all of altadena
  group_by(area_name,res_type, damage_category) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,res_type, damage_category, hi_lead_label) %>%
  summarise(count=n(),
            prc=count/min(total)*100,
            total=min(total))

final_restype_df <- rbind(final_restype_df_e_w, final_restype_df_alt)
# check
check <- final_restype_df %>%
  group_by(area_name,res_type, damage_category) %>%
  summarise(sum=sum(prc))
# looks good adds to 100%

## Properties with high lead levels by owner type and damage level first
final_owner_df_e_w <- lead_damage_restype_df_all %>% 
  group_by(area_name,owner_renter, damage_category) %>% 
  mutate(total=n()) %>% # calculate total for denominator of analysis--e.g., owner-occupied homes in West Altadena that were significantly damaged
  ungroup() %>%
  group_by(area_name,owner_renter, damage_category, hi_lead_label) %>%
  summarise(count=n(), # calculate numerator
            prc=count/min(total)*100, # calculate %
            total=min(total)) # carry through the total

final_owner_df_alt<- lead_damage_restype_df_all %>% 
  mutate(area_name="Altadena") %>% # dummy for all of altadena to repeat for all of altadena
  group_by(area_name,owner_renter, damage_category) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,owner_renter, damage_category, hi_lead_label) %>%
  summarise(count=n(),
            prc=count/min(total)*100,
            total=min(total))

final_owner_df <- rbind(final_owner_df_e_w, final_owner_df_alt)
# check
check <- final_owner_df %>%
  group_by(area_name,owner_renter, damage_category) %>%
  summarise(sum=sum(prc))
# looks good adds to 100%

#### Step 5: upload tables to postgres ####
# dbWriteTable(con, name = "analysis_restype_damage_lead_sept2025", value = final_restype_df, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_restype_damage_lead_sept2025"
# indicator <- "Data on residential property types (single-family, multifamily, etc.) for all of Altadena, West Altadena, East Altadena by damage type and high lead levels (e.g., in areas at or above the lead safety threshold of >=80mg/kg by damage level) - what % of significantly damaged residential properties are in high lead level areas in West Altadena."
# source <- "Multiple Data Sources"
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_lead.docx"
# column_names <- colnames(final_restype_df) # Get column names
# column_names
# column_comments <- c(
#   "area name -  Altadena, East/West of Lake Avenue",
#   "type of residence",
#   "type of damage",
#   "high lead level >=80 or not, could be in an area not tested - numerator",
#   "percent of properties in that res type and damage level in a high lead area or not for that area of Altadena",
#   "total properties in that res type and damage level combo for that area of Altadena-- denominator"
#   )
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

# owner renter table next
# dbWriteTable(con, name = "analysis_owner_renter_damage_lead_sept2025", value = final_owner_df, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_owner_renter_damage_lead_sept2025"
# indicator <- "Data on ownership types for all of Altadena, West Altadena, East Altadena by damage type and high lead levels (e.g., in areas at or above the lead safety threshold of >=80mg/kg by damage level) - what % of significantly damaged owner-occupied properties are in high lead level areas in West Altadena."
# source <- "Multiple Data Sources"
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_lead.docx"
# column_names <- colnames(final_owner_df) # Get column names
# column_names
# column_comments <- c(
#   "area name -  Altadena, East/West of Lake Avenue",
#   "ownership type",
#   "type of damage",
#   "high lead level >=80 or not, could be in an area not tested - numerator",
#   "percent of properties in that res type and damage level in a high lead area or not for that area of Altadena",
#   "total properties in that res type and damage level combo for that area of Altadena-- denominator"
#   )
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#### Step 6: close database connection ####
dbDisconnect(con)
