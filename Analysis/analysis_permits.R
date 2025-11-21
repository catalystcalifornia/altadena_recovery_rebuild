## Producing tables for characteristics of properties that have started permitting
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_analysis_permits.docx

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
housing_jan <- st_read(con, query="SELECT * FROM data.rel_assessor_residential_jan2025 WHERE residential = 'true'") 

damage <- st_read(con, query="SELECT * FROM data.rel_assessor_damage_level") 

permits <- st_read(con, query="SELECT ain, rebuild_status FROM data.rel_parcel_rebuild_status_2025_10")

altadena_e_w <- st_read(con, query="SELECT ain, area_name, area_label FROM data.rel_assessor_altadena_parcels_jan2025") #dropping geom since it's not needed for analysis

#### Step 2: Explore permit data -----
duplicate_rows <- permits[(duplicated(permits$ain, fromLast = FALSE) | duplicated(permits$ain, fromLast = TRUE)), ]
# no duplicates

# select just the residential parcels
permits_res <- permits %>%
  filter(ain %in% housing_jan$ain)

nrow(permits_res)
length(unique(permits_res$ain))
# all unique

table(permits_res$rebuild_status,useNA='always')

# join data together
all_df <- housing_jan %>% # want the parcels from january
  left_join(altadena_e_w, by="ain") %>%
  left_join(damage, by="ain") %>%
  left_join(permits_res, by="ain")

# check
table(all_df$damage_category,useNA='always')
table(all_df$res_type,useNA='always')
table(all_df$rebuild_status,useNA='always') # no longer any NAs
nrow(all_df)
length(unique(all_df$ain))

# check those that have debris removal completed
check <- all_df %>%
  group_by(rebuild_status,damage_category) %>%
  summarise(count=n())
# 38 properties have debris removal completed but have no damage
# fine to keep as is, we control for damage level later

all_df <- all_df 
         
#### Step 3: First ANALYSIS- [analysis_permits_area_jan2025] ####
## What percentage of residential properties for each damage are in each permit stage by area
# e.g., x% of significantly damaged properties that are in construction are in west altadena
analysis_permits_area_e_w <- all_df %>%
  group_by(damage_category,rebuild_status,) %>%
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(damage_category,rebuild_status,area_name) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

## compare to what % of properties in each area are in each damage category, e.g., x% of significantly damaged properties are in west alt.
analysis_damage_area <- all_df %>% 
  group_by(damage_category) %>%
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(damage_category,area_name) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total)) 

analysis_permits_final <- analysis_permits_area_e_w

# # Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_permits_area_jan2025", value = analysis_permits_final, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_permits_area_jan2025"
# indicator <- "Distribution of residential properties by area in each damage category, e.g., x% of significantly damaged properties that are in construction are in west altadena
# Include all damage categories but most important to pay attention to significantly damaged"
# source <- "Source: LA County Assessor Data, January 2025 & Scraped data."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_permits.docx"
# column_names <- colnames(analysis_permits_final) # Get column names
# column_names
# column_comments <- c(
#   "damage category",
#   "permit status",
#   "area",
#   "count of properties in that damage category, permit status, and area - numerator",
#   "percent of properties in that area out of properties all properties in each damage and permit combo",
#   "total residential properties that are within that damage category and permit status - denominator")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


# # Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_damage_area_jan2025", value = analysis_damage_area, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_damage_area_jan2025"
# indicator <- "Distribution of permitting stages within each damage category by area in Altadena, e.g., x% of significantly damaged properties are in west alt. Can be used to compare sales or permit distributions"
# source <- "Source: LA County Assessor Data, January 2025 & CalFire data."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_permits.docx"
# column_names <- colnames(analysis_damage_area) # Get column names
# column_names
# column_comments <- c(
#   "damage category",
#   "area",
#   "count of properties in that damage category and area - numerator",
#   "percent of properties in that area out of all properties in each damage category ",
#   "total residential properties that are within that damage category  - denominator")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 4: Second ANALYSIS- [analysis_permits_damage_jan2025] ####
## what % of significantly damaged properties have started permits?
analysis_damage_e_w <- all_df %>% 
  group_by(area_name,damage_category) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,damage_category,rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_damage_alt <- all_df %>% 
  mutate(area_name="Altadena") %>%
  group_by(area_name,damage_category) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,damage_category,rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_damage_final<- rbind(analysis_damage_e_w,
                               analysis_damage_alt)


# # Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_permits_damage_jan2025", value = analysis_damage_final, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_permits_damage_jan2025"
# indicator <- "Distribution of permitting stages by damage category and area in Altadena, e.g., what % of significantly damaged residential properties in West Altadena are awaiting construction
# Most important to pay attention to the significantly damaged category. Permit stages dont necessarily apply to no damage or some damage properties, but some have pursued permits regardless--some damage might need permits to repair minor structures or repairs"
# source <- "Source: LA County Assessor Data, January 2025 & Scraped data."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_permits.docx"
# column_names <- colnames(analysis_damage_final) # Get column names
# column_names
# column_comments <- c(
#   "area",
#   "damage category",
#   "permit status",
#   "count of properties - numerator",
#   "percent of properties ",
#   "total residential properties in the area - denominator")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 5: Third ANALYSIS- [analysis_permits_restype_jan2025] ####
# At what rate are properties rebuilding by residential type for significantly damaged properties
all_df_damaged <- all_df %>% filter(damage_category=="Significant Damage")

e_w <- all_df_damaged %>% 
  group_by(area_name,res_type) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,res_type,rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

alt<- all_df_damaged %>% 
  mutate(area_name="Altadena") %>%
  group_by(area_name,res_type) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,res_type,rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_permits_restype <- rbind(e_w,
                                alt) 

# # Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_permits_restype_jan2025", value = analysis_permits_restype, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_permits_restype_jan2025"
# indicator <- "Distribution of permitting stages by residential type and area in Altadena for significantly damaged residential properties only, e.g., what % of significantly damaged single-family properties in West Altadena are awaiting construction"
# source <- "Source: LA County Assessor Data, January 2025 & Scraped data."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_permits.docx"
# column_names <- colnames(analysis_permits_restype) # Get column names
# column_names
# column_comments <- c(
#   "area",
#   "residential type",
#   "permit status",
#   "count of properties - numerator",
#   "percent of properties ",
#   "total residential properties in the area that were significantly damaged - denominator")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 6: Fourth ANALYSIS- [analysis_permits_owner_renter_jan2025] ####
# At what rate are properties rebuilding by ownership type for significantly damaged properties
e_w <- all_df_damaged %>% 
  group_by(area_name,owner_renter) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,owner_renter,rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

alt<- all_df_damaged %>% 
  mutate(area_name="Altadena") %>%
  group_by(area_name,owner_renter) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,owner_renter,rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_permits_owner <- rbind(e_w,
                                  alt) 

# # Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_permits_owner_renter_jan2025", value = analysis_permits_owner, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_permits_owner_renter_jan2025"
# indicator <- "Distribution of permitting stages by ownership type and area in Altadena for significantly damaged residential properties only, e.g., what % of significantly damaged owner-occupied properties in West Altadena are awaiting construction"
# source <- "Source: LA County Assessor Data, January 2025 & Scraped data."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_permits.docx"
# column_names <- colnames(analysis_permits_owner) # Get column names
# column_names
# column_comments <- c(
#   "area",
#   "ownership type",
#   "permit status",
#   "count of properties - numerator",
#   "percent of properties ",
#   "total residential properties in the area that were significantly damaged - denominator")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#### Step 8: close connection ####
dbDisconnect(con)
