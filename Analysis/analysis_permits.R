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
table(all_df$rebuild_status,useNA='always') # no longer NAs
nrow(all_df)
length(unique(all_df$ain))

missing_permit <- all_df %>%
  filter(is.na(rebuild_status))
# sent these AINs to HK for investigation - HK: no longer NA, updated permit type methods 

all_df <- all_df %>%
  mutate(rebuild_status=case_when(
    is.na(rebuild_status) ~ "Need more info",
    rebuild_status=="Debris Removal Completed"  ~ NA,
    rebuild_status== "Debris Removal Not Applicable" ~ NA,
    .default=rebuild_status))
         
#### Step 3: First ANALYSIS- [analysis_permits_area_sept2025] ####
## What percentage of residential properties have started permits by area
analysis_permits_area_e_w <- all_df %>%
  group_by(area_name) %>%
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name, rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

# HK: Note for above - All "Debris Removal Complete" (n=38) are parcels with no damage
# With the majority occurring in West Altadena for "Assessor Parcel Outside of Fire Area"
# should filter this status out? Additionally "Debris Removal Not Applicable" is 
# only applied to No Damage parcels, may want to exclude as well?

analysis_permits_area_alt <- all_df %>% 
  mutate(area_name="Altadena") %>%
  group_by(area_name) %>%
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name, rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_permits_final <- rbind(analysis_permits_area_e_w, analysis_permits_area_alt)

# # Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_permits_area_jan2025", value = analysis_permits_final, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_permits_area_jan2025"
# indicator <- "Distribution of permitting stages by area in Altadena, e.g., what % of residential properties in West Altadena are awaiting construction"
# source <- "Source: LA County Assessor Data, January 2025 & Scraped data."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_permits.docx"
# column_names <- colnames(analysis_permits_final) # Get column names
# column_names
# column_comments <- c(
#   "area",
#   "permit status",
#   "count of properties",
#   "percent of properties ",
#   "total residential properties in the area")
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
# indicator <- "Distribution of permitting stages by damage category and area in Altadena, e.g., what % of significantly damaged residential properties in West Altadena are awaiting construction"
# source <- "Source: LA County Assessor Data, January 2025 & Scraped data."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_permits.docx"
# column_names <- colnames(analysis_damage_final) # Get column names
# column_names
# column_comments <- c(
#   "area",
#   "damage category",
#   "permit status",
#   "count of properties",
#   "percent of properties ",
#   "total residential properties in the area")
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
#   "count of properties",
#   "percent of properties ",
#   "total residential properties in the area that were significantly damaged")
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
#   "count of properties",
#   "percent of properties ",
#   "total residential properties in the area that were significantly damaged")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#### Step 8: close connection ####
dbDisconnect(con)
