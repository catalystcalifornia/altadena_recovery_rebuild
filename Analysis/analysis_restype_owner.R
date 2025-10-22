## Producing Analysis Tables for Residential Properties in Altadena before and after the Eaton Fire 
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_analysis_housing.docx

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

housing_damage <- st_read(con, query="SELECT * FROM data.rel_assessor_damage_level") 

altadena_e_w <- st_read(con, query="SELECT ain, area_name, area_label FROM data.rel_assessor_altadena_parcels_jan2025") #dropping geom since it's not needed for analysis

#### Step 2: combine all three data frames and clean up ####

##first combine housing_jan and altadena_e_w
housing_jan_e_w <- housing_jan  %>%
  left_join(altadena_e_w, by= "ain")
# View(head(housing_jan_e_w))

##second combine with housing_damage
all_df <- housing_jan_e_w  %>%
  left_join(housing_damage, by= "ain")
# View(head(all_df))

#### Step 3: FIRST ANALYSIS- [analysis_restype_jan2025] ####
# Distribution of residential property types (single-family, multifamily, etc.) for all of Altadena, West Altadena, East Altadena in Jan 2025 (all properties whether or not destroyed), e.g., in jan 2025, X% of residential properties in Altadena were single family homes
analysis_restype_jan2025 <- all_df %>% 
  group_by(res_type) %>% 
  summarise(altadena_count = n(),
            west_count = sum(area_name == "West", na.rm = TRUE),
            east_count = sum(area_name == "East", na.rm = TRUE)) %>% 
  mutate(altadena_prc = altadena_count/sum(altadena_count)*100,
         west_prc = west_count/sum(west_count)*100,
         east_prc = east_count/sum(east_count)*100)



#### Step 4: SECOND ANALYSIS- [analysis_restype_damage] ####
# Distribution of residential property types (single-family, multifamily, etc.) by damage category for all of Altadena, West Altadena, East Altadena, e.g., what % of significantly damaged properties were single family, etc.
analysis_restype_damage <- all_df %>% 
  group_by(res_type, damage_category) %>% 
  summarise(altadena_count = n(),
            west_count = sum(area_name == "West", na.rm = TRUE),
            east_count = sum(area_name == "East", na.rm = TRUE)) %>% 
  mutate(altadena_prc = altadena_count/sum(altadena_count)*100,
         west_prc = west_count/sum(west_count)*100,
         east_prc = east_count/sum(east_count)*100)

#### Step 5: THIRD ANALYSIS- [analysis_owner_renter_jan2025] ####
# Distribution of homeownership types (homeowner, renter) for all of Altadena, West Altadena, East Altadena in Jan 2025 (all properties whether or not destroyed), e.g., in jan 2025, X% of residential properties in Altadena were occupied by homeowners
analysis_owner_renter_jan2025 <- all_df %>% 
  group_by(owner_renter) %>% 
  summarise(altadena_count = n(),
            west_count = sum(area_name == "West", na.rm = TRUE),
            east_count = sum(area_name == "East", na.rm = TRUE)) %>% 
  mutate(altadena_prc = altadena_count/sum(altadena_count)*100,
         west_prc = west_count/sum(west_count)*100,
         east_prc = east_count/sum(east_count)*100)

#### Step 6: FOURTH ANALYSIS- [analysis_owner_renter_damage] ####
# Distribution of homeownership types (homeowner, renter) by damage category for all of Altadena, West Altadena, East Altadena, e.g., what % of significantly damaged properties were single family, etc.
analysis_owner_renter_damage <- all_df %>% 
  group_by(owner_renter, damage_category) %>% 
  summarise(altadena_count = n(),
            west_count = sum(area_name == "West", na.rm = TRUE),
            east_count = sum(area_name == "East", na.rm = TRUE)) %>% 
  mutate(altadena_prc = altadena_count/sum(altadena_count)*100,
         west_prc = west_count/sum(west_count)*100,
         east_prc = east_count/sum(east_count)*100)

#### Step 7: Upload tables to postgres and add table/column comments ####

# dbWriteTable(con, name = "analysis_restype_jan2025", value = analysis_restype_jan2025, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_restype_jan2025"
# indicator <- "Data on distribution of residential property types (single-family, multifamily, etc.) for all of Altadena, West Altadena, East Altadena in Jan 2025 (all properties whether or not destroyed), e.g., in jan 2025, X% of residential properties in Altadena were single family homes"
# source <- "Source: LA County Assessor Data, January 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_housing.docx"
# column_names <- colnames(analysis_restype_jan2025) # Get column names
# column_comments <- c(
#   "type of residence",
#   "count of housing stock for all of Altadena",
#   "count of housing stock for West Altadena",
#   "count of housing stock for East Altadena",
#   "percent of housing stock for all of Altadena",
#   "percent of housing stock for West Altadena",
#   "percent of housing stock for East Altadena"
#   )
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
# 
# dbWriteTable(con, name = "analysis_restype_damage", value = analysis_restype_damage, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_restype_damage"
# indicator <- "Data on residential property types (single-family, multifamily, etc.) by damage category for all of Altadena, West Altadena, East Altadena, e.g., what % of single family properties were significantly damaged, etc."
# source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_housing.docx"
# column_names <- colnames(analysis_restype_damage) # Get column names
# column_comments <- c(
#   "type of residence",
#   "damage category",
#   "count of housing stock by damage type for all of Altadena",
#   "count of housing stock by damage type for West Altadena",
#   "count of housing stock by damage type for East Altadena",
#   "percent of housing stock by damage type for all of Altadena",
#   "percent of housing stock by damage type for West Altadena",
#   "percent of housing stock by damage type for East Altadena"
# )
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
# 
# dbWriteTable(con, name = "analysis_owner_renter_jan2025", value = analysis_owner_renter_jan2025, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_owner_renter_jan2025"
# indicator <- "Data on distribution of homeownership types (homeowner, renter) for all of Altadena, West Altadena, East Altadena in Jan 2025 (all properties whether or not destroyed), e.g., in jan 2025, X% of residential properties in Altadena were occupied by homeowners"
# source <- "Source: LA County Assessor Data, January 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_housing.docx"
# column_names <- colnames(analysis_owner_renter_jan2025) # Get column names
# column_comments <- c(
#   "type of homeownership",
#   "count of housing stock for all of Altadena",
#   "count of housing stock for West Altadena",
#   "count of housing stock for East Altadena",
#   "percent of housing stock for all of Altadena",
#   "percent of housing stock for West Altadena",
#   "percent of housing stock for East Altadena"
# )
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
# 
# dbWriteTable(con, name = "analysis_owner_renter_damage", value = analysis_owner_renter_damage, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_owner_renter_damage"
# indicator <- "Data on homeownership types (homeowner, renter) by damage category for all of Altadena, West Altadena, East Altadena, e.g., what % of single family properties were significantly damaged, etc."
# source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_housing.docx"
# column_names <- colnames(analysis_owner_renter_damage) # Get column names
# column_comments <- c(
#   "type of homeownership",
#   "damage category",
#   "count of housing stock by damage type for all of Altadena",
#   "count of housing stock by damage type for West Altadena",
#   "count of housing stock by damage type for East Altadena",
#   "percent of housing stock by damage type for all of Altadena",
#   "percent of housing stock by damage type for West Altadena",
#   "percent of housing stock by damage type for East Altadena"
# )
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 8: close connection ####
dbDisconnect(con)
