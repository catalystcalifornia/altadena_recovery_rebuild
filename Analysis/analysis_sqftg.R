## Producing Analysis Tables for the average square footage of properties lost in Altadena 
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_analysis_sqftg.docx

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

#### Step 3: FIRST ANALYSIS- [analysis_sqftg_jan2025] ####
analysis_sqftg_jan2025 <- all_df %>% 
  group_by(res_type) %>% 
  summarise(altadena_sqftg_sum = sum(total_square_feet, na.rm = TRUE),
            west_sqftg_sum = sum(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_sum = sum(total_square_feet[area_name == "East"], na.rm = TRUE),
            altadena_sqftg_avg = mean(total_square_feet, na.rm = TRUE),
            west_sqftg_avg = mean(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_avg = mean(total_square_feet[area_name == "East"], na.rm = TRUE),
            altadena_sqftg_med = median(total_square_feet, na.rm = TRUE),
            west_sqftg_med = median(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_med = median(total_square_feet[area_name == "East"], na.rm = TRUE),
            #running standard deviation to see how spread out the unit sizes are, do they vary a lot or a little 
            altadena_sqftg_sd = sd(total_square_feet, na.rm = TRUE), 
            west_sqftg_sd = sd(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_sd = sd(total_square_feet[area_name == "East"], na.rm = TRUE),
            #running minimum to see the smallest value 
            altadena_sqftg_min = min(total_square_feet, na.rm = TRUE), 
            west_sqftg_min = min(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_min = min(total_square_feet[area_name == "East"], na.rm = TRUE),
            #running maximum to see the largest value 
            altadena_sqftg_max = max(total_square_feet, na.rm = TRUE), 
            west_sqftg_max = max(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_max = max(total_square_feet[area_name == "East"], na.rm = TRUE),
            .groups = "drop") %>%
            #making table long vs wide
              pivot_longer(
                cols = -res_type,
                names_to = c("area", ".value"),
                names_pattern = "(altadena|west|east)_(sqftg_sum|sqftg_avg|sqftg_med|sqftg_sd|sqftg_min|sqftg_max)"
              ) 

#### Step 4: SECOND ANALYSIS- [analysis_sqftg_damage] ####
analysis_sqftg_damage <- all_df %>% 
  mutate(damage_category = ifelse(is.na(damage_category), "No Damage", damage_category)) %>%
  group_by(res_type, damage_category) %>% 
  summarise(altadena_sqftg_sum = sum(total_square_feet, na.rm = TRUE),
            west_sqftg_sum = sum(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_sum = sum(total_square_feet[area_name == "East"], na.rm = TRUE),
            altadena_sqftg_avg = mean(total_square_feet, na.rm = TRUE),
            west_sqftg_avg = mean(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_avg = mean(total_square_feet[area_name == "East"], na.rm = TRUE),
            altadena_sqftg_med = median(total_square_feet, na.rm = TRUE),
            west_sqftg_med = median(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_med = median(total_square_feet[area_name == "East"], na.rm = TRUE),
            #running standard deviation to see how spread out the unit sizes are, do they vary a lot or a little 
            altadena_sqftg_sd = sd(total_square_feet, na.rm = TRUE), 
            west_sqftg_sd = sd(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_sd = sd(total_square_feet[area_name == "East"], na.rm = TRUE),
            #running minimum to see the smallest value 
            altadena_sqftg_min = min(total_square_feet, na.rm = TRUE), 
            west_sqftg_min = min(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_min = min(total_square_feet[area_name == "East"], na.rm = TRUE),
            #running maximum to see the largest value 
            altadena_sqftg_max = max(total_square_feet, na.rm = TRUE), 
            west_sqftg_max = max(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_max = max(total_square_feet[area_name == "East"], na.rm = TRUE),
            .groups = "drop") %>%
  #making table long vs wide
  pivot_longer(
    cols = -c(res_type, damage_category),
    names_to = c("area", ".value"),
    names_pattern = "(altadena|west|east)_(sqftg_sum|sqftg_avg|sqftg_med|sqftg_sd|sqftg_min|sqftg_max)"
  ) 


#### Step 5: Upload tables to postgres and add table/column comments ####

dbWriteTable(con, name = "analysis_sqftg_jan2025", value = analysis_sqftg_jan2025, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_sqftg_jan2025"
indicator <- "Data on average & median square footage of properties in Altadena, West Altadena, East Altadena by residential type in Jan 2025 (e.g., what was the average square footage of single family properties in Altadena)"
source <- "Source: LA County Assessor Data, January 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_sqftg.docx"
column_names <- colnames(analysis_sqftg_jan2025) # Get column names
column_comments <- c(
  "type of residence",
  "area",
  "sum of square footage",
  "average of square footage",
  "median of square footage",
  "standard deviation of square footage",
  "minimum of square footage",
  "maximum of square footage")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

dbWriteTable(con, name = "analysis_sqftg_damage", value = analysis_sqftg_damage, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_sqftg_damage"
indicator <- "Data on average & median square footage of properties lost in Altadena, West Altadena, East Altadena by residential type  (what was the median square footage of multifamily properties that sustained significant damage in West Altadena)"
source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_sqftg.docx"
column_names <- colnames(analysis_sqftg_damage) # Get column names
column_comments <- c(
  "type of residence",
  "damage category",
  "area",
  "sum of square footage",
  "average of square footage",
  "median of square footage",
  "standard deviation of square footage",
  "minimum of square footage",
  "maximum of square footage")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 6: close connection ####
dbDisconnect(con)
