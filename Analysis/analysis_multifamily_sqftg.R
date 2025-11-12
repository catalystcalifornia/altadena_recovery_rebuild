## Producing Analysis Tables for the average square footage of units in multifamily properties before and after the fire.
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_analysis_multifamily_sqftg.docx

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
#filtering for multifamily here
housing_jan <- st_read(con, query="SELECT * FROM data.rel_assessor_residential_jan2025 WHERE residential = 'true' AND res_type = 'Multifamily'") 

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

#### Step 3: Create a column that calculates the average square footage per unit on the Multifamily properties ####
df <- all_df %>% 
  mutate(avg_sqft_per_unit = total_square_feet/total_units)

#### Step 4: FIRST ANALYSIS- [analysis_multifamily_sqftg_jan2025] ####
analysis_multifamily_sqftg_jan2025 <- df %>% 
  group_by(res_type) %>% 
  summarise(altadena_sqftg_sum = sum(total_square_feet, na.rm = TRUE),
            west_sqftg_sum = sum(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_sum = sum(total_square_feet[area_name == "East"], na.rm = TRUE),
            altadena_units_sum = sum(total_units, na.rm = TRUE),
            west_units_sum = sum(total_units[area_name == "West"], na.rm = TRUE),
            east_units_sum = sum(total_units[area_name == "East"], na.rm = TRUE),
            altadena_sqftg_avg = mean(avg_sqft_per_unit, na.rm = TRUE),
            west_sqftg_avg = mean(avg_sqft_per_unit[area_name == "West"], na.rm = TRUE),
            east_sqftg_avg = mean(avg_sqft_per_unit[area_name == "East"], na.rm = TRUE),
            .groups = "drop") %>%
  #making table long vs wide
  pivot_longer(
    cols = -res_type,
    names_to = c("area_name", ".value"),
    names_pattern = "(altadena|west|east)_(sqftg_sum|units_sum|sqftg_avg)"
  )  %>%
  mutate(across(everything(), ~ifelse(is.nan(.) | is.infinite(.), NA, .))) %>%
  select(area_name, everything())

# QA: Calculate totals a different way and make sure results match 
sum(df$total_units[df$area_label=="West Altadena"]) # 1600 --matches
sum(df$total_square_feet[df$area_label=="West Altadena"]) # 1452780 --matches

#### QA Step 4: Alternative method for calculating average-----------------------

# Maria's method takes the mean of all individual average sq ftage per parcel for each geo level (east/west/all of altadena)

## This methodology is more influenced by outliers. 
# Look to see what thevariation in # of units is:

table(all_df$total_units) # not much variation most are 2 units but there are parcels with more than 2.
all_df %>%
  filter(total_units > 2) %>%
  summarise(count = n()) # 371 parcels have more than 2 units


# Alternatively what we could do is calculate the total square footage/total units for all geo levels:

analysis_multifamily_sqftg_jan2025_jz <- df %>% 
  group_by(res_type) %>% 
  summarise(altadena_sqftg_sum = sum(total_square_feet, na.rm = TRUE),
            west_sqftg_sum = sum(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_sum = sum(total_square_feet[area_name == "East"], na.rm = TRUE),
            altadena_units_sum = sum(total_units, na.rm = TRUE),
            west_units_sum = sum(total_units[area_name == "West"], na.rm = TRUE),
            east_units_sum = sum(total_units[area_name == "East"], na.rm = TRUE),
            altadena_sqftg_avg = altadena_sqftg_sum/altadena_units_sum,
            west_sqftg_avg = west_sqftg_sum/west_units_sum,
            east_sqftg_avg = east_sqftg_sum/east_units_sum,
            .groups = "drop") %>%
  #making table long vs wide
  pivot_longer(
    cols = -res_type,
    names_to = c("area_name", ".value"),
    names_pattern = "(altadena|west|east)_(sqftg_sum|units_sum|sqftg_avg)"
  )  %>%
  mutate(across(everything(), ~ifelse(is.nan(.) | is.infinite(.), NA, .))) %>%
  select(area_name, everything())


# check to make sure results look right

sum(all_df$total_square_feet[all_df$area_label=="West Altadena"])/sum(all_df$total_units[all_df$area_label=="West Altadena"])

# Notes:
## These values produce slightly lower averages than in the alternative method. I think this method is probably going to be a better approach
# Because this is more about the average UNIT size which is what people in Altadena actually live in --UNITS as opposed to PARCELS 


# I am going to push my version to postgres for the team to review and compare:

dbWriteTable(con, name = "analysis_multifamily_sqftg_jan2025_method2", value = analysis_multifamily_sqftg_jan2025_jz, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_multifamily_sqftg_jan2025_jz"
indicator <- "Data on sum of and average square footage of properties in Altadena, West Altadena, East Altadena by residential type of multifamily properties in Jan 2025. This calculates the average
square footage by calculating total square footage in each geography / total number of units in each geography. Geography refers to West/ East/All of Altadena."
source <- "Source: LA County Assessor Data, January 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_multifamily_sqftg.docx"
column_names <- colnames(analysis_multifamily_sqftg_jan2025_jz) # Get column names
column_comments <- c(
  "area",
  "type of residence",
  "sum of square footage",
  "sum of units",
  "average of square footage")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 5: SECOND ANALYSIS- [analysis_multifamily_sqftg_damage] ####
analysis_multifamily_sqftg_damage <- df %>% 
  group_by(res_type, damage_category) %>% 
  summarise(altadena_sqftg_sum = sum(total_square_feet, na.rm = TRUE),
            west_sqftg_sum = sum(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_sum = sum(total_square_feet[area_name == "East"], na.rm = TRUE),
            altadena_units_sum = sum(total_units, na.rm = TRUE),
            west_units_sum = sum(total_units[area_name == "West"], na.rm = TRUE),
            east_units_sum = sum(total_units[area_name == "East"], na.rm = TRUE),
            altadena_sqftg_avg = mean(avg_sqft_per_unit, na.rm = TRUE),
            west_sqftg_avg = mean(avg_sqft_per_unit[area_name == "West"], na.rm = TRUE),
            east_sqftg_avg = mean(avg_sqft_per_unit[area_name == "East"], na.rm = TRUE),
            .groups = "drop") %>%
  #making table long vs wide
  pivot_longer(
    cols = -c(res_type, damage_category),
    names_to = c("area_name", ".value"),
    names_pattern = "(altadena|west|east)_(sqftg_sum|units_sum|sqftg_avg)"
  )  %>%
  mutate(across(everything(), ~ifelse(is.nan(.) | is.infinite(.), NA, .))) %>%
  select(area_name, everything())

#### QA Step 5:-------------------------------

# First quick QA check that Maria's values check out
sum(all_df$total_units[all_df$area_label=="East Altadena" & all_df$damage_category == "Significant Damage" ]) # 228 --matches
sum(all_df$total_square_feet[all_df$area_label=="East Altadena" & all_df$damage_category == "Significant Damage" ]) # 212728 --matches


# Now going to again calculate the average the alternative method:

analysis_multifamily_sqftg_damage_jz <- df %>% 
  group_by(res_type, damage_category) %>% 
  summarise(altadena_sqftg_sum = sum(total_square_feet, na.rm = TRUE),
            west_sqftg_sum = sum(total_square_feet[area_name == "West"], na.rm = TRUE),
            east_sqftg_sum = sum(total_square_feet[area_name == "East"], na.rm = TRUE),
            altadena_units_sum = sum(total_units, na.rm = TRUE),
            west_units_sum = sum(total_units[area_name == "West"], na.rm = TRUE),
            east_units_sum = sum(total_units[area_name == "East"], na.rm = TRUE),
            
            altadena_sqftg_avg = altadena_sqftg_sum/altadena_units_sum,
            west_sqftg_avg = west_sqftg_sum/west_units_sum,
            east_sqftg_avg= east_sqftg_sum/east_units_sum,
            .groups = "drop") %>%
  #making table long vs wide
  pivot_longer(
    cols = -c(res_type, damage_category),
    names_to = c("area_name", ".value"),
    names_pattern = "(altadena|west|east)_(sqftg_sum|units_sum|sqftg_avg)"
  )  %>%
  mutate(across(everything(), ~ifelse(is.nan(.) | is.infinite(.), NA, .))) %>%
  select(area_name, everything())

# Quick check
sum(all_df$total_square_feet[all_df$area_label=="East Altadena" & all_df$damage_category == "Significant Damage" ])/
sum(all_df$total_units[all_df$area_label=="East Altadena" & all_df$damage_category == "Significant Damage" ])

# similarly these averages are slightly lower than the other method.

# Push my table to postgres for team to compare------------

dbWriteTable(con, name = "analysis_multifamily_sqftg_damage_method2", value = analysis_multifamily_sqftg_damage_jz, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_multifamily_sqftg_damage_jz"
indicator <- "Data on sum of and average square footage of properties in Altadena, West Altadena, East Altadena by residential type and by damage category of 
multifamily properties. This calculates average square footage by taking total square footage in each geography level for each damage category and divide it by the total number of units in each geography level and each damage category."
source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_multifamily_sqftg.docx"
column_names <- colnames(analysis_multifamily_sqftg_damage_jz) # Get column names
column_comments <- c(
  "area",
  "type of residence",
  "damage category",
  "sum of square footage",
  "sum of units",
  "average of square footage")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 6: Upload tables to postgres and add table/column comments ####

dbWriteTable(con, name = "analysis_multifamily_sqftg_jan2025_method1", value = analysis_multifamily_sqftg_jan2025, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_multifamily_sqftg_jan2025"
indicator <- "Data on sum of and average square footage of properties in Altadena, West Altadena, East Altadena by residential type of multifamily properties in Jan 2025"
source <- "Source: LA County Assessor Data, January 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_multifamily_sqftg.docx"
column_names <- colnames(analysis_multifamily_sqftg_jan2025) # Get column names
column_comments <- c(
  "area",
  "type of residence",
  "sum of square footage",
  "sum of units",
  "average of square footage")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

dbWriteTable(con, name = "analysis_multifamily_sqftg_damage_method1", value = analysis_multifamily_sqftg_damage, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_multifamily_sqftg_damage"
indicator <- "Data on sum of and average square footage of properties in Altadena, West Altadena, East Altadena by residential type and by damage category of multifamily properties"
source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_multifamily_sqftg.docx"
column_names <- colnames(analysis_multifamily_sqftg_damage) # Get column names
column_comments <- c(
  "area",
  "type of residence",
  "damage category",
  "sum of square footage",
  "sum of units",
  "average of square footage")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)



#### Step 7: close connection ####
dbDisconnect(con)








