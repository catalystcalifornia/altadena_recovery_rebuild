## Producing Analysis Tables for units in Altadena before and after the Eaton Fire 
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_analysis_units.docx

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

#### Step 3: FIRST ANALYSIS- [analysis_units_jan2025] ####
analysis_units_jan2025 <- all_df %>% 
  group_by(res_type) %>% 
  summarise(altadena_tot_units = sum(total_units, na.rm = TRUE),
            altadena_rent_units = sum(landlord_units, na.rm = TRUE),
            west_tot_units = sum(total_units[area_name == "West"], na.rm = TRUE),
            west_rent_units = sum(landlord_units[area_name == "West"], na.rm = TRUE),
            east_tot_units = sum(total_units[area_name == "East"], na.rm = TRUE),
            east_rent_units = sum(landlord_units[area_name == "East"], na.rm = TRUE),
            .groups = "drop") %>%
  mutate(altadena_prc_tot = altadena_tot_units/sum(altadena_tot_units)*100,
         altadena_prc_rent = altadena_rent_units/sum(altadena_rent_units)*100,
         west_prc_tot = west_tot_units/sum(west_tot_units)*100,
         west_prc_rent = west_rent_units/sum(west_rent_units)*100,
         east_prc_tot = east_tot_units/sum(east_tot_units)*100,
         east_prc_rent = east_rent_units/sum(east_rent_units)*100) %>%
  pivot_longer(
    cols = -res_type,
    names_to = c("area", ".value"),
    names_pattern = "(altadena|west|east)_(tot_units|rent_units|prc_tot|prc_rent)"
  ) 
#### Step 4: SECOND ANALYSIS- [analysis_units_damage] ####
analysis_units_damage <- all_df %>% 
  mutate(damage_category = ifelse(is.na(damage_category), "No Damage", damage_category)) %>%
  group_by(res_type, damage_category) %>% 
  summarise(altadena_tot_units = sum(total_units, na.rm = TRUE),
            altadena_rent_units = sum(landlord_units, na.rm = TRUE),
            west_tot_units = sum(total_units[area_name == "West"], na.rm = TRUE),
            west_rent_units = sum(landlord_units[area_name == "West"], na.rm = TRUE),
            east_tot_units = sum(total_units[area_name == "East"], na.rm = TRUE),
            east_rent_units = sum(landlord_units[area_name == "East"], na.rm = TRUE),
            .groups = "drop") %>%
  mutate(altadena_prc_tot = altadena_tot_units/sum(altadena_tot_units)*100,
         altadena_prc_rent = altadena_rent_units/sum(altadena_rent_units)*100,
         west_prc_tot = west_tot_units/sum(west_tot_units)*100,
         west_prc_rent = west_rent_units/sum(west_rent_units)*100,
         east_prc_tot = east_tot_units/sum(east_tot_units)*100,
         east_prc_rent = east_rent_units/sum(east_rent_units)*100) %>%
  pivot_longer(
    cols = c(altadena_tot_units:east_prc_rent),
    names_to = c("area", ".value"),
    names_pattern = "(altadena|west|east)_(tot_units|rent_units|prc_tot|prc_rent)"
  ) 

#### Step 5: THIRD ANALYSIS- [analysis_multifamily_damage] ####
analysis_multifamily_damage <- all_df %>% 
  mutate(damage_category = ifelse(is.na(damage_category), "No Damage", damage_category)) %>%
  filter(damage_category == "Significant Damage",
         res_type == "Multifamily") %>% 
  group_by(total_units) %>%
  summarise(count_unit = sum(total_units, na.rm = TRUE),
            avg_unit_size = mean(total_units, na.rm = TRUE),
            med_unit_size = median(total_units, na.rm = TRUE),
            .groups = "drop") %>%
  mutate(prc_unit_size = count_unit/sum(count_unit)*100,
         total_units = as.character(total_units)) %>%
  bind_rows( #adding a row for all units
    summarise(
      ., 
      total_units = "all units",
      count_unit = sum(count_unit),
      avg_unit_size = sum(count_unit) / nrow(.),  
      med_unit_size = median(med_unit_size),
      prc_unit_size = sum(count_unit) / sum(count_unit) * 100  
    )
  ) %>%
  rename(num_of_units = total_units)

# JZ QA alternative code------------

# I am unclear if by total units we mean, the sum of the units (already in Maria's original code)
# OR if it should be the count of n-unit units. i.e) how many 5-unit units or how many 13-unit units are there?
# I also am interpreting the average/median size of units out of ALL multifamily severly damaged units.
# I am just going to make an alternative analysis with my different interpretation and if it is completely wrong it can be deleted.
# I am NOT overwriting Maria's original analysis_multifamily_damage table in postgres

analysis_multifamily_damage_jz <- all_df %>% 
  mutate(damage_category = ifelse(is.na(damage_category), "No Damage", damage_category)) %>%
  filter(damage_category == "Significant Damage",
         res_type == "Multifamily") %>% 
  
  # this is my interpretation of average/median unit size I assumed it was out of all the multifamily severly damaged units
  # what is the average unit size and median unit size:
  
  mutate(total=n(), # grabbing a total for just total multifamily severely damaged units
    avg_unit_size = mean(total_units, na.rm = TRUE),
    med_unit_size = median(total_units, na.rm = TRUE),
    .groups = "drop") %>%
  
  group_by(total_units) %>%
  
  # my interpretation of the numerator we nede for prc calcs is to take the count of each n-unit type. i.e.) how many 5-unit multifamily residents are there
  mutate(count_unit = sum(!is.na(total_units)),
         total_units=as.numeric(total_units))%>%
  
ungroup()%>%
  mutate(total_5more_units=sum(!is.na(count_unit)[total_units>=5]))%>%
  group_by(total_units)%>%
         
         # calculate prc unit size for units that are less than 5, and then for all units 
         # greater than 5, make the numerator the sum of all the units greater than 5 and divide that by the total
       mutate(prc_unit_size = ifelse(total_units<5, count_unit/total*100,
                                total_5more_units / total * 100)) %>%
  slice(1)%>%
  rename(num_of_units=total_units)%>%
  select(num_of_units, count_unit, avg_unit_size, med_unit_size, prc_unit_size)%>%
  arrange(num_of_units)%>%
  mutate(num_of_units=as.character(num_of_units))
 


#### Step 6: Upload tables to postgres and add table/column comments ####
# 
dbWriteTable(con, name = "analysis_units_jan2025", value = analysis_units_jan2025, overwrite = TRUE)
schema <- "data"
table_name <- "analysis_units_jan2025"
indicator <- "Data on total units, total rental units (landlord_units column) in Altadena, West Altadena, East Altadena by residential type (e.g., total units in single family homes, total rental units in single family homes) in January 2025 (before the fire)"
source <- "Source: LA County Assessor Data, January 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_units.docx"
column_names <- colnames(analysis_units_jan2025) # Get column names
column_comments <- c(
  "type of residence",
  "area",
  "count of total units",
  "count of rental units",
  "percent of total units",
  "percent of rental units")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

dbWriteTable(con, name = "analysis_units_damage", value = analysis_units_damage, overwrite = TRUE)
schema <- "data"
table_name <- "analysis_units_damage"
indicator <- "Data on total units lost, total rental units lost in Altadena, West Altadena, East Altadena by residential type (e.g., total rental units lost in multifamily homes) → count lost as significant damage, but include separately units that sustained some damage"
source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_units.docx"
column_names <- colnames(analysis_units_damage) # Get column names
column_comments <- c(
  "type of residence",
  "damage category",
  "area",
  "count of total units",
  "count of rental units",
  "percent of total units",
  "percent of rental units"
)
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

# dbWriteTable(con, name = "analysis_multifamily_damage", value = analysis_multifamily_damage, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_multifamily_damage"
# indicator <- "Data on the multifamily units lost (significant damage), what were their sizes? e.g., what was the average unit size, what was the median size, what percentage were 2 units, 3-4 units, or 5 or more units"
# source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_units.docx"
# column_names <- colnames(analysis_multifamily_damage) # Get column names
# column_comments <- c(
#   "number of units in a building",
#   "count of units of this many",
#   "average unit size",
#   "median unit size",
#   "percentge unit size") 
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


dbWriteTable(con, name = "analysis_multifamily_damage_jz", value = analysis_multifamily_damage, overwrite = TRUE)
schema <- "data"
table_name <- "analysis_multifamily_damage_jz"
indicator <- "Data on the multifamily units lost (significant damage), what were their sizes? e.g., what was the average unit size, what was the median size, what percentage were 2 units, 3-4 units, or 5 or more units"
source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_units.docx"
column_names <- colnames(analysis_multifamily_damage) # Get column names
column_comments <- c(
  "number of n units",
  "count of units of this many",
  "average unit size",
  "median unit size",
  "percentge unit size. Unit size greater than 5 is all agggregated and it is the percent of all units that are 5 or greater.")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)



#### Step 7: close connection ####
dbDisconnect(con)
