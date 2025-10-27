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
# sum(is.na(housing_jan_e_w$area_name)) # check


##second combine with housing_damage
all_df <- housing_jan_e_w  %>%
  left_join(housing_damage, by= "ain")
# View(head(all_df))
# sum(is.na(all_df$ain)) # check
# sum(is.na(all_df$damage_category)) # check


#### Step 3: FIRST ANALYSIS- [analysis_units_jan2025] - What were the total units and rental units by residential type - single-family, multifamily before the fire ####
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
         altadena_total_all_units = sum(altadena_tot_units, na.rm=TRUE),
         altadena_total_rent_units = sum(altadena_rent_units, na.rm=TRUE),
         west_prc_tot = west_tot_units/sum(west_tot_units)*100,
         west_prc_rent = west_rent_units/sum(west_rent_units)*100,
         west_total_all_units = sum(west_tot_units, na.rm=TRUE),
         west_total_rent_units = sum(west_rent_units, na.rm=TRUE),
         east_prc_tot = east_tot_units/sum(east_tot_units)*100,
         east_prc_rent = east_rent_units/sum(east_rent_units)*100,
         east_total_all_units = sum(east_tot_units, na.rm=TRUE),
         east_total_rent_units = sum(east_rent_units, na.rm=TRUE)) %>%
  pivot_longer(
    cols = -res_type,
    names_to = c("area_name", ".value"),
    names_pattern = "(altadena|west|east)_(tot_units|rent_units|prc_tot|prc_rent|total_all_units|total_rent_units)"
  ) 

# clean up table for clarity
analysis_units_jan2025 <- analysis_units_jan2025 %>%
  select(area_name, res_type, tot_units, total_all_units, prc_tot, rent_units, total_rent_units, prc_rent) %>%
  rename(all_units_count=tot_units,
         all_units_total=total_all_units,
         all_units_prc=prc_tot,
         rent_units_count=rent_units,
         rent_units_total=total_rent_units,
        rent_units_prc=prc_rent) %>%
  arrange(area_name)

# check analysis
check <- analysis_units_jan2025 %>%
  group_by(area_name) %>%
  summarise(sum=sum(all_units_prc),
            sum_=sum(rent_units_prc)) # looks good

#### Step 4: SECOND ANALYSIS- [analysis_units_damage] -- What percentage of rental units and all units were destroyed by residential type ####
analysis_units_damage <- all_df %>% 
  group_by(res_type, damage_category) %>% 
  summarise(altadena_tot_units = sum(total_units, na.rm = TRUE),
            altadena_rent_units = sum(landlord_units, na.rm = TRUE),
            west_tot_units = sum(total_units[area_name == "West"], na.rm = TRUE),
            west_rent_units = sum(landlord_units[area_name == "West"], na.rm = TRUE),
            east_tot_units = sum(total_units[area_name == "East"], na.rm = TRUE),
            east_rent_units = sum(landlord_units[area_name == "East"], na.rm = TRUE),
            .groups = "drop") %>%
  group_by(res_type) %>%
  mutate(altadena_prc_tot = altadena_tot_units/sum(altadena_tot_units)*100,
         altadena_prc_rent = altadena_rent_units/sum(altadena_rent_units)*100,
         altadena_total_all_units = sum(altadena_tot_units, na.rm=TRUE),
         altadena_total_rent_units = sum(altadena_rent_units, na.rm=TRUE),
         west_prc_tot = west_tot_units/sum(west_tot_units)*100,
         west_prc_rent = west_rent_units/sum(west_rent_units)*100,
         west_total_all_units = sum(west_tot_units, na.rm=TRUE),
         west_total_rent_units = sum(west_rent_units, na.rm=TRUE),
         east_prc_tot = east_tot_units/sum(east_tot_units)*100,
         east_prc_rent = east_rent_units/sum(east_rent_units)*100,
         east_total_all_units = sum(east_tot_units, na.rm=TRUE),
         east_total_rent_units = sum(east_rent_units, na.rm=TRUE)) %>%
  ungroup() %>%
  pivot_longer(
    cols = c(altadena_tot_units:east_total_rent_units),
    names_to = c("area_name", ".value"),
    names_pattern = "(altadena|west|east)_(tot_units|rent_units|prc_tot|prc_rent|total_all_units|total_rent_units)"
  ) 

# clean up table for clarity
analysis_units_damage  <- analysis_units_damage  %>%
  select(area_name, res_type, damage_category,tot_units, total_all_units, prc_tot, rent_units, total_rent_units, prc_rent) %>%
  rename(all_units_count=tot_units,
         all_units_total=total_all_units,
         all_units_prc=prc_tot,
         rent_units_count=rent_units,
         rent_units_total=total_rent_units,
         rent_units_prc=prc_rent) %>%
  arrange(area_name,res_type)

analysis_units_damage[sapply(analysis_units_damage, is.nan)] <- NA

# check analysis
check <- analysis_units_damage  %>%
  group_by(area_name, res_type) %>%
  summarise(sum=sum(all_units_prc,na.rm=TRUE),
            sum_=sum(rent_units_prc,na.rm=TRUE)) # looks good zeros are from boarding houses that had 0 counts and totals

#### Step 5: THIRD ANALYSIS- [analysis_multifamily_jan2025] -- look at size of multifamily properties in january 2025 ####
# filter for multifamily properties and add a field for the size of them grouping into categories
all_df_multifamily <- all_df %>% 
  filter(res_type == "Multifamily") %>% 
  mutate(
    multifamily_unit_category=
      case_when(
        total_units>0 & total_units<=2 ~ "Two Units",
        total_units>2 & total_units<5 ~ "Three to Four Units",
        total_units>=5 ~ "Five or More Units",
        TRUE ~ NA
      ))

# check 
check <- all_df_multifamily %>% group_by(multifamily_unit_category,total_units) %>% summarise(count=n())
# looks good, one building with 0 units

# run for east and west
analysis_multifamily_e_w <- all_df_multifamily %>%
  filter(!is.na(multifamily_unit_category)) %>% # filter out NA
  group_by(area_name) %>%
  mutate(property_total=n(), # grabbing a total for just total multifamily unit properties by area
       unit_size_avg = mean(total_units, na.rm = TRUE), # average unit size for the area
       unit_size_med = median(total_units, na.rm = TRUE) # median unit size for the area
       ) %>%
  ungroup() %>%
  group_by(area_name,multifamily_unit_category,property_total,unit_size_avg,unit_size_med) %>%
  summarise(property_count = n(), # properties in that category of multifamily units
         property_prc=n()/property_total*100) %>% # out of multifamily properties what percent are in this category
  slice(1)

# check
check <- all_df_multifamily %>%
  filter(!is.na(multifamily_unit_category)) %>%
  filter(area_name=='West') %>%
  nrow() # checks out with total

check <- all_df_multifamily %>%
  filter(!is.na(multifamily_unit_category)) %>%
  filter(area_name=='West') %>%
  count(multifamily_unit_category) # checks out with the count fields

# run for altadena overall
analysis_multifamily_alt<- all_df_multifamily %>%
  filter(!is.na(multifamily_unit_category)) %>%
  mutate(property_total=n(), # grabbing a total for just total multifamily unit properties by area
         unit_size_avg = mean(total_units, na.rm = TRUE), # average unit size for the area
         unit_size_med = median(total_units, na.rm = TRUE) # median unit size for the area
  ) %>%
  ungroup() %>%
  group_by(multifamily_unit_category,property_total,unit_size_avg,unit_size_med) %>%
  summarise(property_count = n(), # total properties in that category
            property_prc=n()/property_total*100) %>%
  slice(1)

# check
median(all_df_multifamily$total_units,na.rm=TRUE)
mean(all_df_multifamily$total_units,na.rm=TRUE)
# checks out

# bind together and clean up
analysis_multifamily_jan2025 <- rbind(analysis_multifamily_alt %>%
                                        mutate(area_name='Altadena'), 
                                      analysis_multifamily_e_w) %>%
  select(area_name,multifamily_unit_category, property_count,property_total,property_prc,unit_size_avg, unit_size_med)




#### Step 6: FOURTH ANALYSIS- [analysis_multifamily_damage] -- what percentage of multifamily properties were destroyed by damage category ----

# run for east and west - percent of each unit type damaged in each area
analysis_multifamily_e_w_damage <- all_df_multifamily %>%
  filter(!is.na(multifamily_unit_category)) %>% # filter out NA
  group_by(area_name,multifamily_unit_category) %>%
  mutate(property_total=n()) %>% # grabbing a total for multifamily unit properties by area
  ungroup() %>%
  group_by(area_name,multifamily_unit_category,damage_category,property_total) %>%
  summarise(property_damage_count = n(), # properties in that category of multifamily units that were damaged
            property_damage_prc=n()/property_total*100) %>% # percent of multifamily units in the area in the damage category
  slice(1) %>%
  ungroup()

# check
check <- analysis_multifamily_e_w_damage  %>%
  group_by(area_name,multifamily_unit_category) %>%
  summarise(sum=sum(property_damage_prc)) # checks out

# what is the average size of units damaged
analysis_multifamily_e_w_damage_avg <- all_df_multifamily %>%
  filter(!is.na(multifamily_unit_category)) %>% # filter out NA
   group_by(area_name,damage_category) %>%
   summarise(property_damage_count=n(), # total multifamily properties in damage category
     unit_size_avg = mean(total_units, na.rm = TRUE), # average unit size for the area and damage category
                     unit_size_med = median(total_units, na.rm = TRUE)) # median unit size for the area and damage category


# run for all of altadena - percent of each unit type damaged 
analysis_multifamily_alt_damage <- all_df_multifamily %>%
  filter(!is.na(multifamily_unit_category)) %>% # filter out NA
  group_by(multifamily_unit_category) %>%
  mutate(property_total=n()) %>% # grabbing a total for multifamily unit properties
  ungroup() %>%
  group_by(multifamily_unit_category,damage_category,property_total) %>%
  summarise(property_damage_count = n(), # properties in that category of multifamily units that were damaged
            property_damage_prc=n()/property_total*100) %>% # percent of multifamily units in the damage category
  slice(1) %>%
  ungroup()

# check
check <- analysis_multifamily_alt_damage  %>%
  group_by(multifamily_unit_category) %>%
  summarise(sum=sum(property_damage_prc)) # checks out

# what is the average size of units damaged
analysis_multifamily_alt_damage_avg <- all_df_multifamily %>%
  filter(!is.na(multifamily_unit_category)) %>% # filter out NA
  group_by(damage_category) %>%
  summarise(property_damage_count=n(), # total multifamily properties in damage category
            unit_size_avg = mean(total_units, na.rm = TRUE), # average unit size for the damage category
            unit_size_med = median(total_units, na.rm = TRUE)) # median unit size for the damage category


# bind together and clean up
analysis_multifamily_damage_prc <- rbind(analysis_multifamily_alt_damage %>%
                                        mutate(area_name='Altadena'), 
                                      analysis_multifamily_e_w_damage) %>%
  select(area_name,multifamily_unit_category, damage_category, property_damage_count,property_total,property_damage_prc)

# bind together and clean up
analysis_multifamily_damage_avg <- rbind(analysis_multifamily_alt_damage_avg %>%
                                       mutate(area_name='Altadena') %>%
                                       ungroup(), 
                                     analysis_multifamily_e_w_damage_avg %>%
                                       ungroup()) %>%
  select(area_name,damage_category,property_damage_count,unit_size_avg, unit_size_med)



#### Step 6: Upload tables to postgres and add table/column comments ####
# dbWriteTable(con, name = "analysis_units_jan2025", value = analysis_units_jan2025, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_units_jan2025"
indicator <- "Data on total units, total rental units (landlord_units column) in Altadena, West Altadena, East Altadena by residential type (e.g., total units in single family homes, total rental units in single family homes) in January 2025 (before the fire)"
source <- "Source: LA County Assessor Data, January 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_units.docx"
column_names <- colnames(analysis_units_jan2025) # Get column names
column_comments <- c(
  "area",
  "type of residence",
  "count of all units by res type - rental or owner - num",
  "total units in the area - rental or owner - denom",
  'percent of all units in each res type for each area',
  "count of rental units by res type - num",
  "total rental units in the area - denom",
  'percent of rental units in each res type for each area')
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

# dbWriteTable(con, name = "analysis_units_damage", value = analysis_units_damage, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_units_damage"
indicator <- "Data on total units lost, total rental units lost in Altadena, West Altadena, East Altadena by residential type (e.g., total rental units lost in multifamily homes) → count lost as significant damage, but include separately units that sustained some damage"
source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_units.docx"
column_names <- colnames(analysis_units_damage) # Get column names
column_comments <- c(
  "area",
  "type of residence",
  "damage category",
  "count of all units by res type and damage category - rental and owner units - num",
  "total units within each res type in the area - denom",
  'percent of all units in the res type for that area that sustained each damage level',
  "count of rental units by res type and damage category - num",
  "total rental units within each res type  in the area - denom",
  'percent of rental units in the res type for that area that sustained each damage level')
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


# dbWriteTable(con, name = "analysis_multifamily_jan2025", value = analysis_multifamily_jan2025, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_multifamily_jan2025"
indicator <- "Data on the multifamily units by area and multifamily unit size, the average unit size, the median size, and count by select categories. Only includes analysis for properties flagged as multifamily (two or more units on property)"
source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_units.docx"
column_names <- colnames(analysis_multifamily_jan2025) # Get column names
column_comments <- c(
  "area name",
  'multifamily unit size category 1-2, 3-4, or 5 or more',
  "number of multifamily properties in the category",
  "total multifamily properties in the area",
  "percent of multifamily properties in the category",
  "average unit size for the area",
  "median unit size for the area")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


# dbWriteTable(con, name = "analysis_multifamily_damage_avg", value = analysis_multifamily_damage_avg, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_multifamily_damage_avg"
indicator <- "Data on the multifamily units by area and damage category, Includes the average unit size and the median size for all multifamily properties in the area within each damage category. Only includes analysis for properties flagged as multifamily (two or more units on property)"
source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_units.docx"
column_names <- colnames(analysis_multifamily_damage_avg) # Get column names
column_comments <- c(
  "area name",
  "damage category",
  "total multifamily properties in the area and damage category",
  "average multifamily unit size in the area and damage category",
  "median unit size in the area and damage category")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

dbWriteTable(con, name = "analysis_multifamily_damage_prc", value = analysis_multifamily_damage_prc, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_multifamily_damage_prc"
indicator <- "Data on the multifamily units by area, unit size category, and damage category, count and percentages of multifamily properties in each size category that were or were not damaged"
source <- "Source: LA County Assessor Data, January 2025. CAL FIRE Damage Data, September 2025."
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_units.docx"
column_names <- colnames(analysis_multifamily_damage_prc) # Get column names
column_comments <- c(
  "area name",
  "multifamily unit size category 1-2, 3-4, 5 or more",
  "Damage category",
  "Number of multifamily properties in the area within each size category and damage category combo - numerator",
  "Total multifamily properties in the area and size category- denominator",
  "percentage of multifamily properties in the size category that were in each damage level")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#### Step 7: close connection ####
dbDisconnect(con)
