## Analyze the zoning characteristics of damaged properties for possibilities in rebuilding
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

damage <- st_read(con, query="SELECT * FROM data.rel_assessor_damage_level_sept2025") %>%
  filter(damage_category=='Significant Damage') # just interested in damaged parcels

zoning_lac <- st_read(con, query = "Select * FROM data.lac_unincorporated_zoning_3310 WHERE plng_area = 'West San Gabriel Valley'") # filter for zoning area in Altadena

# parcel shapes for september
parcels_sept <-  st_read(con, query="SELECT * FROM data.rel_assessor_altadena_parcels_sept2025", geom="geom")

# check
# mapview(zoning_lac) + mapview(parcels_sept)

#### Step 2: Join parcel data to zoning data ####
st_crs(zoning_lac)
st_crs(parcels_sept)
# right projections

# select residential parcels only
parcels_sept <- parcels_sept %>%
  filter(ain_sept %in% housing_sept$ain_sept)

# select damaged parcels only
parcels_sept_damage <- parcels_sept %>%
  filter(ain_sept %in% damage$ain_sept)

# join parcels to zones based on centroid
parcels_zoning <- st_join(st_centroid(parcels_sept_damage), zoning_lac, left=TRUE)

# check join
table(parcels_zoning$z_category, useNA='always')
# no NAs

nrow(parcels_zoning)
length(unique(parcels_zoning$ain_sept))
# no duplicates

### Step 3: Explore zoning #####
# compare how zoning in Assessor data matches up to our join/data
parcels_zoning <- parcels_zoning %>%
  left_join(housing_sept %>% select(ain_sept, use_code, zoning_code), by="ain_sept")

check <- parcels_zoning %>%
  group_by(zoning_code,zone) %>%
  summarise(count=n())

View(check)
# some discrepancies in intensity, let's use our zoning data, which doesn't have blanks either

# do an initial summary to determine which field to use and we can recode from there
zone_table1 <- parcels_zoning %>%
  group_by(zone) %>%
  summarise(property_count=n())
  
zone_cat_table1 <- parcels_zoning %>%
  group_by(z_category) %>%
  summarise(property_count=n())
# mostly all R1 the zone provides further detail about intensity than z_category
# so first do zone category then drill down into R1

### Step 4: Calculate the potential uses by area #####
# First the percentage of damaged properties in each area with their overall zoning categories
# drop geometry 
all_df <- parcels_zoning %>%
  st_drop_geometry()

analysis_zone_category_e_w <- all_df %>%
  group_by(area_name) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,z_category) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_zone_category_alt <- all_df %>%
  mutate(area_name='Altadena') %>%
  group_by(area_name) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,z_category) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_z_category <- rbind(analysis_zone_category_e_w,
                               analysis_zone_category_alt)


# recode uses
unique(analysis_z_category$z_category)
# based on https://planning.lacounty.gov/wp-content/uploads/2024/04/WSGVAP_LargeMap_Zoning_240330_Altadena.pdf
# https://planning.lacounty.gov/wp-content/uploads/2022/04/map_z_02_Altadena.pdf
# R-1-7500
# C-2: Neighborhood Business
# C-3: General Commercial
# R-1: Single-Family Residence
# R-2: Two-Family Residence
# R-3-()U: Limited Density Multiple Residence
# R-A: Residential Agricultural
# A-1: Light Agriculture
# O-S: Open Space
# SP: Specific Plan

zone_cat_table1 <- zone_cat_table1 %>%
  mutate(z_category_label=
           case_when(
             z_category=="C-2" ~ "Neighborhood Business",
             z_category=="C-3" ~ "General Commercial",
             z_category=="R-1" ~ "Single-Family Residence",
             z_category=="R-2" ~ "Two-Family Residence",
             z_category=="R-3-()U" ~ "Limited Density Multiple Residence",
             z_category=="R-A" ~ "Residential Agricultural",
             z_category=="A-1" ~ "Light Agriculture",
             z_category=="O-S" ~ "Open Space",
             z_category=="SP" ~ "Specific Plan",
             TRUE ~ NA
           )) %>%
  st_drop_geometry()

# join labels
analysis_z_category <- analysis_z_category  %>%
  left_join(zone_cat_table1 %>% select(z_category,z_category_label)) 

# clean up for postgres 
analysis_z_category_final <- analysis_z_category %>%
  select(area_name, z_category, z_category_label, count, prc, total) %>%
  rename(zone_count=count,
         zone_prc=prc,
         area_total=total) %>% ungroup()

# Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_zone_categories_sept2025", value = analysis_z_category_final, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_zone_categories_sept2025"
# indicator <- "Allowable zone categories for significantly damaged properties in each area, e.g., % of significantly damaged properties in West Altadena with a R-1 (single family residence) allowable use"
# source <- "Source: LA County Assessor Data, September 2025 & LA County plannning data"
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_zones.docx"
# column_names <- colnames(analysis_z_category_final) # Get column names
# column_names
# column_comments <- c(
#   "area name",
#   "zoning category short label",
#   "zoning category long label",
#   "count of properties in the area within the zone",
#   "prc of properties in the area within the zone",
#   "total properties in the zone")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

### Step 5: Calculate the zones within R-1 #####
# The percentage of damaged properties within R-1 by lot size minimums
# can't find specific information on the zones, but I believe they are based on what the minimum lot size should be
# other municipality in LA County https://codelibrary.amlegal.com/codes/baldwinpark/latest/baldwinpark_ca/0-0-0-9435
# https://altadenatowncouncil.org/documents/Altadena_CSD.pdf
# each lot size has it's own requirements
analysis_zone_e_w <- all_df %>%
  filter(z_category=='R-1') %>% # keep just R-1
  group_by(area_name) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,zone) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_zone_alt <- all_df %>%
  mutate(area_name='Altadena') %>%
  filter(z_category=='R-1') %>% # keep just R-1
  group_by(area_name) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,zone) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

analysis_zone <- rbind(analysis_zone_e_w,
                             analysis_zone_alt)


# recode limits
unique(analysis_zone$zone)
# don't recode just specify in table comments and writeup

# clean up for postgres 
analysis_zone_final <- analysis_zone %>%
  select(area_name, zone, count, prc, total) %>%
  rename(zone_count=count,
         zone_prc=prc,
         area_total=total) %>% ungroup()

# Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_zone_r1_sept2025", value = analysis_zone_final, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_zone_r1_sept2025"
# indicator <- "Allowable zone categories and lot sizes for significantly damaged properties in R1 (single family) zones for each area, e.g., % of significantly damaged R-1 properties in West Altadena with a R-1 minimum 7500 lot size use"
# source <- "Source: LA County Assessor Data, September 2025 & LA County plannning data"
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_zones.docx"
# column_names <- colnames(analysis_zone_final) # Get column names
# column_names
# column_comments <- c(
#   "area name",
#   "zone with specific lot size for R-1",
#   "count of properties in the area within the zone",
#   "prc of properties in the area within the zone",
#   "total properties in the zone")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 8: close connection ####
dbDisconnect(con)
