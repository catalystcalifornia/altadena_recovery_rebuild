## Importing Fire Perimeter Data 
## DATA SOURCE: CAL FIRE Damage Inspection (DINS) Data June 2, 2025 
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_import_dmg_insp.docx

#### Step 0: Set Up ####
#load libraries
library(dplyr)
library(purrr)
library(stringr)
library(sf)
library(RPostgres)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)


#### Step 1: Pull in raw data and prep ####

#Raw Data Source URL: https://gis.data.cnra.ca.gov/datasets/CALFIRE-Forestry::cal-fire-damage-inspection-dins-data/explore

#downloaded here
fire_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\CAL FIRE Damage Inspection (DINS) Data\\POSTFIRE.shp")

#check SRID
st_crs(fire_shp) #3857

#transform to 3310/ California Albers preferred for CA statistics/analysis
fire_shp_3310 <- st_transform(fire_shp, 3310) 

#rename columns
#1st, pull the column names from csv and view
step1_col_names <- names(read_csv("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\CAL FIRE Damage Inspection (DINS) Data\\col_names.csv", n_max = 0))
View(tibble::tibble(original = step1_col_names))

#2nd, clean column names and view
step2_col_names <- step1_col_names %>%
  # make lowercase
  str_to_lower() %>%
  # delete spaces and punctuation, add underscores but delete trailing underscores 
  str_replace_all("[^a-z0-9]+", "_") %>%
  str_replace_all("^_+|_+$", "")

#3rd, create df to see if cleaning is good and what needs to be renamed
step3_col_names <- as.data.frame(tibble(original = step1_col_names, cleaned = step2_col_names))
View(step3_col_names)

#4th, rename columns as needed
cols_rename <- c(
    "street_type_e_g_road_drive_lane_etc" = "street_type",
    "street_suffix_e_g_apt_23_blding_c" = "street_suffix",
    "incident_number_e_g_caaeu_123456" = "incident_number",
    "if_affected_1_9_where_did_fire_start" = "affected_start",
    "if_affected_1_9_what_started_fire" = "affected_cause",
    "of_damaged_outbuildings_120_sqft" = "n_damaged_outbuildings_120_sqft",
    "of_non_damaged_outbuildings_120_sqft" = "n_non_damaged_outbuildings_120_sqft",
    "distance_residence_to_utility_misc_structure_gt_120_sqft" = "distance_residence_to_utility_misc_structure",
    "globalid" = "global_id"
  )

step4_col_names <- step3_col_names %>%
  mutate(
    cleaned = ifelse(cleaned %in% names(cols_rename),
                     cols_rename[cleaned],
                     cleaned)
  ) %>%
  filter(!cleaned %in% c("objectid", "x", "y")) %>%
  add_row(original = "Geometry", cleaned = "geometry")

#5th, apply to shapefile
names(fire_shp_3310) <- step4_col_names$cleaned

#### Step 2: Use export function to push to postgres ####

export_shpfile(con=con, df=fire_shp_3310, schema="data",
               table_name= "eaton_fire_dmg_insp_3310",
               geometry_column = "geometry")

dbSendQuery(con, "COMMENT ON TABLE data.eaton_fire_dmg_insp_3310 IS
            'Damage Inspection data for the Eaton Fire from CAL FIRE eGIS, June 2, 2025 in SRID 3310
            Data imported on 9-4-25
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_dmg_insp.docx
            Source: https://gis.data.ca.gov/datasets/CALFIRE-Forestry::california-fire-perimeters-all/explore'")

for(i in seq_len(nrow(step4_col_names))) {
  col <- step4_col_names$cleaned[i]
  comment_text <- step4_col_names$original[i]
  
  sql <- sprintf(
    "COMMENT ON COLUMN data.eaton_fire_dmg_insp_3310.%s IS '%s';",
    DBI::dbQuoteIdentifier(con, col),
    comment_text
  )
  
  dbExecute(con, sql)
}
