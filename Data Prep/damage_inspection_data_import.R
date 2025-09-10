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
colnames(fire_shp_3310) <- tolower(colnames(fire_shp_3310))
#### Step 2: Use export function to push to postgres ####

# export_shpfile(con=con, df=fire_shp_3310, schema="data",
#                table_name= "eaton_fire_dmg_insp_3310",
#                geometry_column = "geometry")
# 
# dbSendQuery(con, "COMMENT ON TABLE data.eaton_fire_dmg_insp_3310 IS
#             'Damage Inspection data for the Eaton Fire from CAL FIRE eGIS, June 2, 2025 in SRID 3310
#             Data imported on 9-4-25
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_dmg_insp.docx
#             Source: https://gis.data.ca.gov/datasets/CALFIRE-Forestry::california-fire-perimeters-all/explore'")
