## Importing Fire Perimeter Data 
## DATA SOURCE: CAL FIRE eGIS July 20, 2025 
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_import_fire_perimeter.docx

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

#Raw Data Source URL: https://gis.data.ca.gov/datasets/CALFIRE-Forestry::california-fire-perimeters-all/explore?filters=eyJGSVJFX05BTUUiOlsiRUFUT04iXX0%3D&location=34.238755%2C-117.902524%2C10.00

#downloaded here
fire_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\California_Historic_Fire_Perimeters_5734478986827458227\\California_Fire_Perimeters_(all).shp")

#check SRID
st_crs(fire_shp) #3857

#transform to 4326/ needed for leaflet visualizations 
fire_shp_4326 <- st_transform(fire_shp, 4326) 

#transform to 3310/ California Albers preferred for CA statistics/analysis
#we first have to filter because transform was taking too long otherwise
eaton <- fire_shp %>%
  filter(FIRE_NAME == "EATON")

colnames(eaton) <- tolower(colnames(eaton))

fire_shp_3310 <- st_transform(eaton, 3310) 

#### Step 2: Use export function to push to postgres ####

# export_shpfile(con=con, df=fire_shp_4326, schema="data", 
#                table_name= "cal_fire_prmtr_4326", 
#                geometry_column = "geometry")

export_shpfile(con=con, df=fire_shp_3310, schema="data",
               table_name= "eaton_fire_prmtr_3310",
               geometry_column = "geometry")

# dbSendQuery(con, "COMMENT ON TABLE data.cal_fire_prmtr_4326 IS 
#             'Fire Perimeter data from CAL FIRE eGIS, July 20, 2025 in SRID 4326. 
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_fire_perimeter.docx'")

dbSendQuery(con, "COMMENT ON TABLE data.eaton_fire_prmtr_3310 IS
            'Fire Perimeter data for the Eaton Fire from CAL FIRE eGIS, July 20, 2025 in SRID 3310
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_fire_perimeter.docx'")
