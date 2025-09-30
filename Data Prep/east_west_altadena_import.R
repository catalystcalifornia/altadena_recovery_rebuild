## Importing East and West Altadena perimeters divided at Lake Avenue
## DATA SOURCE: 2023 TIGER Census Place Boundaries
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_drawing_perimeters.docx

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

#Raw Data Source URL: https://osfm.fire.ca.gov/what-we-do/community-wildfire-preparedness-and-mitigation/fire-hazard-severity-zones

#downloaded here
east_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Shapefiles\\EAST_fire_perimeter_v3.shp")
west_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Shapefiles\\WEST_fire_perimeter_v3.shp")

#check SRID
st_crs(east_shp) #4269
st_crs(west_shp) #4269

#more cleaning 
colnames(east_shp) <- tolower(colnames(east_shp))
colnames(west_shp) <- tolower(colnames(west_shp))

#transform to 3310/ California Albers preferred for CA statistics/analysis
east_shp <- st_transform(east_shp, 3310) 
west_shp <- st_transform(west_shp, 3310) 

#### Step 2: Use export function to push to postgres ####

# export_shpfile(con=con, df=east_shp, schema="data",
#                table_name= "east_altadena_3310",
#                geometry_column = "geometry")
# 
# export_shpfile(con=con, df=west_shp, schema="data",
#                table_name= "west_altadena_3310",
#                geometry_column = "geometry")
# 
# # Comment on table FOR EAST
# dbSendQuery(con, "COMMENT ON TABLE data.east_altadena_3310 IS
#            'Perimeter of East Altadena divided at Lake Avenue in SRID 3310
#             Data imported on 9-30-25
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_drawing_perimeters.docx
#             Source: https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2023&layergroup=Place'")
# 
# # Comment on table FOR WEST
# dbSendQuery(con, "COMMENT ON TABLE data.west_altadena_3310 IS
#            'Perimeter of West Altadena divided at Lake Avenue in SRID 3310
#             Data imported on 9-30-25
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_drawing_perimeters.docx
#             Source: https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2023&layergroup=Place'")
