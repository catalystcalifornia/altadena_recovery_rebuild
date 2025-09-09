## Importing Fire Hazard Severity Zones Data 
## DATA SOURCE: Fire Hazard Severity Zones (Local and State)
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_import_fhsz.docx

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
state_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\SRA FHSZ Data Effective 04.01.2024.shp")
local_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\LRA FHSZ Combined Data 2025.shp")
  
#check SRID
st_crs(state_shp) #3310
st_crs(local_shp) #3310

#more cleaning 
colnames(state_shp) <- tolower(colnames(state_shp))
colnames(local_shp) <- tolower(colnames(local_shp))


#### Step 2: Use export function to push to postgres ####

export_shpfile(con=con, df=state_shp, schema="data",
               table_name= "state_fhsz_3310",
               geometry_column = "geometry")

dbSendQuery(con, "COMMENT ON TABLE data.state_fhsz_3310 IS
            'Data on STATE Fire Hazard Severity Zones sourced from the CAL FIRE in SRID 3310
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_fhsz.docx'")
#FAILED ABOVE

export_shpfile(con=con, df=local_shp, schema="data",
               table_name= "local_fhsz_3310",
               geometry_column = "geometry")


dbSendQuery(con, "COMMENT ON TABLE data.local_fhsz_3310 IS
            'Data on LOCAL Fire Hazard Severity Zones sourced from the CAL FIRE in SRID 3310
            QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_fhsz.docx'")
