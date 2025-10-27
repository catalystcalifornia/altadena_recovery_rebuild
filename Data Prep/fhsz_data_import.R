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
state_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\OSFM Fire Hazard Severity Zones\\FHSZSRA_23_3\\FHSZSRA_23_3.shp")
local_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\OSFM Fire Hazard Severity Zones\\LRA FHSZ Combined Data 2025.shp")

#check SRID
st_crs(state_shp) #3310
st_crs(local_shp) #3310

#more cleaning 
colnames(state_shp) <- tolower(colnames(state_shp))
colnames(local_shp) <- tolower(colnames(local_shp))

# force to 2d geom for state
state_shp <- st_zm(state_shp)

#### Step 2: Use export function to push to postgres ####
# state
# export_shpfile(con=con, df=state_shp, schema="data",
#                table_name= "state_fhsz_3310",
#                geometry_column = "geometry")

# Comment on table and columns
# schema <- "data"
# table_name <- "state_fhsz_3310"
# indicator <- "Data on STATE Fire Hazard Severity Zones sourced from the CAL FIRE Office of State Fire Marshall in SRID 3310
#             Fire zones for state responsibility areas
#             Imported on 9-11-25
#             Notes: The Fire Hazard Severity Zone map evaluates “hazard,” not “risk”. “Hazard” is based on the physical conditions that create a likelihood and expected fire behavior over a 30 to 50-year period without considering mitigation measures such as home hardening, recent wildfire, or fuel reduction efforts. “Risk” is the potential damage a fire can do to the area under existing conditions, accounting for any modifications such as fuel reduction projects, defensible space, and ignition resistant building construction."
# source <- "Source: https://osfm.fire.ca.gov/what-we-do/community-wildfire-preparedness-and-mitigation/fire-hazard-severity-zones"
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_fhsz.docx"
# column_names <- colnames(state_shp) # Get column names
# column_names <- column_names[column_names != "geometry"] # remove geometry column name from list throws an error when adding column comments
# column_comments <- c(
#   "state responsibility area",
#   "Fire hazard severity zone numbers 1 to 3",
#   "Fire hazard severity zone description  moderate, high, very high"
# )
# 
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

# local
# export_shpfile(con=con, df=local_shp, schema="data",
#                table_name= "local_fhsz_3310",
#                geometry_column = "geometry")

# Comment on table and columns
# schema <- "data"
# table_name <- "local_fhsz_3310"
# indicator <- "Data on LOCAL Fire Hazard Severity Zones sourced from the CAL FIRE Office of State Fire Marshall in SRID 3310
#             Fire zones for local responsibility areas as of March 2025
#             Imported on 9-11-25
#             Notes: The Fire Hazard Severity Zone map evaluates “hazard,” not “risk”. “Hazard” is based on the physical conditions that create a likelihood and expected fire behavior over a 30 to 50-year period without considering mitigation measures such as home hardening, recent wildfire, or fuel reduction efforts. “Risk” is the potential damage a fire can do to the area under existing conditions, accounting for any modifications such as fuel reduction projects, defensible space, and ignition resistant building construction. 
#             These are the local fire zones as of March 2025 Phases 1-4 the OSFM has been completing local fire zones in phases, which seems complete"
# source <- "Source: https://osfm.fire.ca.gov/what-we-do/community-wildfire-preparedness-and-mitigation/fire-hazard-severity-zones"
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_fhsz.docx"
# column_names <- colnames(local_shp) # Get column names
# column_names <- column_names[column_names != "geometry"]
# column_comments <- c(
#   "local responsibility area",
#   "Fire hazard severity zone numbers 1 to 3 and separate category of -3",
#   "Fire hazard severity zone description  moderate, high, very high, -3 represents non-wildland, or areas that dont have any direct threat, non-wildland areas further from distance to wildland increases have lower hazard scores compared to those adjacent to wildland"
# )
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#### Step 3: Test export ####
library(leaflet)
library(htmlwidgets)

local_zones <- st_read(con, query="SELECT * FROM data.local_fhsz_3310", geom="geom")

local_zones <- local_zones %>% filter(fhsz>=1) %>% st_transform(4326)

colorpal <- colorFactor(palette = "YlOrRd", domain = local_zones$fhsz_descr)

# map zones by color to check import and conversion
leaflet () %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  addPolygons(data=local_zones,
              fillColor=~colorpal(fhsz_descr),
              fillOpacity=0.7,
              color="white",
              weight=1,
              popup=~fhsz_descr)
# checks out

state_zones <- st_read(con, query="SELECT * FROM data.state_fhsz_3310", geom="geom")

state_zones <- state_zones %>% st_transform(4326)
st_crs(state_zones)

colorpal <- colorFactor(palette = "YlOrRd", domain = state_zones$fhsz_descr)

# map zones by color to check import and conversion
leaflet () %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  addPolygons(data=state_zones,
              fillColor=~colorpal(fhsz_descr),
              fillOpacity=0.7,
              color="white",
              weight=1,
              popup=~fhsz_descr)
# checks out




#### Step 4: Filtering just within Eaton Fire Perimeter and converting to 4326 for mapping ####
eaton_fire <- st_read(con, query="SELECT * FROM data.eaton_fire_prmtr_3310", geom="geom")
eaton_fhsz_local <- local_shp %>% 
  filter(fhsz>=1) %>%
  filter(st_intersects(geometry, eaton_fire, sparse = FALSE)[,1]) %>% st_transform(4326)

# colorpal <- colorFactor(palette = "YlOrRd", domain = eaton_fhsz_local$fhsz_descr)
# leaflet () %>%
#   addProviderTiles(providers$CartoDB.Positron) %>%
#   addPolygons(data=eaton_fhsz_local,
#               fillColor=~colorpal(fhsz_descr),
#               fillOpacity=0.7,
#               color="white",
#               weight=1,
#               popup=~fhsz_descr)

eaton_fhsz_state <- state_shp %>% 
  filter(st_intersects(geometry, eaton_fire, sparse = FALSE)[,1]) %>% st_transform(4326)

# colorpal <- colorFactor(palette = "YlOrRd", domain = eaton_fhsz_state$fhsz_descr)
# leaflet () %>%
#   addProviderTiles(providers$CartoDB.Positron) %>%
#   addPolygons(data=eaton_fhsz_state,
#               fillColor=~colorpal(fhsz_descr),
#               fillOpacity=0.7,
#               color="white",
#               weight=1,
#               popup=~fhsz_descr)


# # state
#  export_shpfile(con=con, df=eaton_fhsz_state, schema="data",
#                 table_name= "eaton_state_fhsz_4326",
#                 geometry_column = "geometry")
# 
# # Comment on table and columns
#  schema <- "data"
#  table_name <- "eaton_state_fhsz_4326"
#  indicator <- "Data on STATE Fire Hazard Severity Zones that intersects with Eaton Fire Perimeter sourced from the CAL FIRE Office of State Fire Marshall in SRID 4326
#              Fire zones for state responsibility areas
#              Imported on 9-11-25
#              Notes: The Fire Hazard Severity Zone map evaluates “hazard,” not “risk”. “Hazard” is based on the physical conditions that create a likelihood and expected fire behavior over a 30 to 50-year period without considering mitigation measures such as home hardening, recent wildfire, or fuel reduction efforts. “Risk” is the potential damage a fire can do to the area under existing conditions, accounting for any modifications such as fuel reduction projects, defensible space, and ignition resistant building construction."
#  source <- "Source: https://osfm.fire.ca.gov/what-we-do/community-wildfire-preparedness-and-mitigation/fire-hazard-severity-zones"
#  qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_fhsz.docx"
#  column_names <- colnames(state_shp) # Get column names
#  column_names <- column_names[column_names != "geometry"] # remove geometry column name from list throws an error when adding column comments
#  column_comments <- c(
#    "state responsibility area",
#    "Fire hazard severity zone numbers 1 to 3",
#    "Fire hazard severity zone description  moderate, high, very high"
#  )
# 
#  add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
# 
# # local
#  export_shpfile(con=con, df=eaton_fhsz_local, schema="data",
#                 table_name= "eaton_local_fhsz_4326",
#                 geometry_column = "geometry")
# 
# # Comment on table and columns
#  schema <- "data"
#  table_name <- "eaton_local_fhsz_4326"
#  indicator <- "Data on LOCAL Fire Hazard Severity Zones that intersects with Eaton Fire perimeter sourced from the CAL FIRE Office of State Fire Marshall in SRID 4326
#              Fire zones for local responsibility areas as of March 2025
#              Imported on 9-11-25
#              Notes: The Fire Hazard Severity Zone map evaluates “hazard,” not “risk”. “Hazard” is based on the physical conditions that create a likelihood and expected fire behavior over a 30 to 50-year period without considering mitigation measures such as home hardening, recent wildfire, or fuel reduction efforts. “Risk” is the potential damage a fire can do to the area under existing conditions, accounting for any modifications such as fuel reduction projects, defensible space, and ignition resistant building construction.
#              These are the local fire zones as of March 2025 Phases 1-4 the OSFM has been completing local fire zones in phases, which seems complete"
#  source <- "Source: https://osfm.fire.ca.gov/what-we-do/community-wildfire-preparedness-and-mitigation/fire-hazard-severity-zones"
#  qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_fhsz.docx"
#  column_names <- colnames(local_shp) # Get column names
#  column_names <- column_names[column_names != "geometry"]
#  column_comments <- c(
#    "local responsibility area",
#    "Fire hazard severity zone numbers 1 to 3 and separate category of -3",
#    "Fire hazard severity zone description  moderate, high, very high, -3 represents non-wildland, or areas that dont have any direct threat, non-wildland areas further from distance to wildland increases have lower hazard scores compared to those adjacent to wildland"
#  )
#  add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
# 
#  