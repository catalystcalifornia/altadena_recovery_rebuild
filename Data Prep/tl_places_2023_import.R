# Import 2023 tiger lines for CA places to rda_shared and 
# import targeted places/cities (Altadena, Pasadena, Sierra Madre, Arcadia)
# to Altadena Recovery and Rebuild database

library(sf)
library(tigris)
library(mapview)

# Can use this to qa the rda_shared table
downloaded_filepath <- "W:\\Data\\Geographies\\tl_2023_06_place\\tl_2023_06_place.shp"

source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")
con_rda <- connect_to_db("rda_shared_data")

target_cities <- c("Altadena","Pasadena","Sierra Madre","Arcadia")

ca_places <- places(state="CA",
                    cb=FALSE, # to get tiger lines
                    year=2023) 

st_crs(ca_places) #4269
ca_places_3310 <- st_transform(ca_places, 3310)

# ## Export to rda_shared
# export_shpfile(con=con_rda,
#                df=ca_places_3310,
#                schema="geographies_ca",
#                table_name="tl_2023_06_places",
#                geometry_column = "geometry")
# 
# dbSendQuery(con_rda, "COMMENT ON TABLE geographies_ca.tl_2023_06_places IS
#             '2023 CA Places Tiger Lines in SRID 3310
#             Imported on 9-27-25
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_tl_places_2023.docx'")



target_places <- st_read(con_rda, query="SELECT * FROM geographies_ca.tl_2023_06_places") %>% 
  filter(NAME %in% target_cities)

# Confirm SRID is 3310
st_crs(target_places) #3310

# Confirm shapes look ok
mapview(target_places)

# ## Export to Altadena DB
# export_shpfile(con=con,
#                df=target_places,
#                schema="data",
#                table_name="tl_2023_places",
#                geometry_column = "geom")
# 
# dbSendQuery(con, "COMMENT ON TABLE data.tl_2023_places IS
#             '2023 Tiger Lines for cities: Altadena, Arcadia, Pasadena, and Sierra Madre (SRID 3310)
#             Imported on 9-27-25
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_tl_places_2023.docx'")

