# Try to join the assessor data to the damage inspection data

# Library and environment set up ----

library(sf)
library(rmapshaper)
library(leaflet)
library(htmlwidgets)
library(tigris)
library(stringr)
# library(gt)
# library(showtext)
# library(extrafont)
# library(readxl)
library(tidyverse)
library(janitor)
# library(ggplot2)

options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")
con_rda <- connect_to_db("rda_shared_data")
con_fires <- connect_to_db("la_fires")

# Load in data/shapes ----
# eaton fire share
eaton_fire <- st_read(con_alt, query="SELECT * FROM data.eaton_fire_prmtr_3310", geom="geom")

# get damage inspection database
eaton_damage <- st_read(con_alt, query="SELECT * FROM data.eaton_fire_dmg_insp_3310", geom="geom")

# reduce columns for joins
eaton_damage_reduced <- eaton_damage %>% select(1:8,
                                                community,
                                                structure_type,structure_category,
                                                units_in_structure_if_multi_unit,
                                                apn_parcel, year_built_parcel,site_address_parcel,assessed_improved_value_parcel)
  
# get city boundaries from tigris for mapping
lac_places <- places(state="CA", year=2023) %>% filter(NAME %in% c("Altadena","Pasadena"))
st_crs(lac_places)
lac_places <- st_transform(lac_places,3310)

# get assessor parcels
assessor_parcels <- st_read(con_alt, query="Select * from data.assessor_parcels_jan2025", geom="geom")

# get assessor data
assessor_data <- st_read(con_alt, query="Select * from data.assessor_data_jan2025")


# Try joining calfire data to parcel shapes point to polygon ----
## set projections
st_crs(assessor_parcels) # set to 3310
st_crs(eaton_damage) # good

assessor_parcels <- st_transform(assessor_parcels, 3310)

## Perform the spatial join
joined_points <- st_join(eaton_damage_reduced, assessor_parcels, join=st_within, left=TRUE)

na_assessor <- joined_points %>% filter(is.na(ain))

# check the ones that didn't join


lac_places_4326 <- st_transform(lac_places, 4326)
na_assessor_4326 <- st_transform(na_assessor, 4326)
assessor_parcels_4326 <- st_transform(assessor_parcels, 4326)

leaflet () %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  setView(-118.104631, 34.185104, zoom = 12) %>%
  addPolygons(data=assessor_parcels_4326,
              fillColor="white",
              fillOpacity=0,
              color="purple",
              weight=1,
              opacity=1,
              popup=~ain,
              group="Parcels") %>%
  addPolygons(data=lac_places_4326,
              fillColor="white",
              fillOpacity=0,
              color="green",
              weight=1.5,
              opacity=1,
              popup=~NAME,
              group="Cities") %>%
  addCircleMarkers(data=na_assessor_4326,
                   # lng = ~longitude,
                   # lat = ~latitude,
                   # fillColor = ~pal(damage_ordered), # Color based on category
                   radius = 2,
                   stroke=TRUE,
                   # fill=TRUE,
                   fillOpacity = 1,
                   color="black",
                   weight=.5,
                   opacity=.5,
                   popup = ~paste0(damage,"</br>",
                                   "Parcel #: ", apn_parcel, "</br>",
                                   "Structure Category: ", structure_category, "</br>",
                                   "Address: ", street_number, street_name, "</br>",
                                   "City: ", city),
                   group="Structures Missing Assessor Data"
  ) %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Structures Missing Assessor Data"),
    options = layersControlOptions(collapsed = FALSE)
  )


table(na_assessor$structure_category,useNA='always')
# most are single residence

table(na_assessor$structure_type,useNA='always')
# most are single family residence single story and multi story -- let's focus on where the residential parcels are

table(na_assessor$city,useNA='always')
# most are in arcadia and altadena

na_assessor %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city)
# most residential are in arcadia

length(unique(na_assessor$apn_parcel)) #233 unique parcel numbers
sum(is.na(na_assessor$apn_parcel)) # no blank parcel numbers, we can try joining by parcel number

joined_name <- na_assessor %>% select(1:16) %>% left_join(assessor_parcels%>%st_drop_geometry(), by=c("apn_parcel"="ain"))
sum(is.na(joined_name$objectid)) # only a handful joined

na_assessor_step2 <- joined_name %>% filter(is.na(objectid))

# check the ones that didn't join focus on residential
na_assessor_step2_residential <- na_assessor_step2 %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% st_transform(4326)

leaflet () %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  setView(-118.104631, 34.185104, zoom = 12) %>%
  addPolygons(data=assessor_parcels_4326,
              fillColor="white",
              fillOpacity=0,
              color="purple",
              weight=1,
              opacity=1,
              popup=~ain,
              group="Parcels") %>%
  addPolygons(data=lac_places_4326,
              fillColor="white",
              fillOpacity=0,
              color="green",
              weight=1.5,
              opacity=1,
              popup=~NAME,
              group="Cities") %>%
  addCircleMarkers(data=na_assessor_step2_residential,
                   # lng = ~longitude,
                   # lat = ~latitude,
                   # fillColor = ~pal(damage_ordered), # Color based on category
                   radius = 2,
                   stroke=TRUE,
                   # fill=TRUE,
                   fillOpacity = 1,
                   color="black",
                   weight=.5,
                   opacity=.5,
                   popup = ~paste0(damage,"</br>",
                                   "Parcel #: ", apn_parcel, "</br>",
                                   "Structure Category: ", structure_category, "</br>",
                                   "Address: ", street_number, street_name, "</br>",
                                   "City: ", city),
                   group="Structures Missing Assessor Data"
  ) %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Structures Missing Assessor Data"),
    options = layersControlOptions(collapsed = FALSE)
  )


# try a full address join

assessor_data <- assessor_data %>%
  mutate(zipcode=gsub("0000", "", zip)) %>%
  mutate(city=gsub(" CA ", ", CA", zip)) %>%
  mutate(site_address=paste0(situs_house_no, direction, street_name, ", ", city_state, zipcode)) %>%
  mutate(site_address=gsub("\\s+", " ", site_address)) %>%
  mutate(site_address=gsub(" , ", ", ", site_address))
  
assessor_data %>%
  select(zipcode,site_address,situs_house_no:zip) %>%
  View()

na_assessor_step2 <- na_assessor_step2 %>%
  select(1:16) %>%
  left_join(assessor_data %>%
              mutate(site_address=paste(situs_house_no, direction, street_name, city_state, zip)))
