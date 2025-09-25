# Join the assessor data to the damage inspection data
# create relational tables to identify residential properties
# a relational table with apn/ain crosswalks
# relational tables of addresses
# relational table of damage


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
library(readxl)
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

# add a unique identifier
eaton_damage$din_id <- 1:nrow(eaton_damage)

# reduce columns for joins
eaton_damage_reduced <- eaton_damage %>% select(din_id,1:8,
                                                community,
                                                structure_type,structure_category,
                                                units_in_structure_if_multi_unit,
                                                apn_parcel, year_built_parcel,site_address_parcel,assessed_improved_value_parcel)
  
# get city boundaries from tigris for mapping
lac_places <- places(state="CA", year=2023) %>% filter(NAME %in% c("Altadena","Pasadena","Sierra Madre","Arcadia"))
st_crs(lac_places)
lac_places <- st_transform(lac_places,3310)

# get assessor parcels
assessor_parcels <- st_read(con_alt, query="Select * from data.assessor_parcels_jan2025", geom="geom")

# get assessor data
assessor_data <- st_read(con_alt, query="Select * from data.assessor_data_jan2025")

# Explore the assessor data ----
nrow(assessor_parcels) #85430
length(unique(assessor_parcels$ain)) #85430
nrow(assessor_data) #85465 - slightly more records here
length(unique(assessor_data$ain)) #85465

assessor_data_missing_parcels <- assessor_data %>%
  filter(!ain %in% assessor_parcels$ain)

table(assessor_data_missing_parcels$use_code)
# first character 0 = residential, most are residential, but only 35 properties so not a big deal
# look online
# first parcel has a different number on the map?
# https://portal.assessor.lacounty.gov/parceldetail/5310016001
# this parcel is the same on the map:https://portal.assessor.lacounty.gov/parceldetail/5314026040
# okay to leave out the 35 properties

# create a combined site address field
assessor_data <- assessor_data %>%
  mutate(zipcode=gsub("0000", "", zip)) %>%
  mutate(city=gsub(" CA ", ", CA", city_state)) %>%
  mutate(site_address=paste0(situs_house_no, direction, street_name, ", ", city, zipcode)) %>%
  mutate(site_address=gsub("\\s+", " ", site_address)) %>%
  mutate(site_address=gsub(" , ", ", ", site_address))

assessor_data %>%
  select(zipcode,site_address,situs_house_no:zip) %>%
  View()

##############################################################################
# STEP 1: Joining Calfire DIN data to parcel shapes POINTS to polygon join ----
## set projections
st_crs(assessor_parcels) # set to 3310
st_crs(eaton_damage) # good

assessor_parcels <- st_transform(assessor_parcels, 3310)

## Perform the spatial join ----
joined_points <- st_join(eaton_damage_reduced, assessor_parcels, join=st_within, left=TRUE)

## Check the ones that didn't join ----
na_assessor_points <- joined_points %>% filter(is.na(ain))
nrow(na_assessor_points) #287 didn't join

# prep 4326 layers to explore ones missing data
lac_places_4326 <- st_transform(lac_places, 4326)
na_assessor_4326 <- st_transform(na_assessor_points, 4326)
assessor_parcels_4326 <- st_transform(assessor_parcels, 4326)

# map the data
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
                                   "Address: ", site_address_parcel, "</br>",
                                   "City: ", city),
                   group="Structures Missing Assessor Data - Point Join"
  ) %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Structures Missing Assessor Data - Point Join"),
    options = layersControlOptions(collapsed = FALSE)
  )

# explore the data
table(na_assessor_points$structure_category,useNA='always')
# most are nonresidential commercial, >50 are single residence

table(na_assessor_points$structure_type,useNA='always')
# most are schools, >30 are single family residence single story

table(na_assessor_points$city,useNA='always')
# most are in altadena count 175

# check those that are residential more
na_assessor_points %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city)
# most residential are in Altadena

length(unique(na_assessor_points$apn_parcel)) #120 unique parcel numbers
sum(is.na(na_assessor_points$apn_parcel)) # no blank parcel numbers, we can try joining by parcel number

##############################################################################
# STEP 2: Joining Calfire DIN data to parcel shapes NAME apn to ain join ----
# join by apn and ain
joined_name <- na_assessor_points %>% select(1:17) %>% left_join(assessor_parcels%>%st_drop_geometry(), keep=TRUE, by=c("apn_parcel"="ain"))
sum(is.na(joined_name$ain)) # only a handful joined still 280 missing

na_assessor_name <- joined_name %>% filter(is.na(ain))

## Check the ones that didn't join ------
# focus on residential
na_assessor_name_residential <- na_assessor_name %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% st_transform(4326)

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
  addCircleMarkers(data=na_assessor_name_residential,
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
                                   "Address: ", site_address_parcel, "</br>",
                                   "City: ", city),
                   group="Structures Missing Assessor Data - Name Join"
  ) %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Structures Missing Assessor Data - Name Join"),
    options = layersControlOptions(collapsed = FALSE)
  )

# explore the data
table(na_assessor_name$structure_category,useNA='always')
# most are nonresidential commercial, >50 are single residence

table(na_assessor_name$structure_type,useNA='always')
# most are schools, >30 are single family residence single story

table(na_assessor_name$city,useNA='always')
# most are in altadena count 169

# check those that are residential more
na_assessor_name %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city)
# most residential are in Altadena

length(unique(na_assessor_name$apn_parcel)) #113 unique parcel numbers
sum(is.na(na_assessor_name$site_address_parcel)) # no blank addresses

##############################################################################
# STEP 3: Joining Calfire DIN data to parcel shapes SITE ADDRESS site address join ----
# try a full address join
joined_site_address <- na_assessor_name %>%
  select(1:17) %>%
  left_join(assessor_data, keep=TRUE, by=c("site_address_parcel"="site_address"))

# check the ones that had many to many join
joined_site_dup_rows <- joined_site_address[joined_site_address$din_id %in% joined_site_address$din_id[duplicated(joined_site_address$din_id)], ]
View(joined_site_dup_rows)
unique(joined_site_dup_rows$ain)
unique(joined_site_dup_rows$site_address_parcel)


assessor_data_dup_search <- assessor_data %>%
  filter(str_detect(site_address, "CANYON CREST RD"))

# examine these AINs
# 5830009022 5830009026

library(mapview)

joined_site_dup_rows_4326 <- st_transform
mapview(joined_site_dup_rows)

# https://portal.assessor.lacounty.gov/parceldetail/5830009023
# these aren't joining to the right parcels, parcels should be 
# unique(joined_site_dup_rows$apn) # zero out these as not joining since they are joining to the wrong parcels
joined_site_address <- joined_site_address %>%
  mutate(across(all_of(18:153), ~ if_else(din_id %in% joined_site_dup_rows$din_id, NA, .)))

# dedup
joined_site_address <- joined_site_address[!duplicated(joined_site_address), ]

## Check the ones that didn't join ------
na_assessor_address <- joined_site_address  %>% filter(is.na(ain))
nrow(na_assessor_address) #269 still not joining


table(na_assessor_address$structure_category,useNA='always')
# most are infrastructure, >50 single family residence

table(na_assessor_address$structure_type,useNA='always')
# most are school, >30 still single family residence

table(na_assessor_address$city.x,useNA='always')
# Most are in altadena 156

na_assessor_address %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city.x)
# most residential are in Altadena

# focus on residential for mapping
na_assessor_address_residential <- na_assessor_address %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% st_transform(4326)

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
  addCircleMarkers(data=na_assessor_address_residential,
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
                                   "Address: ", site_address_parcel, "</br>",
                                   "City: ", city.x),
                   group="Structures Missing Assessor Data - Address Join"
  ) %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Structures Missing Assessor Data - Address Join"),
    options = layersControlOptions(collapsed = FALSE)
  )

# most seem like holes in the parcel data

# Export for Hillary to check
na_assessor_final <- na_assessor_address %>% select(1:17)
library(writexl)
write_xlsx(na_assessor_final, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Prepped\\missing_parcels_excel.csv")


# Pull together and clean up joins -----
joined_structures <- rbind(joined_points %>% 
                             filter(!is.na(ain)) %>%
                                      select(1:17, ain) %>%
                             st_drop_geometry(),
                           joined_name %>% 
                             filter(!is.na(ain)) %>%
                                      select(1:17,ain )%>%
                             st_drop_geometry(),
                    joined_site_address %>%
                             filter(!is.na(ain)) %>% 
                            rename_with(~ gsub("\\.x", "", .x)) %>%   # Remove .x from column names
                           st_drop_geometry() %>%
                      select(1:17,ain)) 

joined_points_dup_rows <- joined_points[joined_points$din_id %in% joined_points$din_id[duplicated(joined_points$din_id)], ]
unique(joined_points_dup_rows$ain)

mapview(joined_points_dup_rows)

joined_points_dedup_rows <- joined_points_dup_rows %>%
  group_by(din_id) %>%
  filter(apn_parcel==ain) %>%
  ungroup()

length(unique(joined_points_dedup_rows$din_id))
