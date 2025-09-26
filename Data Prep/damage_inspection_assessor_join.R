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
library(readxl)
library(tidyverse)
library(janitor)
library(mapview)
library(writexl)

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
                                                apn_parcel, year_built_parcel,site_address_parcel,assessed_improved_value_parcel) %>%
  mutate(street_address=paste0(street_number, " ", street_name, " ", street_type))

  
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

# includes duplicate records, explore those
joined_points_dup_rows <- joined_points[joined_points$din_id %in% joined_points$din_id[duplicated(joined_points$din_id)], ]
length(unique(joined_points_dup_rows$ain)) #218 ains
length(unique(joined_points_dup_rows$din_id)) #86 unique structures


mapview(joined_points_dup_rows)
# essentially condos can have overlapping parcel shapes so some condo joined to multiple shapes -- keep records where the ain equals the apn

joined_points_dedup_rows <- joined_points_dup_rows %>%
  group_by(din_id) %>%
  filter(apn_parcel==ain) %>%
  ungroup()

length(unique(joined_points_dedup_rows$din_id)) #75 that end up joining well so keep these

joined_points_fail <- joined_points_dup_rows %>%
  filter(!din_id %in% joined_points_dedup_rows$din_id)
length(unique(joined_points_fail$din_id)) #11 - 11+75=86 so back to the original unique structures 

# null out ones that did not join well so they get carried into next step
joined_points <- joined_points %>%
  mutate(across(all_of(19:46), ~ if_else(din_id %in% joined_points_fail$din_id, NA, .)))

# null out the duplicates that we need to bind to the dataframe
joined_points <- joined_points %>%
  mutate(across(all_of(19:46), ~ if_else(din_id %in% joined_points_dedup_rows$din_id, NA, .)))

# dedup
joined_points  <- joined_points [!duplicated(joined_points ), ]

# now add in the dups that we cleaned up
joined_points <- rbind(joined_points %>%
  filter(!din_id %in% joined_points_dedup_rows$din_id),
  joined_points_dedup_rows)

nrow(eaton_damage_reduced)
nrow(joined_points)
# back to original

## Check the ones that didn't join ----
na_assessor_points <- joined_points %>% filter(is.na(ain))
nrow(na_assessor_points) #298 didn't join

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
# most are in altadena count 186

# check those that are residential more
na_assessor_points %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city)
# most residential are in Altadena

length(unique(na_assessor_points$apn_parcel)) #121 unique parcel numbers
sum(is.na(na_assessor_points$apn_parcel)) # no blank parcel numbers, we can try joining by parcel number

##############################################################################
# STEP 2: Joining Calfire DIN data to parcel shapes NAME apn to ain join ----
# join by apn and ain
joined_name <- na_assessor_points %>% select(1:18) %>% left_join(assessor_parcels%>%st_drop_geometry(), keep=TRUE, by=c("apn_parcel"="ain"))
sum(is.na(joined_name$ain)) # only a handful joined still 291 missing
nrow(joined_name)
nrow(na_assessor_points)
# count the same

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
# most are in altadena count 180

# check those that are residential more
na_assessor_name %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city)
# most residential are in Altadena

length(unique(na_assessor_name$apn_parcel)) #114 unique parcel numbers
sum(is.na(na_assessor_name$site_address_parcel)) # no blank addresses

##############################################################################
# STEP 3: Joining Calfire DIN data to parcel shapes SITE ADDRESS site address join ----
# try a full address join
joined_site_address <- na_assessor_name %>%
  select(1:18) %>%
  left_join(assessor_data, keep=TRUE, by=c("site_address_parcel"="site_address"))

# check the ones that had many to many join
joined_site_dup_rows <- joined_site_address[joined_site_address$din_id %in% joined_site_address$din_id[duplicated(joined_site_address$din_id)], ]
View(joined_site_dup_rows)

unique(joined_site_dup_rows$ain) #two ains joined
unique(joined_site_dup_rows$site_address_parcel) # one site

assessor_data_dup_search <- assessor_data %>%
  filter(str_detect(site_address, "CANYON CREST RD"))

# examine these AINs
# 5830009022 5830009026 online

mapview(joined_site_dup_rows)

unique(joined_site_dup_rows$apn)

# https://portal.assessor.lacounty.gov/parceldetail/5830009023
# these aren't joining to the right parcels -- they are joining to parcels with the same address but that are different in location and characteristics
# zero out these as not joining since they are joining to the wrong parcels
joined_site_address <- joined_site_address %>%
  mutate(across(all_of(19:154), ~ if_else(din_id %in% joined_site_dup_rows$din_id, NA, .)))

# dedup
joined_site_address <- joined_site_address[!duplicated(joined_site_address), ]

## Check the ones that didn't join ------
na_assessor_address <- joined_site_address  %>% filter(is.na(ain))
nrow(na_assessor_address) #280 still not joining


table(na_assessor_address$structure_category,useNA='always')
# most are infrastructure, >50 single family residence

table(na_assessor_address$structure_type,useNA='always')
# most are school, >30 still single family residence

table(na_assessor_address$city.x,useNA='always')
# Most are in altadena 171

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

# most seem like holes in the parcel data - vacant land or other

# Export for Hillary to check for more matches in original assessor data
na_assessor_final <- na_assessor_address %>% select(1:17)
# write_xlsx(na_assessor_final, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Prepped\\missing_parcels_excel.csv") # this one exported on 9-25-25
# write_xlsx(na_assessor_final, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Prepped\\missing_jan_parcels_092625.xlsx") 


# Pull together and clean up joins -----
joined_structures <- rbind(joined_points %>% 
                             filter(!is.na(ain)) %>%
                                      select(1:18, ain),
                           joined_name %>% 
                             filter(!is.na(ain)) %>%
                                      select(1:18,ain ),
                    joined_site_address %>%
                             filter(!is.na(ain)) %>% 
                            rename_with(~ gsub("\\.x", "", .x)) %>%   # Remove .x from column names
                      select(1:18,ain)) 

nrow(eaton_damage_reduced)-nrow(joined_structures) # difference is 280, the number in the exported missing data


# Map data together for QA -----
joined_structures_residential <- joined_structures %>%
  left_join(assessor_data %>% 
              mutate(ain=as.character(ain)) %>%
              select(ain, use_code, site_address, ends_with("_units"), ends_with("_square_feet")), recording_date,
            by="ain") %>%
  select(-landlord_units) %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
         total_sq_ft = rowSums(across(ends_with("_square_feet")))) %>%
           filter(str_detect(use_code, "^0")) %>%                       
         filter(grepl("ALTADENA",site_address))

assessor_parcels_map<- assessor_parcels_4326 %>%
left_join(assessor_data %>% 
            mutate(ain=as.character(ain)) %>%
            select(ain, use_code, site_address, ends_with("_units"), ends_with("_square_feet")), recording_date,
          by="ain")%>%
  select(-landlord_units) %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
         total_sq_ft = rowSums(across(ends_with("_square_feet"))))

df_map <- joined_structures_residential %>% st_transform(4326)

assessor_parcels_map <-assessor_parcels_map %>%
  filter(grepl("ALTADENA",site_address))
  
pal <- colorFactor(palette = "Dark2", domain = df_map$structure_category)

full_map <- leaflet () %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  setView(-118.1539581, 34.1924798, zoom = 14) %>%
  addPolygons(data=assessor_parcels_map,
              fillColor="white",
              fillOpacity=0,
              color="purple",
              weight=1,
              opacity=1,
              popup=~paste0("Assessor AIN: ", ain, "</br>",
              "Assessor Use Code: ", use_code, "</br>",
              "Assessor Address: ", site_address, "</br>",
              "Assessor Total Parcel Units: ", total_units, "</br>",
              "Assessor Total Sq Ft: ", total_sq_ft),
              group="Parcels") %>%
  addPolygons(data=lac_places_4326,
              fillColor="white",
              fillOpacity=0,
              color="green",
              weight=1.5,
              opacity=1,
              popup=~NAME,
              group="Cities") %>%
  addCircleMarkers(data=df_map,
                   radius = 2,
                   stroke=TRUE,
                   fillColor = ~pal(structure_category), # Color based on category
                   fillOpacity = 1,
                   color="black",
                   weight=.5,
                   opacity=.5,
                   popup = ~paste0(damage,"</br>",
                                   "CalFire APN #: ", apn_parcel, "</br>",
                                   "CalFire Structure Category: ", structure_category, "</br>",
                                   "CalFire Site Address: ", site_address_parcel, "</br>",
                                   "CalFire Street Address: ", street_address, "</br>",
                                   "Assessor AIN: ", ain, "</br>",
                                   "Assessor Use Code: ", use_code, "</br>",
                                   "Assessor Address: ", site_address, "</br>",
                                   "Assessor Total Parcel Units: ", total_units, "</br>",
                                   "Assessor Total Sq Ft: ", total_sq_ft),
                   group="Matched CalFire Structures to Assessor Data"
  ) %>%
  addLegend(pal = pal, values = df_map$structure_category, title = "Structure Category") %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
    options = layersControlOptions(collapsed = FALSE)
  )

saveWidget(full_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\full_map.html", selfcontained = TRUE)

## Sample sets of structures for QA ------
# randomly sample and then map a set of 20 properties 4 times
# Get a random sample of unique ains
sampled_ids <- sample(unique(df_map$ain), 20)

# Filter the data frame to keep rows with the sampled ains
sample_jz <- df_map[df_map$ain %in% sampled_ids, ]

sample_jz <- sample_jz %>% arrange(ain)

pal <- colorFactor(palette = "Dark2", domain = sample_jz$structure_category)

sample_jz_map <- leaflet () %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  setView(-118.1539581, 34.1924798, zoom = 14) %>%
  addPolygons(data=assessor_parcels_map,
              fillColor="white",
              fillOpacity=0,
              color="purple",
              weight=1,
              opacity=1,
              popup=~paste0("Assessor AIN: ", ain, "</br>",
                            "Assessor Use Code: ", use_code, "</br>",
                            "Assessor Address: ", site_address, "</br>",
                            "Assessor Total Parcel Units: ", total_units, "</br>",
                            "Assessor Total Sq Ft: ", total_sq_ft),
              group="Parcels") %>%
  addPolygons(data=lac_places_4326,
              fillColor="white",
              fillOpacity=0,
              color="green",
              weight=1.5,
              opacity=1,
              popup=~NAME,
              group="Cities") %>%
  addCircleMarkers(data=sample_jz,
                   radius = 4,
                   stroke=TRUE,
                   fillColor = ~pal(structure_category), # Color based on category
                   fillOpacity = 1,
                   color="black",
                   weight=.5,
                   opacity=.5,
                   popup = ~paste0(damage,"</br>",
                                   "CalFire APN #: ", apn_parcel, "</br>",
                                   "CalFire Structure Category: ", structure_category, "</br>",
                                   "CalFire Address: ", site_address_parcel, "</br>",
                                   "CalFire Street Address: ", street_address, "</br>",
                                   "Assessor AIN: ", ain, "</br>",
                                   "Assessor Use Code: ", use_code, "</br>",
                                   "Assessor Address: ", site_address, "</br>",
                                   "Assessor Total Parcel Units: ", total_units, "</br>",
                                   "Assessor Total Sq Ft: ", total_sq_ft),
                   group="Matched CalFire Structures to Assessor Data"
  ) %>%
  addLegend(pal = pal, values = sample_jz$structure_category, title = "Structure Category") %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
    options = layersControlOptions(collapsed = FALSE)
  )

saveWidget(sample_jz_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\jz_map.html", selfcontained = FALSE)
write_xlsx(sample_jz, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\jz_sample.xlsx") 

## second sample
# remove prior sample
temp_df <- df_map %>% filter(!ain %in% sample_jz$ain)

# Get a random sample of unique ains
sampled_ids <- sample(unique(temp_df$ain), 20)

# Filter the data frame to keep rows with the sampled ains
sample_hk <- df_map[df_map$ain %in% sampled_ids, ]

sample_hk <- sample_hk %>% arrange(ain)

pal <- colorFactor(palette = "Dark2", domain = sample_hk$structure_category)

sample_hk_map <- leaflet () %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  setView(-118.1539581, 34.1924798, zoom = 14) %>%
  addPolygons(data=assessor_parcels_map,
              fillColor="white",
              fillOpacity=0,
              color="purple",
              weight=1,
              opacity=1,
              popup=~paste0("Assessor AIN: ", ain, "</br>",
                            "Assessor Use Code: ", use_code, "</br>",
                            "Assessor Address: ", site_address, "</br>",
                            "Assessor Total Parcel Units: ", total_units, "</br>",
                            "Assessor Total Sq Ft: ", total_sq_ft),
              group="Parcels") %>%
  addPolygons(data=lac_places_4326,
              fillColor="white",
              fillOpacity=0,
              color="green",
              weight=1.5,
              opacity=1,
              popup=~NAME,
              group="Cities") %>%
  addCircleMarkers(data=sample_hk,
                   radius = 4,
                   stroke=TRUE,
                   fillColor = ~pal(structure_category), # Color based on category
                   fillOpacity = 1,
                   color="black",
                   weight=.5,
                   opacity=.5,
                   popup = ~paste0(damage,"</br>",
                                   "CalFire APN #: ", apn_parcel, "</br>",
                                   "CalFire Structure Category: ", structure_category, "</br>",
                                   "CalFire Address: ", site_address_parcel, "</br>",
                                   "CalFire Street Address: ", street_address, "</br>",
                                   "Assessor AIN: ", ain, "</br>",
                                   "Assessor Use Code: ", use_code, "</br>",
                                   "Assessor Address: ", site_address, "</br>",
                                   "Assessor Total Parcel Units: ", total_units, "</br>",
                                   "Assessor Total Sq Ft: ", total_sq_ft),
                   group="Matched CalFire Structures to Assessor Data"
  ) %>%
  addLegend(pal = pal, values = sample_hk$structure_category, title = "Structure Category") %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
    options = layersControlOptions(collapsed = FALSE)
  )

saveWidget(sample_hk_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\hk_map.html", selfcontained = FALSE)
write_xlsx(sample_hk, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\hk_sample.xlsx") 


## third sample - focus on multifamily
temp_df <- df_map %>%
  filter(str_detect(use_code, "^02") |str_detect(use_code, "^03") | str_detect(use_code, "^04") | str_detect(use_code, "^05")) 

# Get a random sample of unique ains
sampled_ids <- sample(unique(temp_df$ain), 20)

# Filter the data frame to keep rows with the sampled ains
sample_mtk <- df_map[df_map$ain %in% sampled_ids, ]

sample_mtk <- sample_mtk %>% arrange(ain)

pal <- colorFactor(palette = "Dark2", domain = sample_mtk$structure_category)

sample_mtk_map <- leaflet () %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  setView(-118.1539581, 34.1924798, zoom = 14) %>%
  addPolygons(data=assessor_parcels_map,
              fillColor="white",
              fillOpacity=0,
              color="purple",
              weight=1,
              opacity=1,
              popup=~paste0("Assessor AIN: ", ain, "</br>",
                            "Assessor Use Code: ", use_code, "</br>",
                            "Assessor Address: ", site_address, "</br>",
                            "Assessor Total Parcel Units: ", total_units, "</br>",
                            "Assessor Total Sq Ft: ", total_sq_ft),
              group="Parcels") %>%
  addPolygons(data=lac_places_4326,
              fillColor="white",
              fillOpacity=0,
              color="green",
              weight=1.5,
              opacity=1,
              popup=~NAME,
              group="Cities") %>%
  addCircleMarkers(data=sample_mtk,
                   radius = 4,
                   stroke=TRUE,
                   fillColor = ~pal(structure_category), # Color based on category
                   fillOpacity = 1,
                   color="black",
                   weight=.5,
                   opacity=.5,
                   popup = ~paste0(damage,"</br>",
                                   "CalFire APN #: ", apn_parcel, "</br>",
                                   "CalFire Structure Category: ", structure_category, "</br>",
                                   "CalFire Address: ", site_address_parcel, "</br>",
                                   "CalFire Street Address: ", street_address, "</br>",
                                   "Assessor AIN: ", ain, "</br>",
                                   "Assessor Use Code: ", use_code, "</br>",
                                   "Assessor Address: ", site_address, "</br>",
                                   "Assessor Total Parcel Units: ", total_units, "</br>",
                                   "Assessor Total Sq Ft: ", total_sq_ft),
                   group="Matched CalFire Structures to Assessor Data"
  ) %>%
  addLegend(pal = pal, values = sample_mtk$structure_category, title = "Structure Category") %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
    options = layersControlOptions(collapsed = FALSE)
  )

saveWidget(sample_mtk_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\mtk_map.html", selfcontained = FALSE)
write_xlsx(sample_mtk, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\mtk_sample.xlsx") 


## fourth sample - focus on condos
temp_df <- df_map %>%
  filter(str_detect(use_code, "C$")) 

sample_emg <- temp_df %>% arrange(ain)

pal <- colorFactor(palette = "Dark2", domain = sample_emg$structure_category)

sample_emg_map <- leaflet () %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  setView(-118.1539581, 34.1924798, zoom = 14) %>%
  addPolygons(data=assessor_parcels_map,
              fillColor="white",
              fillOpacity=0,
              color="purple",
              weight=1,
              opacity=1,
              popup=~paste0("Assessor AIN: ", ain, "</br>",
                            "Assessor Use Code: ", use_code, "</br>",
                            "Assessor Address: ", site_address, "</br>",
                            "Assessor Total Parcel Units: ", total_units, "</br>",
                            "Assessor Total Sq Ft: ", total_sq_ft),
              group="Parcels") %>%
  addPolygons(data=lac_places_4326,
              fillColor="white",
              fillOpacity=0,
              color="green",
              weight=1.5,
              opacity=1,
              popup=~NAME,
              group="Cities") %>%
  addCircleMarkers(data=sample_emg,
                   radius = 4,
                   stroke=TRUE,
                   fillColor = ~pal(structure_category), # Color based on category
                   fillOpacity = 1,
                   color="black",
                   weight=.5,
                   opacity=.5,
                   popup = ~paste0(damage,"</br>",
                                   "CalFire APN #: ", apn_parcel, "</br>",
                                   "CalFire Structure Category: ", structure_category, "</br>",
                                   "CalFire Address: ", site_address_parcel, "</br>",
                                   "CalFire Street Address: ", street_address, "</br>",
                                   "Assessor AIN: ", ain, "</br>",
                                   "Assessor Use Code: ", use_code, "</br>",
                                   "Assessor Address: ", site_address, "</br>",
                                   "Assessor Total Parcel Units: ", total_units, "</br>",
                                   "Assessor Total Sq Ft: ", total_sq_ft),
                   group="Matched CalFire Structures to Assessor Data"
  ) %>%
  addLegend(pal = pal, values = sample_emg$structure_category, title = "Structure Category") %>%
  addLayersControl(
    overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
    options = layersControlOptions(collapsed = FALSE)
  )

saveWidget(sample_emg_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\emg_map.html", selfcontained = FALSE)
write_xlsx(sample_emg, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\emg_sample.xlsx") 

