# Join the assessor data to the damage inspection data
# create a crosswalk of the damage inspection data to the january assessor data


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
dins_damage <- st_read(con_alt, query="SELECT * FROM data.eaton_fire_dmg_insp_3310", geom="geom")


# reduce columns for joins
dins_reduced <- dins_damage %>% select(1:9,
                                                community,
                                                structure_type,structure_category,
                                                units_in_structure_if_multi_unit,
                                                apn_parcel, year_built_parcel,site_address_parcel,assessed_improved_value_parcel) %>%
  mutate(street_address=toupper(paste0(street_number, " ", street_name, " ", street_type, ", ", city, ", ", state, " ", zip_code)))

  
# get city boundaries from tigris for mapping
lac_places <- st_read(con_alt, query="Select * from data.tl_2023_places", geom="geom") %>%
  filter(NAME %in% c("Altadena","Pasadena"))
st_crs(lac_places)

# get assessor parcels
assessor_parcels <- st_read(con_alt, query="Select * from data.assessor_parcels_universe_jan2025", geom="geom")

# get assessor data
assessor_data <- st_read(con_alt, query="Select * from data.assessor_data_universe_jan2025")

# Explore the assessor data ----
nrow(assessor_parcels) #43851
length(unique(assessor_parcels$ain)) #43851
nrow(assessor_data) #54865 - slightly more records here
length(unique(assessor_data$ain)) #54865 

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
  mutate(site_address=paste0(situs_house_no, " ", direction, " ", street_name, ", ", city, zipcode)) %>%
  mutate(site_address=gsub("\\s+", " ", site_address)) %>%
  mutate(site_address=gsub(" , ", ", ", site_address))

assessor_data %>%
  select(zipcode,site_address,situs_house_no:zip) %>%
  View()

##############################################################################
# Step 1: Focus on structures in Altadena/Pasadena ----
## set projections
st_crs(dins_reduced) # good
st_crs(lac_places) #good

# select all structures either in the pasadena or altadena boundary
dins_reduced <- st_join(dins_reduced, lac_places%>%select(NAME), join=st_within, left=FALSE)

dins_reduced <- dins_reduced %>% rename(tl_place_name=NAME)

# check
mapview(dins_reduced) +
  mapview(lac_places)
# looks good

##############################################################################
# STEP 2: Split assessor data by condos vs. not condos ----
# create a separate sf object and df for condos as these will require special treatment
# get a df of condos separately
assessor_data_condos <- assessor_data %>%
  filter(str_detect(use_code, "C$") | str_detect(use_code, "E$") ) 

# check
table(assessor_data_condos$use_code)
# excludes residential, commercial, and industrial condos, I think that's okay

# make the assessor data df everything but condos
assessor_data <- assessor_data %>% filter(!ain %in% assessor_data_condos$ain)

# make a condo assessor parcel df object
assessor_parcels_condos <- assessor_parcels %>% filter(ain %in% assessor_data_condos$ain)

# make the assessor parcel df object everything but condos
assessor_parcels <- assessor_parcels %>% filter(!ain %in% assessor_data_condos$ain)

# running the joins separately for condos vs. not condos

##############################################################################
# Part 1: Non-condo data ----
##############################################################################
# we are going to do a point to polygon, then an ain/apn number join, and then a site address join for everything but condos

##############################################################################
## STEP 3: Joining Calfire DIN data to parcel shapes POINTS to polygon join ----
## set projections
st_crs(assessor_parcels) # good
st_crs(dins_reduced) # good

## Perform the spatial join ----
joined_points <- st_join(dins_reduced, assessor_parcels, join=st_within, left=TRUE)

# includes duplicate records, explore those
joined_points_dup_rows <- joined_points[joined_points$din_id %in% joined_points$din_id[duplicated(joined_points$din_id)], ]
length(unique(joined_points_dup_rows$ain)) #10 ains
length(unique(joined_points_dup_rows$din_id)) #14 unique structures
# null these out and carry through to address join instead

mapview(joined_points_dup_rows)
# condos and commercial

# null out ones that are duplicates so we try them in next step
joined_points <- joined_points %>%
  mutate(across(all_of(20:51), ~ if_else(din_id %in% joined_points_dup_rows$din_id, NA, .)))

# dedup
joined_points  <- joined_points [!duplicated(joined_points ), ]

nrow(dins_reduced)
nrow(joined_points)
# back to original

## Check the ones that didn't join ----
na_assessor_points <- joined_points %>% filter(is.na(ain))
nrow(na_assessor_points) #96 didn't join

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
# most are single residence could be condos

table(na_assessor_points$structure_type,useNA='always')
# most are single family residence multi story

table(na_assessor_points$city,useNA='always')
# most are in altadena count 68

# check those that are residential more
na_assessor_points %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city)
# most residential are in Altadena

length(unique(na_assessor_points$apn_parcel)) #18 unique parcel numbers
sum(is.na(na_assessor_points$apn_parcel)) # no blank parcel numbers, we can try joining by parcel number

##############################################################################
## STEP 4: Joining Calfire DIN data to parcel shapes NAME apn to ain join ----
# join by apn and ain
joined_name <- na_assessor_points %>% select(1:19) %>% left_join(assessor_parcels%>%st_drop_geometry(), keep=TRUE, by=c("apn_parcel"="ain"))
sum(is.na(joined_name$ain)) # only a handful missing still 78
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
# most are residential >50 multiple residence

table(na_assessor_name$structure_type,useNA='always')
# most are multi family residence multi story

table(na_assessor_name$city,useNA='always')
# most are in altadena count 51

# check those that are residential more
na_assessor_name %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city)
# most residential are in Altadena

length(unique(na_assessor_name$apn_parcel)) #10 unique parcel numbers
sum(is.na(na_assessor_name$site_address_parcel)) # no blank addresses

##############################################################################
## STEP 5: Joining Calfire DIN data to parcel shapes SITE ADDRESS site address join ----
# try a full address join
joined_site_address <- na_assessor_name %>%
  select(1:19) %>%
  left_join(assessor_data, keep=TRUE, by=c("site_address_parcel"="site_address"))

nrow(na_assessor_name)
nrow(joined_site_address)
# same number
length(unique(na_assessor_name$din_id))
length(unique(joined_site_address$din_id))
# same number


## Check the ones that didn't join ------
na_assessor_address <- joined_site_address  %>% filter(is.na(ain))
nrow(na_assessor_address) #78 still not joining, could be condos


table(na_assessor_address$structure_category,useNA='always')
# similar counts as last attempt

table(na_assessor_address$structure_type,useNA='always')
# similar counts as last attempt

table(na_assessor_address$city.x,useNA='always')
# similar counts as last attempt

na_assessor_address %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city.x)
# similar counts as last attempt

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

##############################################################################
## STEP 6: Pull together and clean up joins -----
joined_structures_noncondos <- rbind(joined_points %>% 
                             filter(!is.na(ain)) %>%
                                      select(1:19, ain),
                           joined_name %>% 
                             filter(!is.na(ain)) %>%
                                      select(1:19,ain ),
                    joined_site_address %>%
                             filter(!is.na(ain)) %>% 
                            rename_with(~ gsub("\\.x", "", .x)) %>%   # Remove .x from column names
                      select(1:19,ain)) 

nrow(dins_reduced)-nrow(joined_structures_noncondos) # just 78 missing--see if they are condos
length(unique(joined_structures_noncondos$din_id)) # 16975
nrow(joined_structures_noncondos) #same count

##############################################################################
# Part 2: Condo data ----
##############################################################################
# we are going to a street address to site address join

##############################################################################
## STEP 7: Joining UnJoined Calfire DIN data to assessor data by street address ----
## create a full address field
dins_reduced_condos <- dins_reduced %>% filter(!dins_reduced$din_id %in% joined_structures_noncondos$din_id)

## Perform the address join ----
# try a full address join
joined_site_addres_condos <- dins_reduced_condos %>%
  left_join(assessor_data_condos, keep=TRUE, by=c("street_address"="site_address"))

nrow(dins_reduced_condos)
nrow(joined_site_addres_condos)

## Check the ones that didn't join ----
na_assessor_condo_address <- joined_site_addres_condos %>% filter(is.na(ain))
nrow(na_assessor_condo_address) #78 didn't join

unique(na_assessor_condo_address$apn)
# only 10 unique APNs

unique(na_assessor_condo_address$site_address_parcel)

# E Sacramento Street should just be Sacramento st
# E Palm Street should be E Palm St
dins_reduced_condos <- dins_reduced_condos %>%
  mutate(street_address=gsub("E SACRAMENTO STREET", "SACRAMENTO ST", street_address)) %>%
  mutate(street_address=gsub("E PALM STREET", "E PALM ST", street_address))

joined_site_addres_condos <- dins_reduced_condos %>%
  left_join(assessor_data_condos, keep=TRUE, by=c("street_address"="site_address"))

nrow(dins_reduced_condos)
nrow(joined_site_addres_condos)


# joined_dup_rows <- joined_site_addres_condos[joined_site_addres_condos$din_id %in% joined_site_addres_condos$din_id[duplicated(joined_site_addres_condos$din_id)], ]

## Check the ones that didn't join ----
na_assessor_condo_address <- joined_site_addres_condos %>% filter(is.na(ain))
nrow(na_assessor_condo_address) #62 didn't join

unique(na_assessor_condo_address$apn)
# only 9 unique APNs

unique(na_assessor_condo_address$site_address_parcel)
 # Export these and clean later

na_assessor_universe <- na_assessor_condo_address %>% select(1:19)
write_xlsx(na_assessor_universe, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Prepped\\missing_jan_parcels_100125.xlsx")


##############################################################################
## STEP 8: Pull together and clean up join -----
joined_structures_condos <- rbind(joined_site_addres_condos %>% 
                                       filter(!is.na(ain)) %>% 
                                       rename_with(~ gsub("\\.x", "", .x)) %>%   # Remove .x from column names
                                       select(1:19,ain)) 


##########################################################################################
# Export crosswalk of dins to ain numbers --------
final_df <- rbind(joined_structures_noncondos, joined_structures_condos)
nrow(dins_reduced)-nrow(final_df) # just 62 missing--see if they are condos
length(unique(final_df$din_id)) # 16991
nrow(final_df) #same count

table_name <- "crosswalk_dins_assessor_jan2025"
schema <- "data"
indicator <- "Crosswalk of damage inspection database to assessor ain numbers. Each row is a structure in the damage inspection database."
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/damage_inspection_assessor_join.R "
qa_filepath<-"  QA_sheet_relational_tables.docx "
dbWriteTable(con_alt, Id(schema, table_name), final_df,
             overwrite = FALSE, row.names = FALSE)

# # Add metadata 
# column_names <- colnames(final_df) # Get column names
# column_comments <- c('')
# 
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


# # prep 4326 layers to explore ones missing data
# lac_places_4326 <- st_transform(lac_places, 4326)
# na_assessor_4326 <- st_transform(na_assessor_points, 4326)
# assessor_parcels_4326 <- st_transform(assessor_parcels, 4326)
# 
# # map the data
# leaflet () %>%
#   addProviderTiles(providers$CartoDB.Positron) %>%
#   setView(-118.104631, 34.185104, zoom = 12) %>%
#   addPolygons(data=assessor_parcels_4326,
#               fillColor="white",
#               fillOpacity=0,
#               color="purple",
#               weight=1,
#               opacity=1,
#               popup=~ain,
#               group="Parcels") %>%
#   addPolygons(data=lac_places_4326,
#               fillColor="white",
#               fillOpacity=0,
#               color="green",
#               weight=1.5,
#               opacity=1,
#               popup=~NAME,
#               group="Cities") %>%
#   addCircleMarkers(data=na_assessor_4326,
#                    radius = 2,
#                    stroke=TRUE,
#                    # fill=TRUE,
#                    fillOpacity = 1,
#                    color="black",
#                    weight=.5,
#                    opacity=.5,
#                    popup = ~paste0(damage,"</br>",
#                                    "Parcel #: ", apn_parcel, "</br>",
#                                    "Structure Category: ", structure_category, "</br>",
#                                    "Address: ", site_address_parcel, "</br>",
#                                    "City: ", city),
#                    group="Structures Missing Assessor Data - Point Join"
#   ) %>%
#   addLayersControl(
#     overlayGroups = c("Parcels", "Cities","Structures Missing Assessor Data - Point Join"),
#     options = layersControlOptions(collapsed = FALSE)
#   )
# 
# # explore the data
# table(na_assessor_points$structure_category,useNA='always')
# # >300 are single residence could be condos
# 
# table(na_assessor_points$structure_type,useNA='always')
# # most are single family residence multi story
# 
# table(na_assessor_points$city,useNA='always')
# # most are in altadena count 336
# 
# # check those that are residential more
# na_assessor_points %>% filter(structure_category %in% c("Single Residence","Multiple Residence")) %>% count(city)
# # most residential are in Altadena
# 
# length(unique(na_assessor_points$apn_parcel)) #329 unique parcel numbers
# sum(is.na(na_assessor_points$apn_parcel)) # no blank parcel numbers, we can try joining by parcel number
# 
# 
# # Map data together for QA -----
# joined_structures_residential <- joined_structures %>%
#   left_join(assessor_data %>% 
#               mutate(ain=as.character(ain)) %>%
#               select(ain, use_code, site_address, ends_with("_units"), ends_with("_square_feet")), recording_date,
#             by="ain") %>%
#   select(-landlord_units) %>%
#   mutate(total_units = rowSums(across(ends_with("_units"))),
#          total_sq_ft = rowSums(across(ends_with("_square_feet")))) %>%
#            filter(str_detect(use_code, "^0")) %>%                       
#          filter(grepl("ALTADENA",site_address))
# 
# assessor_parcels_map<- assessor_parcels_4326 %>%
# left_join(assessor_data %>% 
#             mutate(ain=as.character(ain)) %>%
#             select(ain, use_code, site_address, ends_with("_units"), ends_with("_square_feet")), recording_date,
#           by="ain")%>%
#   select(-landlord_units) %>%
#   mutate(total_units = rowSums(across(ends_with("_units"))),
#          total_sq_ft = rowSums(across(ends_with("_square_feet"))))
# 
# df_map <- joined_structures_residential %>% st_transform(4326)
# 
# assessor_parcels_map <-assessor_parcels_map %>%
#   filter(grepl("ALTADENA",site_address))
#   
# pal <- colorFactor(palette = "Dark2", domain = df_map$structure_category)
# 
# full_map <- leaflet () %>%
#   addProviderTiles(providers$CartoDB.Positron) %>%
#   setView(-118.1539581, 34.1924798, zoom = 14) %>%
#   addPolygons(data=assessor_parcels_map,
#               fillColor="white",
#               fillOpacity=0,
#               color="purple",
#               weight=1,
#               opacity=1,
#               popup=~paste0("Assessor AIN: ", ain, "</br>",
#               "Assessor Use Code: ", use_code, "</br>",
#               "Assessor Address: ", site_address, "</br>",
#               "Assessor Total Parcel Units: ", total_units, "</br>",
#               "Assessor Total Sq Ft: ", total_sq_ft),
#               group="Parcels") %>%
#   addPolygons(data=lac_places_4326,
#               fillColor="white",
#               fillOpacity=0,
#               color="green",
#               weight=1.5,
#               opacity=1,
#               popup=~NAME,
#               group="Cities") %>%
#   addCircleMarkers(data=df_map,
#                    radius = 2,
#                    stroke=TRUE,
#                    fillColor = ~pal(structure_category), # Color based on category
#                    fillOpacity = 1,
#                    color="black",
#                    weight=.5,
#                    opacity=.5,
#                    popup = ~paste0(damage,"</br>",
#                                    "CalFire APN #: ", apn_parcel, "</br>",
#                                    "CalFire Structure Category: ", structure_category, "</br>",
#                                    "CalFire Site Address: ", site_address_parcel, "</br>",
#                                    "CalFire Street Address: ", street_address, "</br>",
#                                    "Assessor AIN: ", ain, "</br>",
#                                    "Assessor Use Code: ", use_code, "</br>",
#                                    "Assessor Address: ", site_address, "</br>",
#                                    "Assessor Total Parcel Units: ", total_units, "</br>",
#                                    "Assessor Total Sq Ft: ", total_sq_ft),
#                    group="Matched CalFire Structures to Assessor Data"
#   ) %>%
#   addLegend(pal = pal, values = df_map$structure_category, title = "Structure Category") %>%
#   addLayersControl(
#     overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
#     options = layersControlOptions(collapsed = FALSE)
#   )
# 
# saveWidget(full_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\full_map.html", selfcontained = TRUE)
# 
# 
# 
# 
# 
# ## Sample sets of structures for QA ------
# # randomly sample and then map a set of 20 properties 4 times
# # Get a random sample of unique ains
# sampled_ids <- sample(unique(df_map$ain), 20)
# 
# # Filter the data frame to keep rows with the sampled ains
# sample_jz <- df_map[df_map$ain %in% sampled_ids, ]
# 
# sample_jz <- sample_jz %>% arrange(ain)
# 
# pal <- colorFactor(palette = "Dark2", domain = sample_jz$structure_category)
# 
# sample_jz_map <- leaflet () %>%
#   addProviderTiles(providers$CartoDB.Positron) %>%
#   setView(-118.1539581, 34.1924798, zoom = 14) %>%
#   addPolygons(data=assessor_parcels_map,
#               fillColor="white",
#               fillOpacity=0,
#               color="purple",
#               weight=1,
#               opacity=1,
#               popup=~paste0("Assessor AIN: ", ain, "</br>",
#                             "Assessor Use Code: ", use_code, "</br>",
#                             "Assessor Address: ", site_address, "</br>",
#                             "Assessor Total Parcel Units: ", total_units, "</br>",
#                             "Assessor Total Sq Ft: ", total_sq_ft),
#               group="Parcels") %>%
#   addPolygons(data=lac_places_4326,
#               fillColor="white",
#               fillOpacity=0,
#               color="green",
#               weight=1.5,
#               opacity=1,
#               popup=~NAME,
#               group="Cities") %>%
#   addCircleMarkers(data=sample_jz,
#                    radius = 4,
#                    stroke=TRUE,
#                    fillColor = ~pal(structure_category), # Color based on category
#                    fillOpacity = 1,
#                    color="black",
#                    weight=.5,
#                    opacity=.5,
#                    popup = ~paste0(damage,"</br>",
#                                    "CalFire APN #: ", apn_parcel, "</br>",
#                                    "CalFire Structure Category: ", structure_category, "</br>",
#                                    "CalFire Address: ", site_address_parcel, "</br>",
#                                    "CalFire Street Address: ", street_address, "</br>",
#                                    "Assessor AIN: ", ain, "</br>",
#                                    "Assessor Use Code: ", use_code, "</br>",
#                                    "Assessor Address: ", site_address, "</br>",
#                                    "Assessor Total Parcel Units: ", total_units, "</br>",
#                                    "Assessor Total Sq Ft: ", total_sq_ft),
#                    group="Matched CalFire Structures to Assessor Data"
#   ) %>%
#   addLegend(pal = pal, values = sample_jz$structure_category, title = "Structure Category") %>%
#   addLayersControl(
#     overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
#     options = layersControlOptions(collapsed = FALSE)
#   )
# 
# saveWidget(sample_jz_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\jz_map.html", selfcontained = FALSE)
# write_xlsx(sample_jz, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\jz_sample.xlsx") 
# 
# ## second sample
# # remove prior sample
# temp_df <- df_map %>% filter(!ain %in% sample_jz$ain)
# 
# # Get a random sample of unique ains
# sampled_ids <- sample(unique(temp_df$ain), 20)
# 
# # Filter the data frame to keep rows with the sampled ains
# sample_hk <- df_map[df_map$ain %in% sampled_ids, ]
# 
# sample_hk <- sample_hk %>% arrange(ain)
# 
# pal <- colorFactor(palette = "Dark2", domain = sample_hk$structure_category)
# 
# sample_hk_map <- leaflet () %>%
#   addProviderTiles(providers$CartoDB.Positron) %>%
#   setView(-118.1539581, 34.1924798, zoom = 14) %>%
#   addPolygons(data=assessor_parcels_map,
#               fillColor="white",
#               fillOpacity=0,
#               color="purple",
#               weight=1,
#               opacity=1,
#               popup=~paste0("Assessor AIN: ", ain, "</br>",
#                             "Assessor Use Code: ", use_code, "</br>",
#                             "Assessor Address: ", site_address, "</br>",
#                             "Assessor Total Parcel Units: ", total_units, "</br>",
#                             "Assessor Total Sq Ft: ", total_sq_ft),
#               group="Parcels") %>%
#   addPolygons(data=lac_places_4326,
#               fillColor="white",
#               fillOpacity=0,
#               color="green",
#               weight=1.5,
#               opacity=1,
#               popup=~NAME,
#               group="Cities") %>%
#   addCircleMarkers(data=sample_hk,
#                    radius = 4,
#                    stroke=TRUE,
#                    fillColor = ~pal(structure_category), # Color based on category
#                    fillOpacity = 1,
#                    color="black",
#                    weight=.5,
#                    opacity=.5,
#                    popup = ~paste0(damage,"</br>",
#                                    "CalFire APN #: ", apn_parcel, "</br>",
#                                    "CalFire Structure Category: ", structure_category, "</br>",
#                                    "CalFire Address: ", site_address_parcel, "</br>",
#                                    "CalFire Street Address: ", street_address, "</br>",
#                                    "Assessor AIN: ", ain, "</br>",
#                                    "Assessor Use Code: ", use_code, "</br>",
#                                    "Assessor Address: ", site_address, "</br>",
#                                    "Assessor Total Parcel Units: ", total_units, "</br>",
#                                    "Assessor Total Sq Ft: ", total_sq_ft),
#                    group="Matched CalFire Structures to Assessor Data"
#   ) %>%
#   addLegend(pal = pal, values = sample_hk$structure_category, title = "Structure Category") %>%
#   addLayersControl(
#     overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
#     options = layersControlOptions(collapsed = FALSE)
#   )
# 
# saveWidget(sample_hk_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\hk_map.html", selfcontained = FALSE)
# write_xlsx(sample_hk, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\hk_sample.xlsx") 
# 
# 
# ## third sample - focus on multifamily
# temp_df <- df_map %>%
#   filter(str_detect(use_code, "^02") |str_detect(use_code, "^03") | str_detect(use_code, "^04") | str_detect(use_code, "^05")) 
# 
# # Get a random sample of unique ains
# sampled_ids <- sample(unique(temp_df$ain), 20)
# 
# # Filter the data frame to keep rows with the sampled ains
# sample_mtk <- df_map[df_map$ain %in% sampled_ids, ]
# 
# sample_mtk <- sample_mtk %>% arrange(ain)
# 
# pal <- colorFactor(palette = "Dark2", domain = sample_mtk$structure_category)
# 
# sample_mtk_map <- leaflet () %>%
#   addProviderTiles(providers$CartoDB.Positron) %>%
#   setView(-118.1539581, 34.1924798, zoom = 14) %>%
#   addPolygons(data=assessor_parcels_map,
#               fillColor="white",
#               fillOpacity=0,
#               color="purple",
#               weight=1,
#               opacity=1,
#               popup=~paste0("Assessor AIN: ", ain, "</br>",
#                             "Assessor Use Code: ", use_code, "</br>",
#                             "Assessor Address: ", site_address, "</br>",
#                             "Assessor Total Parcel Units: ", total_units, "</br>",
#                             "Assessor Total Sq Ft: ", total_sq_ft),
#               group="Parcels") %>%
#   addPolygons(data=lac_places_4326,
#               fillColor="white",
#               fillOpacity=0,
#               color="green",
#               weight=1.5,
#               opacity=1,
#               popup=~NAME,
#               group="Cities") %>%
#   addCircleMarkers(data=sample_mtk,
#                    radius = 4,
#                    stroke=TRUE,
#                    fillColor = ~pal(structure_category), # Color based on category
#                    fillOpacity = 1,
#                    color="black",
#                    weight=.5,
#                    opacity=.5,
#                    popup = ~paste0(damage,"</br>",
#                                    "CalFire APN #: ", apn_parcel, "</br>",
#                                    "CalFire Structure Category: ", structure_category, "</br>",
#                                    "CalFire Address: ", site_address_parcel, "</br>",
#                                    "CalFire Street Address: ", street_address, "</br>",
#                                    "Assessor AIN: ", ain, "</br>",
#                                    "Assessor Use Code: ", use_code, "</br>",
#                                    "Assessor Address: ", site_address, "</br>",
#                                    "Assessor Total Parcel Units: ", total_units, "</br>",
#                                    "Assessor Total Sq Ft: ", total_sq_ft),
#                    group="Matched CalFire Structures to Assessor Data"
#   ) %>%
#   addLegend(pal = pal, values = sample_mtk$structure_category, title = "Structure Category") %>%
#   addLayersControl(
#     overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
#     options = layersControlOptions(collapsed = FALSE)
#   )
# 
# saveWidget(sample_mtk_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\mtk_map.html", selfcontained = FALSE)
# write_xlsx(sample_mtk, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\mtk_sample.xlsx") 
# 
# 
# ## fourth sample - focus on condos
# temp_df <- df_map %>%
#   filter(str_detect(use_code, "C$")) 
# 
# sample_emg <- temp_df %>% arrange(ain)
# 
# pal <- colorFactor(palette = "Dark2", domain = sample_emg$structure_category)
# 
# sample_emg_map <- leaflet () %>%
#   addProviderTiles(providers$CartoDB.Positron) %>%
#   setView(-118.1539581, 34.1924798, zoom = 14) %>%
#   addPolygons(data=assessor_parcels_map,
#               fillColor="white",
#               fillOpacity=0,
#               color="purple",
#               weight=1,
#               opacity=1,
#               popup=~paste0("Assessor AIN: ", ain, "</br>",
#                             "Assessor Use Code: ", use_code, "</br>",
#                             "Assessor Address: ", site_address, "</br>",
#                             "Assessor Total Parcel Units: ", total_units, "</br>",
#                             "Assessor Total Sq Ft: ", total_sq_ft),
#               group="Parcels") %>%
#   addPolygons(data=lac_places_4326,
#               fillColor="white",
#               fillOpacity=0,
#               color="green",
#               weight=1.5,
#               opacity=1,
#               popup=~NAME,
#               group="Cities") %>%
#   addCircleMarkers(data=sample_emg,
#                    radius = 4,
#                    stroke=TRUE,
#                    fillColor = ~pal(structure_category), # Color based on category
#                    fillOpacity = 1,
#                    color="black",
#                    weight=.5,
#                    opacity=.5,
#                    popup = ~paste0(damage,"</br>",
#                                    "CalFire APN #: ", apn_parcel, "</br>",
#                                    "CalFire Structure Category: ", structure_category, "</br>",
#                                    "CalFire Address: ", site_address_parcel, "</br>",
#                                    "CalFire Street Address: ", street_address, "</br>",
#                                    "Assessor AIN: ", ain, "</br>",
#                                    "Assessor Use Code: ", use_code, "</br>",
#                                    "Assessor Address: ", site_address, "</br>",
#                                    "Assessor Total Parcel Units: ", total_units, "</br>",
#                                    "Assessor Total Sq Ft: ", total_sq_ft),
#                    group="Matched CalFire Structures to Assessor Data"
#   ) %>%
#   addLegend(pal = pal, values = sample_emg$structure_category, title = "Structure Category") %>%
#   addLayersControl(
#     overlayGroups = c("Parcels", "Cities","Matched CalFire Structures to Assessor Data"),
#     options = layersControlOptions(collapsed = FALSE)
#   )
# 
# saveWidget(sample_emg_map, file = "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\emg_map.html", selfcontained = FALSE)
# write_xlsx(sample_emg, "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Maps\\emg_sample.xlsx") 
# 
# # check the ones that had many to many join
# joined_site_dup_rows <- joined_site_address[joined_site_address$din_id %in% joined_site_address$din_id[duplicated(joined_site_address$din_id)], ]
# View(joined_site_dup_rows)
# 
# unique(joined_site_dup_rows$ain) #two ains joined
# unique(joined_site_dup_rows$site_address_parcel) # one site
# 
# assessor_data_dup_search <- assessor_data %>%
#   filter(str_detect(site_address, "CANYON CREST RD"))
# 
# # examine these AINs
# # 5830009022 5830009026 online
# 
# mapview(joined_site_dup_rows)
# 
# unique(joined_site_dup_rows$apn)
# 
# # https://portal.assessor.lacounty.gov/parceldetail/5830009023
# # these aren't joining to the right parcels -- they are joining to parcels with the same address but that are different in location and characteristics
# # zero out these as not joining since they are joining to the wrong parcels
# joined_site_address <- joined_site_address %>%
#   mutate(across(all_of(19:154), ~ if_else(din_id %in% joined_site_dup_rows$din_id, NA, .)))
# 
# # dedup
# joined_site_address <- joined_site_address[!duplicated(joined_site_address), ]
