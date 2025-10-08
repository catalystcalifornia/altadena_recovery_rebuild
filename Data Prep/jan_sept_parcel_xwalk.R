# Create a crosswalk of Jan 2025 and Sept 2025 Altadena parcel data


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

# get assessor parcels and add an identifier column

parcels_jan <- st_read(con_alt, query="Select * from data.assessor_parcels_universe_jan2025", geom="geom")%>%
  mutate(flag="jan")
parcels_sept <- st_read(con_alt, query="Select * from data.assessor_parcels_universe_sept2025", geom="geom")%>%
  mutate(flag="sept")

# get assessor data: not sure if we need this though so commenting out for now

# assessor_jan <- st_read(con_alt, query="Select * from data.assessor_data_universe_jan2025")
# assessor_sept <- st_read(con_alt, query="Select * from data.assessor_data_universe_sept2025")

# double check CRS of both of parcel shapes
st_crs(parcels_jan) #3310 good
st_crs(parcels_sept) #3310 good


# Intersect Jan and Sept parcel shapes-------------------------------------

parcels_join <- st_join(parcels_jan, parcels_sept, join = st_intersects)
class(parcels_join) # check object type is sf data frame which is what we want

# clean up column names for clarity: all .x columns are from the jan parcel df, all .y columns are from the sept parcel df
names(parcels_join) <- gsub("\\.x$", "_jan", names(parcels_join))
names(parcels_join) <- gsub("\\.y$", "_sept", names(parcels_join))

# see how many unique AINs are there for jan and sept: same number

length(unique(parcels_join$ain_jan)) #54874
length(unique(parcels_join$ain_sept)) #54874

# look at result just the AIN column
join_ain<-parcels_join%>%select(ain_jan, ain_sept)

# Pull out rows where teh AINs are matching
same_ain <- parcels_join %>%
  filter(!is.na(ain_jan) & ain_jan == ain_sept)%>%select(ain_jan, ain_sept)

# See how many AINs is that:
length(unique(same_ain$ain_sept)) #54825 

# Now I want to extract the opposite: the rows where the jan ain and sept ain are not matching
not_matching_ain <- parcels_join %>%
  filter(!is.na(ain_jan) & !is.na(ain_sept) & ain_jan != ain_sept)%>%select(ain_jan, ain_sept)

# See how many AINs is that:
length(unique(not_matching_ain$ain_sept)) #54813 


# See if any AINs are NA after the join 

sept_not_joined <- parcels_join %>%
  filter(is.na(ain_sept))%>%View() # None

jan_not_joined <- parcels_join %>%
  filter(is.na(ain_sept))%>%View() # None

# Pull out duplicate AINs and count

duplicate_ain <- parcels_join %>%
  add_count(ain_jan, name = "ain_count_jan") %>%
  add_count(ain_sept, name = "ain_count_sept") %>%
  filter(ain_count_jan > 1| ain_count_sept > 1) %>% 
  select(ain_jan, ain_count_jan, ain_sept, ain_count_sept)

# see how many unique AINs are among duplicates

length(unique(duplicate_ain$ain_jan)) #54817
length(unique(duplicate_ain$ain_sept)) #54817

# map of duplicates
mapview(duplicate_ain)
