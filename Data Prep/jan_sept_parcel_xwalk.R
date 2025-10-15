# Create a crosswalk of Jan 2025 and Sept 2025 Altadena parcel data


# Library and environment set up ----

library(sf)
# library(rmapshaper)
# library(leaflet)
# library(htmlwidgets)
# library(tigris)
# library(stringr)
# library(readxl)
# library(tidyverse)
# library(janitor)
library(mapview)
# library(writexl)

options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")
# con_rda <- connect_to_db("rda_shared_data")
# con_fires <- connect_to_db("la_fires")

# get assessor parcels and add an identifier column

parcels_jan <- st_read(con_alt, query="SELECT parcels.ain, parcels.geom, stats.use_code 
                       FROM data.assessor_parcels_universe_jan2025 parcels
                       LEFT JOIN data.assessor_data_universe_jan2025 stats
                       ON parcels.ain=stats.ain") %>%
  mutate(flag="jan") %>%
  mutate(area = st_area(geom))
parcels_sept <- st_read(con_alt, query="SELECT parcels.ain, parcels.geom, stats.use_code 
                       FROM data.assessor_parcels_universe_sept2025 parcels
                       LEFT JOIN data.assessor_data_universe_sept2025 stats
                       ON parcels.ain=stats.ain")%>%
  mutate(flag="sept") %>%
  mutate(area = st_area(geom))

# get assessor data: not sure if we need this though so commenting out for now

# assessor_jan <- st_read(con_alt, query="Select * from data.assessor_data_universe_jan2025")
# assessor_sept <- st_read(con_alt, query="Select * from data.assessor_data_universe_sept2025")

# double check CRS of both of parcel shapes
st_crs(parcels_jan)$epsg #3310 good
st_crs(parcels_sept)$epsg #3310 good


# Intersect Jan and Sept parcel shapes-------------------------------------

parcels_join <- st_join(parcels_jan, parcels_sept, join = st_intersects,
                        suffix = c("_jan", "_sept"))


class(parcels_join) # check object type is sf data frame which is what we want

# Perform checks on the intersect------------------------------------------

# See how many unique AINs are there for jan and sept: same number

length(unique(parcels_join$ain_jan)) #54874
length(unique(parcels_join$ain_sept)) #54874

# look at result just the AIN column:
# This is definitely not a clean 1:1 for most of the AINs. It also seems there might be a many to many relationship
# with the AINs as well. 

# For example, AIN 5317001003 in Jan matches with 5 different Sept AINs, one of which matches 5317001003

join_ain<-parcels_join%>%select(ain_jan, intersect_pct_jan, ain_sept, intersect_pct_sept)
join_ain_filter <- join_ain %>%
  filter(intersect_pct_jan > 80 & intersect_pct_sept > 80)

# See if any AINs are NA after the join 

sept_not_joined <- parcels_join %>%
  filter(is.na(ain_sept))%>%View() # None

jan_not_joined <- parcels_join %>%
  filter(is.na(ain_sept))%>%View() # None

# Pull out rows where the AINs are matching
same_ain <- parcels_join %>%
  filter(!is.na(ain_jan) & ain_jan == ain_sept) %>%
  select(ain_jan, intersect_pct_jan, ain_sept, intersect_pct_sept)

# See how many AINs is that:
length(unique(same_ain$ain_sept)) #54825 

# Now I want to extract the opposite: the rows where the jan ain and sept ain are not matching
not_matching_ain <- parcels_join %>%
  filter(!is.na(ain_jan) & !is.na(ain_sept) & ain_jan != ain_sept) %>%
  select(ain_jan, intersect_pct_jan, ain_sept, intersect_pct_sept)

# See how many AINs is that:
length(unique(not_matching_ain$ain_sept)) #54813 

# Pull out duplicate AINs and count
duplicate_ain <- parcels_join %>%
  add_count(ain_jan, name = "ain_count_jan") %>%
  add_count(ain_sept, name = "ain_count_sept") %>%
  filter(ain_count_jan > 1| ain_count_sept > 1) %>% 
  select(ain_jan, ain_count_jan, ain_sept, ain_count_sept)

# see how many unique AINs are among duplicates
length(unique(duplicate_ain$ain_jan)) #54817
length(unique(duplicate_ain$ain_sept)) #54817

# # map of duplicates - not this is very large and takes long to load
# mapview(duplicate_ain)


# QA Notes: Recommending this approach 

# Start here: find out which shapes are the same in jan and sept
match_parcels <- rbind(parcels_jan, parcels_sept) %>%
  mutate(geom_wkt = st_as_text(geom)) %>%
  st_drop_geometry() %>%
  group_by(geom_wkt) %>%
  mutate(group_count = n()) %>%
  mutate(dupe_id = ifelse(group_count>1,cur_group_id(), NA)) %>%
  ungroup() 

# See which shapes have a matching ain (or not)
match_parcels_wide <- match_parcels %>%
  select(-area) %>%
  pivot_wider(
    names_from = flag,
    values_from = flag,
    names_prefix = "flag_"
  ) %>%
  mutate(
    flag_jan = ifelse(flag_jan =="jan", 1, 0),
    flag_sept = ifelse(flag_sept =="sept", 1, 0)) %>%
  mutate(
    flag_jan = replace_na(flag_jan, 0),
    flag_sept = replace_na(flag_sept, 0)) %>%
  mutate(same_ain = ifelse(flag_jan + flag_sept == 2, 1, 0))

same_ains <- match_parcels_wide %>% 
  filter(same_ain==1) %>% # 54508
  mutate(status = "shapes match, same ain")
length(unique(same_ains$ain)) # 54508; 44354 unique parcels

length(unique(same_ains$dupe_id)) # 44354 unique parcels

diff_ains <- match_parcels_wide %>% 
  filter(!is.na(dupe_id) & same_ain==0) %>%
  group_by(dupe_id) %>%
  filter(n()>1) %>%
  ungroup() %>% # 376; 173 unique parcels
  mutate(status = "shapes match, diff ain")

length(unique(diff_ains$dupe_id)) # 173 unique parcels

# get shapes that have no duplicates, run intersect
unique_parcels <- match_parcels_wide %>%
  filter(is.na(dupe_id)) %>% # 356 parcels with no exact shape matches, should run intersect
  mutate(status = "no shape match, run intersect")

jan_unique <- unique_parcels %>% filter(flag_jan==1) # 187

sept_unique <- unique_parcels %>% filter(flag_sept==1) # 169


combined <- rbind(same_ains, diff_ains, unique_parcels)
