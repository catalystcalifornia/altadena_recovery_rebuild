# Objective: Modify jan_sept_parcel_xwalk.R to run regularly each month
# Should include QA steps for key stages of analysis to flag irregularities
# Or other issues worth additional review.

##### Step 0: Set up, initial prep #####
# Library and environment set up ----
source("W:\\RDA Team\\R\\credentials_source.R")
library(sf)
library(mapview)

options(scipen=999)

con <- connect_to_db("altadena_recovery_rebuild")

# Key variables - tracking current and previous months and years
date_ran <- as.character(Sys.Date())
curr_year <- strsplit(date_ran, "-", fixed=TRUE)[[1]][1] # year
curr_month <- strsplit(date_ran, "-", fixed=TRUE)[[1]][2] # month
prev_month <- ifelse(curr_month == "01", "12", 
                     sprintf("%02d", as.numeric(curr_month) - 1))
prev_year <- ifelse(curr_month == "01", 
                    as.character(as.numeric(curr_year) - 1), 
                    curr_year)
prev_prev_month <- ifelse(prev_month == "01", "12", 
                          sprintf("%02d", as.numeric(prev_month) - 1))
prev_prev_year <- ifelse(prev_month == "01", 
                         as.character(as.numeric(curr_year) - 1), 
                         prev_year)

# Key variables - current and previous tables needed for script
current_parcels_table <- paste("dashboard.assessor_parcels_universe", curr_year, curr_month, sep="_")
current_stats_table <- paste("dashboard.assessor_data_universe", curr_year, curr_month, sep="_")
prev_parcels_table <- paste("dashboard.assessor_parcels_universe", prev_year, prev_month, sep="_")
prev_stats_table <- paste("dashboard.assessor_data_universe", prev_year, prev_month, sep="_")
prev_xwalk_table <- paste("dashboard.crosswalk_assessor", prev_prev_month, prev_month, prev_year, sep="_")
  
# Key variables - export variables
schema <- 'dashboard'
table_name <- paste("crosswalk_assessor", prev_month, curr_month, curr_year, sep = "_")


# get previous assessor parcels and add an identifier column
parcels_prev <- st_read(con, query=paste("SELECT parcels.ain, parcels.geom, stats.use_code, stats.situs_house_no, stats.direction, stats.street_name, stats.unit, stats.city_state 
                       FROM", prev_parcels_table, "parcels
                       LEFT JOIN", prev_stats_table, "stats
                       ON parcels.ain=stats.ain")) %>%
  mutate(flag="prev") %>%
  mutate(area = st_area(geom))

parcels_curr <- st_read(con, query=paste("SELECT parcels.ain, parcels.geom, stats.use_code, stats.situs_house_no, stats.direction, stats.street_name, stats.unit, stats.city_state 
                       FROM", curr_parcels_table, "parcels
                       LEFT JOIN", curr_stats_table, "stats
                       ON parcels.ain=stats.ain")) %>%
  mutate(flag="curr") %>%
  mutate(area = st_area(geom))

# double check CRS of both of parcel shapes
st_crs(parcels_prev)$epsg #3310 good
st_crs(parcels_curr)$epsg #3310 good