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
curr_year <- "2025" # strsplit(date_ran, "-", fixed=TRUE)[[1]][1] # year
curr_month <- "09" # strsplit(date_ran, "-", fixed=TRUE)[[1]][2] # month
prev_month <- "01" # ifelse(curr_month == "01", "12", sprintf("%02d", as.numeric(curr_month) - 1))
prev_year <- "2025" # ifelse(curr_month == "01", as.character(as.numeric(curr_year) - 1),  curr_year)
prev_prev_month <- ifelse(prev_month == "01", "12", 
                          sprintf("%02d", as.numeric(prev_month) - 1))
prev_prev_year <- ifelse(prev_month == "01", 
                         as.character(as.numeric(curr_year) - 1), 
                         prev_year)

# Key variables - current and previous tables needed for script
curr_parcels_table <- paste("dashboard.assessor_parcels_universe", curr_year, curr_month, sep="_")
curr_stats_table <- paste("dashboard.assessor_data_universe", curr_year, curr_month, sep="_")
prev_parcels_table <- paste("dashboard.assessor_parcels_universe", prev_year, prev_month, sep="_")
prev_stats_table <- paste("dashboard.assessor_data_universe", prev_year, prev_month, sep="_")
prev_xwalk_table <- "dashboard.crosswalk_assessor_01_09_2025" # paste("dashboard.crosswalk_assessor", prev_prev_month, prev_month, prev_year, sep="_")
  
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
cat(paste("Confirm EPSG of prev parcel shapes is 3310:", 
          ifelse(st_crs(parcels_prev)$epsg=="3310", "TRUE", paste("FALSE - EPSG is", st_crs(parcels_prev)$epsg))))
cat(paste("Confirm EPSG of curr parcel shapes is 3310:", 
          ifelse(st_crs(parcels_prev)$epsg=="3310", "TRUE", paste("FALSE - EPSG is", st_crs(parcels_prev)$epsg))))


##### Step 1: find out which shapes are the same in prev and curr parcels #####
all_parcels <- rbind(parcels_prev, parcels_curr) %>% 
  # create address field - can use to match changing parcels later
  mutate(address=paste(situs_house_no, direction, street_name, unit, city_state)) %>%
  mutate(address=gsub("\\s+", " ", address)) %>% 
  select(-c(situs_house_no, direction, street_name, unit, city_state)) %>%
  # group by shape using WKT - note: this takes a couple mins
  mutate(geom_wkt = st_as_text(geom)) %>%
  st_drop_geometry()

match_parcels <- all_parcels %>%
  group_by(geom_wkt) %>%
  mutate(group_count = n()) %>%
  # shapes appearing more than once get a group id (dupe_id), if it's unique then NA
  mutate(dupe_id = ifelse(group_count>1,cur_group_id(), NA)) %>%
  ungroup() 

# QA Checks
cat(paste("Total number of parcels:", nrow(all_parcels)))
check <- data.frame(table(match_parcels$dupe_id, useNA = "always"))
cat(paste("Number of duplicated shapes:", nrow(check)-1))
cat(paste("Number of unduplicated shapes:", check$Freq[is.na(check$Var1)]))
cat(paste("Number of shapes accounted for is the same as number of all parcels:", sum(check$Freq)==nrow(all_parcels)))


# Make wider, to see if AINs match across the same shape (or something else)
match_parcels_wide <- match_parcels %>%
  select(-c(area, use_code, address)) %>%
  pivot_wider(
    names_from = flag,
    values_from = flag,
    names_prefix = "flag_") %>%
  mutate(
    # Convert flags to binary (1/0) and handle NAs in one step
    flag_prev = as.integer(!is.na(flag_prev)),
    flag_curr = as.integer(!is.na(flag_curr)),
    # Flag for same AIN in both months
    same_ain = as.integer(flag_prev == 1 & flag_curr == 1)) %>%
  # Calculate totals by dupe_id
  group_by(dupe_id) %>%
  mutate(
    total_prev = sum(flag_prev),
    total_curr = sum(flag_curr),
    # Shape match: has dupe_id and both months present
    shape_match = as.integer(!is.na(dupe_id) & total_prev > 0 & total_curr > 0)) %>%
  mutate(same_counts = ifelse(total_prev==total_curr, 1, 0)) %>%
  ungroup() %>%
  select(dupe_id, ain, shape_match, same_ain, same_counts, everything()) %>%
  mutate(
    status = case_when(
      shape_match==0 ~ "run intersect by month", # no shape or ain match
      shape_match==1 & same_ain == 0 & same_counts==1 & group_count==2 ~ "diff ain pair, simple xwalk",
      shape_match==1 & same_ain == 0 & same_counts==1 & group_count>2 ~ "ambiguous matches, needs closer look",
      shape_match==1 & same_ain == 0 & same_counts==0 & group_count>2 ~ "uneven matches, needs closer look",
      shape_match==1 & same_ain == 1 & same_counts==1 ~ "same ains, simple xwalk",
      .default = "undefined status, please review"))

check <- as.data.frame(table(match_parcels_wide$status, useNA="always"))
cat(paste("Number of undefined relationships between prev and curr parcel shapes (0 is good):", check$Freq[is.na(check$Var1)]))


##### Step 2: Compile crosswalk based on status (relationship between prev and curr shapes) #####



rename(paste0("flag_", prev_month)=flag_prev,
       paste0("flag_", curr_month)=flag_curr
