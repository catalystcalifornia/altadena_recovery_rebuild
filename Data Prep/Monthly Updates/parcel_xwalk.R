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
prev_xwalk_table <- "dashboard.crosswalk_assessor_universe" # paste("dashboard.crosswalk_assessor", prev_prev_month, prev_month, prev_year, sep="_")

# Key variables - export variables
schema <- 'dashboard'
table_name <- paste("crosswalk_assessor", prev_month, curr_month, curr_year, sep = "_")

# get the universe of shapes from the last crosswalk
prev_walk <- dbGetQuery(con, paste("SELECT ain_curr as ain_prev FROM", prev_xwalk_table))

# get previous assessor parcels and add an identifier column
# prev parcels should be based on the crosswalk filtered for residential and significantly damaged parcels
parcels_prev <- st_read(con, query=paste("SELECT parcels.ain, parcels.geom, stats.use_code, stats.situs_house_no, stats.direction, stats.street_name, stats.unit, stats.city_state 
                       FROM", prev_parcels_table, "parcels
                       LEFT JOIN", prev_stats_table, "stats
                       ON parcels.ain=stats.ain")) %>%
  mutate(flag="prev") %>%
  filter(ain %in% prev_walk$ain_prev) %>%
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

check <- as.data.frame(table(match_parcels_wide$status, useNA="ifany"))
cat(paste("Number of undefined relationships between prev and curr parcel shapes (0 is good):", check$Freq[check$Var1=="undefined status, please review"]))


##### Step 2: Compile crosswalk based on status (relationship between prev and curr shapes) #####
### Status: "same ains, simple xwalk"
same_ain <- match_parcels_wide %>%
  filter(status == "same ains, simple xwalk") 
#54674

length(unique(same_ain$dupe_id)) # 44516

same_ain_xwalk <- same_ain %>%
  select(ain, dupe_id) %>%
  mutate(ain_prev = ain,
         ain_curr = ain) %>%
  select(-ain)  %>%
  left_join(all_parcels %>% 
              filter(flag=="prev") %>% select(ain,use_code,address) %>% st_drop_geometry, 
            by=c("ain_prev"="ain")) %>%
  left_join(all_parcels %>% 
              filter(flag=="prev") %>% select(ain,use_code,address) %>% st_drop_geometry, 
            by=c("ain_curr"="ain")) %>%
  mutate(source="same shape, same ain",
         status = "no change") %>%
  rename(use_code_prev=use_code.x,
         use_code_curr=use_code.y,
         address_prev=address.x,
         address_curr=address.y)

### Status: "diff ain pair, simple xwalk"
diff_ain <- match_parcels_wide %>%
  filter(status == "diff ain pair, simple xwalk") 

length(unique(diff_ain$dupe_id)) # 4

# notes: 
# two of these pairs, jan parcel deleted and renamed to sept ain
# other two pairs, sept parcel is wrong? jan parcel is correct.

diff_ain_xwalk <- diff_ain %>%
  group_by(dupe_id) %>%
  summarise(
    ain_prev = ain[flag_prev == 1],
    ain_curr = ain[flag_curr == 1]) %>%
  left_join(all_parcels %>% 
              filter(flag=="prev") %>% select(ain,use_code,address) %>% st_drop_geometry, 
            by=c("ain_prev"="ain")) %>%
  left_join(all_parcels %>% 
              filter(flag=="prev") %>% select(ain,use_code,address) %>% st_drop_geometry, 
            by=c("ain_curr"="ain")) %>%
  mutate(source="same shape, different ain",
         status = "same shape, ain renamed or deleted") %>%
  rename(use_code_prev=use_code.x,
         use_code_curr=use_code.y,
         address_prev=address.x,
         address_curr=address.y)

### Status: "ambiguous matches, needs closer look"
ambiguous <- match_parcels_wide %>%
  filter(status == "ambiguous matches, needs closer look") 

length(unique(ambiguous$dupe_id)) # 2
cat(paste("Do we have the status 'ambiguous matches, needs closer look'?:", ifelse(nrow(ambiguous)>1, "TRUE", "FALSE")))

# note: if ambiguous is TRUE, use assessor's portal to compare prev and curr AINs, e.g., https://portal.assessor.lacounty.gov/parceldetail/5722013039
# if the current parcel is the correct AIN then nothing more needs to happen
# otherwise will need to update code, update "status" below, and re-export crosswalk table

ambiguous_xwalk <- ambiguous %>%
  group_by(dupe_id) %>%
  summarise(
    ain_prev = ain[flag_prev == 1],
    ain_curr = ain[flag_curr == 1]) %>%
  left_join(all_parcels %>% 
              filter(flag=="prev") %>% select(ain,use_code,address) %>% st_drop_geometry, 
            by=c("ain_prev"="ain")) %>%
  left_join(all_parcels %>% 
              filter(flag=="prev") %>% select(ain,use_code,address) %>% st_drop_geometry, 
            by=c("ain_curr"="ain")) %>%
  mutate(source="same shape, different, ambiguous ain",
         status = "same shape, jan ain deleted") %>%
  rename(use_code_prev=use_code.x,
         use_code_curr=use_code.y,
         address_prev=address.x,
         address_curr=address.y)

### Status: "uneven matches, needs closer look"
uneven <-  match_parcels_wide %>%
  filter(status == "uneven matches, needs closer look")

# length(unique(uneven$dupe_id)) # 1

# note: this shape is for a parcel deleted in 12/2024, does not match september data files
# does have valid assessor portal results, but deleted in 12/2024 the other parcels matched are in the sept shapefile but not in the assessor data
# ains 5734023084 to 5734023100 - 16 units, could be in development but no information on use codes
# https://portal.assessor.lacounty.gov/parceldetail/5734023022
# parcels_sept %>% filter(grepl('573402308',ain)) %>% select(ain,use_code)
# this was a commercial property
# address: 139 S OAK KNOLL AVE PASADENA CA 91101-2608
# Recommend revising so september records keep their ain, but the jan ain is the original 5734023022

uneven_xwalk <- uneven %>%
  group_by(dupe_id) %>%
  summarise(
    ain_prev = ain[flag_prev == 1],
    ain_curr = ain[flag_curr == 1]) %>%
  left_join(all_parcels %>% 
              filter(flag=="prev") %>% select(ain,use_code,address) %>% st_drop_geometry, 
            by=c("ain_prev"="ain")) %>%
  left_join(all_parcels %>% 
              filter(flag=="prev") %>% select(ain,use_code,address) %>% st_drop_geometry, 
            by=c("ain_curr"="ain")) %>%
  mutate(source="same shape, different, multiple ains",
         status = "same shape, ain changed, multiple ains split on same shape") %>%
  rename(use_code_prev=use_code.x,
         use_code_curr=use_code.y,
         address_prev=address.x,
         address_curr=address.y)

### Status: "run intersect by month"
intersect_prev <- match_parcels_wide %>%
  filter(status == "run intersect by month" & flag_prev==1) # 193

prev_parcels_filtered <- parcels_prev %>% filter(ain %in% intersect_prev$ain)

prev_join <- st_intersection(prev_parcels_filtered, parcels_curr) %>%
  mutate(area_intersect = st_area(geom)) %>%
  mutate(pct_prev_overlap = as.numeric(area_intersect)/as.numeric(area)*100,
         pct_curr_overlap = as.numeric(area_intersect)/as.numeric(area.1)*100) %>%
  mutate(address=paste(situs_house_no, direction, street_name, unit, city_state),
         address.1=paste(situs_house_no.1, direction.1, street_name.1, unit.1, city_state.1)) %>%
  mutate(address=gsub("\\s+", " ", address),
         address.1=gsub("\\s+", " ", address.1)) %>%
  select(-c(situs_house_no, direction, street_name, unit, city_state,
            situs_house_no.1, direction.1, street_name.1, unit.1, city_state.1)) %>%
  group_by(ain) %>%
  mutate(count= n()) %>%
  ungroup() %>%
  mutate(address_match = ifelse(address==address.1, 1, 0),
         # see if use code is NA, possible marker for data quality in sept file
         curr_use_code_na = ifelse(is.na(use_code.1), 1, 0), 
         # see if ain is in sept shp file
         in_curr_shp = ifelse(ain %in% parcels_curr$ain, 1, 0), 
         ain_match = ifelse(ain==ain.1, 1 ,0)) %>%
  filter(pct_prev_overlap>0)

# explore duplicates
prev_join_dupes <- prev_join %>% 
  select(ain) %>% 
  filter(duplicated(.)) # 32

dup_matches <- prev_join %>%
  filter(ain %in% prev_join_dupes$ain)


# see https://portal.assessor.lacounty.gov/parceldetail/5327012023
# neither 5327012023 or 5327012026 are in the september file but online map suggests closest match to 5327012026

# first keep results where overlapping parcels have same ain -- keep use code to see
jan_ain_match <-jan_join %>% 
  filter(ain_match==1) %>%
  st_drop_geometry() %>%
  select(ain, ain.1, pct_jan_overlap, pct_sept_overlap, use_code, address,use_code.1,address.1) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(source="spatial intersect, ain match",
         status="same ain, slight parcel change") %>%
  rename(use_code_jan=use_code,
         use_code_sept=use_code.1,
         address_jan=address,
         address_sept=address.1)

View(jan_ain_match)

# matches under a 70% jan overlap seem to be just slight changes to shapes looking at maps online and comparing to postgres
# https://portal.assessor.lacounty.gov/parceldetail/5832024005
# SELECT ain, st_transform(geom,4326) FROM data.assessor_parcels_universe_jan2025 where ain='5832024005'

length(unique(jan_ain_match$ain_jan)) # 151

jan_leftover <- jan_join %>% 
  st_drop_geometry() %>%
  filter(!(ain %in% jan_ain_match$ain_jan)) %>% 
  filter(pct_jan_overlap>10)

length(unique(jan_leftover$ain)) # 42

# look at duplicates
jan_leftover_dupes <- jan_leftover %>%
  select(ain) %>% filter(duplicated(.))
dup_matches <- jan_leftover %>% filter(ain %in% jan_leftover_dupes$ain)
# very few duplicates

# keep records where there is a 100% overlap with the original january file and there is a september record
jan_revise_ain_merged <- jan_leftover %>%
  select(ain, ain.1,pct_jan_overlap,pct_sept_overlap,use_code, address,use_code.1,address.1) %>%
  filter(pct_jan_overlap>=100) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(source="spatial intersect, 100% overlap",
         status="jan parcel merged or split") %>%
  rename(use_code_jan=use_code,
         use_code_sept=use_code.1,
         address_jan=address,
         address_sept=address.1)

View(jan_revise_ain_merged)

# see what's leftover
jan_leftover_v2 <- jan_leftover %>% 
  filter(!ain %in% jan_revise_ain_merged$ain_jan) 

# keep records where there is greater than a 90% overlap with the original january file and there is a september record
jan_revise_ain_split <- jan_leftover_v2 %>%
  select(ain, ain.1,pct_jan_overlap,pct_sept_overlap,use_code, address,use_code.1,address.1) %>%
  filter(pct_jan_overlap>=90) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(source="spatial intersect, over 90% overlap",
         status="jan parcel merged or split") %>%
  rename(use_code_jan=use_code,
         use_code_sept=use_code.1,
         address_jan=address,
         address_sept=address.1)

View(jan_revise_ain_split)


# see what's leftover
jan_leftover_v3 <- jan_leftover_v2 %>% 
  filter(!ain %in% jan_revise_ain_split$ain_jan) 

# https://portal.assessor.lacounty.gov/parceldetail/5757029054 split parcel
# https://portal.assessor.lacounty.gov/parceldetail/5709030009 parcel change - 5709030033 is a better match
# SELECT ain, st_transform(geom,4326) FROM data.assessor_parcels_universe_jan2025 where ain='5709030009'

jan_revise_ain_manual <- jan_leftover_v3 %>%
  filter(ain.1!='5709030034') %>%
  select(ain, ain.1,pct_jan_overlap,pct_sept_overlap,use_code, address,use_code.1,address.1) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(source="spatial intersect, manual match",
         status="jan parcel merged or split") %>%
  rename(use_code_jan=use_code,
         use_code_sept=use_code.1,
         address_jan=address,
         address_sept=address.1)

intersect_jan_xwalk <- bind_rows(jan_ain_match, jan_revise_ain_merged, jan_revise_ain_split, jan_revise_ain_manual)

# look at duplicates and make sure they make sense
dup_matches <- intersect_jan_xwalk[intersect_jan_xwalk$ain_jan %in% intersect_jan_xwalk$ain_jan[duplicated(intersect_jan_xwalk$ain_jan)], ]
# looks fine, some were looked at manually
# https://portal.assessor.lacounty.gov/parceldetail/5719022111
# 5719022101 and 5719022108 are matching to 5719022111 and 5719022114 larger parcels they were merged to

# notes: some are matching to sept ains with no assessor details (e.g., use_codes and address fields are NA)
# pull below and spot checked and these are not returning valid assessor parcel details 
# didn't have use codes for january either

sept_data_nas <- intersect_jan_xwalk %>% filter(is.na(use_code_sept))
# sort(unique(sept_data_nas$ain_jan))
# length(unique(sept_data_nas$ain_jan)) #40

##### combine xwalks and export #####
combined_xwalks <- bind_rows(intersect_jan_xwalk, uneven_xwalk, ambiguous_xwalk, diff_ain_xwalk, same_ain_xwalk) 

# check dups
dup_matches <- combined_xwalks[combined_xwalks$ain_jan %in% combined_xwalks$ain_jan[duplicated(combined_xwalks$ain_jan)], ]
# looks fine, includes one instance of a commercial property being split



rename(paste0("flag_", prev_month)=flag_prev,
       paste0("flag_", curr_month)=flag_curr
