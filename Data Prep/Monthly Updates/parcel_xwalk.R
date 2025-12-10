# Objective: Modify jan_sept_parcel_xwalk.R to run regularly each month
# Should include QA steps for key stages of analysis to flag irregularities
# Or other issues worth additional review.

# Step 0: Set up, initial prep ------
# Library and environment set up
source("W:\\RDA Team\\R\\credentials_source.R")
library(sf)
library(mapview)

options(scipen=999)

con <- connect_to_db("altadena_recovery_rebuild")

#### MONTHLY UPDATE: Variables to update each time ------------------
# Key variables - tracking current and previous months and years
# update with current year and month and previous time crosswalk was run
date_ran <- as.character(Sys.Date())
curr_year <- "2025" # strsplit(date_ran, "-", fixed=TRUE)[[1]][1] # year
curr_month <- "12" # strsplit(date_ran, "-", fixed=TRUE)[[1]][2] # month
prev_month <- "09" # ifelse(curr_month == "01", "12", sprintf("%02d", as.numeric(curr_month) - 1))
prev_year <- "2025" # ifelse(curr_month == "01", as.character(as.numeric(curr_year) - 1),  curr_year)
# prev_prev_month <- ifelse(prev_month == "01", "12", 
#                           sprintf("%02d", as.numeric(prev_month) - 1))
# prev_prev_year <- ifelse(prev_month == "01", 
#                          as.character(as.numeric(curr_year) - 1), 
#                          prev_year)
##### STOP UPDATE 



#### Pull in tables and universe needed for script ----
# This defines variables for current and previous tables needed for script
curr_parcels_table <- paste("dashboard.assessor_parcels_universe", curr_year, curr_month, sep="_")
curr_stats_table <- paste("dashboard.assessor_data_universe", curr_year, curr_month, sep="_")
prev_parcels_table <- paste("dashboard.assessor_parcels_universe", prev_year, prev_month, sep="_")
prev_stats_table <- paste("dashboard.assessor_data_universe", prev_year, prev_month, sep="_")
prev_xwalk_table <-  "dashboard.crosswalk_assessor_01_09_2025" # paste("dashboard.crosswalk_assessor", prev_prev_month, prev_month, prev_year, sep="_")
parcel_universe <- dbGetQuery(con, "SELECT * from dashboard.parcel_universe_2025_01") # all significantly damaged, residential Jan parcels

###### MONTHLY UPDATE - Get universe of parcels ----
## Revisit once new data comes in for now starting with january parcels - no script to QA crosswalk_assessor_01_09_2025 that's being pulled for now just using the parcel universe
# # # Prep: get the universe of shapes (jan res + sig dmg) and filter the prev crosswalk
# prev_xwalk <- dbGetQuery(con, paste("SELECT ain_2025_01,", paste("ain", prev_year, prev_month, sep="_"), "AS ain_prev FROM", prev_xwalk_table)) %>%
#   filter(ain_2025_01 %in% parcel_universe$ain_2025_01)
# xwalk_cols <- dbGetQuery(con, paste("SELECT * FROM", prev_xwalk_table)) %>%
#   colnames()

prev_xwalk <- dbGetQuery(con, paste("SELECT ain_2025_01,", paste("ain", prev_year, prev_month, sep="_"), "AS ain_prev FROM", prev_xwalk_table)) 

###### STOP UPDATE

# This pulls in the previous and current assessor parcels and data
# get previous assessor parcels and add an identifier column
# prev parcels should be based on the crosswalk filtered for residential and significantly damaged parcels
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
          ifelse(st_crs(parcels_curr)$epsg=="3310", "TRUE", paste("FALSE - EPSG is", st_crs(parcels_curr)$epsg))))


# Step 1: Bind previous and current parcels and flag parcels with same shape, ains, and counts -------
# Bind together prev and current and add field for the geom
all_parcels <- rbind(parcels_prev, parcels_curr) %>% 
  # create address field - can use to match changing parcels later
  mutate(address=paste(situs_house_no, direction, street_name, unit, city_state)) %>%
  mutate(address=gsub("\\s+", " ", address)) %>% 
  select(-c(situs_house_no, direction, street_name, unit, city_state)) %>%
  # add geom in text using WKT - note: this takes a couple mins
  mutate(geom_wkt = st_as_text(geom)) %>%
  st_drop_geometry()

# group by geom text to get number of duplicate shapes
match_parcels <- all_parcels %>%
  group_by(geom_wkt) %>%
  # number of times shape of parcel is duplicated
  mutate(group_count = n()) %>%
  # shapes appearing more than once get a group id (dupe_id), if it's unique then NA
  mutate(dupe_id = ifelse(group_count>1,cur_group_id(), NA)) %>%
  ungroup() 

# QA Checks
cat(paste("Total number of curr and prev parcels:", nrow(all_parcels)))
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
    xwalk_type = case_when(
      shape_match==0 ~ "run intersect by month", # no shape or ain match
      shape_match==1 & same_ain == 0 & same_counts==1 & group_count==2 ~ "same shape, diff ains, same counts",
      shape_match==1 & same_ain == 0 & same_counts==1 & group_count>2 ~ "same shape, diff ains, needs closer look",
      shape_match==1 & same_ain == 0 & same_counts==0 & group_count>2 ~ "same shape, diff ains, needs closer look",
      shape_match==1 & same_ain == 1 & same_counts==1 ~ "same shape, same ains, same counts",
      .default = NA))

check <- as.data.frame(table(match_parcels_wide$xwalk_type , useNA="always"))
cat(paste("Number of undefined relationships between prev and curr parcel shapes (0 is good):", check$Freq[is.na(check$Var1)]))
print(check)
##### Monthly Update if you see some status types from before missing--that's okay, they will just produce 0 data frames and code will still run  #######
## December note - with parcels limited to Altadena, this dropped other xwalk types, ran code anyways but dataframes yield 0

##### Monthly Update if you update the status fields, you'll need to update the filters in following steps #######

# Step 2: Compile crosswalk based on matching shapes and ains -----------
########### Same Shapes and AINs 
same_shape_ain <- match_parcels_wide %>%
  filter(xwalk_type  %in% c("same shape, same ains, same counts"))

xwalk_same_shape_ain <- same_shape_ain %>%
  select(ain, dupe_id, xwalk_type, same_ain, same_counts, group_count) %>%
  mutate(ain_prev = ain,
         ain_curr = ain) %>% 
  select(-ain) %>%
  left_join(all_parcels %>% 
              filter(flag == "prev") %>% 
              select(ain, use_code, address) %>% 
              st_drop_geometry(), 
            by = c("ain_prev" = "ain")) %>%
  left_join(all_parcels %>% 
              filter(flag == "curr") %>% 
              select(ain, use_code, address) %>% 
              st_drop_geometry(), 
            by = c("ain_curr" = "ain")) %>%
  mutate(
    xwalk_type = "same shape, same ains, same counts" ,
    status = "no change") %>%
  rename(use_code_prev = use_code.x,
         use_code_curr = use_code.y,
         address_prev = address.x,
         address_curr = address.y)

# Check for NAs and unique AINs
cat(paste("Number of rows matches unique number of current ains:", nrow(xwalk_same_shape_ain)==length(unique(xwalk_same_shape_ain$ain_curr))))
cat(paste("Number of rows matches unique number of previous ains:", nrow(xwalk_same_shape_ain)==length(unique(xwalk_same_shape_ain$ain_prev))))

# Step 3a: Compile crosswalk based on matching shapes, but different ains -----------
same_shape_diff_ain <- match_parcels_wide %>%
  filter(xwalk_type %in% c("same shape, diff ains, same counts"))

# QA Check
nrow(same_shape_diff_ain) # if 0 expect an error below

xwalk_same_shape_diff_ain <- same_shape_diff_ain  %>%
  group_by(dupe_id,xwalk_type) %>%
  summarise(
  ain_prev = ain[flag_prev == 1],
  ain_curr = ain[flag_curr == 1]) %>%
  left_join(all_parcels %>% 
              filter(flag == "prev") %>% 
              select(ain, use_code, address) %>% 
              st_drop_geometry(), 
            by = c("ain_prev" = "ain")) %>%
  left_join(all_parcels %>% 
              filter(flag == "curr") %>% 
              select(ain, use_code, address) %>% 
              st_drop_geometry(), 
            by = c("ain_curr" = "ain")) %>%
    mutate(status =  "same shape, ain renamed or deleted"
        ) %>%
  rename(use_code_prev = use_code.x,
         use_code_curr = use_code.y,
         address_prev = address.x,
         address_curr = address.y)

# Dec qa flag on below because dataframe yields 0, can still proceed:
# `summarise()` has grouped output by 'dupe_id', 'xwalk_type'. You can override using the `.groups` argument.
# Warning message:
#   Returning more (or less) than 1 row per `summarise()` group was deprecated in dplyr 1.1.0.
# i Please use `reframe()` instead.
# i When switching from `summarise()` to `reframe()`, remember that `reframe()` always returns an ungrouped data frame and adjust accordingly.

# Step 3b: Compile crosswalk based on matching shapes, but different ains and counts -----------
same_shape_diff_count_ain <- match_parcels_wide %>%
  filter(xwalk_type %in% c("same shape, diff ains, needs closer look"))

# QA Check
nrow(same_shape_diff_ain) # if 0 expect an error below

xwalk_same_shape_diff_count_ain <- same_shape_diff_ain  %>%
  group_by(dupe_id,xwalk_type) %>%
  summarise(
    ain_prev = ain[flag_prev == 1],
    ain_curr = ain[flag_curr == 1]) %>%
  left_join(all_parcels %>% 
              filter(flag == "prev") %>% 
              select(ain, use_code, address) %>% 
              st_drop_geometry(), 
            by = c("ain_prev" = "ain")) %>%
  left_join(all_parcels %>% 
              filter(flag == "curr") %>% 
              select(ain, use_code, address) %>% 
              st_drop_geometry(), 
            by = c("ain_curr" = "ain")) %>%
  mutate(status =  "same shape, ain renamed, deleted, or split"
  ) %>%
  rename(use_code_prev = use_code.x,
         use_code_curr = use_code.y,
         address_prev = address.x,
         address_curr = address.y)

# Dec qa flag on below because dataframe yields 0, can still proceed:
# `summarise()` has grouped output by 'dupe_id', 'xwalk_type'. You can override using the `.groups` argument.
# Warning message:
#   Returning more (or less) than 1 row per `summarise()` group was deprecated in dplyr 1.1.0.
# i Please use `reframe()` instead.
# i When switching from `summarise()` to `reframe()`, remember that `reframe()` always returns an ungrouped data frame and adjust accordingly.

# Step 4: Compile crosswalk based on spatial intersect -----------
diff_shape <- match_parcels_wide %>%
  filter(xwalk_type=="run intersect by month")

prev_parcels_filtered_ains <- diff_shape %>% filter(flag_prev==1) %>% select(ain)
curr_parcels_filtered_ains <- diff_shape %>% filter(flag_curr==1)%>% select(ain)

prev_parcels_filtered <- parcels_prev %>% filter(ain %in% prev_parcels_filtered_ains$ain)
curr_parcels_filtered <- parcels_curr %>% filter(ain %in% curr_parcels_filtered_ains$ain)

intersection <- st_intersection(prev_parcels_filtered, curr_parcels_filtered) %>%
  mutate(area_intersect = st_area(geom)) %>%
  mutate(pct_overlap_prev = as.numeric(area_intersect)/as.numeric(area)*100,
         pct_overlap_curr = as.numeric(area_intersect)/as.numeric(area.1)*100) %>%
  mutate(address_prev=paste(situs_house_no, direction, street_name, unit, city_state),
         address_curr=paste(situs_house_no.1, direction.1, street_name.1, unit.1, city_state.1)) %>%
  mutate(address_prev=gsub("\\s+", " ", address_prev),
         address_curr=gsub("\\s+", " ", address_curr)) %>%
  select(-c(situs_house_no, direction, street_name, unit, city_state,
            situs_house_no.1, direction.1, street_name.1, unit.1, city_state.1)) %>%
  group_by(ain) %>%
  mutate(count= n()) %>%
  ungroup() %>%
  mutate(address_match = ifelse(address_prev==address_curr, 1, 0),
         # see if use code is NA, possible marker for data quality in assessor file
         curr_use_code_na = ifelse(is.na(use_code.1), 1, 0), 
         # see if ain is in curr assessor file
         in_curr_shp = ifelse(ain %in% parcels_curr$ain, 1, 0), 
         ain_match = ifelse(ain==ain.1, 1 ,0)) %>%
  filter(pct_overlap_prev>0)

# Keep matching AINs first
xwalk_diff_shape_same_ain <- intersection %>%
  # Filter for relevant records
  filter(ain_match == 1) %>%
  st_drop_geometry() %>%
  mutate(
    xwalk_type="spatial intersect, ain match",
    status="same ain, slight parcel change"
    ) %>%
  select(ain, ain.1, pct_overlap_prev, pct_overlap_curr, 
         use_code, address_prev, use_code.1, address_curr, 
         status, xwalk_type) %>%
  rename(
    ain_prev = ain,
    ain_curr = ain.1,
    use_code_prev = use_code,
    use_code_curr = use_code.1
  )

# Keep remaining where overlap is greater than 90%
xwalk_diff_shape_overlap <- intersection %>%
  # Filter for relevant records 
  filter(pct_overlap_prev >= 90) %>%
  st_drop_geometry() %>%
  mutate(
    xwalk_type="spatial intersect, 90% match",
    status="jan parcel merged or split"
  ) %>%
  select(ain, ain.1, pct_overlap_prev, pct_overlap_curr, 
         use_code, address_prev, use_code.1, address_curr, 
         status, xwalk_type) %>%
  rename(
    ain_prev = ain,
    ain_curr = ain.1,
    use_code_prev = use_code,
    use_code_curr = use_code.1
  ) %>%
  # drop records in the spatial intersect ain match
  filter(!ain_prev %in% xwalk_diff_shape_same_ain$ain_prev)

# Step 5: Monthly Update: Compile crosswalks - Make sure to pull in any xwalk tables you added to script ------
xwalk_df <- bind_rows(
  xwalk_same_shape_ain %>% 
    select(ain_prev, ain_curr, use_code_prev, use_code_curr, 
           address_prev, address_curr, xwalk_type, status, dupe_id),
  xwalk_same_shape_diff_ain %>% 
    select(ain_prev, ain_curr, use_code_prev, use_code_curr, 
           address_prev, address_curr, xwalk_type, status, dupe_id),
  xwalk_same_shape_diff_count_ain %>% 
    select(ain_prev, ain_curr, use_code_prev, use_code_curr, 
           address_prev, address_curr, xwalk_type, status, dupe_id),
  xwalk_diff_shape_same_ain %>% 
    select(ain_prev, ain_curr, pct_overlap_prev, pct_overlap_curr,
           use_code_prev, use_code_curr, address_prev, address_curr, 
           xwalk_type, status),
  xwalk_diff_shape_overlap %>% 
  select(ain_prev, ain_curr, pct_overlap_prev, pct_overlap_curr,
         use_code_prev, use_code_curr, address_prev, address_curr, 
         xwalk_type, status)) 



# Step 6:  Monthly Update: Filter for universe and check/modify for missing Jan parcels ------
#### Will need to update and pull in the previous xwalk that has been filtered overtime for the january universe
# filter just for the parcel universe
final_xwalk <- xwalk_df %>%
  left_join(prev_xwalk, by="ain_prev") %>%
  select(ain_2025_01, ain_prev, ain_curr, everything()) %>%
  # rename columns for export
  rename_with(~ gsub("_prev$", paste("", prev_year, prev_month, sep="_"), .x)) %>%
  rename_with(~ gsub("_curr$", paste("",curr_year, curr_month, sep="_"), .x)) %>%
  filter(ain_2025_01 %in% parcel_universe$ain_2025_01)

# check that all january ains are accounted for
missing_jan_parcels <- parcel_universe %>% anti_join(final_xwalk, by=c("ain_2025_01"))
nrow(missing_jan_parcels)
##### QA CHECK SHOULD BE ZERO #####
## Dec QA
# https://portal.assessor.lacounty.gov/parceldetail/5842008010 - split into 5842008017 and 5842008018

# Step 7: Add records dropped and finalize table ------
xwalk_missing_parcels <- intersection %>%
  filter(ain %in% missing_jan_parcels$ain_2025_01) %>%
  st_drop_geometry() %>%
  mutate(
    xwalk_type="spatial intersect, manual match",
    status="jan parcel merged or split"
  ) %>%
  select(ain, ain.1, pct_overlap_prev, pct_overlap_curr, 
         use_code, address_prev, use_code.1, address_curr, 
         status, xwalk_type) %>%
  rename(
    ain_prev = ain,
    ain_curr = ain.1,
    use_code_prev = use_code,
    use_code_curr = use_code.1
  ) 

xwalk_missing_parcels <- xwalk_missing_parcels %>%
  left_join(prev_xwalk, by="ain_prev") %>%
  select(ain_2025_01, ain_prev, ain_curr, everything()) %>%
  # rename columns for export
  rename_with(~ gsub("_prev$", paste("", prev_year, prev_month, sep="_"), .x)) %>%
  rename_with(~ gsub("_curr$", paste("",curr_year, curr_month, sep="_"), .x))

final_xwalk <- bind_rows(final_xwalk,xwalk_missing_parcels)

# QA CHECK DUPLICATES and review them--multiple of the previous ains matching
dup_matches_jan <- final_xwalk[final_xwalk$ain_2025_01 %in% final_xwalk$ain_2025_01[duplicated(final_xwalk$ain_2025_01)], ]
dup_matches_prev <- final_xwalk[final_xwalk$ain_2025_09 %in% final_xwalk$ain_2025_09[duplicated(final_xwalk$ain_2025_09)], ]
dup_matches_curr <- final_xwalk[final_xwalk$ain_2025_12 %in% final_xwalk$ain_2025_12[duplicated(final_xwalk$ain_2025_12)], ]
# We know these parcels have been merged though the data on the portal seems to be delayed 
# https://portal.assessor.lacounty.gov/parceldetail/5841023009
# https://portal.assessor.lacounty.gov/parceldetail/5842008010
# MAKE SURE THIS DOESNT YIELD DUPLICATES IN RELATIONAL TABLES THOUGH

# Step 8: Monthly Update - Final QA Check -----
# Skim parcels that changed--if months are closer together this should be fewer in record
# look at instances of parcel changes
qa_parcel_change <- final_xwalk %>% filter(ain_2025_09!=ain_2025_12)
# these all make sense based on assessor portal checks


# Step 9: Export -------
# Key variables - export variables
schema <- 'dashboard'
table_name <- paste("crosswalk_assessor", curr_year, prev_month, curr_month, sep = "_")
indicator <- "Updated Crosswalk of Assessor AINs from prev(ious) to curr(ent) shapes based on significantly damaged residential parcels from January"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\monthly_updates\\QA_parcel_xwalk.docx"


source <- "Data Prep\\Monthly Updates\\parcel_xwalk.R"
dbWriteTable(con, Id(schema, table_name), final_xwalk,
             overwrite = FALSE, row.names = FALSE)

colnames(final_xwalk)
col_comments <- c("ain, suffix denotes respective assessor data version",
                  "ain, suffix denotes respective assessor data version",
                  "respective use code",
                  "respective use code",
                  "respective site address with unit",
                  "respective site address with unit",
                  "qa column that refers to source of the crosswalk",
                  "notes on any change to the parcel shape or ain name between respective versions",
                  "qa column that tracks duplicate parcel shapes",
                  "pct intersect overlap with respective shape only for records that required an intersect (unmatched polygons)",
                  "pct intersect overlap with respective shape only for records that required an intersect (unmatched polygons)")


add_table_comments(con=con, schema=schema,table_name=table_name,indicator = indicator, qa_filepath = qa_filepath,
                   source=source,column_names = colnames(final_xwalk), column_comments = col_comments)


# Additional QA Checks -----
# compare changes in prev and curr xwalk
# prev xwalk
prev_xwalk <- st_read(con, query="SELECT * from dashboard.crosswalk_assessor_01_09_2025")

# new xwalk
curr_xwalk <- st_read(con, query="SELECT * from dashboard.crosswalk_assessor_2025_09_12")

# what's in the old xwalk but missing in the new one by jan ains
missing_jan <- prev_xwalk %>% anti_join(curr_xwalk, by=c("ain_2025_01"="ain_2025_01"))
# none

# what's in the old xwalk but missing in the new one by sept ains
missing_sept <- prev_xwalk %>% anti_join(curr_xwalk, by=c("ain_2025_09"="ain_2025_09"))
# none

nrow(curr_xwalk)
length(unique(curr_xwalk$ain_2025_01))
nrow(prev_xwalk)
length(unique(prev_xwalk$ain_2025_01))
length(unique(prev_xwalk$ain_2025_01))-length(unique(curr_xwalk$ain_2025_01))
# gap of 0 added or dropped

# test for instances where the new xwalk doesn't match the old xwalk-jan-sept
test_xwalk_result <- curr_xwalk %>% left_join(prev_xwalk %>% mutate(old=TRUE), by=c("ain_2025_01"="ain_2025_01","ain_2025_09"="ain_2025_09"))

# not a match records -- 0 NA
table(test_xwalk_result$old,useNA='always')

# # filter for NA or not a matching record with prev xwalk to explore
# mismatch <- filter(test_xwalk_result, is.na(old))

# check method for parcels
table(prev_xwalk$xwalk_type,useNA='always')
table(curr_xwalk$xwalk_type,useNA='always')
# lower need for spatial intersect--maybe because less change in files closer together or something else?

