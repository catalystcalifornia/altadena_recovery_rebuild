# Objective: Modify jan_sept_parcel_xwalk.R to run regularly each month
# Should include QA steps for key stages of analysis to flag irregularities
# Or other issues worth additional review.

##### Step 0: Set up, initial prep #####
# Library and environment set up
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
# prev_prev_month <- ifelse(prev_month == "01", "12", 
#                           sprintf("%02d", as.numeric(prev_month) - 1))
# prev_prev_year <- ifelse(prev_month == "01", 
#                          as.character(as.numeric(curr_year) - 1), 
#                          prev_year)

# Key variables - current and previous tables needed for script
curr_parcels_table <- paste("dashboard.assessor_parcels_universe", curr_year, curr_month, sep="_")
curr_stats_table <- paste("dashboard.assessor_data_universe", curr_year, curr_month, sep="_")
prev_parcels_table <- paste("dashboard.assessor_parcels_universe", prev_year, prev_month, sep="_")
prev_stats_table <- paste("dashboard.assessor_data_universe", prev_year, prev_month, sep="_")
prev_xwalk_table <-  "dashboard.crosswalk_assessor_01_09_2025" # paste("dashboard.crosswalk_assessor", prev_prev_month, prev_month, prev_year, sep="_")
jan_parcel_universe <- "dashboard.jan_parcel_universe" # all significantly damaged, residential Jan parcels

# Key variables - export variables
schema <- 'dashboard'
table_name <- paste("crosswalk_assessor", prev_month, curr_month, curr_year, sep = "_")

# Prep: get the universe of shapes (jan res + sig dmg) and filter the prev crosswalk
parcel_universe <- dbGetQuery(con, paste("SELECT ain_2025_01 FROM", jan_parcel_universe))

prev_xwalk <- dbGetQuery(con, paste("SELECT ain_2025_01,", paste("ain", prev_year, prev_month, sep="_"), "AS ain_prev FROM", prev_xwalk_table)) %>%
  filter(ain_2025_01 %in% parcel_universe$ain_2025_01)
xwalk_cols <- dbGetQuery(con, paste("SELECT * FROM", prev_xwalk_table)) %>%
  colnames()

# get previous assessor parcels and add an identifier column
# prev parcels should be based on the crosswalk filtered for residential and significantly damaged parcels
parcels_prev <- st_read(con, query=paste("SELECT parcels.ain, parcels.geom, stats.use_code, stats.situs_house_no, stats.direction, stats.street_name, stats.unit, stats.city_state 
                       FROM", prev_parcels_table, "parcels
                       LEFT JOIN", prev_stats_table, "stats
                       ON parcels.ain=stats.ain")) %>%
  mutate(flag="prev") %>%
  filter(ain %in% parcel_universe$ain_2025_01) %>%
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


##### Step 1: find out which shapes are the same in prev and curr parcels #####
all_parcels <- rbind(parcels_prev, parcels_curr) %>% 
  # create address field - can use to match changing parcels later
  mutate(address=paste(situs_house_no, direction, street_name, unit, city_state)) %>%
  mutate(address=gsub("\\s+", " ", address)) %>% 
  select(-c(situs_house_no, direction, street_name, unit, city_state)) 

# group by geom to get number of duplicate shapes
match_parcels <- all_parcels %>%
  # add geom in text using WKT - note: this takes a couple mins
  mutate(geom_wkt = st_as_text(geom)) %>%
  st_drop_geometry() %>%
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
    status = case_when(
      shape_match==0 ~ "run intersect by month", # no shape or ain match
      shape_match==1 & same_ain == 0 & same_counts==1 & group_count==2 ~ "diff ain pair, simple xwalk",
      shape_match==1 & same_ain == 0 & same_counts==1 & group_count>2 ~ "ambiguous matches, needs closer look",
      shape_match==1 & same_ain == 0 & same_counts==0 & group_count>2 ~ "uneven matches, needs closer look",
      shape_match==1 & same_ain == 1 & same_counts==1 ~ "same ains, simple xwalk",
      .default = NA))

check <- as.data.frame(table(match_parcels_wide$status, useNA="always"))
cat(paste("Number of undefined relationships between prev and curr parcel shapes (0 is good):", check$Freq[is.na(check$Var1)]))


##### Step 2: Compile crosswalk based on matching shapes and ains #####
########### Same Shapes and AINs -----
same_shape_ain <- match_parcels_wide %>%
  filter(status=="same ains, simple xwalk")

same_shape_ain_xwalk <- same_shape_ain %>%
  select(ain, dupe_id, status, same_ain, same_counts, group_count) %>%
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
    source = case_when(
      same_ain == 1 & same_counts == 1 ~ "same shape, same ain",
      same_ain == 1 & same_counts == 0 ~ "same shape, same ain, condo",
      .default = NA
    ),
    status = case_when(
      same_ain == 1 & same_counts == 1 ~ "no change",
      same_ain == 1 & same_counts == 0 ~ "same ain, condo units",
      .default = NA
    )
  ) %>%
  rename(use_code_prev = use_code.x,
         use_code_curr = use_code.y,
         address_prev = address.x,
         address_curr = address.y)

# Check for NAs and unique AINs
cat(paste("Number of rows matches unique number of current ains:", nrow(same_shape_ain_xwalk)==length(unique(same_shape_ain_xwalk$ain_curr))))
cat(paste("Number of rows matches unique number of previous ains:", nrow(same_shape_ain_xwalk)==length(unique(same_shape_ain_xwalk$ain_prev))))
cat(paste("No null use codes in current or previous file: ", "Prev - ", sum(is.na(same_shape_ain_xwalk$use_code_prev)), " Current -", sum(is.na(same_shape_ain_xwalk$use_code_curr))))

########### Same Shapes, different AINs -----
same_shape_diff_ain <- match_parcels_wide %>%
  filter(shape_match==1 & same_ain==0) 

same_shape_diff_ain_xwalk <- same_shape_diff_ain  %>%
  group_by(dupe_id) %>%
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
  mutate(
    source = case_when(
      same_ain == 0 & same_counts == 1 & group_count == 2 ~ "same shape, different ain",
      same_ain == 0 & same_counts == 1 & group_count > 2 ~ "same shape, different, ambiguous ain",
      same_ain == 0 & same_counts == 0 & group_count > 2 ~ "same shape, different, multiple ains",
      .default = NA
    ),
    status = case_when(
      same_ain == 0 & same_counts == 1 & group_count == 2 ~ "same shape, ain renamed or deleted",
      same_ain == 0 & same_counts == 1 & group_count > 2 ~ "same shape, prev ain deleted",
      same_ain == 0 & same_counts == 0 & group_count > 2 ~ "same shape, ain changed, multiple ains split on same shape",
      .default = NA
    )
  ) %>%
  rename(use_code_prev = use_code.x,
         use_code_curr = use_code.y,
         address_prev = address.x,
         address_curr = address.y)


### Different shapes - needs intersect
diff_shape <- match_parcels_wide %>%
  filter(shape_match==0)

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

# Create crosswalk with case_when for status and source
diff_shape_xwalk <- intersection %>%
  # Filter for relevant records
  filter((ain_match == 1 | pct_overlap_prev > 10)) %>%
  st_drop_geometry() %>%
  mutate(
    status = case_when(
      ain_match == 1 ~ "same ain, slight parcel change",
      TRUE ~ "prev parcel merged or split"
    ),
    source = case_when(
      ain_match == 1 ~ "spatial intersect, ain match",
      pct_overlap_prev >= 100 ~ "spatial intersect, 100% overlap",
      pct_overlap_prev >= 90 ~ "spatial intersect, over 90% overlap",
      TRUE ~ "spatial intersect, manual match"
    )
  ) %>%
  select(ain, ain.1, pct_overlap_prev, pct_overlap_curr, 
         use_code, address_prev, use_code.1, address_curr, 
         status, source) %>%
  rename(
    ain_prev = ain,
    ain_curr = ain.1,
    use_code_prev = use_code,
    use_code_curr = use_code.1
  )

final_xwalk <- bind_rows(
  same_shape_xwalk %>% 
    select(ain_prev, ain_curr, use_code_prev, use_code_curr, 
           address_prev, address_curr, source, status, dupe_id),
  diff_shape_xwalk %>% 
    select(ain_prev, ain_curr, pct_overlap_prev, pct_overlap_curr,
           use_code_prev, use_code_curr, address_prev, address_curr, 
           source, status))  %>%
  # rename columns for export
  rename_with(~ gsub("_prev$", paste("", prev_year, prev_month, sep="_"), .x)) %>%
  rename_with(~ gsub("_curr$", paste("",curr_year, curr_month, sep="_"), .x))

##### Step 3: Export #####
indicator <- "Updated Crosswalk of Assessor AINs from prev(ious) to curr(ent) shapes based on significantly damaged residential parcels from January"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\monthly_updates\\QA_parcel_xwalk.docx"

table_name <- paste(table_name, "test", sep="_")# remove once this looks good (just use normal table_name set at top of script)
source <- "Data Prep\\Monthly Updates\\parcel_xwalk.R"
dbWriteTable(con, Id(schema, table_name), final_xwalk,
             overwrite = FALSE, row.names = FALSE)
dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", table_name, " IS '", indicator, "
            Data imported on ", date_ran, ". ",
                            "QA DOC: ", qa_filepath,
                            " Source: ", source, "'"))
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


##### QA Checks #####
old_xwalk_all <- st_read(con, query="SELECT * from data.crosswalk_assessor_jan_sept_2025")
new_xwalk <- st_read(con, query="SELECT * from dashboard.crosswalk_assessor_01_09_2025_test")

old_xwalk <- old_xwalk_all %>% filter(ain_jan %in% parcel_universe$ain_2025_01)

missing_jan <- old_xwalk %>% anti_join(new_xwalk, by=c("ain_jan"="ain_2025_01"))
missing_sept <- old_xwalk %>% anti_join(new_xwalk, by=c("ain_sept"="ain_2025_09"))

nrow(new_xwalk)
length(unique(new_xwalk$ain_2025_01))
nrow(old_xwalk)
length(unique(old_xwalk$ain_jan))
length(unique(old_xwalk$ain_jan))-length(unique(new_xwalk$ain_2025_01))
# gap of 53 added to xwalk test?


missing_jan <- new_xwalk %>% anti_join(old_xwalk, by=c("ain_2025_01"="ain_jan"))
missing_sept <- new_xwalk %>% anti_join(old_xwalk, by=c("ain_2025_09"="ain_sept"))

# na use codes?
test <- old_xwalk_all %>% filter(ain_sept=="5844003040" | ain_jan=="5844003040")

test_xwalk_result <- new_xwalk %>% left_join(old_xwalk_all %>% mutate(old=TRUE), by=c("ain_2025_01"="ain_jan","ain_2025_09"="ain_sept"))

table(test_xwalk_result$old,useNA='always')

mismatch <- filter(test_xwalk_result, is.na(old))

# changed parcels
test_xwalk_result_2 <- mismatch %>% left_join(old_xwalk_all %>% mutate(old=TRUE), by=c("ain_2025_01"="ain_jan")) %>%
  select("ain_2025_01","ain_2025_09","ain_sept", everything())





