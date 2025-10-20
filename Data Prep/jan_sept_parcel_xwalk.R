# Create a crosswalk of Jan 2025 and Sept 2025 Altadena parcel data


# Library and environment set up ----

library(sf)
library(mapview)

options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")
# con_rda <- connect_to_db("rda_shared_data")
# con_fires <- connect_to_db("la_fires")

# get assessor parcels and add an identifier column

parcels_jan <- st_read(con_alt, query="SELECT parcels.ain, parcels.geom, stats.use_code, stats.situs_house_no, stats.direction, stats.street_name, stats.unit, stats.city_state 
                       FROM data.assessor_parcels_universe_jan2025 parcels
                       LEFT JOIN data.assessor_data_universe_jan2025 stats
                       ON parcels.ain=stats.ain") %>%
  mutate(flag="jan") %>%
  mutate(area = st_area(geom))

parcels_sept <- st_read(con_alt, query="SELECT parcels.ain, parcels.geom, stats.use_code, stats.situs_house_no, stats.direction, stats.street_name, stats.unit, stats.city_state 
                       FROM data.assessor_parcels_universe_sept2025 parcels
                       LEFT JOIN data.assessor_data_universe_sept2025 stats
                       ON parcels.ain=stats.ain")%>%
  mutate(flag="sept") %>%
  mutate(area = st_area(geom))

# get assessor data: not sure if we need this though so commenting out for now

 assessor_jan <- st_read(con_alt, query="Select * from data.assessor_data_universe_jan2025")
 assessor_sept <- st_read(con_alt, query="Select * from data.assessor_data_universe_sept2025")

# double check CRS of both of parcel shapes
st_crs(parcels_jan)$epsg #3310 good
st_crs(parcels_sept)$epsg #3310 good


# # Intersect Jan and Sept parcel shapes-------------------------------------
# 
# parcels_join <- st_join(parcels_jan, parcels_sept, join = st_intersects,
#                         suffix = c("_jan", "_sept"))
# 
# 
# class(parcels_join) # check object type is sf data frame which is what we want
# 
# # Perform checks on the intersect------------------------------------------
# 
# # See how many unique AINs are there for jan and sept: same number
# 
# length(unique(parcels_join$ain_jan)) #54874
# length(unique(parcels_join$ain_sept)) #54874
# 
# # look at result just the AIN column:
# # This is definitely not a clean 1:1 for most of the AINs. It also seems there might be a many to many relationship
# # with the AINs as well. 
# 
# # For example, AIN 5317001003 in Jan matches with 5 different Sept AINs, one of which matches 5317001003
# 
# join_ain<-parcels_join%>%select(ain_jan, intersect_pct_jan, ain_sept, intersect_pct_sept)
# join_ain_filter <- join_ain %>%
#   filter(intersect_pct_jan > 80 & intersect_pct_sept > 80)
# 
# # See if any AINs are NA after the join 
# 
# sept_not_joined <- parcels_join %>%
#   filter(is.na(ain_sept))%>%View() # None
# 
# jan_not_joined <- parcels_join %>%
#   filter(is.na(ain_sept))%>%View() # None
# 
# # Pull out rows where the AINs are matching
# same_ain <- parcels_join %>%
#   filter(!is.na(ain_jan) & ain_jan == ain_sept) %>%
#   select(ain_jan, intersect_pct_jan, ain_sept, intersect_pct_sept)
# 
# # See how many AINs is that:
# length(unique(same_ain$ain_sept)) #54825 
# 
# # Now I want to extract the opposite: the rows where the jan ain and sept ain are not matching
# not_matching_ain <- parcels_join %>%
#   filter(!is.na(ain_jan) & !is.na(ain_sept) & ain_jan != ain_sept) %>%
#   select(ain_jan, intersect_pct_jan, ain_sept, intersect_pct_sept)
# 
# # See how many AINs is that:
# length(unique(not_matching_ain$ain_sept)) #54813 
# 
# # Pull out duplicate AINs and count
# duplicate_ain <- parcels_join %>%
#   add_count(ain_jan, name = "ain_count_jan") %>%
#   add_count(ain_sept, name = "ain_count_sept") %>%
#   filter(ain_count_jan > 1| ain_count_sept > 1) %>% 
#   select(ain_jan, ain_count_jan, ain_sept, ain_count_sept)
# 
# # see how many unique AINs are among duplicates
# length(unique(duplicate_ain$ain_jan)) #54817
# length(unique(duplicate_ain$ain_sept)) #54817

# # map of duplicates - not this is very large and takes long to load
# mapview(duplicate_ain)


# QA Notes: Recommending this approach 

##### Step 1: find out which shapes are the same in jan and sept #####
match_parcels <- rbind(parcels_jan, parcels_sept) %>%
  # add address to see if that helps with matching shapes with different ains later
  mutate(address=paste(situs_house_no, direction, street_name, unit, city_state)) %>%
  mutate(address=gsub("\\s+", " ", address)) %>%
  select(-c(situs_house_no, direction, street_name, unit, city_state)) %>%
  # group by shape and see how many duplicates there are - note: this takes a couple mins
  mutate(geom_wkt = st_as_text(geom)) %>%
  st_drop_geometry() %>%
  group_by(geom_wkt) %>%
  mutate(group_count = n()) %>%
  # shapes appearing more than once get a group id (dupe_id), if it's unique then NA
  mutate(dupe_id = ifelse(group_count>1,cur_group_id(), NA)) %>%
  ungroup() 
# 109748
# check <- data.frame(table(match_parcels$dupe_id, useNA = "ifany"))
# number of dupe groups - 44524
# number of unique shapes - 354
# total unique shapes - 44878

# Make wider, clean up values to filter later
match_parcels_wide <- match_parcels %>%
  select(-c(area, use_code, address)) %>%
  pivot_wider(
    names_from = flag,
    values_from = flag,
    names_prefix = "flag_"
  ) %>%
  mutate(
    # Convert flags to binary (1/0) and handle NAs in one step
    flag_jan = as.integer(!is.na(flag_jan)),
    flag_sept = as.integer(!is.na(flag_sept)),
    # Flag for same AIN in both months
    same_ain = as.integer(flag_jan == 1 & flag_sept == 1)
  ) %>%
  # Calculate totals by dupe_id
  group_by(dupe_id) %>%
  mutate(
    total_jan = sum(flag_jan),
    total_sept = sum(flag_sept),
    # Shape match: has dupe_id and both months present
    shape_match = as.integer(!is.na(dupe_id) & total_jan > 0 & total_sept > 0)
  ) %>%
  mutate(even_counts = ifelse(total_jan==total_sept, 1, 0)) %>%
  ungroup() %>%
  select(dupe_id, ain, shape_match, same_ain, even_counts, everything()) %>%
  mutate(
    status = case_when(
      shape_match==0 ~ "run intersect by month",
      shape_match==1 & same_ain == 0 & even_counts==1 & group_count==2 ~ "diff ain pair, simple xwalk",
      shape_match==1 & same_ain == 0 & even_counts==1 & group_count>2 ~ "ambiguous matches, needs closer look",
      shape_match==1 & same_ain == 0 & even_counts==0 & group_count>2 ~ "uneven matches, needs closer look",
      shape_match==1 & same_ain == 1 & even_counts==1 ~ "same ains, simple xwalk",
      .default = "undefined status, please review")
  )

table(match_parcels_wide$status)
## diff ain pair, simple xwalk ambiguous matches, needs closer look               run intersect by month              same ains, simple xwalk    uneven matches, needs closer look 
## 8                                    4                                  370                                54674                                   18 


##### Review each status type separately to create a xwalk, then combine at end #####
### Status: "same ains, simple xwalk"
same_ain <- match_parcels_wide %>%
  filter(status == "same ains, simple xwalk") 
#54674

 length(unique(same_ain$dupe_id)) # 44516

same_ain_xwalk <- same_ain %>%
  select(ain, dupe_id) %>%
  mutate(ain_jan = ain,
         ain_sept = ain) %>%
  select(-ain) %>%
  mutate(status = "same ains, simple xwalk")


### Status: "diff ain pair, simple xwalk"
diff_ain <- match_parcels_wide %>%
  filter(status == "diff ain pair, simple xwalk") 

 length(unique(diff_ain$dupe_id)) # 4

diff_ain_xwalk <- diff_ain %>%
  group_by(dupe_id) %>%
  summarise(
    ain_jan = ain[flag_jan == 1],
    ain_sept = ain[flag_sept == 1]) %>%
  mutate(status = "diff ain pair, simple xwalk")

### Status: "ambiguous matches, needs closer look"
ambiguous <- match_parcels_wide %>%
  filter(status == "ambiguous matches, needs closer look") 

 length(unique(ambiguous$dupe_id)) # 2

# note: these are two cases where the jan ain is for a parcel deleted in 2024 and the september ain is the correct parcel ain for that address
# https://portal.assessor.lacounty.gov/parceldetail/5722013039
# https://portal.assessor.lacounty.gov/parceldetail/5722013905

# https://portal.assessor.lacounty.gov/parceldetail/5739001072
# https://portal.assessor.lacounty.gov/parceldetail/5739001900

# xwalk will include a jan_ain_revised
ambiguous_xwalk <- ambiguous %>%
  group_by(dupe_id) %>%
  summarise(
    ain_jan = ain[flag_jan == 1],
    ain_sept = ain[flag_sept == 1]) %>%
  mutate(
    ain_jan_revised = ain_sept,
    status = "ambiguous matches, needs closer look")


### Status: "uneven matches, needs closer look"
uneven <-  match_parcels_wide %>%
  filter(status == "uneven matches, needs closer look")

# length(unique(uneven$dupe_id)) # 1

# note: this shape is for a parcel deleted in 12/2024, none of the matching sept shapes
# have valid assessor portal results
# also this is a commercial property
# address: 139 S OAK KNOLL AVE PASADENA CA 91101-2608
# Recommend revising to NA

uneven_xwalk <- uneven %>%
  group_by(dupe_id) %>%
  summarise(
    ain_jan = ain[flag_jan == 1],
    ain_sept = ain[flag_sept == 1]) %>%
  mutate(ain_jan_revised="999",
         ain_sept_revised="999",
         status="uneven matches, needs closer look")


### Status: "run intersect by month"
intersect_jan <- match_parcels_wide %>%
  filter(status == "run intersect by month" & flag_jan==1) # 193

jan_parcels_filtered <- parcels_jan %>% filter(ain %in% intersect_jan$ain)

jan_join <- st_intersection(jan_parcels_filtered, parcels_sept) %>%
  mutate(area_intersect = st_area(geom)) %>%
  mutate(pct_jan_overlap = as.numeric(area_intersect)/as.numeric(area)*100,
         pct_sept_overlap = as.numeric(area_intersect)/as.numeric(area.1)*100) %>%
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
         sept_na = ifelse(is.na(use_code.1), 1, 0),
         in_sept_shp = ifelse(ain %in% parcels_sept$ain, 1, 0),
         ain_match = ifelse(ain==ain.1, 1 ,0)
         ) %>%
  filter(pct_jan_overlap>0)

# get results where overlapping parcels have same ain
jan_ain_match <-jan_join %>% 
  filter(ain_match==1) %>%
  st_drop_geometry() %>%
  select(ain, ain.1) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(status="run intersect by month")

 length(unique(jan_ain_match$ain_jan)) # 151

jan_leftover <- jan_join %>% 
  st_drop_geometry() %>%
  filter(!(ain %in% jan_ain_match$ain_jan)) %>% 
  filter(pct_jan_overlap>10)

length(unique(jan_leftover$ain)) # 42

# jan ain is a deleted parcel, should be revised to sept ain
jan_revise_ain <- jan_leftover %>%
  filter(address_match==1 | sept_na==0) %>% # 3
  select(ain, ain.1) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(ain_jan_revised=ain_sept,
         status="run intersect by month")

# these should have revised sept_ains as jan ain
bad_sept_ains <- jan_leftover %>% 
  filter(!(ain%in%jan_revise_ain$ain_jan)) %>% 
  group_by(ain) %>% 
  slice(which.max(pct_jan_overlap)) %>% # 39
  select(ain, ain.1) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(ain_sept_revised=ain_jan,
         status="run intersect by month")

intersect_jan_xwalk <- bind_rows(jan_ain_match, jan_revise_ain, bad_sept_ains) %>%
  mutate(dupe_id=NA)

### notes leftover - can skip to export
# notes: some are matching to sept ains with no assessor details (e.g., use_codes and address fields are NA)
# pull below and spot checked and these are not returning valid assessor parcel details 

jan_nas <- jan_join %>% filter(is.na(use_code.1))
# sort(unique(jan_nas$ain.1))
## [1] "5327012026" "5327012027" "5709030033" "5709030034" "5711010080" "5713008086" "5719022111" "5719022114" "5720001013" "5720001014" "5720001015" "5720001016" "5720001017" "5723015082"
## [15] "5725002918" "5726018095" "5742001038" "5746016092" "5748036037" "5748036038" "5757029055" "5757029056" "5825020910" "5830015029" "5841023022" "5847020027"

# I'm not going to deal with sept parcels at the moment, just putting it as a placeholder here.
intersect_sept <- match_parcels_wide %>%
  filter(status == "run intersect by month" & flag_sept==1)

##### combine xwalks and export #####
combined_xwalks <- bind_rows(intersect_jan_xwalk, uneven_xwalk, ambiguous_xwalk, diff_ain_xwalk, same_ain_xwalk) 

# # # Export
# schema <- "data"
# table_name <- "crosswalk_assessor_jan_sept_2025"
# indicator <- "Crosswalk of 2025 Assessor AINs from January to September using the respective shp universe tables."
# qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_assessor_shp_xwalk.docx"
# 
# source <- "Data Prep\\jan_sept_parcel_xwalk.R"
# dbWriteTable(con_alt, Id(schema, table_name), combined_xwalks,
#              overwrite = FALSE, row.names = FALSE)
# dbSendQuery(con_alt, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
#             Data imported on 10-15-25. ",
#                         "QA DOC: ", qa_filepath,
#                         " Source: ", source, "'"))
# colnames(combined_xwalks)
# col_comments <- c("ain in january shp data",
#                   "ain in september shp data",
#                   "qa column that refers to categories used to crosswalk",
#                   "includes an alternative ain if ain_jan is associated with a deleted parcel or 999 if ain_jan should be excluded from analysis",
#                   "includes an alternative ain if ain_sept is associated with a deleted parcel or 999 if ain_sept should be excluded from analysis",
#                   "qa column that tracks duplicate parcel shapes")
# 
# 
# add_table_comments(con=con_alt, schema=schema,table_name=table_name,indicator = indicator, qa_filepath = qa_filepath,
#                    source=source,column_names = colnames(combined_xwalks), column_comments = col_comments)

# JZ QA 10/20----------------------------------------------

# AINs in assessor_data_universe_jan2025 but NOT in combined_xwalk

missing_in_xwalk <- assessor_jan %>%
  anti_join(combined_xwalks, by = c("ain" = "ain_jan"))

setdiff(assessor_jan, combined_xwalks)

# AINs in combined_xwalk but NOT in assessor_data_universe_jan2025

missing_in_assessor <- combined_xwalks %>%
  anti_join(assessor_jan, by = c("ain_jan" = "ain")) # This produces 48 jan AINs that are in the xwalk but not in the assessor data

# I want to see of those 48 jan AINS from the xwalk if their corresponding sept AIN is in the jan and sept assessor data:

check_missing_sept_ain_in_jan_assessor <- assessor_jan %>%
  filter(ain %in% missing_in_assessor$ain_sept)

# Of the missing Jan AINs, one of the corresponding Sept AINs is in the Jan Assessor data --this one makes sense because it is the one with the updated jan AIN
# So really there are only 47 Jan AINs in the xwalk missing in the Jan assessor data

# Check missing ains in the sept assessor data:

check_missing_jan_ain_in_sept_assessor <- assessor_sept %>%
  filter(ain %in% missing_in_assessor$ain_jan)

# of the 47 missing Jan AINs, 13 of them are NOT missing in the Sept Assessor data


check_missing_sept_ain_in_sept_assessor <- assessor_sept %>%
  filter(ain %in% missing_in_assessor$ain_sept) 

# of the 47 missing Jan AINs, 15 of their corresponding Sept AINs are NOT missing in the Sept Assessor data


