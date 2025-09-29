# join all structures from CalFire Eaton fire damage assessment to an assessor parcel

library(dplyr)
library(data.table)
library(sf)
library(mapview)

options(scipen = 999) # turn off scientific notation for batch queries
source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\assessor_data_functions.R")
con <- connect_to_db("altadena_recovery_rebuild")

##### Define/load data #####
jan_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250106/parcel.shp"

calfire <- st_read(con, quer="SELECT global_id, apn_parcel, geom as geometry FROM data.eaton_fire_dmg_insp_3310")
st_crs(calfire)

##### Run intersection ######
# jan_parcel_matches <- batch_filter_shapefile_intersect(shp_path=jan_shp_path, target_points=calfire, chunk_size = 10000)
st_crs(jan_parcel_matches) # 2229

# mapview(jan_parcel_matches) 

jan_parcel_matches_3310 <- st_transform(jan_parcel_matches, 3310)
jan_intersects_results <- st_join(calfire, jan_parcel_matches_3310, join=st_intersects) %>%
  mutate(match_date="jan")

# get damage points with multiple parcel matches
check_point_dupes <- jan_intersects_results %>%
  select(global_id, AIN, GlobalID, geometry) %>%
  st_drop_geometry() %>%
  group_by(global_id) %>%
  summarise(freq = n()) %>%
  filter(freq>1) %>%
  right_join(calfire, by="global_id") %>%
  filter(!is.na(freq)) %>%
  left_join(jan_intersects_results, by=c("global_id", "apn_parcel"))

length(unique(check_point_dupes$global_id)) #92

# these seem to be condos but there is a record where apn=ain for each
# will keep those records and discard the rest
ain_apn_matches <- check_point_dupes %>%
  filter(apn_parcel==AIN) %>%
  select(global_id, apn_parcel, AIN)

length(unique(ain_apn_matches$global_id)) #81

# the remaining 11 are the ones from missing_parcels.R that are condos that  
# have their own AIN/APN but for some reason are all recorded with the same apn
# in CalFire. To reconcile will manually define the correct AIN
# then filter for matching apn and global_id

# correct ain:
# "5833019043", # 155 E Palm Street Altadena CA 91001
# "5833019044", # 157 E Palm Street Altadena CA 91001
# "5833019045", # 173 E Palm Street Altadena CA 91001
# "5833019046", # 175 E Palm Street Altadena CA 91001
# "5833019047", # 177 E Palm Street Altadena CA 91001
# "5833019048", # 181 E Palm Street Altadena CA 91001
# "5833019049", # 185 E Palm Street Altadena CA 91001
# "5833019050", # 187 E Palm Street Altadena CA 91001
# "5833019051", # 193 E Palm Street Altadena CA 91001
# "5833019052", # 195 E Palm Street Altadena CA 91001
# "5833019053" # 197 E Palm Street Altadena CA 91001

street_nums <- dbGetQuery(con, "SELECT global_id, apn_parcel, street_number FROM data.eaton_fire_dmg_insp_3310 WHERE apn_parcel='5833019042'") %>%
  mutate(new_apn = case_when(
    street_number==155 ~ "5833019043",
    street_number==157 ~ "5833019044",
    street_number==173 ~ "5833019045",
    street_number==175 ~ "5833019046",
    street_number==177 ~ "5833019047",
    street_number==181 ~ "5833019048",
    street_number==185 ~ "5833019049",
    street_number==187 ~ "5833019050",
    street_number==193 ~ "5833019051",
    street_number==195 ~ "5833019052",
    street_number==197 ~ "5833019053",
  ))

# the remaining 11 structures
ain_apn_no_match <- setdiff(check_point_dupes$global_id, ain_apn_matches$global_id) %>%
  data.frame(global_id=.) %>%
  left_join(jan_intersects_results, by="global_id") %>%
  left_join(street_nums) %>%
  filter(new_apn==AIN) %>%
  select(global_id, apn_parcel, AIN)

# combine corrected records
deduped <- rbind(ain_apn_no_match, ain_apn_matches) %>%
  left_join(jan_intersects_results) %>%
  select(-geometry)

# get freq count again
check_point_dupes <- check_point_dupes %>%
  select(global_id, apn_parcel, AIN, freq)

# join freq col drop records that have duplicates
remove_dupes <- jan_intersects_results %>%
  left_join(check_point_dupes) %>% 
  filter(is.na(freq)) %>%
  select(-freq) %>%
  st_drop_geometry()

# combine for final de-duped df
final <- rbind(remove_dupes, deduped)

##### Check for NAs #####
na_ains <- final %>% filter(is.na(AIN))

calfire <- st_read(con, query="SELECT * FROM data.eaton_fire_dmg_insp_3310")
calfire_na <- na_ains %>%
  left_join(calfire, by="global_id") %>%
  st_as_sf(sf_column_name="geom", crs=st_crs(calfire)) 

mapview(calfire_na)
# all the mount wilson addresses should be apn/ain: 5862017310 - the parcel shapes in the assessor portal are not clipped properly https://portal.assessor.lacounty.gov/parceldetail/5862017310
# 3308 bellaire rd has an AIN: 5833010028; looks like coordinate is slightly off

# 5829002013 - apn looks correct and matches ain based on address here: https://portal.assessor.lacounty.gov/parceldetail/5829002013
# same for others: 5832009005, 5853007006, 5862017310
# search csv and shapefile for this AIN
check <- calfire_na %>% st_drop_geometry() %>% select(apn_parcel.x) %>% pull()

# based on apns expect they'll be in csv 2 and/or 3
jan_2_results <- batch_process_assessor_data(
  csv_file=jan_csv_2,
  target_list = check,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE)

jan_shp_results <- batch_filter_shapefile(
  shp_path=jan_shp_path,
  target_ains=check,
  chunk_size = 5000,
  ain_column = "AIN")

check_csv_results <- jan_2_results %>%
  left_join(jan_shp_results, by=c("ï»¿AIN"="AIN")) %>%
  st_as_sf(sf_column_name="_ogr_geometry_", crs=st_crs(jan_shp_results))

st_crs(calfire_na)

# can see coordinates for these four structures are just outside of their parcels
mapview(calfire_na) + mapview(check_csv_results)

##### run nearest feature to see if we can grab the correct parcel for structures with NA AIN #####
# Note - best to import parcel shp to pg (done for Jan 2025) for this to run quickly
# check within 100 meters
calfire_na_2229 <-st_transform(calfire_na, 2229)
nearest_parcels <- find_nearest_postgis(con, unmatched_points=calfire_na_2229, max_distance = 100) 
calfire_na_parcel <- calfire_na %>%
  mutate(row_id=row_number()) %>%
  left_join(nearest_parcels, by=c("row_id"="point_index")) %>% 
  select(global_id, apn_parcel.x, nearest_ain, distance, starts_with("street"), city, state, zip_code, everything()) %>%
  rename(apn_parcel=apn_parcel.x)

# based on above - results match what we wanted
# replace the NA AINs in final with these results
nearest_ain <- calfire_na_parcel %>%
  st_drop_geometry() %>%
  select(global_id, apn_parcel, nearest_ain)

# get shp data for these ains
nearest_ain_results <- batch_filter_shapefile(
  shp_path=jan_shp_path,
  target_ains=nearest_ain$nearest_ain,
  chunk_size = 5000,
  ain_column = "AIN")

replace_ain <- nearest_ain_results %>%
  select(GlobalID, AIN) %>%
  st_drop_geometry() %>%
  left_join(nearest_ain, by=c("AIN"="nearest_ain")) 

#drop previous NA AINs
final_dropna <- final %>% #18422
  filter(!is.na(AIN)) %>%
  select(global_id, apn_parcel, AIN, GlobalID) 


final2 <- rbind(final_dropna, replace_ain) %>%
  rename(calfire_apn=apn_parcel,
         assessor_ain=AIN,
         calfire_global_id = global_id,
         assessor_global_id = GlobalID)


#### Final review and export #####
length(unique(final2$calfire_global_id))  #18422
length(unique(final2$calfire_apn)) #11068
length(unique(final2$assessor_global_id)) # 11086
length(unique(final2$assessor_ain))  # 11086


##### Export to postgres #####
charvect = rep('varchar', ncol(final2)) 
names(charvect) <- colnames(final2)  

table_name <- 'calfire_assessor_xwalk_jan2025'
schema<- 'data'
indicator <- "Crosswalk of points from CalFIRE damage assessment to LAC Assessor parcels from Jan 2025" 
source <- "R Script: Data Prep\\crosswalk_calfire_assessor.R"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_import_assessor_data.docx"

table_comment <- paste0(indicator, source)

dbWriteTable(con,
             Id(schema = schema, table = table_name),
             final2, overwrite = FALSE, row.names = FALSE,  field.types = charvect)


#Add comment on table and columns
column_names <- colnames(final2) 
column_comments <- c(
  "CalFire-assigned ID - should be unique alone or in conjunction with calfire_apn",
  "CalFire-assigned Assessor''s Parcel Number (APN); can differ from assessor_ain",
  "LAC Assessor-assigned Assessor''s Identification Number (AIN)",
  "LAC Assessor-assigned ID - should be unique alone (or in conjunction with assessor_ain)",
  "Version of Assessor data used to create the crosswalk"
)

add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

