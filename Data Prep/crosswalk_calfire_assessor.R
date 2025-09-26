options(scipen = 999) # turn off scientific notation for batch queries

library(dplyr)
library(data.table)
library(sf)
library(mapview)

options(scipen = 999) # turn off scientific notation for batch queries
source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\assessor_data_functions.R")
con <- connect_to_db("altadena_recovery_rebuild")

##### SHP FILES #####
jan_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250106/parcel.shp"

##### get calfire damage points #####

calfire <- st_read(con, quer="SELECT global_id, apn_parcel, geom as geometry FROM data.eaton_fire_dmg_insp_3310")
st_crs(calfire)

jan_parcel_matches <- batch_filter_shapefile_intersect(shp_path=jan_shp_path, target_points=calfire, chunk_size = 10000)
st_crs(jan_parcel_matches)
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

ain_apn_matches <- check_point_dupes %>%
  filter(apn_parcel==AIN) %>%
  select(global_id, apn_parcel, AIN)

length(unique(ain_apn_matches$global_id)) #81

# these are the ones from missing_parcels.R that are condos that should 
# have their own AIN/APN. Don't understand the duplicates and that there
# are unique global_id values within but to reconcile will pull in the 
# these from pg using apn_parcel and then filter for matching apn and global_id

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

no_matches <- setdiff(check_point_dupes$global_id, ain_apn_matches$global_id) %>%
  data.frame(global_id=.) %>%
  left_join(jan_intersects_results, by="global_id") %>%
  left_join(street_nums) %>%
  filter(new_apn==AIN) %>%
  select(global_id, apn_parcel, AIN)
  
fixed_dupes <- rbind(no_matches, ain_apn_matches) %>%
  left_join(jan_intersects_results) %>%
  select(-geometry)

check_point_dupes <- check_point_dupes %>%
  select(global_id, apn_parcel, AIN, freq)

remove_dupes <- jan_intersects_results %>%
  left_join(check_point_dupes) %>% 
  filter(is.na(freq)) %>%
  select(-freq) %>%
  st_drop_geometry()

final <- rbind(remove_dupes, fixed_dupes)


length(unique(final$global_id))
length(unique(final$apn_parcel))
length(unique(final$GlobalID))
length(unique(final$AIN))

final <- final %>% 
  select(global_id, apn_parcel, AIN, GlobalID, match_date) %>%
  rename(calfire_apn=apn_parcel,
         assessor_ain=AIN,
         calfire_global_id = global_id,
         assessor_global_id = GlobalID)

##### Export to postgres #####
charvect = rep('varchar', ncol(final)) 
names(charvect) <- colnames(final)  

table_name <- 'calfire_assessor_xwalk_jan2025'
schema<- 'data'
indicator <- "Crosswalk of points from CalFIRE damage assessment to LAC Assessor parcels from Jan 2025" 
source <- "R Script: Data Prep\\crosswalk_calfire_assessor.R"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_import_assessor_data.docx"

table_comment <- paste0(indicator, source)

dbWriteTable(con,
             Id(schema = schema, table = table_name),
             final, overwrite = FALSE, row.names = FALSE,  field.types = charvect)


#Add comment on table and columns
column_names <- colnames(final) 
column_comments <- c(
  "CalFire-assigned ID - should be unique alone or in conjunction with calfire_apn",
  "CalFire-assigned Assessor''s Parcel Number (APN); can differ from assessor_ain",
  "LAC Assessor-assigned Assessor''s Identification Number (AIN)",
  "LAC Assessor-assigned ID - should be unique alone (or in conjunction with assessor_ain)",
  "Version of Assessor data used to create the crosswalk"
)

add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

# # look into one example - old
# calfire %>% filter(global_id=="061a5a1e-bcb2-4726-87cd-324a84044932")
# multiple_parcels <- jan_intersects_results %>% filter(global_id=="061a5a1e-bcb2-4726-87cd-324a84044932") %>% select(GlobalID) %>%
#   st_drop_geometry() %>% pull()
# 
# multiple_parcels_shp <- batch_filter_shapefile(shp_path=jan_shp_path, target_ains=multiple_parcels, chunk_size = 10000, ain_column = "GlobalID") 
# 
# mapview(multiple_parcels_shp)
