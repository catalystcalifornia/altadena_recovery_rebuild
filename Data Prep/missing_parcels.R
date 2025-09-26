options(scipen = 999) # turn off scientific notation for batch queries

library(mapview)

source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\assessor_data_functions.R")
con <- connect_to_db("altadena_recovery_rebuild")


##### use st_intersects to determine which damage points match to an existing Assessor parcel shape
# # damage points not matched to a parcel
missing_parcels <- read.csv("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Prepped\\missing_parcels.csv") %>%
  mutate(geom = as.numeric(gsub("c(", "", geom, fixed = TRUE)),
         geom2 = as.numeric(gsub(")", "", geom2, fixed=TRUE))) %>%
  rename(longitude=geom,
         latitude=geom2) %>%
  filter(!is.na(latitude)) %>%
  filter(!is.na(longitude)) # no geoms, filter out before we can st as sf


sept_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250902/parcels.shp"
jan_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250106/parcel.shp"
# 
# # 101 parcel shapes
# missing_parcels <- st_as_sf(missing_parcels, coords=c("longitude", "latitude"), crs=3310)
# sept_matches <- batch_filter_shapefile_spatial(shp_path = sept_shp_path, target_points = missing_parcels, chunk_size = 10000)
# sept_matches_3310 <- st_transform(sept_matches, 3310)
# 
# # 101 parcel shapes
# jan_matches <- batch_filter_shapefile_spatial(shp_path = jan_shp_path, target_points = missing_parcels, chunk_size = 10000)
# jan_matches_3310 <- st_transform(jan_matches, 3310)
# 
# # visually parcels looks to be the same
# mapview(jan_matches, col.regions = "yellow") + 
#   mapview(sept_matches, col.regions = "darkgreen") + 
#   mapview(missing_parcels, col.regions = "black")

# # join
# sept_intersects_results <- st_join(missing_parcels, sept_matches_3310, join=st_intersects) %>%
#   mutate(match_date="sept")
# 
# jan_intersects_results <- st_join(missing_parcels, jan_matches_3310, join=st_intersects) %>%
#   mutate(match_date="jan")
# 
# setdiff(sept_intersects_results$AIN, jan_intersects_results$AIN)
# setdiff(jan_intersects_results$AIN, sept_intersects_results$AIN)
# 
# ##### Cross check matched parcels to postgres tables
# ain_results <- unique(c(sept_intersects_results$AIN, jan_intersects_results$AIN))
# ain_results <- data.frame(ain=ain_results)
# 
# write.csv(ain_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/ain_missing_parcel_matches.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")
# 

##### Use AIN from matched Assessor shape and filter Assessor CSVs for additional data #####
ain_results <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/ain_missing_parcel_matches.csv")         %>%
  mutate(ain=as.character(ain)) %>%
  pull()

# Get additional AIN/APN from Elycia
missing_parcel_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Prepped\\missing_jan_parcels_092625.csv"
jan_missing <- read.csv(missing_parcel_filepath) 

missing_parcels <- missing_parcels %>%
  st_drop_geometry() 

# Combine for unique list of APN/AINs that we want to search Assessor CSVs
all_missing <- full_join(missing_parcels, jan_missing, by="din_id", keep=FALSE) %>%
  unique()

# 105 unique apn/ain
apns <- all_missing %>%
  select(apn_parcel.y) %>%
  unique()
  pull()

sept_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 1.csv"
sept_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 2.csv"
sept_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 3.csv"

jan_csv_1 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 1.csv"
jan_csv_2 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 2.csv"
jan_csv_3 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 3.csv"

# # Run on Jan csvs
# jan_1_filter <- batch_process_assessor_data(csv_file=jan_csv_1, 
#                                              target_list = apns, 
#                                              filter_column="ï»¿AIN",
#                                              chunk_size = 10000, 
#                                              debug_filter=TRUE)
# 
# jan_2_filter <- batch_process_assessor_data(csv_file=jan_csv_2, 
#                                              target_list = apns, 
#                                              filter_column="ï»¿AIN",
#                                              chunk_size = 10000, 
#                                              debug_filter=TRUE)
# 
# jan_3_filter <- batch_process_assessor_data(csv_file=jan_csv_3, 
#                                              target_list = apns, 
#                                              filter_column="ï»¿AIN",
#                                              chunk_size = 10000, 
#                                              debug_filter=TRUE)
# 
# # combine results and write to csv
# all_results<- rbind(jan_1_filter, jan_2_filter, jan_3_filter, fill=TRUE)
# 
# write.csv(all_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/missing_parcel_jan_csv_matches.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")
#
#
# # Run on Sept CSVs
# sept_1_filter <- batch_process_assessor_data(csv_file=sept_csv_1,
#                         target_list = apns,
#                         filter_column="ï»¿AIN",
#                         chunk_size = 10000,
#                         debug_filter=TRUE)
# 
# sept_2_filter <- batch_process_assessor_data(csv_file=sept_csv_2,
#                                              target_list = apns,
#                                              filter_column="ï»¿AIN",
#                                              chunk_size = 10000,
#                                              debug_filter=TRUE)
# 
# sept_3_filter <- batch_process_assessor_data(csv_file=sept_csv_3,
#                                              target_list = apns,
#                                              filter_column="ï»¿AIN",
#                                              chunk_size = 10000,
#                                              debug_filter=TRUE)
# 
# # combine results and write to csv
# all_results<- rbind(sept_1_filter, sept_2_filter, sept_3_filter)
# 
# write.csv(all_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/missing_parcel_sept_csv_matches.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")


##### compare results #####
sept_missing <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/missing_parcel_sept_csv_matches.csv")
jan_missing <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/missing_parcel_jan_csv_matches.csv")

length(unique(jan_missing$Ã.Â.Â.AIN)) # 100
length(unique(sept_missing$Ã.Â.Â.AIN)) # 100

# both result sets are the same
check <- full_join(sept_missing, jan_missing, by= "Ã.Â.Â.AIN") %>%
  mutate(`Ã.Â.Â.AIN`=as.character(`Ã.Â.Â.AIN`))
apns <- as.data.frame(apns)
# compare to initial list of apns to see what has no match
check <- check %>%
  full_join(apns, by=c("Ã.Â.Â.AIN"="apn_parcel.y"))

no_csv_match <- check %>%
  filter(is.na(TRA.x)) %>%
  select(`Ã.Â.Â.AIN`) %>%
  left_join(all_missing, by=c("Ã.Â.Â.AIN"="apn_parcel.y")) %>%
  select(-apn_parcel.x) %>%
  rename(apn=`Ã.Â.Â.AIN`) %>%
  left_join(all_missing, by="din_id")

unique(no_csv_match$apn)

##### Review AINs with no CSV matches #####

#"5764031012" # Get an error from assessor portal "can't load results"
# 1 associated address: 501 Lotus Lane blding D Sierra Madre CA
# address has two other associated AINs:
## 5764031010 (single family residence): https://portal.assessor.lacounty.gov/parceldetail/5764031010
## 5764031001 (vacant land): https://portal.assessor.lacounty.gov/parceldetail/5764031001

# "5764031011" # Get an error from assessor portal "can't load results"
# 1 associated address: 501 Lotus Lane blding A Sierra Madre CA (same as above)

# "5761002008" (institutional) # Parcel status: DELETED (https://portal.assessor.lacounty.gov/parceldetail/5761002008)
# 7 associated addresses - none appear to be residential: 
## 700 N Sunnyside Avenue, Sierra Madre, CA 91024 (church)
## 700 N Sunnyside Avenue blding A, Sierra Madre, CA 91024 
## 700 N Sunnyside Avenue blding C, Sierra Madre, CA 91024 
## 700 N Sunnyside Avenue blding D, Sierra Madre, CA 91024
## 700 N Sunnyside Avenue blding E, Sierra Madre, CA 91024
### above are also associated with another deleted AIN: 5761002007 (institutional use code)
## W Carter Avenue blding B, Sierra Madre, CA 91024
## W Carter Avenue blding A, Sierra Madre, CA 91024
### above have no explicit record in assessor portal

# "None"       # Can't look up none - will try associated addresses
# Only residence with "None": 3308 bellaire drive, altadena, ca 91001
# current AIN: 5833010028 (https://portal.assessor.lacounty.gov/parceldetail/5833010028)

# "5833019042" # Parcel status: DELETED (https://portal.assessor.lacounty.gov/parceldetail/5833019042)
# 11 associated addresses - ALL appear to be residential: 
## 155 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019043 (condominium)
## 157 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019044 (condominium)
## 173 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019045 (condominium)
## 175 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019046 (condominium)
## 177 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019047 (condominium)
## 181 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019048 (condominium)
## 185 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019049 (condominium)
## 187 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019050 (condominium)
## 193 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019051 (condominium)
## 195 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019052 (condominium)
## 197 E Palm Street Altadena CA 91001 - https://portal.assessor.lacounty.gov/parceldetail/5833019053 (condominium)

updated_apns <- c("5833010028", # 3308 bellaire drive, altadena, ca 91001
              "5764031010", # 501 Lotus Lane blding D Sierra Madre CA
              "5833019043", # 155 E Palm Street Altadena CA 91001
              "5833019044", # 157 E Palm Street Altadena CA 91001
              "5833019045", # 173 E Palm Street Altadena CA 91001
              "5833019046", # 175 E Palm Street Altadena CA 91001
              "5833019047", # 177 E Palm Street Altadena CA 91001
              "5833019048", # 181 E Palm Street Altadena CA 91001
              "5833019049", # 185 E Palm Street Altadena CA 91001
              "5833019050", # 187 E Palm Street Altadena CA 91001
              "5833019051", # 193 E Palm Street Altadena CA 91001
              "5833019052", # 195 E Palm Street Altadena CA 91001
              "5833019053" # 197 E Palm Street Altadena CA 91001
              ) 

matched_apns <- check %>%
  filter(!is.na(TRA.x)) %>%
  select(`Ã.Â.Â.AIN`) %>%
  rename(apn=`Ã.Â.Â.AIN`) %>%
  pull()

all_new_apns <- c(updated_apns, matched_apns)
##### check for shapes and csv records
new_apn_shps_jan <- batch_filter_shapefile(shp_path=jan_shp_path, target_ains=new_apns, chunk_size = 10000, ain_column = "AIN")

new_apn_shps_sept <- batch_filter_shapefile(shp_path=sept_shp_path, target_ains=new_apns, chunk_size = 10000, ain_column = "AIN")

mapview(new_apn_shps_jan)