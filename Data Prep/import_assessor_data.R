# Define the universe of assessor parcels for the East/West Altadena Analysis

##### Step 0 #####
# Load libraries, script options, functions, and minor file prep 
library(dplyr)
library(data.table)
library(sf)
library(mapview)
library(stringr)

options(scipen = 999) # turn off scientific notation for batch queries

source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\assessor_data_functions.R")

con <- connect_to_db("altadena_recovery_rebuild")

# # Zipped assessor data downloaded to D: drive from EMG's OneDrive
# assessor_data_folder <- "D:/Assessor Data FULL/OneDrive_2025-09-23.zip"
# temp_extract_dir <- "D:/temp_extract/"
# 
# # Don't need to rerun # Unzipped in a temporary D:/ folder "temp_extract" (fread wasn't working so I used PowerShell/terminal)
# unzipped_result <- system(paste0('powershell "Expand-Archive -Path \\"', assessor_data_folder, '\\" -DestinationPath \\"',temp_extract_dir,'\\" -Force"'))
# 
# # Confirm files extracted - by listing filenames in the temp folder
# extracted_files <- list.files(temp_extract_dir, recursive = TRUE, full.names = TRUE)
# print(extracted_files)

# Define file locations we'll need
jan_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250106/parcel.shp"
sept_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250902/parcels.shp"

jan_csv_1 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 1.csv"
jan_csv_2 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 2.csv"
jan_csv_3 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 3.csv"

sept_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 1.csv"
sept_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 2.csv"
sept_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 3.csv"

sept_custom_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 Custom DS04 Part 1.csv"
sept_custom_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 Custom DS04 Part 2.csv"
sept_custom_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 Custom DS04 Part 3.csv"

##### Step 1: Intersect Jan Shps #####
# Spatial Intersect of Jan and Sept parcel shp files to 
# Altadena/Pasadena city boundaries

# city_perimeters <- st_read(con, query='select "NAME" as name, geom as geometry from data.tl_2023_places;') %>%
#   filter(name == "Altadena" | name == "Pasadena") %>%
#   st_transform(2229)
# st_crs(city_perimeters)$epsg # 2229

# altadena_shp <- city_perimeters %>% filter(name=="Altadena")
# pasadena_shp <- city_perimeters %>% filter(name=="Pasadena")

# # jan intersection
# jan_intersect <- batch_intersect_shapefile(
#   shp_path=jan_shp_path, 
#   target_geo=city_perimeters, 
#   chunk_size = 10000,
#   retain_cols = c("name")) # 54874

# length(unique(jan_intersect$AIN)) # 54874
# table(jan_intersect$matched_name, useNA = "ifany")
# # Altadena           Pasadena        Pasadena; Altadena 
# # 13951              40342           581 

# # clean up before export #
# colnames(jan_intersect) <- tolower(colnames(jan_intersect))
# jan_intersect_3310 <- st_transform(jan_intersect, 3310)

# # # export results
# source <- "Los Angeles County Assessor; Data Dictionary: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Extract\\FIELD DEF -- SBF.html"
# qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_import_assessor_data.docx"
# schema <- "data"
# indicator <- "Jan 2025 Parcels that intersect Altadena and Pasadena 2023 place tiger lines."
# 
# export_shpfile(con=con, 
#                df=jan_intersect_3310, 
#                schema="data", 
#                table_name="assessor_parcels_universe_jan2025", 
#                srid = "", geometry_type = "", 
#                geometry_column = "geometry")
# 
# 
# dbSendQuery(con, paste0("COMMENT ON TABLE data.assessor_parcels_universe_jan2025 IS '", indicator, "
#             Data imported on 10-01-25. ",
#             "QA DOC: ", qa_filepath,
#             " Source: ", source, "'"))

jan_intersect <- st_read(con, query="SELECT * FROM data.assessor_parcels_universe_jan2025;")
st_crs(jan_intersect)$epsg # 3310


##### Step 2: Filter Jan CSVs #####
# Filter Jan csv data for AINs found in Step 1 #
jan_ain_universe <- jan_intersect %>% st_drop_geometry() %>% select(ain) %>% pull()

# # January
# jan_1_ain_filter <- batch_filter_csv_data(
#   csv_file=jan_csv_1,
#   target_list = jan_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=TRUE)
# 
# jan_2_ain_filter <- batch_filter_csv_data(
#   csv_file=jan_csv_2,
#   target_list = jan_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=TRUE)
# 
# jan_3_ain_filter <- batch_filter_csv_data(
#   csv_file=jan_csv_3,
#   target_list = jan_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=TRUE)
# 
# # Combine results
# jan_csv_ains <- rbind(jan_1_ain_filter, 
#                           jan_2_ain_filter, 
#                           jan_3_ain_filter) # 54826
# 
# colnames(jan_csv_ains) <- tolower(gsub(" ", "_", colnames(jan_csv_ains)))
# 
# jan_csv_ains <- jan_csv_ains %>%
#   rename(ain = `ï»¿ain`) %>%
#   mutate(ain=as.character(ain)) 
# 
# length(unique(jan_csv_ains$ain)) # 54826

# ### Export to postgres
# table_name <- "assessor_data_universe_jan2025"
# indicator <- "Assessor data from January 2025 that matches Altadena and Pasadena parcels in assessor_parcels_universe_jan2025 table."
# dbWriteTable(con, Id(schema, table_name), jan_csv_ains,
#              overwrite = FALSE, row.names = FALSE)
# dbSendQuery(con, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
#             Data imported on 10-01-25. ",
#                         "QA DOC: ", qa_filepath,
#                         " Source: ", source, "'"))


##### Repeat Step 1: Filter Sept Shps #####
# # sept intersection
# sept_intersect <- batch_intersect_shapefile(
#   shp_path=sept_shp_path, 
#   target_geo=city_perimeters, 
#   chunk_size = 10000,
#   retain_cols = c("name")) # 54874

# length(unique(sept_intersect$AIN)) # 54874
# table(sept_intersect$matched_name, useNA = "ifany") 
# # joint parcel count is the same as jan, individual are slightly different
# # Altadena           Pasadena           Pasadena; Altadena 
# # 13947              40346              581 

# # clean up before export #
# colnames(sept_intersect) <- tolower(colnames(sept_intersect))
# sept_intersect_3310 <- st_transform(sept_intersect, 3310)
# 
# # export results
# source <- "Los Angeles County Assessor; Data Dictionary: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Extract\\FIELD DEF -- SBF.html"
# qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_import_assessor_data.docx"
# schema <- "data"
# indicator <- "Sept 2025 Parcels that intersect Altadena and Pasadena 2023 place tiger lines."
# 
# export_shpfile(con=con, df=sept_intersect_3310, schema="data", table_name="assessor_parcels_universe_sept2025", srid = "", geometry_type = "", geometry_column = "geometry")
# dbSendQuery(con, paste0("COMMENT ON TABLE data.assessor_parcels_universe_sept2025 IS '", indicator, "
#             Data imported on 10-01-25. ",
#             "QA DOC: ", qa_filepath,
#             " Source: ", source, "'"))

sept_intersect <- st_read(con, query="SELECT * FROM data.assessor_parcels_universe_sept2025;")
st_crs(sept_intersect)$epsg # 3310


##### Repeat Step 2: Filter Sept CSVs (standard AND custom) #####
# Filter Jan csv data for AINs found in Step 1 #
sept_ain_universe <- sept_intersect %>% st_drop_geometry() %>% select(ain) %>% pull()

# # Sept Standard
# sept_1_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_csv_1,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=TRUE)
# 
# sept_2_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_csv_2,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=TRUE)
# 
# sept_3_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_csv_3,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=TRUE)
# 
# # Combine results
# sept_csv_ains <- rbind(sept_1_ain_filter,
#                           sept_2_ain_filter,
#                           sept_3_ain_filter) # 54826
# 
# colnames(sept_csv_ains) <- tolower(gsub(" ", "_", colnames(sept_csv_ains)))
# 
# sept_csv_ains <- sept_csv_ains %>%
#   rename(ain = `ï»¿ain`) %>%
#   mutate(ain=as.character(ain))
# 
# length(unique(sept_csv_ains$ain)) # 54797

# ### Export to postgres
# table_name <- "assessor_data_universe_sept2025"
# indicator <- "Assessor data from September 2025 that matches Altadena and Pasadena parcels in assessor_parcels_universe_sept2025 table."
# dbWriteTable(con, Id(schema, table_name), sept_csv_ains,
#              overwrite = TRUE, row.names = FALSE)
# dbSendQuery(con, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
#             Data imported on 10-01-25. ",
#                         "QA DOC: ", qa_filepath,
#                         " Source: ", source, "'"))


# # Sept CUSTOM
# sept_custom_1_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_custom_csv_1,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=TRUE)
# 
# sept_custom_2_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_custom_csv_2,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=TRUE)
# 
# sept_custom_3_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_custom_csv_3,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=TRUE)
# 
# # Combine results
# sept_custom_csv_ains <- rbind(sept_custom_1_ain_filter,
#                        sept_custom_2_ain_filter,
#                        sept_custom_3_ain_filter) # 54826
# 
# colnames(sept_custom_csv_ains) <- tolower(gsub(" ", "_", colnames(sept_custom_csv_ains)))
# 
# sept_custom_csv_ains <- sept_custom_csv_ains %>%
#   rename(ain = `ï»¿ain`) %>%
#   mutate(ain=as.character(ain))
# 
# length(unique(sept_custom_csv_ains$ain)) # 54797

# ### Export to postgres
# table_name <- "assessor_customdata_universe_sept2025"
# indicator <- "Custom Assessor data from September 2025 that matches Altadena and Pasadena parcels in assessor_parcels_universe_sept2025 table."
# dbWriteTable(con, Id(schema, table_name), sept_custom_csv_ains,
#              overwrite = FALSE, row.names = FALSE)
# dbSendQuery(con, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
#             Data imported on 10-01-25. ",
#                         "QA DOC: ", qa_filepath,
#                         " Source: ", source, "'"))


##### QA Clean-up #####
## Address double-encoding error in original data files
# # jan data tables
# sql_rename_col <- "ALTER TABLE data.assessor_data_universe_jan2025 RENAME COLUMN exemption_type to exemption_type_original;"
# dbSendQuery(con, sql_rename_col)
# 
# sql_create_new_col <- "ALTER TABLE data.assessor_data_universe_jan2025 ADD COLUMN exemption_type VARCHAR(5);"
# dbSendQuery(con, sql_create_new_col)
# 
# sql_set_values <- "UPDATE data.assessor_data_universe_jan2025 SET exemption_type = exemption_type_original;"
# dbSendQuery(con, sql_set_values)
# 
# sql_recode <- "UPDATE data.assessor_data_universe_jan2025 SET exemption_type = NULL WHERE exemption_type = 'Ã¿';"
# dbSendQuery(con, sql_recode)
# 
# # sept data tables
# sql_rename_col <- "ALTER TABLE data.assessor_data_universe_sept2025 RENAME COLUMN exemption_type to exemption_type_original;"
# dbSendQuery(con, sql_rename_col)
# 
# sql_create_new_col <- "ALTER TABLE data.assessor_data_universe_sept2025 ADD COLUMN exemption_type VARCHAR(5);"
# dbSendQuery(con, sql_create_new_col)
# 
# sql_set_values <- "UPDATE data.assessor_data_universe_sept2025 SET exemption_type = exemption_type_original;"
# dbSendQuery(con, sql_set_values)
# 
# sql_recode <- "UPDATE data.assessor_data_universe_sept2025 SET exemption_type = NULL WHERE exemption_type = 'Ã¿';"
# dbSendQuery(con, sql_recode)



##### REVIEW: Two kinds of AIN mismatches between CSV and SHP files #####
# For permit scraping we are primarily interested in mismatches where the associated 
# use code starts with zero (residential) and does NOT end with V (vacant lot)
### 1. AINs from the SHP files that have no CSV match
# January - jan_ain_universe
jan_shp_ain <- jan_ain_universe
jan_csv_ain <- dbGetQuery(con, statement="SELECT * FROM data.assessor_data_universe_jan2025")

jan_mismatch_1 <- data.frame(ain=setdiff(jan_shp_ain, jan_csv_ain$ain)) %>%
  left_join(jan_intersect,by="ain") %>%
  st_as_sf(sf_column_name="geom",
           crs=st_crs(jan_intersect)) %>%
  mutate(jan=1)

mapview(jan_mismatch_1, col.regions="red")


# September - STANDARD 
sept_shp_ain <- sept_ain_universe
sept_csv_ain <- dbGetQuery(con, statement="SELECT * FROM data.assessor_data_universe_sept2025")

sept_mismatch_1 <- data.frame(ain=setdiff(sept_shp_ain, sept_csv_ain$ain)) %>%
  left_join(sept_intersect,by="ain") %>%
  st_as_sf(sf_column_name="geom",
           crs=st_crs(sept_intersect)) %>%
  mutate(sept=1)

mapview(sept_mismatch_1, col.regions="red")


# September - CUSTOM
sept_custom_csv_ain <- dbGetQuery(con, statement="SELECT * FROM data.assessor_customdata_universe_sept2025")

sept_custom_mismatch_1 <- data.frame(ain=setdiff(sept_shp_ain, sept_custom_csv_ain$ain)) %>%
  left_join(sept_intersect,by="ain") %>%
  st_as_sf(sf_column_name="geom",
           crs=st_crs(sept_intersect)) %>%
  mutate(sept=1)

mapview(sept_custom_mismatch_1, col.regions="red")

# Review
# Looks like the same AINs missing in standard csv is also missing in custom
compare_sept_mismatch_1 <- full_join(st_drop_geometry(sept_mismatch_1), 
                                     st_drop_geometry(sept_custom_mismatch_1), 
                                     by="ain", suffix=c(".std", ".cstm"))

# compare jan and sept
compare_jan_sept_mismatch_1 <- full_join(st_drop_geometry(sept_mismatch_1), 
                                         st_drop_geometry(jan_mismatch_1), 
                                         by="ain", suffix=c(".sept", ".jan")) %>%
  mutate(src_ain = case_when(is.na(objectid.sept)~ "jan only",
                          is.na(objectid.jan)~"sept only",
                          .default = "both")) %>%
  select(ain, src_ain, everything())

# table(compare_jan_sept_mismatch_1$src_ain, useNA = "ifany")
# # both  jan only sept only 
# # 33        15        44 
# both <- compare_jan_sept_mismatch_1 %>% filter(src_ain=="both") %>%
#   left_join(jan_intersect, by="ain") %>%
#   st_as_sf(sf_column_name="geom",
#            crs=st_crs(jan_intersect))
# 
# mapview(jan_mismatch_1, col.regions="yellow", alpha.regions=0.8, color="green") + 
#   mapview(sept_mismatch_1, col.regions="blue", alpha.regions=0.8, color="green") +
#   mapview(both, col.regions="green", alpha.regions=0.8, color="green")

# looked up all altadena AINs on the assessor portal:
## 5830015029 (No result)
## 5831016035 (No result)
## 5831016036 (No result)
## 5839016025 - 0100 (Shell: https://portal.assessor.lacounty.gov/parceldetail/5839016025)
## 5839016026 (No result)
## 5841023022 (No result)
## 5847020027 (No result)
## 5863003900 - 010V (Shell: https://portal.assessor.lacounty.gov/parceldetail/5863003900)

# see if there are any calfire points that intersect
calfire <- st_read(con, query="SELECT * FROM data.eaton_fire_dmg_insp_3310")

all_mismatch_1 <- compare_jan_sept_mismatch_1 %>% 
  select(ain, src_ain, objectid.jan, objectid.sept) %>% 
  left_join(select(jan_intersect, ain, geom), by="ain") %>% 
  left_join(select(sept_intersect, ain, geom), by="ain") %>%
  mutate(geom = case_when(src_ain=="both" ~ geom.x,
                          src_ain=='sept only'~geom.y,
                          src_ain=='jan only'~geom.x)) %>%
  select(-c(geom.y, geom.x)) %>%
  st_as_sf(sf_column_name = "geom", crs=3310)

calfire_matches <- st_intersection(all_mismatch_1, calfire)

# Review matches and check if APN is correct AIN (using assessor portal)
unique(calfire_matches$ain)

# "5830015029" - apn: 5830015015 - 0100: https://portal.assessor.lacounty.gov/parceldetail/5830015015
# "5831016035" - apn: 5831016033 - 0100: https://portal.assessor.lacounty.gov/parceldetail/5831016033
# "5831016036" - apn: 5831016032 - 0100: https://portal.assessor.lacounty.gov/parceldetail/5831016032
# "5841023022" - apn: 5841023009 - 0100: https://portal.assessor.lacounty.gov/parceldetail/5841023009
# "5847020027" - apn: 5847020011 - 0100: https://portal.assessor.lacounty.gov/parceldetail/5847020011

matched <-  all_mismatch_1 %>% filter(ain %in% unique(calfire_matches$ain)) #5 
leftover <- all_mismatch_1 %>% filter(!(ain %in% unique(calfire_matches$ain))) #87

mapview(matched, col.regions="green", color ="green") + 
  mapview(leftover, col.regions="red", color ="red")

# manually cross check ten leftovers with assessor parcel map - see if there's an obvious explanation:
# 1. AIN: 5863003900 (in both; AIN matches portal, but use_code not relevant - 010V) - https://portal.assessor.lacounty.gov/parceldetail/5863003900
# 2. AIN: 5825020910 (sept only; AIN matches portal but bad response) https://portal.assessor.lacounty.gov/parceldetail/5825020910
# 3. AIN: 5725002918 (in both, AIN matches portal, but use_code not relevant - 1210) - https://portal.assessor.lacounty.gov/parceldetail/5725002918
# 4. AIN: 5726018095 (sept only, AIN does not match portal, with revised AIN use_code not relevant - 7100) - https://portal.assessor.lacounty.gov/parceldetail/5726018022
# 5. AIN: 5757005048 (jan only; AIN matches portal, but use_code not relevant - 7100) - https://portal.assessor.lacounty.gov/parceldetail/5757005048
# 6. AIN: 5746025908 (jan only; AIN matches portal, but use_code not relevant - 300V) - https://portal.assessor.lacounty.gov/parceldetail/5746025908
# 7. AIN: 5742001038 (sept only; AIN matches portal but bad response) - https://portal.assessor.lacounty.gov/parceldetail/5742001038
# 8. AIN: 5760023013 (in both; AIN matches portal, but use_code not relevant - 010V) - https://portal.assessor.lacounty.gov/parceldetail/5760023013
# 9. AIN: 5839016026 (in both; AIN matches portal but bad response) - https://portal.assessor.lacounty.gov/parceldetail/5839016026
# 10.AIN: 5728014061 (in both; AIN matches portal, but use_code not relevant - 100V) - https://portal.assessor.lacounty.gov/parceldetail/5728014061

# Summary:
# We are limited in how we can check AINs that exist in the shp files but have no matching csv record
# We joined the unmatched parcels to our other geo source (CalFire) to see if the APN is more accurate
# In all cases where the CalFire point matched to a parcel (with st_intersection), the APN value was different and
# a better ID to get parcel data from the Assessor portal. Additionally, all parcels have relevant use codes.
#   
# We had 87 AINs that did not match to CalFire data and observed a few categories:
#  1. AIN matches assessor portal, but associated use code is not relevant for permit analysis
#     (e.g., code does not start with a zero (is non-residential); ends with a V (is a vacant lot))
#     - Recommendation: Exclude these AINs from permit scraping
#  2. AIN matches assessor portal, but bad response (no page loads)
#     My sense here is that this means the assessor map and the shp AIN may have a data quality issue but our most feasible recourse is via the CalFire crosswalk
#     In some cases CalFire points fall outside of the relevant parcel - it's likely that the APN for these points should be used to revise the shp AIN (like above)
#     - Recommendation: in CalFire crosswalk run st_nearest_feature on unmatched points against universe parcel shapes and confirm this is true, then update revised_ain column in shp tables
#  3. AIN does not match portal, AIN revised via Assessor portal but has use_code not relevant for permit analysis 
#     - Recommendation: Wait for CalFire crosswalk and see how many unmatched AINs still exist - determine if we should manually revise leftovers with Assessor portal and update the revised_ain col
# 
# QA Note: we should probably rerun this step, replacing it with the CalFire crosswalk once it's finalized


### 2. Get AINs from CSVs (based on situs city_state filter), and review which ones are not in the SHP ain universes
city_list <- c("Altadena", "Pasadena")

# January
# # filter csvs
# jan_1_results <- batch_filter_csv_data(
#   csv_file=jan_csv_1,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=FALSE)
# 
# jan_2_results <- batch_filter_csv_data(
#   csv_file=jan_csv_2,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=FALSE)
# 
# jan_3_results <- batch_filter_csv_data(
#   csv_file=jan_csv_3,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=FALSE)
# 
# # combine
# all_jan_results <- bind_rows(jan_1_results, jan_2_results, jan_3_results) #62170
# 
# # clean column names
# colnames(all_jan_results)
# colnames(all_jan_results)[1] <- "AIN"
# colnames(all_jan_results) <- gsub(" ", "_", tolower(colnames(all_jan_results)))
# colnames(all_jan_results)
# 
# # unique AIN for each row
# length(unique(all_jan_results$ain)) #62170
# 
# # frequency table of situs 'city_state' field
# table(all_jan_results$city_state, useNA = "ifany")


# # export results to csv (keeping all columns for QA)
# write.csv(all_jan_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_jan_2025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

# import
all_jan_results <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_jan_2025.csv")

# compare to jan ain universe
# in csv, but not in universe
jan_mismatch_2 <- data.frame(ain=setdiff(all_jan_results$ain, jan_ain_universe)) %>% # 9887
  left_join(all_jan_results, by="ain") 

table(jan_mismatch_2$use_code) # Note: 48 end in V

# check if these are in .shp (to cross out possibility they are outside the city perimeters)
jan_csv_ains <- jan_mismatch_2 %>% select(ain) %>% pull()

check_jan_ <- batch_filter_shapefile(shp_path=jan_shp_path, target_ains=jan_csv_ains, chunk_size = 10000, ain_column = "AIN")

jan_mismatch_2 <- jan_mismatch_2 %>%
  mutate(shp_match = ifelse(ain %in% check_jan_$AIN, 1,0)) %>%
  filter(shp_match==0) %>% # 13 with relevant use codes
  mutate(jan=1)
table(jan_mismatch_2$use_code, useNA = "ifany")

# Note some of these look to overlap with some addresses that matched to Calfire data in mismatch_1


# # September- standard
# # filter csvs
# sept_1_results <- batch_filter_csv_data(
#   csv_file=sept_csv_1,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=FALSE)
# 
# sept_2_results <- batch_filter_csv_data(
#   csv_file=sept_csv_2,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=FALSE)
# 
# sept_3_results <- batch_filter_csv_data(
#   csv_file=sept_csv_3,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE,
#   exact_match=FALSE)
# 
# # combine
# all_sept_results <- bind_rows(sept_1_results, sept_2_results, sept_3_results) #62183
# 
# # clean column names
# colnames(all_sept_results)
# colnames(all_sept_results)[1] <- "AIN"
# colnames(all_sept_results) <- gsub(" ", "_", tolower(colnames(all_sept_results)))
# colnames(all_sept_results)
# 
# # unique AIN for each row
# length(unique(all_sept_results$ain)) #62183
# 
# # frequency table of situs 'city_state' field
# table(all_sept_results$city_state, useNA = "ifany")


# # export results to csv (keeping all columns for QA)
# write.csv(all_sept_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_sept_2025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

# import
all_sept_results <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_sept_2025.csv")

# # compare to jan ain universe
# # in csv, but not in universe
# sept_mismatch_2 <- data.frame(ain=setdiff(all_sept_results$ain, sept_ain_universe)) %>% # 9915
#   left_join(all_sept_results, by="ain") 
# 
# table(sept_mismatch_2$use_code) 
# 
# # check if these are in .shp (to cross out possibility they are outside the city perimeters)
# sept_csv_ains <- sept_mismatch_2 %>% select(ain) %>% pull()
# 
# check_sept_ <- batch_filter_shapefile(shp_path=sept_shp_path, target_ains=sept_csv_ains, chunk_size = 10000, ain_column = "AIN") # 9862
# 
# sept_mismatch_2 <- sept_mismatch_2 %>%
#   mutate(shp_match = ifelse(ain %in% check_sept_$AIN, 1,0)) %>%
#   filter(shp_match==0) %>% # 53
#   filter(grepl("^0", use_code)) %>% # 27 residential, 3 are vacant lots
#   mutate(sept=1)
# 
# # with relevant use codes
# 
# table(sept_mismatch_2$use_code, useNA = "ifany")
# 
# # Note some of these look to overlap with some addresses came up when dealing with mismatch_1
# 
# all_mismatch_2 <- full_join(jan_mismatch_2, sept_mismatch_2, by="ain") %>%
#   select(ain, jan, sept, use_code.x, use_code.y, everything()) # 30, 27 if we exclude vacant lots
# 
# # # export results to csv (keeping all columns for QA)
# # write.csv(all_mismatch_2,
# #           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/all_mismatch_2.csv",
# #           row.names=FALSE,
# #           fileEncoding = "UTF-8")
# 
# 
# 
# # September (standard and custom csvs)
# # filter csvs
# sept_1_results <- batch_filter_csv_data(
#   csv_file=sept_csv_1,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# sept_2_results <- batch_filter_csv_data(
#   csv_file=sept_csv_2,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# sept_3_results <- batch_filter_csv_data(
#   csv_file=sept_csv_3,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# # combine
# all_sept_results <- bind_rows(sept_1_results, sept_2_results, sept_3_results)
# 
# # clean column names
# colnames(all_sept_results)
# colnames(all_sept_results)[1] <- "AIN"
# colnames(all_sept_results) <- gsub(" ", "_", tolower(colnames(all_sept_results)))
# colnames(all_sept_results)
# 
# # unique AIN for each row
# length(unique(all_sept_results$ain)) # 66096
# 
# # frequency table of situs 'city_state' field
# sept_city_results <- as.data.frame(table(all_sept_results$city_state, useNA = "ifany"))

# # export results to csv (keeping all columns for QA)
# write.csv(all_sept_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_sept_2025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

# read in
all_sept_results <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_sept_2025.csv")






