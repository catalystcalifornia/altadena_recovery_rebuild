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

##### Step 1: Intersect Jan Shps #####
# Spatial Intersect of Jan and Sept parcel shp files to 
# Altadena/Pasadena city boundaries

city_perimeters <- st_read(con, query='select "NAME" as name, geom as geometry from data.tl_2023_places;') %>%
  filter(name == "Altadena" | name == "Pasadena") %>%
  st_transform(2229)

altadena_shp <- city_perimeters %>% filter(name=="Altadena")
pasadena_shp <- city_perimeters %>% filter(name=="Pasadena")
jan_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250106/parcel.shp"
sept_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250902/parcels.shp"

# Prep/check SRIDS
st_crs(city_perimeters)$epsg # 2229

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

# CSV Files
jan_csv_1 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 1.csv"
jan_csv_2 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 2.csv"
jan_csv_3 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 3.csv"

# # January
# jan_1_ain_filter <- batch_filter_csv_data(
#   csv_file=jan_csv_1,
#   target_list = jan_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# jan_2_ain_filter <- batch_filter_csv_data(
#   csv_file=jan_csv_2,
#   target_list = jan_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# jan_3_ain_filter <- batch_filter_csv_data(
#   csv_file=jan_csv_3,
#   target_list = jan_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE)
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

# Standard CSVs
sept_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 1.csv"
sept_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 2.csv"
sept_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 3.csv"

# Custom CSVs
sept_custom_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 Custom DS04 Part 1.csv"
sept_custom_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 Custom DS04 Part 2.csv"
sept_custom_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 Custom DS04 Part 3.csv"

# # Sept Standard
# sept_1_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_csv_1,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# sept_2_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_csv_2,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# sept_3_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_csv_3,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE)
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
#              overwrite = FALSE, row.names = FALSE)
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
#   debug_filter=TRUE)
# 
# sept_custom_2_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_custom_csv_2,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# sept_custom_3_ain_filter <- batch_filter_csv_data(
#   csv_file=sept_custom_csv_3,
#   target_list = sept_ain_universe,
#   filter_column="ï»¿AIN",
#   chunk_size = 10000,
#   debug_filter=TRUE)
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










# 
# altadena_jan <- jan_intersect %>% filter(matched_name=="Altadena")
# pasadena_jan <- jan_intersect %>% filter(matched_name=="Pasadena")
# combo_jan <- jan_intersect %>% filter(matched_name=="Pasadena; Altadena")
# ## Visualize - map exported to W:\Project\RDA Team\Altadena Recovery and Rebuild\Maps\import_assessor\jan_universe_09302925
# mapviewOptions(basemaps = "CartoDB.Positron")
# mapview(altadena_shp, col.regions="yellow") + 
#   mapview(pasadena_shp, col.regions="skyblue") +
#   mapview(altadena_jan, col.regions="darkgoldenrod2") +
#   mapview(pasadena_jan, col.regions="royalblue") +
#   mapview(combo_jan, col.regions="chartreuse4")







# altadena_sept <- sept_intersect %>% filter(matched_name=="Altadena")
# pasadena_sept <- sept_intersect %>% filter(matched_name=="Pasadena")
# combo_sept <- sept_intersect %>% filter(matched_name=="Pasadena; Altadena")
# ## Visualize - map exported to W:\Project\RDA Team\Altadena Recovery and Rebuild\Maps\import_assessor\sept_universe_09302925
# mapviewOptions(basemaps = "CartoDB.Positron")
# mapview(altadena_shp, col.regions="yellow") +
#   mapview(pasadena_shp, col.regions="skyblue") +
#   mapview(altadena_sept, col.regions="darkgoldenrod2") +
#   mapview(pasadena_sept, col.regions="royalblue") +
#   mapview(combo_sept, col.regions="chartreuse4")


# # compare results 
jan_universe <- jan_intersect %>%
  st_drop_geometry() %>%
  select(AIN) %>%
  mutate(jan_universe = 1)

sept_universe <- sept_intersect %>%
  st_drop_geometry() %>%
  select(AIN) %>%
  mutate(sept_universe = 1)

full_shp_universe <- full_join(jan_universe, sept_universe, by="AIN") %>%
  rename(ain=AIN) %>%
  replace_na(list(jan_universe= 0, sept_universe=0))%>%
  rowwise() %>%
  mutate(diff=ifelse(sum(jan_universe, sept_universe, na.rm=T)==2, 0, 1)) %>%
  ungroup() %>%
  mutate(city_intersect=1)

universe_diff <- full_shp_universe %>% filter(diff==1) # 98; 49 for jan and 49 for sept

##### Step 2 #####
# Identify parcels in CSV not in the spatial shapefile that begin with 
# use code of 0 (zero) and have at least 1 unit 
# CSV Files
jan_csv_1 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 1.csv"
jan_csv_2 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 2.csv"
jan_csv_3 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 3.csv"

sept_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 1.csv"
sept_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 2.csv"
sept_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 3.csv"

##### filter csvs for AINs from Step 1
ain_universe <- full_shp_universe %>% select(ain) %>% pull()

# January
jan_1_ain_filter <- batch_filter_csv_data(
  csv_file=jan_csv_1,
  target_list = ain_universe,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE)

jan_2_ain_filter <- batch_filter_csv_data(
  csv_file=jan_csv_2,
  target_list = ain_universe,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE)

jan_3_ain_filter <- batch_filter_csv_data(
  csv_file=jan_csv_3,
  target_list = ain_universe,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE)

# Combine results
all_jan_csv_ains <- rbind(jan_1_ain_filter, 
                          jan_2_ain_filter, 
                          jan_3_ain_filter) # 54827

colnames(all_jan_csv_ains) <- tolower(gsub(" ", "_", colnames(all_jan_csv_ains)))

all_jan_csv_ains <- all_jan_csv_ains %>%
  rename(ain = `ï»¿ain`) 

length(unique(all_jan_csv_ains$ain)) # 54827

all_jan_csv_ains <- all_jan_csv_ains %>%
  mutate(ain=as.character(ain),
         zipcode=gsub("0000", "", zip),
         city=gsub(" CA ", ", CA", city_state)) %>%
  mutate(site_address=paste0(situs_house_no, direction, street_name, ", ", city, zipcode)) %>%
  mutate(site_address=gsub("\\s+", " ", site_address)) %>%
  mutate(site_address=gsub(" , ", ", ", site_address)) %>%
  select(ain, use_code, site_address, ends_with("_units"), ends_with("_square_feet"), recording_date) %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
         total_sq_ft = rowSums(across(ends_with("_square_feet")))) %>%
  select(ain, use_code, site_address, total_units, total_sq_ft, recording_date) %>%
  mutate(jan_csv = 1)

shp_csv_match <- full_join(all_jan_csv_ains, all_sept_csv_ains, 
                              suffix=c(".jan", ".sept"), keep=FALSE, by="ain") %>% # 54844
  mutate(shp_ain_match=1)

# September
sept_1_ain_filter <- batch_filter_csv_data(
  csv_file=sept_csv_1,
  target_list = ain_universe,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE)

sept_2_ain_filter <- batch_filter_csv_data(
  csv_file=sept_csv_2,
  target_list = ain_universe,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE)

sept_3_ain_filter <- batch_filter_csv_data(
  csv_file=sept_csv_3,
  target_list = ain_universe,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE)

# Combine results
all_sept_csv_ains <- rbind(sept_1_ain_filter, 
                          sept_2_ain_filter, 
                          sept_3_ain_filter) # 54833

colnames(all_sept_csv_ains) <- tolower(gsub(" ", "_", colnames(all_sept_csv_ains)))

all_sept_csv_ains <- all_sept_csv_ains %>%
  rename(ain = `ï»¿ain`) 

length(unique(all_sept_csv_ains$ain)) # 54833

all_sept_csv_ains <- all_sept_csv_ains %>%
  mutate(ain=as.character(ain),
         zipcode=gsub("0000", "", zip),
         city=gsub(" CA ", ", CA", city_state)) %>%
  mutate(site_address=paste0(situs_house_no, direction, street_name, ", ", city, zipcode)) %>%
  mutate(site_address=gsub("\\s+", " ", site_address)) %>%
  mutate(site_address=gsub(" , ", ", ", site_address)) %>%
  select(ain, use_code, site_address, ends_with("_units"), ends_with("_square_feet"), recording_date) %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
         total_sq_ft = rowSums(across(ends_with("_square_feet")))) %>%
  select(ain, use_code, site_address, total_units, total_sq_ft, recording_date) %>%
  mutate(sept_csv = 1)

# combine
shp_csv_match <- full_join(all_jan_csv_ains, all_sept_csv_ains,
                           suffix=c(".jan", ".sept"), keep=FALSE, by="ain") %>% # 54844
  mutate(shp_ain_match=1) %>%

# 79 of the ain universe are not in the csvs
missing_ <- data.frame(ain = setdiff(ain_universe, shp_csv_match$ain)) %>% # 79
  left_join(full_shp_universe, by="ain")

residential_shp_matches <- shp_csv_match %>% 
  # filter for residential partials (use_code starts with zero)
  filter(str_detect(use_code.jan, "^0")) %>% #50696
  mutate(diff_use_code = ifelse(use_code.jan != use_code.sept, 1, 0))

use_code_freq <- as.data.frame(table(residential_shp_matches$use_code.jan, useNA = "ifany"))
# 1237 of jan parcels have vacant use codes (end in 'V') 

#### filter csvs for site addresses in Altadena and Pasadena
city_list <- c("Altadena", "Pasadena")

# January
jan_1_city_filter <- batch_filter_csv_data(
  csv_file=jan_csv_1,
  target_list = city_list,
  filter_column="City State",
  chunk_size = 10000,
  debug_filter=TRUE)

jan_2_city_filter <- batch_filter_csv_data(
  csv_file=jan_csv_2,
  target_list = city_list,
  filter_column="City State",
  chunk_size = 10000,
  debug_filter=TRUE)

jan_3_city_filter <- batch_filter_csv_data(
  csv_file=jan_csv_3,
  target_list = city_list,
  filter_column="City State",
  chunk_size = 10000,
  debug_filter=TRUE)

# Combine results
all_jan_csv_results <- rbind(jan_1_city_filter, jan_2_city_filter, jan_3_city_filter) # 62170

colnames(all_jan_csv_results) <- tolower(gsub(" ", "_", colnames(all_jan_csv_results)))

all_jan_csv_results <- all_jan_csv_results %>%
  rename(ain = `ï»¿ain`) 

length(unique(all_jan_csv_results$ain)) # 62170

all_jan_csv_results <- all_jan_csv_results %>%
  mutate(ain=as.character(ain),
         zipcode=gsub("0000", "", zip),
         city=gsub(" CA ", ", CA", city_state)) %>%
  mutate(site_address=paste0(situs_house_no, direction, street_name, ", ", city, zipcode)) %>%
  mutate(site_address=gsub("\\s+", " ", site_address)) %>%
  mutate(site_address=gsub(" , ", ", ", site_address)) %>%
  select(ain, use_code, site_address, ends_with("_units"), ends_with("_square_feet"), recording_date) %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
         total_sq_ft = rowSums(across(ends_with("_square_feet")))) %>%
  select(ain, use_code, site_address, total_units, total_sq_ft, recording_date) %>%
  filter(str_detect(use_code, "^0")) %>%                       
  mutate(jan_csv=1) # 58474; matches jan and sept shp universes


# September
sept_1_city_filter <- batch_filter_csv_data(
  csv_file=sept_csv_1,
  target_list = city_list,
  filter_column="City State",
  chunk_size = 10000,
  debug_filter=TRUE)

sept_2_city_filter <- batch_filter_csv_data(
  csv_file=sept_csv_2,
  target_list = city_list,
  filter_column="City State",
  chunk_size = 10000,
  debug_filter=TRUE)

sept_3_city_filter <- batch_filter_csv_data(
  csv_file=sept_csv_3,
  target_list = city_list,
  filter_column="City State",
  chunk_size = 10000,
  debug_filter=TRUE)

# Combine results
all_sept_csv_results <- rbind(sept_1_city_filter, sept_2_city_filter, sept_3_city_filter) # 62183
colnames(all_sept_csv_results) <- tolower(gsub(" ", "_", colnames(all_sept_csv_results)))

all_sept_csv_results <- all_sept_csv_results %>%
  rename(ain = `ï»¿ain`) %>%
  mutate(sept_csv = 1)
length(unique(all_sept_csv_results$ain)) # 62183

all_sept_csv_results <- all_sept_csv_results %>%
  mutate(ain=as.character(ain),
         zipcode=gsub("0000", "", zip),
         city=gsub(" CA ", ", CA", city_state)) %>%
  mutate(site_address=paste0(situs_house_no, direction, street_name, ", ", city, zipcode)) %>%
  mutate(site_address=gsub("\\s+", " ", site_address)) %>%
  mutate(site_address=gsub(" , ", ", ", site_address)) %>%
  select(ain, use_code, site_address, ends_with("_units"), ends_with("_square_feet"), recording_date) %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
         total_sq_ft = rowSums(across(ends_with("_square_feet")))) %>%
  select(ain, use_code, site_address, total_units, total_sq_ft, recording_date) %>%
  filter(str_detect(use_code, "^0")) %>%                       
  mutate(sept_csv=1) # 58488; more matches than jan and sept shp universes

# combine
csv_city_results <- full_join(all_jan_csv_results, all_sept_csv_results, 
                             suffix=c(".jan", ".sept"), keep=FALSE, by="ain")  %>% # 58496
  mutate(csv_filter=1)

# write.csv(csv_city_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_results_09302025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

csv_city_results <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_results_09302025.csv")


full_ain_universe <- full_join(full_shp_universe, csv_city_results, by="ain") %>% # 64248
  select(ain, jan_universe, sept_universe, diff, jan_csv, sept_csv, city_intersect, csv_filter, everything()) %>%
  replace_na(list(jan_universe=0,
                  sept_universe=0,
                  jan_csv=0, 
                  sept_csv=0,
                  city_intersect=0,
                  csv_filter=0)) %>%
  rowwise() %>%
  mutate(diff_universe=ifelse(sum(jan_universe, sept_universe, na.rm=T)==2, 0, 1),
         diff_csv=ifelse(sum(jan_csv, sept_csv, na.rm=T)==2, 0, 1),
         diff_method=ifelse(sum(city_intersect, csv_filter, na.rm=T)==2, 0, 1),) %>%
  ungroup()
  

sum(full_ain_universe$jan_universe)    # 54874
sum(full_ain_universe$sept_universe)   # 54874
sum(full_ain_universe$diff_universe)   # 9426

sum(full_ain_universe$jan_csv)         # 58474
sum(full_ain_universe$sept_csv)        # 58488
sum(full_ain_universe$diff_csv)        # 5785

sum(full_ain_universe$city_intersect)  # 54923
sum(full_ain_universe$csv_filter)      # 58496
sum(full_ain_universe$diff_method)     # 15083

