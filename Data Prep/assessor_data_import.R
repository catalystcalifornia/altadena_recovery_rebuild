# Filter and import assessor data to postgres
library(dplyr)
library(data.table)
library(sf)

options(scipen = 999) # turn off scientific notation for batch queries
source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\assessor_data_functions.R")
con <- connect_to_db("altadena_recovery_rebuild")


##### Prep to batch process zipped files #####
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


##### Batch Filter CSVs for targeted cities #####
# Sept CSVs to batch process
sept_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 1.csv"
sept_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 2.csv"
sept_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 3.csv"

# Preview one of the CSVs to see what we need
preview <- fread(sept_csv_1, nrows = 5)
print(preview)
print(names(preview)) # Need "City State" and "AIN"
print(ncol(preview)) # 132

# running with debug_cities=TRUE to get summaries about 'City State' data in each CSV
# Can confirm the function is working so we can set debug_cities=FALSE

# # Sept Part 1
# sept_1_results <- batch_process_assessor_data(
#   csv_file=sept_csv_1,
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA"),
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept Part 2
# sept_2_results <- batch_process_assessor_data(
#   csv_file=sept_csv_2,
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA"),
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept Part 3
# sept_3_results <- batch_process_assessor_data(
#   csv_file=sept_csv_3,
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA"),
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Combine results (only Part 2 of Sept CSVs has results, CSVs are possibly organized by region)
# all_results <- rbind(sept_1_results, sept_2_results, sept_3_results)
# 
# # remove results tables to save space
# rm(sept_1_results)
# rm(sept_2_results)
# rm(sept_3_results)
# gc()
# 
# ##### Do an initial review of results (e.g., ensure unique IDs, note data quality issues, etc.)
# # first column name has weird symbols ("ï»¿AIN"), bad practice of spaces in column names throughout
# colnames(all_results)
# colnames(all_results)[1] <- "AIN"
# colnames(all_results) <- gsub(" ", "_", tolower(colnames(all_results)))
# colnames(all_results)
# 
# # unique AIN for each row
# length(unique(all_results$ain)) # 66116
# 
# # frequency table of situs 'city_state' field
# sept_city_results <- as.data.frame(table(all_results$city_state, useNA = "ifany"))
# 
# # export results to csv (keeping all columns for QA)
# write.csv(all_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_sept_2025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

sept_ains <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_sept_2025.csv")


# Sept CUSTOM CSVs to batch process
sept_custom_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 Custom DS04 Part 1.csv"
sept_custom_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 Custom DS04 Part 2.csv"
sept_custom_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 Custom DS04 Part 3.csv"

# Preview one of the CSVs to see what we need
preview <- fread(sept_custom_csv_1, nrows = 5)
print(preview)
print(names(preview)) # Need "City State" and "AIN"
print(ncol(preview)) # 132

# running with debug_cities=TRUE to get summaries about 'City State' data in each CSV
# Can confirm the function is working so we can set debug_cities=FALSE

# # Sept CUSTOM Part 1
# sept_custom_1_results <- batch_process_assessor_data(
#   csv_file=sept_custom_csv_1,
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA"),
#   filter_column="SitusCity",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept CUSTOM Part 2
# sept_custom_2_results <- batch_process_assessor_data(
#   csv_file=sept_custom_csv_2,
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA"),
#   filter_column="SitusCity",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept CUSTOM Part 3
# sept_custom_3_results <- batch_process_assessor_data(
#   csv_file=sept_custom_csv_3,
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA"),
#   filter_column="SitusCity",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Combine results (only Part 2 of Sept CSVs has results, CSVs are possibly organized by region)
# all_custom_results <- rbind(sept_custom_1_results, sept_custom_2_results, sept_custom_3_results)
# 
# # remove results tables to save space
# rm(sept_custom_1_results)
# rm(sept_custom_2_results)
# rm(sept_custom_3_results)
# gc()
# 
# ##### Do an initial review of results (e.g., ensure unique IDs, note data quality issues, etc.)
# # first column name has weird symbols ("ï»¿AIN"), bad practice of spaces in column names throughout
# colnames(all_custom_results)
# colnames(all_custom_results)[1] <- "AIN"
# colnames(all_custom_results) <- gsub(" ", "_", tolower(colnames(all_custom_results)))
# colnames(all_custom_results)
# 
# # unique AIN for each row
# length(unique(all_custom_results$ain)) # 66090
# 
# # frequency table of situs 'city_state' field
# sept_custom_city_results <- as.data.frame(table(all_custom_results$situscity, useNA = "ifany"))
# 
# # export results to csv (keeping all columns for QA)
# write.csv(all_custom_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_sept_custom_2025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

custom_sept_ains <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_sept_custom_2025.csv") 


# # Jan CSVs to batch process
# jan_csv_1 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 1.csv"
# jan_csv_2 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 2.csv"
# jan_csv_3 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 3.csv"
# 
# jan_1_results <- batch_process_assessor_data(
#   csv_file=jan_csv_1,
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA"),
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# jan_2_results <- batch_process_assessor_data(
#   csv_file=jan_csv_2,
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA"),
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# jan_3_results <- batch_process_assessor_data(
#   csv_file=jan_csv_3,
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA"),
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # note: suddenly jan 3 results has 133 columns (don't think it did before) - 
# # dropping for now (all NA) so it can be joined with other Jan results
# jan_3_results <- jan_3_results %>%
#   select(-`CR LF`)
# # Combine results
# all_jan_results <- rbind(jan_1_results, jan_2_results, jan_3_results)
# 
# # remove results tables to save space
# rm(jan_1_results)
# rm(jan_2_results)
# rm(jan_3_results)
# gc()
# 
# ##### Do an initial review of results (e.g., ensure unique IDs, note data quality issues, etc.)
# # first column name has weird symbols ("ï»¿AIN"), bad practice of spaces in column names throughout
# colnames(all_jan_results)
# colnames(all_jan_results)[1] <- "AIN"
# colnames(all_jan_results) <- gsub(" ", "_", tolower(colnames(all_jan_results)))
# colnames(all_jan_results)
# 
# # unique AIN for each row
# length(unique(all_jan_results$ain)) # 66096
# 
# # frequency table of situs 'city_state' field
# jan_city_results <- as.data.frame(table(all_jan_results$city_state, useNA = "ifany"))
# 
# # export results to csv (keeping all columns for QA)
# write.csv(all_jan_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_jan_2025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")


##### Filter parcel shp files #####
# read in Sept results and keep only AIN
sept_ains <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_sept_2025.csv")

# Explore Sept parcel shpfile
shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250902/parcels.shp"

# Check if file exists
file.exists(shp_path)

# Get file size (mb)
round(file.size(shp_path) / (1024^2), 2) # 623.18 Note: importing leads to memory increase greater than this - unclear why

# get sample data
parcel_sample <- st_read(shp_path, 
                       query="SELECT * FROM parcels LIMIT 1")

names(parcel_sample) # uses AIN
# [1] "OBJECTID"       "ASSRDATA_M"     "PERIMETER"      "PHASE"          "LOT"            "UNIT"           "MOVED"         
# [8] "TRA"            "PCLTYPE"        "SUBDTYPE"       "TRACT"          "USECODE"        "BLOCK"          "UDATE"         
# [15] "EDITORNAME"     "PARCEL_TYP"     "UNIT_NO"        "PM_REF"         "TOT_UNITS"      "AIN"            "GlobalID"      
# [22] "CENTER_X"       "CENTER_Y"       "CENTER_LAT"     "CENTER_LON"     "TRA_id"         "SHAPE_area"     "SHAPE_len"     
# [29] "_ogr_geometry_"

# Convert your AINs to a vector for filtering
target_ains_vector <- sept_ains$ain

# # Filter assessor parcels that match Altadena/Pasadena/Sierra Madre AINs
filtered_parcels <- batch_filter_shapefile(
  shp_path=shp_path,
  target_ains=target_ains_vector,
  chunk_size = 5000,
  ain_column = "AIN")

quick_check <- head(filtered_parcels, 10)

# minor clean up before export
filtered_parcels <- filtered_parcels %>%
  rename(geometry=`_ogr_geometry_`)

colnames(filtered_parcels) <- tolower(colnames(filtered_parcels))

# ##### Export data #####
source <- "Los Angeles County Assessor; Data Dictionary: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Extract\\FIELD DEF -- SBF.html"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_import_assessor_data.docx"
schema <- "data"

# # Sept shp file
# export_shpfile(con=con, df=filtered_parcels, schema="data", table_name="assessor_parcels_sept2025", srid = "", geometry_type = "", geometry_column = "geometry")
# indicator <- "Parcels with site addresses in Altadena, Arcadia, Pasadena, and Sierra Madre as of September 2025."
# dbSendQuery(con, paste0("COMMENT ON TABLE data.assessor_parcels_sept2025 IS '", indicator, "
#             Data imported on 9-23-25. ",
#             "QA DOC: ", qa_filepath,
#             " Source: ", source, "'"))

# # Sept csv
# table_name <- "assessor_data_sept2025"
# indicator <- "Assessor data from September 2025 for parcels in Altadena, Arcadia, Pasadena, and Sierra Madre."
# dbWriteTable(con, Id(schema, table_name), sept_ains,
#              overwrite = FALSE, row.names = FALSE)
# dbSendQuery(con, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
#             Data imported on 9-23-25. ",
#                         "QA DOC: ", qa_filepath,
#                         " Source: ", source, "'"))

# # Sept custom csv
# table_name <- "assessor_custom_data_sept2025"
# indicator <- "Custom assessor data from September 2025 for parcels in Altadena, Arcadia, Pasadena, and Sierra Madre."
# dbWriteTable(con, Id(schema, table_name), custom_sept_ains,
#              overwrite = FALSE, row.names = FALSE)
# dbSendQuery(con, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
#             Data imported on 9-23-25. ",
#                         "QA DOC: ", qa_filepath,
#                         " Source: ", source, "'"))


# read in Jan results and keep only AIN
jan_ains <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_jan_2025.csv") 

# Explore Sept parcel shpfile
jan_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250106/parcel.shp"

# Convert your AINs to a vector for filtering
target_jan_ains <- jan_ains$ain

# # Filter assessor parcels that match Altadena/Pasadena/Sierra Madre AINs
# filtered_parcels <- batch_filter_shapefile(
#   shp_path=jan_shp_path,
#   target_ains=target_jan_ains,
#   chunk_size = 5000,
#   ain_column = "AIN")

quick_check <- head(filtered_parcels, 10)

# minor clean up before export
filtered_parcels <- filtered_parcels %>%
  rename(geometry=`_ogr_geometry_`)

colnames(filtered_parcels) <- tolower(colnames(filtered_parcels))

##### Export Jan 2025 data #####
# # Filtered shp file
# export_shpfile(con=con, df=filtered_parcels, schema="data", table_name="assessor_parcels_jan2025", srid = "", geometry_type = "", geometry_column = "geometry")
# indicator <- "Parcels with site addresses in Altadena, Arcadia, Pasadena, and Sierra Madre  as of January 2025."
# dbSendQuery(con, paste0("COMMENT ON TABLE data.assessor_parcels_jan2025 IS '", indicator, "
#             Data imported on 9-23-25. ",
#             "QA DOC: ", qa_filepath,
#             " Source: ", source, "'"))


# # Filtered csv file
# table_name <- "assessor_data_jan2025"
# indicator <- "Custom assessor data from January 2025 for parcels in Altadena, Arcadia, Pasadena, and Sierra Madre."
# dbWriteTable(con, Id(schema, table_name), jan_ains,
#              overwrite = FALSE, row.names = FALSE)
# dbSendQuery(con, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
#             Data imported on 9-23-25. ",
#                         "QA DOC: ", qa_filepath,
#                         " Source: ", source, "'"))


##### Compare Sept to Jan AINS and parcels #####
# AINs
sept_only_ains <- anti_join(sept_ains, jan_ains, by="ain") %>%
  select(ain) 

jan_only_ains <- anti_join(jan_ains, sept_ains, by="ain") %>%
  select(ain) 

all_ains <- rbind(sept_ains, jan_ains) %>%
  select(ain) %>%
  distinct() %>%
  mutate(source_data = 
           case_when(ain %in% sept_only_ains$ain ~ "sept only",
                     ain %in% jan_only_ains$ain ~ "jan only",
                     .default = "both"))

# Parcels
sept_parcels <- st_read(con, query='SELECT ain FROM data.assessor_parcels_sept2025') %>%
  st_drop_geometry()

jan_parcels <- st_read(con, query='SELECT ain FROM data.assessor_parcels_jan2025') %>%
  st_drop_geometry()

# Add columns to check if AIN has parcel match
all_ains <- all_ains %>%
  mutate(sept_parcel_match = ifelse(ain %in% sept_parcels$AIN, "yes", "no"),
         jan_parcel_match = ifelse(ain %in% jan_parcels$AIN, "yes", "no"))

# # export results to csv (for QA)
# write.csv(all_ains,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/jan_sept_ain_comparison.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

