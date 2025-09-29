# Filter and import assessor data to postgres
# In three parts: 
## (1) Get list of AINs by filtering CSVs on site city based on list of targeted city names
## (2) Get list of AINs from spatial join of CalFire damage points to parcels
## (3) Get list of AINs from spatial join of targeted city perimeters to parcels

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


##### (1) Filter CSVs with targeted cities list, retain AINs #####
# Sept CSVs to batch process
sept_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 1.csv"
sept_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 2.csv"
sept_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 3.csv"

# targeted city names
city_list <- c("ALTADENA", "PASADENA", "SIERRA MADRE", "ARCADIA")

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
#   target_cities = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept Part 2
# sept_2_results <- batch_process_assessor_data(
#   csv_file=sept_csv_2,
#   target_cities = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept Part 3
# sept_3_results <- batch_process_assessor_data(
#   csv_file=sept_csv_3,
#   target_cities = city_list,
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
#   target_cities = city_list,
#   filter_column="SitusCity",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept CUSTOM Part 2
# sept_custom_2_results <- batch_process_assessor_data(
#   csv_file=sept_custom_csv_2,
#   target_cities = city_list,
#   filter_column="SitusCity",
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept CUSTOM Part 3
# sept_custom_3_results <- batch_process_assessor_data(
#   csv_file=sept_custom_csv_3,
#   target_cities = city_list,
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
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# jan_2_results <- batch_process_assessor_data(
#   csv_file=jan_csv_2,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE)
# 
# jan_3_results <- batch_process_assessor_data(
#   csv_file=jan_csv_3,
#   target_list = city_list,
#   filter_column="City State",
#   chunk_size = 10000,
#   debug_filter=TRUE)
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

jan_ains_csv <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_jan_2025.csv") %>% #85465
  mutate(ain=as.character(ain)) 
jan_ains_csv_filter<- jan_ains_csv %>%
  select(ain) %>%
  distinct() %>%
  mutate(csv_filter=1) #85465

##### (2) Get AINs from intersection of CalFire structure points with parcels #####
calfire_ains <- dbGetQuery(con, statement="SELECT * FROM data.calfire_assessor_xwalk_jan2025;") %>% #18422
  select(assessor_ain) %>%
  distinct() %>%
  rename(ain=assessor_ain) %>%
  mutate(calfire_intersect=1) # 11086

# compare to results from city list filter
new_ains <- setdiff(calfire_ains$ain, jan_ains_csv_filter$ain) # 116

new_ains <- data.frame(ain=new_ains) 

##### (3) Get AINs from intersection of target city perimeters with parcels #####
city_perimeters <- st_read(con, query="SELECT 'NAME' as name, geom as geometry FROM data.tl_2023_places")
st_crs(city_perimeters) #3310

jan_shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250106/parcel.shp"

city_intersect <- batch_filter_shapefile_intersect(shp_path=jan_shp_path, target_points=city_perimeters, chunk_size = 10000)

city_intersect_reduced <- city_intersect %>%
  select(AIN) %>% 
  st_drop_geometry() %>%
  distinct() %>% 
  mutate(city_intersect=1) %>%
  rename(ain=AIN) # 76708

##### Combine all results into one df #####
all_jan_ains <- full_join(jan_ains_csv_filter, select(calfire_ains, ain, calfire_intersect), by="ain")
all_jan_ains <- full_join(all_jan_ains, city_intersect_reduced, by="ain")
all_jan_ains <- all_jan_ains %>%
  replace_na(list(csv_filter=0,
                  calfire_intersect=0,
                  city_intersect=0)) # 89277

# # export to csv
# write.csv(all_jan_ains,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/all_ains_jan2025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

# read back in 
jan_ains <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/all_ains_jan2025.csv") %>%
  mutate(ain=as.character(ain))

jan_ains <- jan_ains %>% filter(!is.na(ain)) # 89277
length(unique(jan_ains$ain)) # 89277

##### Filter parcel shp files #####
### January ###
# Convert your AINs to a vector for filtering
target_jan_ains <- jan_ains %>% select(ain) %>% pull()

# # Filter assessor parcels that match Altadena/Pasadena/Sierra Madre AINs
filtered_parcels <- batch_filter_shapefile(
  shp_path=jan_shp_path,
  target_ains=target_jan_ains,
  chunk_size = 5000,
  ain_column = "AIN") # 89242 - 35 less than expected

check_ <- setdiff(jan_ains$ain, filtered_parcels$AIN)
# check_diff 
# [1] "5310016001" "5314026040" "5315021030" "5327002088" "5720003018" "5720003020" "5725002917" "5726010019" "5726010020" "5728014059"
# [11] "5734025087" "5736026046" "5738005051" "5738013104" "5755016006" "5757005005" "5757030006" "5757030008" "5757030041" "5762029003"
# [21] "5762029024" "5764031010" "5768029045" "5773006004" "5773006005" "5775015011" "5775015027" "5778005008" "5778010137" "5779017011"
# [31] "5783002013" "5831016032" "5831016033" "5839016004" "5839016012"

# all 35 missing are from the csv filter
check_diff <- data.frame(ain=check_) %>%
  left_join(jan_ains, by="ain") %>%
  left_join(jan_ains_csv, by="ain")
  
table(check_diff$city_state, useNA = "ifany")

# ALTADENA                 ALTADENA CA              ARCADIA CA               PASADENA                 PASADENA CA              SIERRA MADRE CA          
# 1                        3                        8                        2                       14                        4 
# SOUTH PASADENA CA        
# 3 

# # spot check a couple to see if parcel has different ain
# mapview(filtered_parcels) # saved as html here: file:///W:/Project/RDA Team/Altadena Recovery and Rebuild/Maps/all_jan_ains.html

# focusing spot check on altadena addresses
# 5831016032 - 3552 HOLLYSLOPE RD, ALTADENA 910010000
## in the shp file associated ain is: 5831016036
## note the assessor portal will not return anything for this ain so no way to get this other than manually
## ain results for this address list: 
#### 5831016026 (deleted)
#### 5831016032 (same as csv)

# 5831016033 - 3554 HOLLYSLOPE RD ALTADENA CA 910010000
## in the shp file associated ain is: 5831016035
## note the assessor portal will not return anything for this ain so no way to get this other than manually
## ain results for this address list: 
#### 5831016028 (deleted)
#### 5831016033 (same as csv)

# 5839016004 - 2230 SANTA ANITA AVE ALTADENA CA 910010000
## in the shp file associated ain is: 5839016026
## note the assessor portal will not return anything for this ain so no way to get this other than manually
## ain results for this address list: 
#### 5839016004 (same as csv)

# 5839016012 - 476 ALAMEDA ST ALTADENA CA 910010000
## in the shp file associated ain is: 5839016025
## note the assessor portal WILL RETURN for this: https://portal.assessor.lacounty.gov/parceldetail/5839016025
#### note parcel status is "shell"
## ain results for this address list: 
#### 5839016012 (same as csv) - note: parcel status "pending delete"

# minor clean up before export
filtered_parcels <- filtered_parcels %>%
  rename(geometry=`_ogr_geometry_`)

colnames(filtered_parcels) <- tolower(colnames(filtered_parcels))

length(unique(filtered_parcels$ain)) # 89242
length(unique(target_jan_ains)) # 89277

##### Export Jan 2025 data #####
# # Filtered shp file
# export_shpfile(con=con, df=filtered_parcels, schema="data", table_name="assessor_parcels_jan2025", srid = "", geometry_type = "", geometry_column = "geometry")
# indicator <- "Jan 2025 Parcels with site addresses or intersections in Altadena, Arcadia, Pasadena, and Sierra Madre or CalFire damage data."
# dbSendQuery(con, paste0("COMMENT ON TABLE data.assessor_parcels_jan2025 IS '", indicator, "
#             Data imported on 9-28-25. ",
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








# # read in Sept results and keep only AIN
# sept_ains <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_sept_2025.csv")
# 
# # Explore Sept parcel shpfile
# shp_path <- "D:/temp_extract/Assessor Data/Assr Data 20250902/parcels.shp"
# 
# # Check if file exists
# file.exists(shp_path)
# 
# # Get file size (mb)
# round(file.size(shp_path) / (1024^2), 2) # 623.18 Note: importing leads to memory increase greater than this - unclear why
# 
# # get sample data
# parcel_sample <- st_read(shp_path, 
#                        query="SELECT * FROM parcels LIMIT 1")
# 
# names(parcel_sample) # uses AIN
# # [1] "OBJECTID"       "ASSRDATA_M"     "PERIMETER"      "PHASE"          "LOT"            "UNIT"           "MOVED"         
# # [8] "TRA"            "PCLTYPE"        "SUBDTYPE"       "TRACT"          "USECODE"        "BLOCK"          "UDATE"         
# # [15] "EDITORNAME"     "PARCEL_TYP"     "UNIT_NO"        "PM_REF"         "TOT_UNITS"      "AIN"            "GlobalID"      
# # [22] "CENTER_X"       "CENTER_Y"       "CENTER_LAT"     "CENTER_LON"     "TRA_id"         "SHAPE_area"     "SHAPE_len"     
# # [29] "_ogr_geometry_"
# 
# # Convert your AINs to a vector for filtering
# target_ains_vector <- sept_ains$ain
# 
# # # Filter assessor parcels that match Altadena/Pasadena/Sierra Madre AINs
# filtered_parcels <- batch_filter_shapefile(
#   shp_path=shp_path,
#   target_ains=target_ains_vector,
#   chunk_size = 5000,
#   ain_column = "AIN")
# 
# quick_check <- head(filtered_parcels, 10)
# 
# # minor clean up before export
# filtered_parcels <- filtered_parcels %>%
#   rename(geometry=`_ogr_geometry_`)
# 
# colnames(filtered_parcels) <- tolower(colnames(filtered_parcels))
# 
# # ##### Export data #####
# source <- "Los Angeles County Assessor; Data Dictionary: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Extract\\FIELD DEF -- SBF.html"
# qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_import_assessor_data.docx"
# schema <- "data"
# 
# # # Sept shp file
# # export_shpfile(con=con, df=filtered_parcels, schema="data", table_name="assessor_parcels_sept2025", srid = "", geometry_type = "", geometry_column = "geometry")
# # indicator <- "Parcels with site addresses in Altadena, Arcadia, Pasadena, and Sierra Madre as of September 2025."
# # dbSendQuery(con, paste0("COMMENT ON TABLE data.assessor_parcels_sept2025 IS '", indicator, "
# #             Data imported on 9-23-25. ",
# #             "QA DOC: ", qa_filepath,
# #             " Source: ", source, "'"))
# 
# # # Sept csv
# # table_name <- "assessor_data_sept2025"
# # indicator <- "Assessor data from September 2025 for parcels in Altadena, Arcadia, Pasadena, and Sierra Madre."
# # dbWriteTable(con, Id(schema, table_name), sept_ains,
# #              overwrite = FALSE, row.names = FALSE)
# # dbSendQuery(con, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
# #             Data imported on 9-23-25. ",
# #                         "QA DOC: ", qa_filepath,
# #                         " Source: ", source, "'"))
# 
# # # Sept custom csv
# # table_name <- "assessor_custom_data_sept2025"
# # indicator <- "Custom assessor data from September 2025 for parcels in Altadena, Arcadia, Pasadena, and Sierra Madre."
# # dbWriteTable(con, Id(schema, table_name), custom_sept_ains,
# #              overwrite = FALSE, row.names = FALSE)
# # dbSendQuery(con, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
# #             Data imported on 9-23-25. ",
# #                         "QA DOC: ", qa_filepath,
# #                         " Source: ", source, "'"))
# 


