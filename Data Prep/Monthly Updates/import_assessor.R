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

# Zipped assessor data downloaded to D: drive from EMG's OneDrive
assessor_date <- "20251201" # Update

assessor_data_folder <- "D:/Assessor Data FULL/OneDrive_2025-12-09.zip"
temp_extract_dir <- "D:/temp_extract/Assessor Data/"

# clear temp_extract first if it exists
if (dir.exists(temp_extract_dir)) {
  unlink(temp_extract_dir, recursive = TRUE)
}

# create empty temp_extract
dir.create(temp_extract_dir, showWarnings = FALSE)

# Don't need to rerun # Unzipped in a temporary D:/ folder "temp_extract" (fread wasn't working so I used PowerShell/terminal)
unzipped_result <- system(paste0('powershell "Expand-Archive -Path \\"', assessor_data_folder, '\\" -DestinationPath \\"',temp_extract_dir,'\\" -Force"'))

# Confirm files extracted - by listing filenames in the temp folder
extracted_files <- list.files(temp_extract_dir, recursive = TRUE, full.names = TRUE)
print(extracted_files)

# Define file locations we'll need
shp_path <- paste0("D:/temp_extract/Assessor Data/Assr Data ", assessor_date, "/parcel.shp")

csv_1 <- "D:/temp_extract/Assessor Data/DS04 Part 1.csv"
csv_2 <- "D:/temp_extract/Assessor Data/DS04 Part 2.csv"
csv_3 <- "D:/temp_extract/Assessor Data/DS04 Part 3.csv"

##### Step 1: Intersect Jan Shps #####
# Spatial Intersect of Jan and Sept parcel shp files to
# Altadena/Pasadena city boundaries

city_perimeters <- st_read(con, query='select name, geom as geometry from data.tl_2023_places;') %>%
  filter(name == "Altadena") %>%
  st_transform(2229)
st_crs(city_perimeters)$epsg # 2229

# altadena_shp <- city_perimeters %>% filter(name=="Altadena")
# pasadena_shp <- city_perimeters %>% filter(name=="Pasadena")

# intersection
if (shp_path %in% extracted_files) {
  shp_intersect <- batch_intersect_shapefile(
    shp_path=shp_path,
    target_geo=city_perimeters,
    chunk_size = 10000,
    retain_cols = c("name")) # This time: 54868 (6 fewer); Last time: 54874
  
} else {
  print("shp file path has changed - compare shp_path to extracted_files list and update accordingly")
}

length(unique(shp_intersect$AIN)) # 54868
table(shp_intersect$matched_name, useNA = "ifany")
# Altadena                  Pasadena              Pasadena; Altadena
# 13948 (3 fewer)             40339 (3 fewer)           581

# jan results
# length(unique(jan_intersect$AIN)) # 54874
# table(jan_intersect$matched_name, useNA = "ifany")
# # Altadena           Pasadena        Pasadena; Altadena
# # 13951              40342           581

# clean up before export #
colnames(shp_intersect) <- tolower(colnames(shp_intersect))
shp_intersect_3310 <- st_transform(shp_intersect, 3310)

# # export results
data_vintage <- "November 2025"
date_ran <- as.character(Sys.Date()) # "2025-12-09"
curr_year <- strsplit(date_ran, "-", fixed=TRUE)[[1]][1] # year
curr_month <- strsplit(date_ran, "-", fixed=TRUE)[[1]][2] # month
source <- "Los Angeles County Assessor; Data Dictionary: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Extract\\FIELD DEF -- SBF.html"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_monthly_import_assessor_data.docx"
schema <- "dashboard"
indicator <- paste(data_vintage, "Parcels that intersect Altadena 2023 place tiger lines.")
shp_table_name <- paste("assessor_parcels_universe", curr_year, curr_month, sep="_")

export_shpfile(con=con,
               df=shp_intersect_3310,
               schema=schema,
               table_name=shp_table_name,
               srid = "", geometry_type = "",
               geometry_column = "geometry")


dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", shp_table_name, " IS '", indicator, "
            Data imported on ", date_ran,". ",
            "QA DOC: ", qa_filepath,
            " Source: ", source, "'"))

shp_intersect <- st_read(con, query=paste0("SELECT * FROM ", schema, ".", shp_table_name, ";"))
# confirm epsg is 3310
st_crs(shp_intersect)$epsg # 3310


##### Step 2: Filter Jan CSVs #####
# Filter Jan csv data for AINs found in Step 1 #
shp_ain_universe <- shp_intersect %>% st_drop_geometry() %>% select(ain) %>% pull()

# January
csv_1_ain_filter <- batch_filter_csv_data(
  csv_file=csv_1,
  target_list = shp_ain_universe,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE,
  exact_match=TRUE)

csv_2_ain_filter <- batch_filter_csv_data(
  csv_file=csv_2,
  target_list = shp_ain_universe,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE,
  exact_match=TRUE)

csv_3_ain_filter <- batch_filter_csv_data(
  csv_file=csv_3,
  target_list = shp_ain_universe,
  filter_column="ï»¿AIN",
  chunk_size = 10000,
  debug_filter=TRUE,
  exact_match=TRUE)

# Combine results
csv_ains_combined <- rbind(csv_1_ain_filter,
                           csv_2_ain_filter,
                           csv_3_ain_filter) # 13933 (Altadena only)

colnames(csv_ains_combined) <- tolower(gsub(" ", "_", colnames(csv_ains_combined)))

csv_ains_combined <- csv_ains_combined %>%
  rename(ain = `ï»¿ain`) %>%
  mutate(ain=as.character(ain))

length(unique(csv_ains_combined$ain)) # 13933

### Export to postgres
csv_table_name <- paste("assessor_data_universe", curr_year, curr_month, sep="_")
indicator <- paste("Assessor data from", data_vintage, "that matches Altadena and Pasadena parcels in", shp_table_name, "table.")
dbWriteTable(con, Id(schema, csv_table_name), csv_ains_combined,
             overwrite = FALSE, row.names = FALSE)
dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", csv_table_name, " IS '", indicator, "
            Data imported on ", date_ran, ".",
                        "QA DOC: ", qa_filepath,
                        " Source: ", source, "'"))



##### QA Clean-up #####
## Address double-encoding error in original data files
sql_csv_table_name <- paste0(schema, ".", csv_table_name)
# csv table
sql_rename_col <- paste("ALTER TABLE", sql_csv_table_name, "RENAME COLUMN exemption_type to exemption_type_original;")
dbSendQuery(con, sql_rename_col)

sql_create_new_col <- paste("ALTER TABLE", sql_csv_table_name, "ADD COLUMN exemption_type VARCHAR(5);")
dbSendQuery(con, sql_create_new_col)

sql_set_values <- paste("UPDATE", sql_csv_table_name, "SET exemption_type = exemption_type_original;")
dbSendQuery(con, sql_set_values)

sql_recode <- paste("UPDATE", sql_csv_table_name, "SET exemption_type = NULL WHERE exemption_type = 'Ã¿';")
dbSendQuery(con, sql_recode)


##### REVIEW: Two kinds of AIN mismatches between CSV and SHP files #####
# For permit scraping we are primarily interested in mismatches where the associated 
# use code starts with zero (residential) and does NOT end with V (vacant lot)

shp_ain <- shp_ain_universe
csv_ain <- dbGetQuery(con, statement=paste0("SELECT * FROM ", schema, ".", csv_table_name, ";"))

### 1. AINs from the SHP files that have no CSV match
mismatch_1 <- data.frame(ain=setdiff(shp_ain, csv_ain$ain)) %>%
  left_join(shp_intersect,by="ain") %>%
  st_as_sf(sf_column_name="geom",
           crs=st_crs(shp_intersect)) 

mapview(mismatch_1, col.regions="red")

# number of mismatch 1
nrow(mismatch_1) # 15

# look up on assessor portal: https://portal.assessor.lacounty.gov/parceldetail/[ain]
# denote parcel status (e.g., no result if none/doesn't exist in portal, Shell, Deleted, etc.)
# prioritize residential and non-vacant if updating script
# "5863003900" - 010v/shell - https://portal.assessor.lacounty.gov/parceldetail/5863003900 <- leftover/mapsearch shows same AIN
# "5831016035" - no response - https://portal.assessor.lacounty.gov/parceldetail/5831016035
# "5831016036" - no response - https://portal.assessor.lacounty.gov/parceldetail/5831016036
# "5839016025" - 0100/shell - https://portal.assessor.lacounty.gov/parceldetail/5839016025 <-- leftover / mapsearch shows same AIN
# "5839016026" - no response - https://portal.assessor.lacounty.gov/parceldetail/5839016026 <-- leftover / mapsearch shows same AIN
# "5847020027" - 0100/active - https://portal.assessor.lacounty.gov/parceldetail/5847020027 (note: misfortune and calamity is NA)
# "5841023022" - no response - https://portal.assessor.lacounty.gov/parceldetail/5841023022 
# "5830015029" - 0100/active - https://portal.assessor.lacounty.gov/parceldetail/5830015029 (note: misfortune and calamity is NA)
# "5842013027" - 010v/active - https://portal.assessor.lacounty.gov/parceldetail/5842013027
# "5843023069" - no response - https://portal.assessor.lacounty.gov/parceldetail/5843023069
# "5843023070" - no response - https://portal.assessor.lacounty.gov/parceldetail/5843023070
# "5842008018" - no response - https://portal.assessor.lacounty.gov/parceldetail/5842008018 <-- matched calfire - destroyed residence
# "5842008017" - no response - https://portal.assessor.lacounty.gov/parceldetail/5842008017 <-- leftover / mapsearch shows same AIN (looks to be part of 5842008018)
# "5827014035" - no response - https://portal.assessor.lacounty.gov/parceldetail/5827014035 <-- matched calfire - no damage 
# "5827014036" - no response - https://portal.assessor.lacounty.gov/parceldetail/5827014036 <-- leftover / mapsearch shows same AIN (looks to be part of 5827014035)

# see if there are any calfire points that intersect
calfire <- st_read(con, query="SELECT * FROM data.eaton_fire_dmg_insp_3310")

calfire_matches <- st_intersection(mismatch_1, calfire) %>%
  select(ain, apn_parcel, damage, everything())

# Review matches and check if APN is correct AIN (using assessor portal map): https://portal.assessor.lacounty.gov/mapsearch
unique(calfire_matches$ain)


matched <-  mismatch_1 %>% filter(ain %in% unique(calfire_matches$ain)) # 10 
leftover <- mismatch_1 %>% filter(!(ain %in% unique(calfire_matches$ain))) # 5


mapview(matched, col.regions="green", color ="green") + 
  mapview(leftover, col.regions="red", color ="red")


### 2. Get AINs from CSVs (based on situs city_state filter), and review which ones are not in the SHP ain universes
city_list <- c("Altadena")

csv_1_results <- batch_filter_csv_data(
  csv_file=csv_1,
  target_list = city_list,
  filter_column="City State",
  chunk_size = 10000,
  debug_filter=TRUE,
  exact_match=FALSE)

csv_2_results <- batch_filter_csv_data(
  csv_file=csv_2,
  target_list = city_list,
  filter_column="City State",
  chunk_size = 10000,
  debug_filter=TRUE,
  exact_match=FALSE)

csv_3_results <- batch_filter_csv_data(
  csv_file=csv_3,
  target_list = city_list,
  filter_column="City State",
  chunk_size = 10000,
  debug_filter=TRUE,
  exact_match=FALSE)

# combine
all_csv_results <- bind_rows(csv_1_results, csv_2_results, csv_3_results) #11741

# clean column names
colnames(all_csv_results)
colnames(all_csv_results)[1] <- "AIN"
colnames(all_csv_results) <- gsub(" ", "_", tolower(colnames(all_csv_results)))
colnames(all_csv_results)

# unique AIN for each row
length(unique(all_csv_results$ain)) #11741

# frequency table of situs 'city_state' field
table(all_csv_results$city_state, useNA = "ifany")


# # export results to csv (keeping all columns for QA)
# write.csv(all_csv_results,
#           file=paste0("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_", curr_year, "_", curr_month, ".csv"),
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

# import
all_csv_results <- read.csv(paste0("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_", curr_year, "_", curr_month, ".csv"))

# compare to shp ain universe
# in csv, but not in shp - prioritize residential and non-vacant
mismatch_2 <- data.frame(ain=setdiff(all_csv_results$ain, shp_ain_universe)) %>% # 182
  left_join(all_csv_results, by="ain") 

table(mismatch_2$use_code) # Note: 146 are residential and non-vacant

# check if these are in .shp (to cross out possibility they are outside the city perimeters)
csv_ains_combined <- mismatch_2 %>% select(ain) %>% pull()

check_ <- batch_filter_shapefile(shp_path=shp_path, target_ains=csv_ains_combined, chunk_size = 10000, ain_column = "AIN")

mismatch_2 <- mismatch_2 %>%
  mutate(shp_match = ifelse(ain %in% check_$AIN, 1,0)) %>%
  filter(shp_match==0) # 13 with relevant use codes

table(mismatch_2$use_code, useNA = "ifany")

# Note some of these look to overlap with some addresses that matched to Calfire data in mismatch_1
