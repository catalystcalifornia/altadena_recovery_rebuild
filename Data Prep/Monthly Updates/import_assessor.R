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

# Zipped assessor data downloaded to D: drive from EMG's OneDrive - should match date suffix in Sharepoint
assessor_date <-"2026-03-02" # Update
assessor_date_clean <- gsub("-", "", assessor_date) 

temp_data_migration_folder <- "Cold Data Migration - D Drive" # for rds data migration
assessor_data_folder <- sprintf("D:/%s/Assessor Data FULL/OneDrive_%s.zip", temp_data_migration_folder, assessor_date)
temp_extract_dir <- "D:/temp_extract/Assessor Data/"

# clear temp_extract first if it exists
if (dir.exists(temp_extract_dir)) {
  unlink(temp_extract_dir, recursive = TRUE)
}

# create empty temp_extract
dir.create(temp_extract_dir, showWarnings = FALSE)

# Unzipped in a temporary D:/ folder "temp_extract" (fread wasn't working so I used PowerShell/terminal)
unzipped_result <- system(paste0('powershell "Expand-Archive -Path \\"', assessor_data_folder, '\\" -DestinationPath \\"',temp_extract_dir,'\\" -Force"'))

# Confirm files extracted - by listing filenames in the temp folder
extracted_files <- list.files(temp_extract_dir, recursive = TRUE, full.names = TRUE)
print(extracted_files)

# [1] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.cpg"                              
# [2] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.dbf"                              
# [3] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.prj"                              
# [4] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.sbn"                              
# [5] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.sbx"                              
# [6] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.shp"                              
# [7] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.shp.HAS026961.21100.17200.sr.lock"
# [8] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.shp.HAS026961.21100.rd.lock"      
# [9] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.shp.xml"                          
# [10] "D:/temp_extract/Assessor Data/March 2026/Assr Data 20260302/parcel.shx"                              
# [11] "D:/temp_extract/Assessor Data/March 2026/DS04 Part 1.csv"                                            
# [12] "D:/temp_extract/Assessor Data/March 2026/DS04 Part 2.csv"                                            
# [13] "D:/temp_extract/Assessor Data/March 2026/DS04 Part 3.csv"         

# Define file locations we'll need
shp_path <- grep("parcel.shp$", extracted_files, value=TRUE)


# April Update
# should also work for future updates (won't need to manually update filepaths if they change) 
csv_1 <- grep("DS04 Part 1.csv", extracted_files, value=TRUE)
csv_2 <- grep("DS04 Part 2.csv", extracted_files, value=TRUE)
csv_3 <- grep("DS04 Part 3.csv", extracted_files, value=TRUE)

# # Dec update
# csv_1 <- "D:/temp_extract/Assessor Data/DS04 Part 1.csv"
# csv_2 <- "D:/temp_extract/Assessor Data/DS04 Part 2.csv"
# csv_3 <- "D:/temp_extract/Assessor Data/DS04 Part 3.csv"

# Pre Dec Update
# csv_1 <- "D:/temp_extract/Assessor Data/DS04 Part 1.csv"
# csv_2 <- "D:/temp_extract/Assessor Data/DS04 Part 2.csv"
# csv_3 <- "D:/temp_extract/Assessor Data/DS04 Part 3.csv"

##### Step 1: Intersect Jan Shps #####
# Spatial Intersect of Jan and Sept parcel shp files to
# Altadena/Pasadena city boundaries

city_perimeters <- st_read(con, query='select name, geom as geometry from data.tl_2023_places;') %>%
  filter(name == "Altadena") %>%
  st_transform(2229)
st_crs(city_perimeters)$epsg # 2229

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

length(unique(shp_intersect$AIN)) 
table(shp_intersect$matched_name, useNA = "ifany")

# March 2026 (april 2026 update)
# Altadena 
# 14528 (580 more)

# December 2025
# Altadena                  Pasadena              Pasadena; Altadena
# 13948 (3 fewer)             40339 (3 fewer)           581

# jan 2025 results
# length(unique(jan_intersect$AIN)) # 54874
# table(jan_intersect$matched_name, useNA = "ifany")
# # Altadena           Pasadena        Pasadena; Altadena
# # 13951              40342           581

# clean up before export #
colnames(shp_intersect) <- tolower(colnames(shp_intersect))
shp_intersect_3310 <- st_transform(shp_intersect, 3310)

# # export results
data_vintage_month <- "03" 
data_vintage_year <- "2026"
date_ran <- as.character(Sys.Date()) 
update_year <-  "2026" # strsplit(date_ran, "-", fixed=TRUE)[[1]][1] # year of dashboard update
update_month <- "04" # strsplit(date_ran, "-", fixed=TRUE)[[1]][2] # month of dashboard update
source <- "Los Angeles County Assessor; Data Dictionary: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Extract\\FIELD DEF -- SBF.html"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_monthly_import_assessor_data.docx"
schema <- "dashboard"
indicator <- sprintf("%s/%s Parcels that intersect Altadena 2023 place tiger lines.", data_vintage_month, data_vintage_year)
shp_table_name <- paste("assessor_parcels_universe", update_year, update_month, sep="_")

# export_shpfile(con=con,
#                df=shp_intersect_3310,
#                schema=schema,
#                table_name=shp_table_name,
#                srid = "", geometry_type = "",
#                geometry_column = "geometry")
# 
# 
# dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", shp_table_name, " IS '", indicator, "
#             Data imported on ", date_ran,". ",
#             "QA DOC: ", qa_filepath,
#             " Source: ", source, "'"))

shp_intersect <- st_read(con, query=paste0("SELECT * FROM ", schema, ".", shp_table_name, ";"))
# confirm epsg is 3310
st_crs(shp_intersect)$epsg # 3310


##### Step 2: Filter CSVs #####
# Filter csv data for AINs found in Step 1 #
shp_ain_universe <- shp_intersect %>% st_drop_geometry() %>% select(ain) %>% pull()

# add these AINs manually, the related

csv_1_ain_filter <- batch_filter_csv_data(
  csv_file=csv_1,
  target_list = shp_ain_universe,
  filter_column="ï»¿AIN", # AIN, sometimes has encoding typos
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
                           csv_3_ain_filter) # 13934 (Altadena only)

colnames(csv_ains_combined) <- tolower(gsub(" ", "_", colnames(csv_ains_combined)))

csv_ains_combined <- csv_ains_combined %>%
  rename(ain = `ï»¿ain`) %>%
  mutate(ain=as.character(ain))

length(unique(csv_ains_combined$ain)) # 13934

# check last sale date
max(as.Date(as.character(csv_ains_combined$last_sale_date),
            format = "%Y%m%d"),
    na.rm = TRUE) # "2025-12-31" updated but seems somewhat outdated for March?

### Export to postgres
csv_table_name <- paste("assessor_data_universe", update_year, update_month, sep="_")
indicator <- sprintf("Assessor data from %s/%s that matches Altadena and Pasadena parcels in %s table.", data_vintage_month, data_vintage_year, shp_table_name)
# dbWriteTable(con, Id(schema, csv_table_name), csv_ains_combined,
#              overwrite = FALSE, row.names = FALSE)
# dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", csv_table_name, " IS '", indicator, "
#             Data imported on ", date_ran, ".",
#                         "QA DOC: ", qa_filepath,
#                         " Source: ", source, "'"))



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

sql_recode <- paste("UPDATE", sql_csv_table_name, 
                    "SET exemption_type = NULL WHERE exemption_type IN ('ÿ', 'Ã¿');")
dbSendQuery(con, sql_recode)


## REVIEW: Two kinds of AIN mismatches between CSV and SHP files ##
# For permit scraping we are primarily interested in mismatches where the associated 
# use code starts with zero (residential) and does NOT end with V (vacant lot)

shp_ain <- shp_ain_universe
csv_ain <- dbGetQuery(con, statement=paste0("SELECT * FROM ", schema, ".", csv_table_name, ";"))

### 1. AINs from the SHP files that have no CSV match
mismatch_1 <- data.frame(ain=setdiff(shp_ain, csv_ain$ain)) %>%
  left_join(shp_intersect,by="ain") %>%
  st_as_sf(sf_column_name="geom",
           crs=st_crs(shp_intersect)) 

# number of mismatch 1
nrow(mismatch_1) # 12
sort(mismatch_1$ain)

# visually confirm shapes are fully in Altadena
mapview(city_perimeters, col.regions="lightgrey") + mapview(mismatch_1, col.regions="red")

## get parcel status manually from assessor portal: https://portal.assessor.lacounty.gov/parceldetail/[ain]
# denote parcel status (e.g., no result if none/doesn't exist in portal, Shell, Deleted, etc.)
# prioritize residential and non-vacant if updating script

## No responses - should figure these out
# "5831016035" - no response (same) - https://portal.assessor.lacounty.gov/parceldetail/5831016035 <-- matched calfire
# "5831016036" - no response (same) - https://portal.assessor.lacounty.gov/parceldetail/5831016036 <-- matched calfire
# "5839016026" - no response (same) - https://portal.assessor.lacounty.gov/parceldetail/5839016026 <-- leftover / mapsearch shows same AIN 
# (NEW) "5841007024" - no response - https://portal.assessor.lacounty.gov/parceldetail/5841007024 <-- matched calfire
# "5843023070" - no response (same) - https://portal.assessor.lacounty.gov/parceldetail/5843023070 <-- matched calfire; looks like it should be 5843023037 - https://portal.assessor.lacounty.gov/parceldetail/5843023037

## Shells - not sure we can do anything with these - no associated addresses
# "5827014035" - 0100/shell - https://portal.assessor.lacounty.gov/parceldetail/5827014035 <-- matched calfire - no damage 
# "5827014036" - 0100/shell - https://portal.assessor.lacounty.gov/parceldetail/5827014036 <-- leftover / mapsearch shows same AIN (looks to be part of 5827014035) 
# "5839016025" - 0100/shell (same) - https://portal.assessor.lacounty.gov/parceldetail/5839016025 <-- leftover / mapsearch shows same AIN 
# "5842008017" - 0100/shell - https://portal.assessor.lacounty.gov/parceldetail/5842008017 <-- leftover / mapsearch shows same AIN (looks to be part of 5842008018) 
# "5842008018" - 0100/shell - https://portal.assessor.lacounty.gov/parceldetail/5842008018 <-- matched calfire - destroyed residence
# "5843023069" - 0101/shell (updated; has response) - https://portal.assessor.lacounty.gov/parceldetail/5843023069 <- matched calfire
# "5863003900" - 010v/shell (same) - https://portal.assessor.lacounty.gov/parceldetail/5863003900 <- leftover/mapsearch shows same AIN

# Previous mismatches (no longer show up in mismatch_1):
# "5830015029" - 0100/active - https://portal.assessor.lacounty.gov/parceldetail/5830015029 (note: misfortune and calamity is NA) 
# "5841023022" - no response - https://portal.assessor.lacounty.gov/parceldetail/5841023022 
# "5842013027" - 010v/active - https://portal.assessor.lacounty.gov/parceldetail/5842013027 

# see if there are any calfire points that intersect
calfire <- st_read(con, query="SELECT din_id, apn_parcel, damage, street_number, street_name, street_type, street_suffix, city, state, zip_code, geom FROM data.eaton_fire_dmg_insp_3310")

calfire_matches <- st_intersection(mismatch_1, calfire) %>%
  select(ain, apn_parcel, damage, everything())

matched <-  mismatch_1 %>% filter(ain %in% unique(calfire_matches$ain)) # 7
sort(matched$ain)
# "5827014035" "5831016035" "5831016036" "5841007024" "5842008018" "5843023069" "5843023070"

leftover <- mismatch_1 %>% filter(!(ain %in% unique(calfire_matches$ain))) # 5
sort(leftover$ain)
# "5827014036" "5839016025" "5839016026" "5842008017" "5863003900"

mapview(matched, col.regions="green", color ="green") + 
  mapview(leftover, col.regions="red", color ="red")

mismatch1_damaged <- calfire_matches %>% 
  st_drop_geometry() %>% 
  # filter for those with "Destroyed (>50%)" since these will be in the universe
  filter(damage=="Destroyed (>50%)") %>%
  select(ain, apn_parcel, damage, street_number, street_name, street_type, 
         street_suffix, city, state, zip_code)

# Review matches with significant damage and check if APN is more accurate (using assessor portal map): 
# https://portal.assessor.lacounty.gov/mapsearch
mismatch1_damaged$apn_parcel # 8
# apn from calfire / assessor shp AIN / use code/ assessor URL
# "5843023037" / "5843023070" - 0101 - https://portal.assessor.lacounty.gov/parceldetail/5843023037
# "5843023016" / "5843023069" - 0101 (pending delete) - https://portal.assessor.lacounty.gov/parceldetail/5843023016
# "5831016032" / "5831016036" - 0100 - https://portal.assessor.lacounty.gov/parceldetail/5831016032
# "5831016033" / "5831016035" - 0100 - https://portal.assessor.lacounty.gov/parceldetail/5831016033
# "5843023016" / "5843023069" - 0101 (pending delete) - https://portal.assessor.lacounty.gov/parceldetail/5843023016
# "5831016032" / "5831016036" - 0100 - https://portal.assessor.lacounty.gov/parceldetail/5831016032
# "5841007017" / "5841007024" - 0100 - https://portal.assessor.lacounty.gov/parceldetail/5841007017
# "5842008010" / "5842008018" - 0100 (pending delete) - https://portal.assessor.lacounty.gov/parceldetail/5842008010

# above confirms that apn_parcel is likely the better ain, 
# we should check that these are in the csv - they are
check <- all_csv_results %>% filter(ain %in% mismatch1_damaged$apn_parcel) #6
n_distinct(mismatch1_damaged$apn_parcel) #6

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
#           file=paste0("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_", update_year, "_", update_month, ".csv"),
#           row.names=FALSE,
#           fileEncoding = "UTF-8")

# import
all_csv_results <- read.csv(paste0("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/csv_city_matches_", update_year, "_", update_month, ".csv"))

# compare to shp ain universe
# in csv, but not in shp - prioritize residential and non-vacant
mismatch_2 <- data.frame(ain=setdiff(all_csv_results$ain, shp_ain_universe)) %>% # 20
  left_join(all_csv_results, by="ain") %>%
  mutate(mismatch1_apn = ifelse(ain %in% mismatch1_damaged$apn_parcel, TRUE, FALSE)) %>%
  select(ain, mismatch1_apn, everything())

table(mismatch_2$mismatch1_apn, mismatch_2$use_code) # mismatch 2 includes all the shp ains that matched to damaged structures in calfire data
table(mismatch_2$use_code) # Note: There are 10 (in addition to the 6 from mismatch1) that are residential and non-vacant

check <- mismatch_2 %>% 
  filter(mismatch1_apn==FALSE & grepl("^0", use_code) & !grepl("V$", use_code)) # 10
check2 <- check %>% 
  mutate(ain=as.character(ain)) %>% 
  left_join(calfire, by=c("ain"="apn_parcel")) %>%
  select(ain, damage, everything()) # 12 - all are NA or No Damage
# review the NA damage for possibly missed parcels
check3 <- check2 %>% filter(is.na(damage)) %>%
  select(ain) %>%
  mutate(ain=as.numeric(ain)) %>%
  left_join(check) # 8

# going to review addresses manually to see what comes out in assessor
# 5825003060 - AIN the same, no parcel change history, this looks possibly not impacted by Eaton - https://portal.assessor.lacounty.gov/parceldetail/5825003060
# 5830004002 - similar to above - https://portal.assessor.lacounty.gov/parceldetail/5830004002
# 5830004003 - similar to above - https://portal.assessor.lacounty.gov/parceldetail/5830004003
# 5830004004 - similar to above - https://portal.assessor.lacounty.gov/parceldetail/5830004004
# 5830004005 - similar to above - https://portal.assessor.lacounty.gov/parceldetail/5830004005
# 5839016004 - similar to above - https://portal.assessor.lacounty.gov/parceldetail/5839016004
# 5839016012 - similar to above - https://portal.assessor.lacounty.gov/parceldetail/5839016012
# 5863003007 - similar to above - https://portal.assessor.lacounty.gov/parceldetail/5863003007

# Conclusion - mismatch type 1 is more of a data quality concern coming from Assessor's Office. 
# Mismatch 2 will give us all of Mismatch 1 and more but on closer inspection the "more" covers
# places where city is "Altadena" but were beyond the fire perimeter or considered out of the
# way of fire damage. This means the NAs from CalFire don't currently appear to be data quality issues
# However we should continue to review in future updates (capacity permitting).


# Resolve the SHP ains that matched to damaged CalFire APN parcels
# I'm going to save the original shp ain column as shp_ain and create a new ain column with the changes
shp_intersect_3310 <- st_read(con, query=paste0("SELECT * FROM ", schema, ".", shp_table_name, ";"))
mismatch_1_replacements <- mismatch1_damaged %>% select(ain, apn_parcel) %>% distinct()

shp_intersect_3310_clean <- shp_intersect_3310 %>%
  rename(shp_ain = ain) %>%
  left_join(mismatch_1_replacements, by = c("shp_ain" = "ain")) %>%
  mutate(ain = coalesce(apn_parcel, shp_ain)) %>%
  select(-apn_parcel)

check <- shp_intersect_3310_clean %>% filter(shp_ain %in% mismatch_1_replacements$ain) %>% select(shp_ain, ain, everything())

# re-export results
data_vintage_month <- "03" 
data_vintage_year <- "2026"
date_ran <- as.character(Sys.Date()) 
update_year <-  "2026" # strsplit(date_ran, "-", fixed=TRUE)[[1]][1] # year of dashboard update
update_month <- "04" # strsplit(date_ran, "-", fixed=TRUE)[[1]][2] # month of dashboard update
source <- "Los Angeles County Assessor; Data Dictionary: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Assessor Data Extract\\FIELD DEF -- SBF.html"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_monthly_import_assessor_data.docx"
schema <- "dashboard"
indicator <- sprintf("%s/%s Parcels that intersect Altadena 2023 place tiger lines.", data_vintage_month, data_vintage_year)
shp_table_name <- paste("assessor_parcels_universe", update_year, update_month, sep="_")


# export_shpfile(con=con,
#                df=shp_intersect_3310_clean,
#                schema=schema,
#                table_name=shp_table_name,
#                srid = "", geometry_type = "",
#                geometry_column = "geom")
# 
# 
# dbSendQuery(con, paste0("COMMENT ON TABLE ", schema, ".", shp_table_name, " IS '", indicator, "
#             Data imported on ", date_ran,". ",
#             "QA DOC: ", qa_filepath,
#             " Source: ", source, "'"))

##### Now we rerun step 2 on the new corrected ains #####
shp_intersect <- st_read(con, query=paste0("SELECT * FROM ", schema, ".", shp_table_name, ";"))
shp_ain_universe <- shp_intersect %>% st_drop_geometry() %>% select(ain) %>% pull()

# add these AINs manually, the related
csv_1_ain_filter <- batch_filter_csv_data(
  csv_file=csv_1,
  target_list = shp_ain_universe,
  filter_column="ï»¿AIN", # AIN, sometimes has encoding typos
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
                           csv_3_ain_filter) # 14522

colnames(csv_ains_combined) <- tolower(gsub(" ", "_", colnames(csv_ains_combined)))

csv_ains_combined <- csv_ains_combined %>%
  rename(ain = `ï»¿ain`) %>%
  mutate(ain=as.character(ain))

length(unique(csv_ains_combined$ain)) # 14522

# check last sale date
max(as.Date(as.character(csv_ains_combined$last_sale_date),
            format = "%Y%m%d"),
    na.rm = TRUE) # "2025-12-31" 

### Export to postgres
csv_table_name <- paste("assessor_data_universe", update_year, update_month, sep="_")
indicator <- sprintf("Assessor data from %s/%s that matches Altadena and Pasadena parcels in %s table.", data_vintage_month, data_vintage_year, shp_table_name)
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

sql_recode <- paste("UPDATE", sql_csv_table_name, 
                    "SET exemption_type = NULL WHERE exemption_type IN ('ÿ', 'Ã¿');")
dbSendQuery(con, sql_recode)