
library(dplyr)
library(data.table)
library(sf)

options(scipen = 999) # turn off scientific notation for batch queries
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")

##### Prep to batch process zipped files #####
# # Zipped assessor data downloaded to D: drive from EMG's OneDrive
# assessor_data_folder <- "D:/Assessor Data FULL/OneDrive_2025-09-17.zip"
# temp_extract_dir <- "D:/temp_extract/"
# 
# # Don't need to rerun # Unzipped in a temporary D:/ folder "temp_extract" (fread wasn't working so I used PowerShell/terminal)
# # unzipped_result <- system(paste0('powershell "Expand-Archive -Path \\"', assessor_data_folder, '\\" -DestinationPath \\"',temp_extract_dir,'\\" -Force"'))
# 
# # Confirm files extracted - by listing filenames in the temp folder
# extracted_files <- list.files(temp_extract_dir, recursive = TRUE, full.names = TRUE)
# print(extracted_files)
# 
# # Sept CSVs to batch process
# sept_csv_1 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 1.csv"
# sept_csv_2 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 2.csv"
# sept_csv_3 <- "D:/temp_extract/Assessor Data/September 2025 DS04 Part 3.csv"
# 
# # Preview one of the CSVs to see what we need
# preview <- fread(sept_csv_1, nrows = 5)
# print(preview)
# print(names(preview)) # Need "City State" and "AIN" - what is TRA?
# print(ncol(preview)) # 132

# Create a function to batch read csvs, filter batch for targeted cities,
# and save matches to dataframe
# Note the function matches the provided target cities to anywhere in the 'City State' col
# (e.g., Pasadena and South Pasadena will be matched)
batch_process_assessor_data <- function(csv_file, target_cities, chunk_size = 10000, debug_cities = TRUE) {
  
  cat("Starting batch processing of:", csv_file, "\n")
  cat("Target cities:", paste(target_cities, collapse = ", "), "\n")
  cat("Chunk size:", chunk_size, "\n")
  cat("City discovery mode:", debug_cities, "\n\n")
  
  # Open file connection
  con <- file(csv_file, "r")
  on.exit({
    close(con)
    gc()
  })
  
  # Read header line and parse column names
  header_line <- readLines(con, n = 1)
  col_names <- trimws(strsplit(header_line, ",")[[1]])
  rm(header_line)
  gc()
  
  cat("Column names found:", length(col_names), "columns\n")
  
  # Verify required columns
  if (!"City State" %in% col_names) {
    cat("ERROR: 'City State' column not found!\n")
    cat("Available columns:", paste(col_names[1:min(10, length(col_names))], collapse = ", "), "...\n")
    stop("Required column 'City State' not found")
  }
  
  cat("✓ 'City State' column found successfully\n")
  cat("Starting chunk processing...\n\n")
  
  results <- list()
  chunk_num <- 1
  total_filtered_rows <- 0
  all_cities_found <- character(0)  # Track all unique cities
  
  repeat {
    # Read chunk of lines
    lines <- readLines(con, n = chunk_size)
    if (length(lines) == 0) {
      cat("No more data to read. Processing complete.\n")
      break
    }
    
    cat("Processing chunk", chunk_num, "with", length(lines), "rows...")
    
    # Convert to data.table with proper column names
    chunk <- fread(text = paste(lines, collapse = "\n"), 
                   header = FALSE, col.names = col_names)
    rm(lines)
    gc()
    
    # CITY DISCOVERY: Show unique cities in first few chunks
    if (debug_cities && chunk_num <= 5) {
      unique_cities <- unique(chunk[["City State"]])
      all_cities_found <- unique(c(all_cities_found, unique_cities))
      cat("\n  Unique cities in chunk", chunk_num, ":\n")
      print(unique_cities[1:min(10, length(unique_cities))])
    }
    
    # Filter for target cities
    city_pattern <- paste(target_cities, collapse = "|")
    filtered <- chunk[grepl(city_pattern, 
                            toupper(trimws(chunk[["City State"]])), 
                            ignore.case = TRUE)]
    
    # Clean up original chunk
    rm(chunk)
    gc()
    
    # Report results for this chunk
    if (nrow(filtered) > 0) {
      cat(" Found", nrow(filtered), "matching rows\n")
      results[[chunk_num]] <- filtered
      total_filtered_rows <- total_filtered_rows + nrow(filtered)
      
      # Show sample cities found
      sample_cities <- unique(filtered[["City State"]])[1:min(3, length(unique(filtered[["City State"]])))]
      cat("  ✓ Target cities found:", paste(sample_cities, collapse = ", "), "\n")
    } else {
      cat(" No matching rows found\n")
      rm(filtered)
    }
    
    chunk_num <- chunk_num + 1
    
    # Progress summary every 25 chunks + show city discovery
    if (chunk_num %% 25 == 0) {
      gc(verbose = FALSE)
      cat("\n--- Progress Summary ---\n")
      cat("Processed", chunk_num - 1, "chunks so far\n")
      cat("Total matching rows found:", total_filtered_rows, "\n")
      
      if (debug_cities) {
        cat("All unique cities discovered so far (", length(all_cities_found), "total):\n")
        # Show cities that might contain our targets
        potential_matches <- all_cities_found[grepl(paste(target_cities, collapse = "|"), 
                                                    toupper(all_cities_found), ignore.case = TRUE)]
        if (length(potential_matches) > 0) {
          cat("  ✓ Potential target matches:", paste(potential_matches, collapse = ", "), "\n")
        } else {
          cat("  ⚠ No target cities found yet. Sample of cities seen:\n")
          print(all_cities_found[1:min(10, length(all_cities_found))])
        }
      }
      cat("Memory cleaned. Continuing...\n\n")
    }
  }
  
  # Final summary with complete city discovery
  cat("\n=== PROCESSING COMPLETE ===\n")
  cat("Total chunks processed:", chunk_num - 1, "\n")
  cat("Total matching rows found:", total_filtered_rows, "\n")
  
  if (debug_cities) {
    cat("\n=== CITY DISCOVERY SUMMARY ===\n")
    cat("Total unique cities found:", length(all_cities_found), "\n")
    
    # Check for potential matches
    potential_matches <- all_cities_found[grepl(paste(target_cities, collapse = "|"), 
                                                toupper(all_cities_found), ignore.case = TRUE)]
    if (length(potential_matches) > 0) {
      cat("Cities containing target names:\n")
      print(potential_matches)
    } else {
      cat("⚠ NO cities found containing target names!\n")
      cat("Sample of all cities in dataset:\n")
      print(all_cities_found[1:min(20, length(all_cities_found))])
    }
  }
  
  # Final data combination
  if (length(results) > 0) {
    cat("\nCombining", length(results), "filtered chunks into final dataset...\n")
    final_data <- rbindlist(results)
    rm(results)
    gc(verbose = TRUE)
    
    cat("✓ Final dataset created with", nrow(final_data), "rows and", ncol(final_data), "columns\n")
    return(final_data)
  } else {
    cat("No data found for the specified cities\n")
    rm(results)
    gc()
    return(data.table())
  }
}

##### Batch Process #####
# running with debug_cities=TRUE to get summaries about 'City State' data in each CSV
# Can confirm the function is working so we can set debug_cities=FALSE

# # Sept Part 1
# sept_1_results <- batch_process_assessor_data(
#   csv_file=sept_csv_1, 
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE"),
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept Part 2
# sept_2_results <- batch_process_assessor_data(
#   csv_file=sept_csv_2, 
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE"),
#   chunk_size = 10000,
#   debug_cities=TRUE)
# 
# # Sept Part 3
# sept_3_results <- batch_process_assessor_data(
#   csv_file=sept_csv_3, 
#   target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE"),
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
# city_results <- as.data.frame(table(all_results$city_state, useNA = "ifany"))
# 
# # export results to csv (keeping all columns for QA)
# write.csv(all_results, 
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_sept_2025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")


##### Filter parcel shpfile #####
# read in results and keep only AIN
ains <- read.csv("W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_sept_2025.csv")

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


##### Batch Process Shapefile with AIN Filtering #####
# Function to batch process shapefile and filter by AIN list
batch_filter_shapefile <- function(shp_path, target_ains, chunk_size = 10000, ain_column = "AIN") {
  cat("=== STARTING OFFSET-BASED BATCH PROCESSING WITH INCREMENTAL COMBINATION ===\n")
  cat("Shapefile path:", shp_path, "\n")
  cat("Target AINs count:", length(target_ains), "\n")
  cat("Chunk size:", chunk_size, "\n\n")
  
  file_name <- gsub(".shp", "",basename(shp_path))
  # Convert target AINs to character for consistent matching
  target_ains <- as.character(target_ains)
  
  # Get total feature count
  total_count <- st_read(shp_path, query=paste0("SELECT COUNT(*) as count FROM ", file_name))$count[1]
  cat("Total features in shapefile:", total_count, "\n")
  
  # Calculate number of chunks
  num_chunks <- ceiling(total_count / chunk_size)
  cat("Processing in", num_chunks, "chunks\n\n")
  
  # Initialize for incremental combination
  final_data <- NULL
  temp_results <- list()
  temp_counter <- 0
  total_matches <- 0
  offset <- 0
  chunk_num <- 1
  
  while (offset < total_count) {
    cat("Processing chunk", chunk_num, "of", num_chunks, 
        "(offset", offset, ")...")
    
    tryCatch({
      # SQL query with LIMIT and OFFSET (scientific notation now disabled)
      sql_query <- paste0("SELECT * FROM ", file_name," LIMIT ", chunk_size, " OFFSET ", offset)
      
      # Read chunk using LIMIT/OFFSET
      chunk_data <- st_read(shp_path, query = sql_query, quiet = TRUE)
      
      if (nrow(chunk_data) == 0) {
        cat(" No more data\n")
        break  # No more data
      }
      
      # Filter chunk against your AIN list
      chunk_data[[ain_column]] <- as.character(chunk_data[[ain_column]])
      filtered_chunk <- chunk_data %>%
        filter(!!sym(ain_column) %in% target_ains)
      
      # INCREMENTAL COMBINATION - avoid memory buildup
      if (nrow(filtered_chunk) > 0) {
        temp_counter <- temp_counter + 1
        temp_results[[temp_counter]] <- filtered_chunk
        total_matches <- total_matches + nrow(filtered_chunk)
        
        cat(" Found", nrow(filtered_chunk), "matches (total:", total_matches, ")")
        
        # Combine every 10 chunks to manage memory
        if (temp_counter >= 10) {
          cat(" [Combining temp results...]")
          temp_combined <- do.call(rbind, temp_results)
          
          if (is.null(final_data)) {
            final_data <- temp_combined
          } else {
            final_data <- rbind(final_data, temp_combined)
          }
          
          # Reset temp storage
          temp_results <- list()
          temp_counter <- 0
          rm(temp_combined)
          gc(verbose = FALSE)
        }
      } else {
        cat(" No matches")
      }
      
      cat("\n")
      
      # Memory cleanup
      rm(chunk_data)
      if (exists("filtered_chunk")) rm(filtered_chunk)
      gc(verbose = FALSE)
      
    }, error = function(e) {
      cat(" ERROR:", e$message, "\n")
      cat("  Problematic offset:", offset, "\n")
    })
    
    offset <- offset + chunk_size
    chunk_num <- chunk_num + 1
    
    # Progress summary every 25 chunks
    if (chunk_num %% 25 == 0) {
      cat("\n--- Progress Summary ---\n")
      cat("Processed", chunk_num - 1, "chunks of", num_chunks, "\n")
      cat("Total matches found so far:", total_matches, "\n")
      invisible(gc())
      cat("Memory cleaned. Continuing...\n\n")
    }
  }
  
  # Handle any remaining temp results
  if (length(temp_results) > 0) {
    cat("Combining final temp results...\n")
    temp_combined <- do.call(rbind, temp_results)
    if (is.null(final_data)) {
      final_data <- temp_combined
    } else {
      final_data <- rbind(final_data, temp_combined)
    }
    rm(temp_combined, temp_results)
    gc()
  }
  
  # Final summary
  cat("\n=== PROCESSING COMPLETE ===\n")
  cat("Total chunks processed:", chunk_num - 1, "\n")
  cat("Total matches found:", total_matches, "\n")
  
  if (!is.null(final_data) && nrow(final_data) > 0) {
    cat("✓ Final dataset:", nrow(final_data), "features,", ncol(final_data), "columns\n")
    return(final_data)
  } else {
    cat("No matches found\n")
    return(NULL)
  }
}

# Convert your AINs to a vector for filtering
target_ains_vector <- ains$ain

# # Filter assessor parcels that match Altadena/Pasadena/Sierra Madre AINs
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

# ##### Export Sept 2025 data #####
# # shp file
# export_shpfile(con=con, df=filtered_parcels, schema="data", table_name="assessor_parcels_sept2025", srid = "", geometry_type = "", geometry_column = "geometry")
# # csv
# table_name <- "assessor_data_sept2025"
# schema <- "data"
# indicator <- "to add"
# source <- "Los Angeles County Assessor"
# qa_filepath <- "to add"
# table_comment <- paste0(indicator, source)
# dbWriteTable(con, Id(schema, table_name), ains,
#              overwrite = FALSE, row.names = FALSE)
# 
# # Comment on table and columns
# column_names <- colnames(jan_ains) 
# column_comments <- c(
#   "to add",
# )
# 
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


##### Compare to Jan 2025 data #####
# Jan CSVs to batch process
jan_csv_1 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 1.csv"
jan_csv_2 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 2.csv"
jan_csv_3 <- "D:/temp_extract/Assessor Data/Jan 2025 DS04 Part 3.csv"

jan_1_results <- batch_process_assessor_data(
  csv_file=jan_csv_1,
  target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE"),
  chunk_size = 10000,
  debug_cities=TRUE)

jan_2_results <- batch_process_assessor_data(
  csv_file=jan_csv_2,
  target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE"),
  chunk_size = 10000,
  debug_cities=TRUE)

jan_3_results <- batch_process_assessor_data(
  csv_file=jan_csv_3,
  target_cities = c("ALTADENA", "PASADENA", "SIERRA MADRE"),
  chunk_size = 10000,
  debug_cities=TRUE)

# Combine results 
all_results <- rbind(jan_1_results, jan_2_results, jan_3_results)

# remove results tables to save space
rm(jan_1_results)
rm(jan_2_results)
rm(jan_3_results)
gc()

##### Do an initial review of results (e.g., ensure unique IDs, note data quality issues, etc.)
# first column name has weird symbols ("ï»¿AIN"), bad practice of spaces in column names throughout
colnames(all_results)
colnames(all_results)[1] <- "AIN"
colnames(all_results) <- gsub(" ", "_", tolower(colnames(all_results)))
colnames(all_results)

# unique AIN for each row
length(unique(all_results$ain)) # 66096

# frequency table of situs 'city_state' field
jan_city_results <- as.data.frame(table(all_results$city_state, useNA = "ifany"))

# # export results to csv (keeping all columns for QA)
# write.csv(all_results,
#           file="W:/Project/RDA Team/Altadena Recovery and Rebuild/Data/Assessor Data Prepped/filtered_ain_jan_2025.csv",
#           row.names=FALSE,
#           fileEncoding = "UTF-8")


##### Filter parcel shpfile #####
# read in results and keep only AIN
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

##### Export Jan 2025 data #####
# # Filtered shp file
# export_shpfile(con=con, df=filtered_parcels, schema="data", table_name="assessor_parcels_jan2025", srid = "", geometry_type = "", geometry_column = "geometry")
# 
# # Filtered csv file
# table_name <- "assessor_data_jan2025"
# schema <- "data"
# indicator <- "to add"
# source <- "Los Angeles County Assessor"
# qa_filepath <- "to add"
# table_comment <- paste0(indicator, source)
# dbWriteTable(con, Id(schema, table_name), jan_ains,
#              overwrite = FALSE, row.names = FALSE)
# 
# # Comment on table and columns
# column_names <- colnames(jan_ains) 
# column_comments <- c(
#   "to add",
# )
# 
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

##### Compare Sept to Jan AINS and parcels #####
# AINs
sept_only_ains <- anti_join(ains, jan_ains, by="ain") %>%
  select(ain) 

jan_only_ains <- anti_join(jan_ains, ains, by="ain") %>%
  select(ain) 

all_ains <- rbind(ains, jan_ains) %>%
  select(ain) %>%
  distinct() %>%
  mutate(source_data = 
           case_when(ain %in% sept_only_ains$ain ~ "sept only",
                     ain %in% jan_only_ains$ain ~ "jan only",
                     .default = "both"))

# Parcels
sept_parcels <- st_read(con, query='SELECT "AIN" FROM data.assessor_parcels_sept2025') %>%
  st_drop_geometry()

jan_parcels <- st_read(con, query='SELECT "AIN" FROM data.assessor_parcels_jan2025') %>%
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

