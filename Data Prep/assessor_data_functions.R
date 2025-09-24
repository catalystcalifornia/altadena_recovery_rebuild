# Creates functions to batch filter csvs for targeted cities, and then batch
# filter shp files and return results as a dataframe.

library(dplyr)
library(data.table)
library(sf)

batch_process_assessor_data <- function(csv_file, 
                                        target_cities, 
                                        filter_column="",
                                        chunk_size = 10000, 
                                        debug_cities = TRUE) {
  # Note the function matches the provided target cities to anywhere in the 'City State' col
  # (e.g., Pasadena and South Pasadena will be matched)
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
  if (!filter_column %in% col_names) {
    cat(paste("ERROR:", filter_column, "column not found!\n"))
    cat("Available columns:", paste(col_names[1:min(10, length(col_names))], collapse = ", "), "...\n")
    stop(paste("Required column", filter_column, "not found"))
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
      unique_cities <- unique(chunk[[filter_column]])
      all_cities_found <- unique(c(all_cities_found, unique_cities))
      cat("\n  Unique cities in chunk", chunk_num, ":\n")
      print(unique_cities[1:min(10, length(unique_cities))])
    }
    
    # Filter for target cities
    city_pattern <- paste(target_cities, collapse = "|")
    filtered <- chunk[grepl(city_pattern, 
                            toupper(trimws(chunk[[filter_column]])), 
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
      sample_cities <- unique(filtered[[filter_column]])[1:min(3, length(unique(filtered[[filter_column]])))]
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


# Function to batch process shapefile and filter by AIN list
batch_filter_shapefile <- function(shp_path, target_ains, chunk_size = 10000, ain_column = "AIN") {
  cat("=== STARTING OFFSET-BASED BATCH PROCESSING ===\n")
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

