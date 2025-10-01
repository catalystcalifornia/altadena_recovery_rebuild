# Creates functions to batch filter csvs for targeted cities, and then batch
# filter shp files and return results as a dataframe.

library(dplyr)
library(data.table)
library(sf)

read_single_chunk_csv <- function(csv_file, 
                                    chunk_number = 1,
                                    chunk_size = 10000){
  cat("Starting batch processing of:", csv_file, "\n")
  cat("Chunk size:", chunk_size, "\n")
  
  # Open file connection
  con <- file(csv_file, "r")
  on.exit({
    close(con)
    gc()
  })
  
  # Read header line and parse column names
  header_line <- readLines(con, n = 1)
  col_names <- strsplit(header_line, ",")[[1]]
  rm(header_line)
  gc()
  
  results <- list()
  chunk_num <- 1
  total_filtered_rows <- 0
  all_values_found <- character(0)  # Track all unique cities
  
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
    
    chunk_num <- chunk_num + 1
    
    if (chunk_num == chunk_number) {
      return(chunk)
  }
  
  }
}


batch_filter_csv_data <- function(csv_file, 
                                        target_list, 
                                        filter_column="",
                                        chunk_size = 10000, 
                                        debug_filter = TRUE) {
  # Note the function matches the provided target list to anywhere in the filter_column
  # (e.g., Pasadena and South Pasadena will be matched)
  cat("Starting batch processing of:", csv_file, "\n")
  cat("Target values:", paste(target_list, collapse = ", "), "\n")
  cat("Chunk size:", chunk_size, "\n")
  cat("Value check mode:", debug_filter, "\n\n")
  
  # Open file connection
  con <- file(csv_file, "r")
  on.exit({
    close(con)
    gc()
  })
  
  # Read header line and parse column names
  header_line <- readLines(con, n = 1)
  col_names <- strsplit(header_line, ",")[[1]]
  rm(header_line)
  gc()
  
  cat("Column names found:", length(col_names), "columns\n")
  
  # Verify required columns
  if (!filter_column %in% col_names) {
    cat(paste("ERROR:", filter_column, "column not found!\n"))
    cat("Available columns:", paste(col_names[1:min(10, length(col_names))], collapse = ", "), "...\n")
    stop(paste("Required column", filter_column, "not found"))
  }
  
  cat(paste("✓ '",filter_column,"' column found successfully\n"))
  cat("Starting chunk processing...\n\n")
  
  results <- list()
  chunk_num <- 1
  total_filtered_rows <- 0
  all_values_found <- character(0)  # Track all unique cities
  
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
    
    # convert filter_column to character type for filtering
    chunk[[filter_column]] <- as.character(chunk[[filter_column]])
    
    # FILTER DISCOVERY: Show unique values of filter_column in first few chunks
    if (debug_filter & chunk_num %% 5 == 0) {
      unique_values <- unique(chunk[[filter_column]])
      all_values_found <- unique(c(all_values_found, unique_values))
      cat("\n  Unique values in chunk", chunk_num, ":\n")
      print(unique_values[1:min(10, length(unique_values))])
    }
    
    # Filter for target value using direct matching
    chunk[[filter_column]] <- toupper(trimws(chunk[[filter_column]]))
    target_list_upper <- toupper(as.character(target_list))
    filtered <- chunk[chunk[[filter_column]] %in% target_list_upper]
    
    
    # Clean up original chunk
    rm(chunk)
    gc()
    
    # Report results for this chunk
    if (nrow(filtered) > 0) {
      cat(" Found", nrow(filtered), "matching rows\n")
      results[[chunk_num]] <- filtered
      total_filtered_rows <- total_filtered_rows + nrow(filtered)
      
      # Show sample values found
      sample_values <- unique(filtered[[filter_column]])[1:min(3, length(unique(filtered[[filter_column]])))]
      cat("  ✓ Target values found:", paste(sample_values, collapse = ", "), "\n")
    } else {
      cat(" No matching rows found\n")
      rm(filtered)
    }
    
    chunk_num <- chunk_num + 1
    
    # Progress summary every 25 chunks + show value/filter discovery
    if (chunk_num %% 25 == 0) {
      gc(verbose = FALSE)
      cat("\n--- Progress Summary ---\n")
      cat("Processed", chunk_num - 1, "chunks so far\n")
      cat("Total matching rows found:", total_filtered_rows, "\n")
      
      if (debug_filter) {
        cat("All unique values discovered so far (", length(all_values_found), "total):\n")
        # Show rows that might contain our targets
        # Check for potential matches
        target_list_upper <- toupper(as.character(target_list))
        potential_matches <- all_values_found[toupper(all_values_found) %in% target_list_upper]
        
        if (length(potential_matches) > 0) {
          cat("  ✓ Potential target matches:", paste(potential_matches, collapse = ", "), "\n")
        } else {
          cat("  ⚠ No target values found yet. Sample of values seen:\n")
          print(all_values_found[1:min(10, length(all_values_found))])
        }
      }
      cat("Memory cleaned. Continuing...\n\n")
    }
  }
  
  # Final summary with complete value/filter discovery
  cat("\n=== PROCESSING COMPLETE ===\n")
  cat("Total chunks processed:", chunk_num - 1, "\n")
  cat("Total matching rows found:", total_filtered_rows, "\n")
  
  if (debug_filter) {
    cat("\n=== VALUE DEBUG SUMMARY ===\n")
    cat("Total unique values found:", length(all_values_found), "\n")
    
    # Check for potential matches
    target_list_upper <- toupper(as.character(target_list))
    potential_matches <- all_values_found[toupper(all_values_found) %in% target_list_upper]
    
    if (length(potential_matches) > 0) {
      cat("Values containing target names:\n")
      print(potential_matches)
    } else {
      cat("⚠ NO values found containing target names!\n")
      cat("Sample of all values in dataset:\n")
      print(all_values_found[1:min(20, length(all_values_found))])
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
    cat("No data found for the specified values\n")
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



batch_intersect_shapefile <- function(shp_path, target_geo, chunk_size = 10000, retain_cols = NULL) {
  cat("=== STARTING SPATIAL CONTAINMENT BATCH PROCESSING ===\n")
  cat("Shapefile path:", shp_path, "\n")
  cat("Target points count:", nrow(target_geo), "\n")
  cat("Chunk size:", chunk_size, "\n")
  if (!is.null(retain_cols)) {
    cat("Retaining columns from target_geo:", paste(retain_cols, collapse = ", "), "\n")
  }
  cat("\n")
  
  # Ensure target_geo is an sf object
  if (!inherits(target_geo, "sf")) {
    stop("target_geo must be an sf object")
  }
  
  # Check if geometry exists
  if (!"geometry" %in% names(target_geo)) {
    stop("target_geo must have a geometry column")
  }
  
  # Detect geometry type
  target_geom_type <- unique(as.character(st_geometry_type(target_geo)))
  cat("Target geometry type(s):", paste(target_geom_type, collapse = ", "), "\n")
  
  # Validate retain_cols
  if (!is.null(retain_cols)) {
    missing_cols <- setdiff(retain_cols, names(target_geo))
    if (length(missing_cols) > 0) {
      stop("Columns not found in target_geo: ", paste(missing_cols, collapse = ", "))
    }
  }
  
  file_name <- gsub(".shp", "", basename(shp_path))
  
  # Get total feature count
  total_count <- st_read(shp_path, query = paste0("SELECT COUNT(*) as count FROM ", file_name))$count[1]
  cat("Total features in shapefile:", total_count, "\n")
  
  # Calculate number of chunks
  num_chunks <- ceiling(total_count / chunk_size)
  cat("Processing in", num_chunks, "chunks\n\n")
  
  # Get CRS info from shapefile for consistency
  sample_geom <- st_read(shp_path, query = paste0("SELECT * FROM ", file_name, " LIMIT 1"), quiet = TRUE)
  shp_crs <- st_crs(sample_geom)$epsg
  
  # Transform points to match shapefile CRS if needed
  if (st_crs(target_geo)$epsg != shp_crs) {
    cat("Transforming points CRS to match shapefile...\n")
    target_geo <- st_transform(target_geo, shp_crs)
  }
  
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
      # SQL query with LIMIT and OFFSET
      sql_query <- paste0("SELECT * FROM ", file_name, " LIMIT ", chunk_size, " OFFSET ", offset)
      
      # Read chunk using LIMIT/OFFSET
      chunk_data <- st_read(shp_path, query = sql_query, quiet = TRUE)
      
      if (nrow(chunk_data) == 0) {
        cat(" No more data\n")
        break  # No more data
      }
      
      # Spatial filtering: check if any target points intersect with chunk polygons
      intersections <- st_intersects(chunk_data, target_geo)
      
      # Find which polygons contain at least one point
      contains_points <- lengths(intersections) > 0
      filtered_chunk <- chunk_data[contains_points, ]
      
      # ENHANCED: Add target_geo information if requested
      if (nrow(filtered_chunk) > 0 && !is.null(retain_cols)) {
        # Get intersection details for filtered polygons
        filtered_intersections <- intersections[contains_points]
        
        # For each feature in filtered chunk, get info about intersecting target_geo features
        matched_info <- lapply(seq_len(nrow(filtered_chunk)), function(i) {
          target_indices <- filtered_intersections[[i]]
          
          if (length(target_indices) == 0) {
            return(NULL)
          }
          
          # Get the matching target features (points or polygons)
          matched_targets <- target_geo[target_indices, ]
          
          # Calculate intersection areas/metrics
          # This works for both point-in-polygon and polygon-polygon intersections
          intersect_metrics <- tryCatch({
            # Perform actual intersection to get geometry
            intersection_geom <- st_intersection(
              st_geometry(filtered_chunk[i, ]), 
              st_geometry(matched_targets)
            )
            
            if (length(intersection_geom) > 0) {
              # Try to calculate area (for polygon intersections)
              area_val <- tryCatch({
                as.numeric(sum(st_area(intersection_geom)))
              }, error = function(e) NA)
              
              # Calculate percentage of chunk feature that intersects
              chunk_area <- tryCatch({
                as.numeric(st_area(filtered_chunk[i, ]))
              }, error = function(e) NA)
              
              pct_intersect <- if (!is.na(area_val) && !is.na(chunk_area) && chunk_area > 0) {
                (area_val / chunk_area) * 100
              } else {
                NA
              }
              
              list(area = area_val, pct = pct_intersect)
            } else {
              list(area = NA, pct = NA)
            }
          }, error = function(e) {
            list(area = NA, pct = NA)
          })
          
          # Create summary data frame
          result_df <- data.frame(
            n_intersecting_features = length(target_indices),
            intersect_area = intersect_metrics$area,
            intersect_pct = intersect_metrics$pct
          )
          
          # Add retained columns (collapse multiple matches into lists or take first)
          for (col in retain_cols) {
            col_values <- st_drop_geometry(matched_targets)[[col]]
            # If multiple matches, create a concatenated string
            if (length(col_values) > 1) {
              result_df[[paste0("matched_", col)]] <- paste(col_values, collapse = "; ")
            } else {
              result_df[[paste0("matched_", col)]] <- col_values
            }
          }
          
          return(result_df)
        })
        
        # Combine matched info with filtered chunk
        matched_df <- do.call(rbind, matched_info)
        filtered_chunk <- cbind(st_drop_geometry(filtered_chunk), matched_df)
        filtered_chunk <- st_sf(filtered_chunk, geometry = st_geometry(chunk_data[contains_points, ]))
      }
      
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
      rm(chunk_data, intersections, contains_points)
      if (exists("filtered_chunk")) rm(filtered_chunk)
      if (exists("matched_info")) rm(matched_info)
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
    if (!is.null(retain_cols)) {
      cat("✓ Added columns: n_intersecting_features, intersect_area, intersect_pct,", 
          paste0("matched_", retain_cols, collapse = ", "), "\n")
    }
    return(final_data)
  } else {
    cat("No matches found\n")
    return(NULL)
  }
}

# For each unmatched point, find nearest parcel
find_nearest_postgis <- function(con, unmatched_points, max_distance = 100) {
  
  # Convert points to WKT for the query
  point_wkt <- st_as_text(st_geometry(unmatched_points))
  
  results <- list()
  
  for (i in 1:nrow(unmatched_points)) {
    query <- paste0("
      SELECT 
        ain,
        ST_Distance(geom, ST_GeomFromText('", point_wkt[i], "', ", st_crs(unmatched_points)$epsg, ")) as distance
      FROM parcel_jan2025 
      WHERE ST_DWithin(geom, ST_GeomFromText('", point_wkt[i], "', ", st_crs(unmatched_points)$epsg, "), ", max_distance, ")
      ORDER BY geom <-> ST_GeomFromText('", point_wkt[i], "', ", st_crs(unmatched_points)$epsg, ")
      LIMIT 1;
    ")
    
    result <- dbGetQuery(con, query)
    if (nrow(result) > 0) {
      results[[i]] <- data.frame(
        point_index = i,
        nearest_ain = result$ain,
        distance = result$distance
      )
    }
  }
  
  do.call(rbind, results)
}



### DELETE
# batch_filter_shapefile_intersect_old <- function(shp_path, target_geo, chunk_size = 10000) {
#   cat("=== STARTING SPATIAL CONTAINMENT BATCH PROCESSING ===\n")
#   cat("Shapefile path:", shp_path, "\n")
#   cat("Target points count:", nrow(target_geo), "\n")
#   cat("Chunk size:", chunk_size, "\n\n")
#   
#   # Ensure target_geo is an sf object
#   if (!inherits(target_geo, "sf")) {
#     stop("target_geo must be an sf object with point geometries")
#   }
#   
#   # Check if points have geometry
#   if (!"geometry" %in% names(target_geo)) {
#     stop("target_geo must have a geometry column")
#   }
#   
#   file_name <- gsub(".shp", "", basename(shp_path))
#   
#   # Get total feature count
#   total_count <- st_read(shp_path, query = paste0("SELECT COUNT(*) as count FROM ", file_name))$count[1]
#   cat("Total features in shapefile:", total_count, "\n")
#   
#   # Calculate number of chunks
#   num_chunks <- ceiling(total_count / chunk_size)
#   cat("Processing in", num_chunks, "chunks\n\n")
#   
#   # Get CRS info from shapefile for consistency
#   sample_geom <- st_read(shp_path, query = paste0("SELECT * FROM ", file_name, " LIMIT 1"), quiet = TRUE)
#   shp_crs <- st_crs(sample_geom)
#   
#   # Transform points to match shapefile CRS if needed
#   if (st_crs(target_geo) != shp_crs) {
#     cat("Transforming points CRS to match shapefile...\n")
#     target_geo <- st_transform(target_geo, shp_crs)
#   }
#   
#   # Initialize for incremental combination
#   final_data <- NULL
#   temp_results <- list()
#   temp_counter <- 0
#   total_matches <- 0
#   offset <- 0
#   chunk_num <- 1
#   
#   while (offset < total_count) {
#     cat("Processing chunk", chunk_num, "of", num_chunks, 
#         "(offset", offset, ")...")
#     
#     tryCatch({
#       # SQL query with LIMIT and OFFSET
#       sql_query <- paste0("SELECT * FROM ", file_name, " LIMIT ", chunk_size, " OFFSET ", offset)
#       
#       # Read chunk using LIMIT/OFFSET
#       chunk_data <- st_read(shp_path, query = sql_query, quiet = TRUE)
#       
#       if (nrow(chunk_data) == 0) {
#         cat(" No more data\n")
#         break  # No more data
#       }
#       
#       # Spatial filtering: check if any target points intersect with chunk polygons
#       # Using st_intersects for containment check
#       intersections <- st_intersects(chunk_data, target_geo)
#       
#       # Find which polygons contain at least one point
#       contains_points <- lengths(intersections) > 0
#       filtered_chunk <- chunk_data[contains_points, ]
#       
#       # INCREMENTAL COMBINATION - avoid memory buildup
#       if (nrow(filtered_chunk) > 0) {
#         temp_counter <- temp_counter + 1
#         temp_results[[temp_counter]] <- filtered_chunk
#         total_matches <- total_matches + nrow(filtered_chunk)
#         
#         cat(" Found", nrow(filtered_chunk), "matches (total:", total_matches, ")")
#         
#         # Combine every 10 chunks to manage memory
#         if (temp_counter >= 10) {
#           cat(" [Combining temp results...]")
#           temp_combined <- do.call(rbind, temp_results)
#           
#           if (is.null(final_data)) {
#             final_data <- temp_combined
#           } else {
#             final_data <- rbind(final_data, temp_combined)
#           }
#           
#           # Reset temp storage
#           temp_results <- list()
#           temp_counter <- 0
#           rm(temp_combined)
#           gc(verbose = FALSE)
#         }
#       } else {
#         cat(" No matches")
#       }
#       
#       cat("\n")
#       
#       # Memory cleanup
#       rm(chunk_data, intersections, contains_points)
#       if (exists("filtered_chunk")) rm(filtered_chunk)
#       gc(verbose = FALSE)
#       
#     }, error = function(e) {
#       cat(" ERROR:", e$message, "\n")
#       cat("  Problematic offset:", offset, "\n")
#     })
#     
#     offset <- offset + chunk_size
#     chunk_num <- chunk_num + 1
#     
#     # Progress summary every 25 chunks
#     if (chunk_num %% 25 == 0) {
#       cat("\n--- Progress Summary ---\n")
#       cat("Processed", chunk_num - 1, "chunks of", num_chunks, "\n")
#       cat("Total matches found so far:", total_matches, "\n")
#       invisible(gc())
#       cat("Memory cleaned. Continuing...\n\n")
#     }
#   }
#   
#   # Handle any remaining temp results
#   if (length(temp_results) > 0) {
#     cat("Combining final temp results...\n")
#     temp_combined <- do.call(rbind, temp_results)
#     if (is.null(final_data)) {
#       final_data <- temp_combined
#     } else {
#       final_data <- rbind(final_data, temp_combined)
#     }
#     rm(temp_combined, temp_results)
#     gc()
#   }
#   
#   # Final summary
#   cat("\n=== PROCESSING COMPLETE ===\n")
#   cat("Total chunks processed:", chunk_num - 1, "\n")
#   cat("Total matches found:", total_matches, "\n")
#   
#   if (!is.null(final_data) && nrow(final_data) > 0) {
#     cat("✓ Final dataset:", nrow(final_data), "features,", ncol(final_data), "columns\n")
#     return(final_data)
#   } else {
#     cat("No matches found\n")
#     return(NULL)
#   }
# }

