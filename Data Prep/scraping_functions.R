library(rvest)
library(chromote)


# Alternative to RSelenium - much simpler and more reliable
# test_chromote() confirms we can navigate to a simple, easily accessible page like google.com
test_chromote <- function() {
  tryCatch({
    # Start Chrome session
    b <- ChromoteSession$new()
    
    # Navigate to Google
    b$Page$navigate("https://www.google.com")
    b$Page$loadEventFired()
    
    # Get title
    result <- b$Runtime$evaluate("document.title")
    title <- result$result$value
    
    message(paste("✓ Successfully loaded page with title:", title))
    
    # Close session
    b$close()
    
    return(TRUE)
  }, error = function(e) {
    message(paste("✗ Error:", e$message))
    return(FALSE)
  })
}


##### General permit scraping functions #####
# The website is a single page application (SPA) will try a longer wait time
wait_for_spa_load <- function(url, max_wait = 20) {
  status <- "error"  # Default to error
  page_source <- NULL
  
  tryCatch({
    b <- ChromoteSession$new()
    b$Page$navigate(url)
    b$Page$loadEventFired()
    
    message("Waiting for SPA to fully load...")
    
    start_time <- Sys.time()
    while(difftime(Sys.time(), start_time, units = "secs") < max_wait) {
      
      elapsed_time <- difftime(Sys.time(), start_time, units = "secs")
      
      # Check if the moduleResultMessage container is visible (page loaded indicator)
      module_loaded <- tryCatch({
        b$Runtime$evaluate('document.querySelector("#moduleResultMessage[aria-hidden=\\"false\\"]") !== null')$result$value
      }, error = function(e) {
        message("Warning: Could not check module loaded status")
        return(FALSE)
      })
      
      message(paste("Waiting for results module... (", round(elapsed_time, 1), "s elapsed)"))
      
      if(module_loaded) {
        message("✓ Results module loaded!")
        
        # Now check for actual results
        results_count <- tryCatch({
          b$Runtime$evaluate('document.querySelectorAll("div[name=\\"label-SearchResult\\"]").length')$result$value
        }, error = function(e) {
          message("Warning: Could not check results count")
          return(0)
        })
        
        message(paste("Results found:", results_count))
        
        if(results_count > 0) {
          message("✓ Search results loaded!")
        } else {
          message("✓ Page loaded - No results found")
        }
        
        status <- "success"
        break
      }
      
      Sys.sleep(2)
    }
    
    # If we exited loop without confirming page load, mark as timeout
    if(status != "success") {
      message("⚠ Timeout: Results module did not load within wait period")
      status <- "timeout"
    }
    
    # Get final page state
    page_source <- tryCatch({
      b$Runtime$evaluate("document.documentElement.outerHTML")$result$value
    }, error = function(e) {
      message("Error getting page source")
      return("")
    })
    
    b$close()
    
  }, error = function(e) {
    message(paste("✗ Error during page load:", e$message))
    status <- "error"
    page_source <- ""
  })
  
  return(list(html = page_source, status = status))
}

# Helper Function to extract required permit data from the structured div layout of LAC Portal search result
extract_permit_data_general <- function(html_content, response_status = "success", ain = NA, retried = FALSE) {
  
  if(is.null(html_content) || html_content == "" || nchar(html_content) < 50) {
    message("Invalid or empty HTML content received")
    
    permit_df <- data.frame(
      record_id = NA,
      permit_number = NA,
      permit_href = NA,
      applied_date = NA,
      type = NA,
      issued_date = NA,
      project_name = NA,
      expiration_date = NA,
      status = NA,
      finalized_date = NA,
      main_parcel = NA,
      address = NA,
      description = NA,
      response_status = response_status,
      ain = ain,
      retried = retried,
      stringsAsFactors = FALSE
    )
    
    return(permit_df)
  }
  
  # Now safe to parse HTML
  page <- read_html(html_content)
  
  # Find all search result containers
  result_containers <- page %>% html_nodes('div[name="label-SearchResult"]')
  
  all_permits <- data.frame()
  
  if (length(result_containers)==0) {
    message("This address has no associated permits")
    
    # Convert to data frame row with NA
    permit_df <- data.frame(
      record_id = NA,
      permit_number = NA,
      permit_href = NA,
      applied_date = NA,
      type = NA,
      issued_date = NA,
      project_name = NA,
      expiration_date = NA,
      status = NA,
      finalized_date = NA,
      main_parcel = NA,
      address = NA,
      description = NA,
      response_status = response_status,
      ain = ain,
      retried = retried,
      stringsAsFactors = FALSE
    )
    
    all_permits <- bind_rows(all_permits, permit_df)
    
  } else {
    
    for(i in 1:length(result_containers)) {
      container <- result_containers[i]
      
      # Define specific extraction for each field
      permit_data <- list(
        permit_number = container %>% html_node('div[name="label-CaseNumber"] a') %>% html_text(trim = TRUE),
        permit_href = container %>% html_node('div[name="label-CaseNumber"] a') %>% html_attr("href"),
        applied_date = container %>% html_node('div[name="label-ApplyDate"] span.margin-md-left') %>% html_text(trim = TRUE),
        type = container %>% html_node('div[name="label-CaseType"] tyler-highlight span') %>% html_text(trim = TRUE),
        issued_date = container %>% html_node('div[name="label-IssuedDate"] span.margin-md-left') %>% html_text(trim = TRUE),
        project_name = container %>% html_node('div[name="label-Project"] tyler-highlight span') %>% html_text(trim = TRUE),
        expiration_date = container %>% html_node('div[name="label-ExpiredDate"] span.margin-md-left') %>% html_text(trim = TRUE),
        status = container %>% html_node('div[name="label-Status"] tyler-highlight span') %>% html_text(trim = TRUE),
        finalized_date = container %>% html_node('div[name="label-FinalizedDate"] span.margin-md-left') %>% html_text(trim = TRUE),
        main_parcel = container %>% html_node('div[name="label-MainParcel"] tyler-highlight span') %>% html_text(trim = TRUE),
        address = container %>% html_node('div[name="label-Address"] tyler-highlight span') %>% html_text(trim = TRUE),
        description = container %>% html_node('div[name="label-Description"] tyler-highlight span') %>% html_text(trim = TRUE)
      )
      
      # Convert to data frame row
      permit_df <- data.frame(
        record_id = i,
        permit_number = ifelse(is.na(permit_data$permit_number), NA, permit_data$permit_number),
        permit_href = ifelse(is.na(permit_data$permit_href), NA, permit_data$permit_href),
        applied_date = ifelse(is.na(permit_data$applied_date), NA, permit_data$applied_date),
        type = ifelse(is.na(permit_data$type), NA, permit_data$type),
        issued_date = ifelse(is.na(permit_data$issued_date), NA, permit_data$issued_date),
        project_name = ifelse(is.na(permit_data$project_name), NA, permit_data$project_name),
        expiration_date = ifelse(is.na(permit_data$expiration_date), NA, permit_data$expiration_date),
        status = ifelse(is.na(permit_data$status), NA, permit_data$status),
        finalized_date = ifelse(is.na(permit_data$finalized_date), NA, permit_data$finalized_date),
        main_parcel = ifelse(is.na(permit_data$main_parcel), NA, permit_data$main_parcel),
        address = ifelse(is.na(permit_data$address), NA, permit_data$address),
        description = ifelse(is.na(permit_data$description), NA, permit_data$description),
        response_status = response_status,
        ain = ain,
        retried = retried,
        stringsAsFactors = FALSE
      )
      
      all_permits <- bind_rows(all_permits, permit_df)
    }
  }
  
  return(all_permits)
}

# Function to receive a portal url (configured to start a search for permits based on provided address)
# and return a data frame of permits
# Now includes automatic retry logic and tracking
scrape_permits_chromote <- function(url, ain = NA, wait_time = 30, max_retries = 1, retry_wait_time = 60) {
  message(paste("Scraping:", url))
  
  # First attempt
  result <- wait_for_spa_load(url, max_wait = wait_time)
  
  # Check if retry is needed
  is_retry <- FALSE
  if(result$status %in% c("timeout", "error") && max_retries > 0) {
    message(paste("⚠ First attempt failed with status:", result$status))
    message(paste("🔄 Retrying with longer wait time (", retry_wait_time, "seconds)..."))
    
    Sys.sleep(5)  # Brief pause before retry
    
    # Retry with longer wait time
    result <- wait_for_spa_load(url, max_wait = retry_wait_time)
    is_retry <- TRUE
    
    if(result$status == "success") {
      message("✓ Retry successful!")
    } else {
      message(paste("✗ Retry also failed with status:", result$status))
    }
  }
  
  # Use custom function to get general data fields, passing all tracking info
  permits <- extract_permit_data_general(html_content = result$html, 
                                         response_status = result$status,
                                         ain = ain,
                                         retried = is_retry)
  
  return(permits)
}



##### Detailed permit scraping functions #####
wait_for_permit_detail_load <- function(url, max_wait = 20) {
  status <- "error"  # Default to error
  page_source <- NULL
  
  tryCatch({
    b <- ChromoteSession$new()
    b$Page$navigate(url)
    b$Page$loadEventFired()
    
    message("Waiting for permit detail page to fully load...")
    
    start_time <- Sys.time()
    while(difftime(Sys.time(), start_time, units = "secs") < max_wait) {
      
      elapsed_time <- difftime(Sys.time(), start_time, units = "secs")
      
      # Check if the permit number container has actual content
      permit_loaded <- tryCatch({
        b$Runtime$evaluate('
          var elem = document.querySelector("#focusText span.ng-binding");
          elem !== null && elem.textContent.trim().length > 0
        ')$result$value
      }, error = function(e) {
        message("Warning: Could not check permit detail loaded status")
        return(FALSE)
      })
      
      message(paste("Waiting for permit detail... (", round(elapsed_time, 1), "s elapsed)"))
      
      if(permit_loaded) {
        message("✓ Permit detail page loaded!")
        
        # Give Angular time to finish rendering
        Sys.sleep(2)
        
        status <- "success"
        break
      }
      
      Sys.sleep(2)
    }
    
    # If we exited loop without confirming page load, mark as timeout
    if(status != "success") {
      message("⚠ Timeout: Permit detail did not load within wait period")
      status <- "timeout"
    }
    
    # Get final page state
    page_source <- tryCatch({
      b$Runtime$evaluate("document.documentElement.outerHTML")$result$value
    }, error = function(e) {
      message("Error getting page source")
      return("")
    })
    
    b$close()
    
  }, error = function(e) {
    message(paste("✗ Error during page load:", e$message))
    status <- "error"
    page_source <- ""
  })
  
  # ALWAYS return a list
  return(list(html = page_source, status = status))
}


# scrape detailed permit data
extract_permit_data_detailed <- function(html_content_main, 
                                         html_content_locations = NULL, 
                                         response_status = "success", 
                                         permit_number = NA,
                                         ain=NA,
                                         retried = FALSE) {
  
  # Initialize empty return structure
  empty_details <- data.frame(
    permit_number = permit_number, ain=ain,
    type = NA, status = NA, project_name = NA, project_name_href = NA,
    applied_date = NA, issued_date = NA, district = NA, assigned_to = NA,
    expire_date = NA, valuation = NA, finalized_date = NA, description = NA,
    completed_percent = NA, in_progress_percent = NA, not_started_percent = NA,
    address = NA, main_address = NA, parcel_number = NA, main_parcel = NA,
    response_status = response_status, retried = retried,
    stringsAsFactors = FALSE
  )
  
  empty_workflow <- data.frame(
    permit_number = character(0),
    ain=character(0),
    workflow_item = character(0),
    status = character(0),
    status_date = character(0),
    stringsAsFactors = FALSE
  )
  
  # Check for invalid HTML
  if(is.null(html_content_main) || html_content_main == "" || nchar(html_content_main) < 50) {
    message("Invalid or empty HTML content received for main page")
    return(list(permit_details = empty_details, workflow = empty_workflow))
  }
  
  # Parse main HTML
  page_main <- tryCatch({
    read_html(html_content_main)
  }, error = function(e) {
    message("Error parsing main HTML: ", e$message)
    return(NULL)
  })
  
  if(is.null(page_main)) {
    return(list(permit_details = empty_details, workflow = empty_workflow))
  }
  
  # ===== SECTION 1: Extract Permit Details =====
  extract_safe <- function(node, selector, attr = NULL) {
    tryCatch({
      element <- node %>% html_node(selector)
      if(is.na(element)) return(NA)
      if(!is.null(attr)) {
        return(element %>% html_attr(attr))
      } else {
        return(element %>% html_text(trim = TRUE))
      }
    }, error = function(e) {
      return(NA)
    })
  }
  
  type <- extract_safe(page_main, '#label-PermitDetail-Type p.form-control-static')
  status <- extract_safe(page_main, '#label-PermitDetail-Status p.form-control-static')
  project_name <- extract_safe(page_main, '#label-PermitDetail-ProjectName a')
  project_name_href <- extract_safe(page_main, '#label-PermitDetail-ProjectName a', 'href')
  applied_date <- extract_safe(page_main, '#label-PermitDetail-ApplicationDate p.form-control-static')
  issued_date <- extract_safe(page_main, '#label-PermitDetail-IssuedDate p.form-control-static')
  district <- extract_safe(page_main, '#label-PermitDetail-District p.form-control-static')
  assigned_to <- extract_safe(page_main, '#label-PermitDetail-AssignedTo a')
  expire_date <- extract_safe(page_main, '#label-PermitDetail-ExpirationDate p.form-control-static')
  valuation <- extract_safe(page_main, '#label-PermitDetail-Valuation p.form-control-static')
  finalized_date <- extract_safe(page_main, '#label-PermitDetail-FinalizedDate p.form-control-static')
  description <- extract_safe(page_main, '#label-PermitDetail-Description')
  
  # ===== SECTION 2: Extract Progress Chart Data =====
  completed_pct <- NA
  in_progress_pct <- NA
  not_started_pct <- NA
  
  tryCatch({
    chart_paths <- page_main %>% html_nodes('#Donut-chart-render path[aria-label]')
    if(length(chart_paths) >= 3) {
      labels <- chart_paths %>% html_attr('aria-label')
      
      # Extract percentages from aria-labels
      for(label in labels) {
        if(grepl("Completed", label)) {
          completed_pct <- stringr::str_extract(label, "\\d+") 
        } else if(grepl("Active", label)) {
          in_progress_pct <- stringr::str_extract(label, "\\d+")
        } else if(grepl("Remaining", label)) {
          not_started_pct <- stringr::str_extract(label, "\\d+")
        }
      }
    }
  }, error = function(e) {
    message("Could not extract progress chart data: ", e$message)
  })
  
  # ===== SECTION 3: Extract Workflow Items =====
  workflow_df <- empty_workflow
  
  tryCatch({
    workflow_divs <- page_main %>% html_nodes('div[ng-repeat="activity in vm.workflowActivities"]')
    
    if(length(workflow_divs) > 0) {
      workflow_list <- list()
      
      for(i in 1:length(workflow_divs)) {
        div <- workflow_divs[i]
        
        # Check icon class to determine if active
        icon_class <- div %>% html_node('i') %>% html_attr('class')
        
        if(is.na(icon_class) || grepl('wf-activity-NotStarted', icon_class)) {
          next  # Skip not started items
        }
        
        # Extract workflow item name
        workflow_item <- div %>% 
          html_node('span[ng-class*="vm.getActivityCssClass"]') %>% 
          html_text(trim = TRUE)
        
        # Extract status text
        status_span <- div %>% html_node('span.wf-summaryLabels')
        status_text <- if(!is.na(status_span)) {
          status_span %>% html_text(trim = TRUE)
        } else {
          ""
        }
        
        # Determine status (check "Not Passed" before "Passed")
        item_status <- NA
        if(grepl("Not Passed", status_text)) {
          item_status <- "Not Passed"
        } else if(grepl("Partial Pass", status_text)) {
          item_status <- "Partial Pass"
        } else if(grepl("Passed", status_text)) {
          item_status <- "Passed"
        } else if(grepl("\\bPass\\b", status_text)) {
          # Match standalone "Pass" using word boundary
          item_status <- "Pass"
        } else if(grepl("Started", status_text)) {
          item_status <- "Started"
        } else if(grepl("Scheduled for", status_text)) {
          item_status <- "Scheduled"
        }
        
        # Extract date
        item_date <- NA
        
        if(!is.na(status_span)) {
          # Get all text from the status span
          full_status_text <- status_span %>% html_text(trim = TRUE)
          
          # Try to extract date from the ng-binding span first (for Passed/Not Passed)
          date_span <- status_span %>% html_node('span.ng-binding')
          if(!is.na(date_span)) {
            date_text <- date_span %>% html_text(trim = TRUE)
            # Remove the colon and trim
            date_text <- gsub("^:\\s*", "", date_text)
            date_text <- trimws(date_text)
            if(nchar(date_text) > 0) {
              item_date <- date_text
            }
          }
        }
        
        # If no date yet, look for scheduled date in ALL wf-summaryLabels spans in this div
        if(is.na(item_date) || item_date == "") {
          all_status_spans <- div %>% html_nodes('span.wf-summaryLabels')
          
          for(span in all_status_spans) {
            span_text <- span %>% html_text(trim = TRUE)
            if(grepl("Scheduled for", span_text)) {
              # Extract the date after "Scheduled for"
              date_match <- stringr::str_extract(span_text, "\\d{2}/\\d{2}/\\d{4}")
              if(!is.na(date_match)) {
                item_date <- date_match
                break
              }
            }
          }
        }
        
        # Add to list if we have a workflow item name
        if(!is.na(workflow_item) && workflow_item != "") {
          workflow_list[[length(workflow_list) + 1]] <- data.frame(
            permit_number = permit_number,
            ain=ain,
            workflow_item = workflow_item,
            status = ifelse(is.na(item_status), "", item_status),
            status_date = ifelse(is.na(item_date), "", item_date),
            stringsAsFactors = FALSE
          )
        }
      }
      
      # Combine all workflow items
      if(length(workflow_list) > 0) {
        workflow_df <- bind_rows(workflow_list)
      }
    }
  }, error = function(e) {
    message("Error extracting workflow data: ", e$message)
  })
  
  # ===== SECTION 4: Extract Location Data =====
  address <- NA
  main_address <- NA
  parcel_number <- NA
  main_parcel <- NA
  
  if(!is.null(html_content_locations) && html_content_locations != "") {
    tryCatch({
      page_locations <- read_html(html_content_locations)
      
      # Extract address
      address <- page_locations %>% 
        html_node('p[id^="Address_State_Info"]') %>% 
        html_text(trim = TRUE)
      
      # Check if main address
      main_address_input <- page_locations %>% 
        html_node('input[id^="chk_address_main"]')
      main_address <- !is.na(main_address_input) && 
        !is.na(html_attr(main_address_input, 'checked'))
      
      # Extract parcel number
      parcel_number <- page_locations %>% 
        html_node('div[id^="Parcel_Number"] p') %>% 
        html_text(trim = TRUE)
      
      # Check if main parcel
      main_parcel_input <- page_locations %>% 
        html_node('input[id^="chk_parcel_main"]')
      main_parcel <- !is.na(main_parcel_input) && 
        !is.na(html_attr(main_parcel_input, 'checked'))
      
    }, error = function(e) {
      message("Error extracting location data: ", e$message)
    })
  }
  
  # ===== Build permit_details data frame =====
  permit_details <- data.frame(
    permit_number = permit_number,
    ain = ain,
    type = ifelse(is.na(type), NA, type),
    status = ifelse(is.na(status), NA, status),
    project_name = ifelse(is.na(project_name), NA, project_name),
    project_name_href = ifelse(is.na(project_name_href), NA, project_name_href),
    applied_date = ifelse(is.na(applied_date), NA, applied_date),
    issued_date = ifelse(is.na(issued_date), NA, issued_date),
    district = ifelse(is.na(district), NA, district),
    assigned_to = ifelse(is.na(assigned_to), NA, assigned_to),
    expire_date = ifelse(is.na(expire_date), NA, expire_date),
    valuation = ifelse(is.na(valuation), NA, valuation),
    finalized_date = ifelse(is.na(finalized_date), NA, finalized_date),
    description = ifelse(is.na(description), NA, description),
    completed_percent = ifelse(is.na(completed_pct), NA, completed_pct),
    in_progress_percent = ifelse(is.na(in_progress_pct), NA, in_progress_pct),
    not_started_percent = ifelse(is.na(not_started_pct), NA, not_started_pct),
    address = ifelse(is.na(address), NA, address),
    main_address = ifelse(is.na(main_address), NA, main_address),
    parcel_number = ifelse(is.na(parcel_number), NA, parcel_number),
    main_parcel = ifelse(is.na(main_parcel), NA, main_parcel),
    response_status = response_status,
    retried = retried,
    stringsAsFactors = FALSE
  )
  
  # Return list with both data frames
  return(list(
    permit_details = permit_details,
    workflow = workflow_df
  ))
}

scrape_permits_detailed <- function(url, url_suffix, ain = NA, permit_number=NA, wait_time = 30, max_retries = 1, retry_wait_time = 60) {
  message(paste("Scraping:", url))
  
  # First attempt
  result1 <- wait_for_permit_detail_load(url, max_wait = wait_time)
  Sys.sleep(2)
  result2 <- wait_for_permit_detail_load(url=paste0(url,url_suffix), max_wait = wait_time)
  
  # Check if retry is needed
  is_retry <- FALSE
  if(result1$status %in% c("timeout", "error") && max_retries > 0) {
    message(paste("⚠ First attempt failed with status:", result1$status))
    message(paste("🔄 Retrying with longer wait time (", retry_wait_time, "seconds)..."))
    
    Sys.sleep(1)  # Brief pause before retry
    
    # Retry with longer wait time
    result1 <- wait_for_permit_detail_load(url, max_wait = retry_wait_time)
    Sys.sleep(1)
    result2 <- wait_for_permit_detail_load(url=paste0(url,url_suffix), max_wait=retry_wait_time)
    is_retry <- TRUE
    
    if(result1$status == "success") {
      message("✓ Retry successful!")
    } else {
      message(paste("✗ Retry also failed with status:", result1$status))
    }
  }
  
  # Use custom function to get general data fields, passing all tracking info
  permits_detailed <- extract_permit_data_detailed(
    html_content_main = result1$html,
    response_status = result1$status,
    html_content_locations=result2$html,
    ain = ain,
    permit_number=permit_number,
    retried = is_retry)
  
  return(permits_detailed)
}

