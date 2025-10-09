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


# Helper function
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
      
      # Check for results
      results_count <- tryCatch({
        b$Runtime$evaluate('document.querySelectorAll("div[name=\\"label-SearchResult\\"]").length')$result$value
      }, error = function(e) {
        message("Warning: Could not check results count")
        return(0)
      })
      
      # Check for "No results found" message
      no_results_found <- tryCatch({
        b$Runtime$evaluate('document.body.innerText.includes("No results were found")' )$result$value
      }, error = function(e) {
        message("Warning: Could not check for no results message")
        return(FALSE)
      })
      
      message(paste("Results found:", results_count, "| No results message:", no_results_found))
      
      # Page is loaded if we have results OR see the no results message
      if(results_count > 0 || no_results_found) {
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
      message("⚠ Timeout: Page did not finish loading within wait period")
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
extract_permit_data_general <- function(html_content, response_status = "success") {
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
      expiration_date = NA,
      status = NA,
      finalized_date = NA,
      main_parcel = NA,
      address = NA,
      description = NA,
      response_status = response_status,  # Add status
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
        response_status = response_status,  # Add status to each permit
        stringsAsFactors = FALSE
      )
      
      all_permits <- bind_rows(all_permits, permit_df)
    }
  }
  
  return(all_permits)
}


# Function to receive a portal url (configured to start a search for permits based on provided address)
# and return a data frame of permits
scrape_permits_chromote <- function(url, wait_time = 15) {
  message(paste("Scraping:", url))
  
  # Parse html with rvest - now returns list with html and status
  result <- wait_for_spa_load(url, max_wait = 25)
  
  # Use custom function to get general data fields, passing status
  permits <- extract_permit_data_general(html_content = result$html, 
                                         response_status = result$status)
  
  return(permits)
}

# Not needed
# debug_scrape <- function(url) {
#   b <- ChromoteSession$new()
#   
#   # Navigate
#   b$Page$navigate(url)
#   b$Page$loadEventFired()
#   Sys.sleep(10)
#   
#   # Check what we actually got
#   current_url <- b$Runtime$evaluate("window.location.href")$result$value
#   title <- b$Runtime$evaluate("document.title")$result$value
#   body_text <- b$Runtime$evaluate("document.body.innerText.substring(0, 500)")$result$value
#   
#   message("URL: ", current_url)
#   message("Title: ", title)
#   message("Body preview: ", body_text)
#   
#   # Check for specific elements
#   search_elements <- b$Runtime$evaluate("document.querySelectorAll('div[name*=\"label-\"]').length")$result$value
#   message("Found label elements: ", search_elements)
#   
#   b$close()
# }
# 
# 
# inspect_search_form <- function() {
#   b <- ChromoteSession$new()
#   b$Page$navigate("https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st=2204%20Grand%20Oaks%20Avenue")
#   b$Page$loadEventFired()
#   Sys.sleep(5)
#   
#   # Get all input elements
#   inputs <- b$Runtime$evaluate("
#     Array.from(document.querySelectorAll('input')).map(input => ({
#       type: input.type,
#       name: input.name,
#       id: input.id,
#       placeholder: input.placeholder,
#       className: input.className
#     }))
#   ")$result$value
#   
#   # Get all buttons
#   buttons <- b$Runtime$evaluate("
#     Array.from(document.querySelectorAll('button')).map(btn => ({
#       text: btn.textContent.trim(),
#       type: btn.type,
#       className: btn.className
#     }))
#   ")$result$value
#   
#   message("Input elements found:")
#   print(inputs)
#   message("Button elements found:")
#   print(buttons)
#   
#   b$close()
# }
# # example use: inspect_search_form()
# 
