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
  b <- ChromoteSession$new()
  b$Page$navigate(url)
  b$Page$loadEventFired()
  
  message("Waiting for SPA to fully load...")
  
  start_time <- Sys.time()
  while(difftime(Sys.time(), start_time, units = "secs") < max_wait) {
    # Use your suggested selector
    results_count <- b$Runtime$evaluate('document.querySelectorAll("div[name=\\"label-SearchResult\\"]").length')$result$value
    
    # Check if search is still loading
    loading_indicators <- b$Runtime$evaluate('document.querySelectorAll(".loading, .spinner, [ng-show*=loading]").length')$result$value
    
    # Check search button state
    search_button <- b$Runtime$evaluate('document.querySelector("button[type=\\"submit\\"], button:contains(\\"Search\\")") ? "found" : "not found"')$result$value
    
    # Form elements for debugging
    form_loaded <- b$Runtime$evaluate("document.querySelectorAll('input, button').length")$result$value
    
    message(paste("Results found:", results_count, "Form elements:", form_loaded, "Loading indicators:", loading_indicators, "Search button:", search_button))
    
    if(results_count > 0) {
      message("✓ Search results loaded!")
      break
    }
    
    # If form is loaded but no results after 15 seconds, try clicking search
    if(form_loaded > 10 && difftime(Sys.time(), start_time, units = "secs") > 15) {
      message("Attempting to trigger search...")
      b$Runtime$evaluate('
        var searchBtn = document.querySelector("button[type=\\"submit\\"], input[type=\\"submit\\"]");
        if(searchBtn) searchBtn.click();
      ')
      Sys.sleep(5)
    }
    
    Sys.sleep(2)
  }
  
  # Get final page state
  page_source <- b$Runtime$evaluate("document.documentElement.outerHTML")$result$value
  b$close()
  
  return(page_source)
}


# Helper Function to extract required permit data from the structured div layout of LAC Portal search result
extract_permit_data_general <- function(html_content) {
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
  
  # Parse html with rvest 
  page <- wait_for_spa_load(url, max_wait = 25)
  
  # Use custom function to get general data fields
  permits <- extract_permit_data_general(html_content=page)
  
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
