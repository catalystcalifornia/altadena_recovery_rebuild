# Objective: Scrape two different sites for information on building permits
# for thousands of addresses/parcels, and assess possibility of regularly 
# scraping these sites.

library(dplyr) 
library(rvest) # to scrape 
# library(httr2) # to scrape metadata table from cde website and avoid being flagged as bot
# library(RSelenium) # another method to scrape with anti-bot detection, very frustrating process of matching chrome and chromedriver versions
# library(netstat)
# library(wdman) # outdated R package - can't support new chrome versions
library(chromote)

# This approach is much simpler and more reliable
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

quick_test_chromote <- function() {
  message("=== QUICK CHROMOTE TEST ===")
  message("Step 1: Testing setup...")
  
  if(test_chromote()) {
    message("Step 2: Chromote is working! You can now use:")
    message("result <- scrape_permits_chromote('your_url_here')")
    return(TRUE)
  } else {
    message("Setup failed. Try:")
    message("1. install.packages('chromote')")
    message("2. Make sure Chrome browser is installed")
    return(FALSE)
  }
}

scrape_permits_chromote <- function(url, wait_time = 15) {
  message(paste("Scraping:", url))
  
  if(!require(chromote, quietly = TRUE)) {
    stop("Please install chromote first: install.packages('chromote')")
  }
  
  b <- NULL
  
  tryCatch({
    # Start Chrome session
    message("Starting Chrome session...")
    b <- ChromoteSession$new()
    
    # Navigate to URL
    message("Loading permit page...")
    b$Page$navigate(url)
    
    # Wait for Cloudflare
    message(paste("Waiting", wait_time, "seconds for Cloudflare..."))
    Sys.sleep(wait_time)
    
    # Check current URL and title
    current_url <- b$Page$getNavigationHistory()$entries[[1]]$url
    title_result <- b$Runtime$evaluate("document.title")
    page_title <- title_result$result$value
    
    message(paste("Current URL:", current_url))
    message(paste("Page title:", page_title))
    
    if(grepl("cloudflare|challenge", page_title, ignore.case = TRUE)) {
      message("Still on Cloudflare page, waiting more...")
      Sys.sleep(10)
    }
    
    # Get page source
    message("Getting page content...")
    page_source_result <- b$Runtime$evaluate("document.documentElement.outerHTML")
    page_source <- page_source_result$result$value
    
    # Parse with rvest (keep your existing parsing logic!)
    page <- read_html(page_source)
    
    # Use your existing extract_permit_data function
    permits <- extract_permit_data(page)
    
    message("✓ Scraping completed successfully!")
    return(permits)
    
  }, error = function(e) {
    message(paste("✗ Error:", e$message))
    return(NULL)
    
  }, finally = {
    # Cleanup
    if(!is.null(b)) {
      try(b$close(), silent = TRUE)
    }
    message("Browser closed.")
  })
}

# Function to extract permit data from the structured div layout
extract_permit_data_specific <- function(html_content) {
  page <- read_html(html_content)
  
  # Find all search result containers
  result_containers <- page %>% html_nodes('div[name="label-SearchResult"]')
  
  all_permits <- data.frame()
  
  for(i in 1:length(result_containers)) {
    container <- result_containers[i]
    
    # Define specific extraction for each field
    permit_data <- list(
      permit_number = container %>% html_node('div[name="label-CaseNumber"] a') %>% html_text(trim = TRUE),
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
      permit_number = ifelse(is.na(permit_data$permit_number), "", permit_data$permit_number),
      applied_date = ifelse(is.na(permit_data$applied_date), "", permit_data$applied_date),
      type = ifelse(is.na(permit_data$type), "", permit_data$type),
      issued_date = ifelse(is.na(permit_data$issued_date), "", permit_data$issued_date),
      project_name = ifelse(is.na(permit_data$project_name), "", permit_data$project_name),
      expiration_date = ifelse(is.na(permit_data$expiration_date), "", permit_data$expiration_date),
      status = ifelse(is.na(permit_data$status), "", permit_data$status),
      finalized_date = ifelse(is.na(permit_data$finalized_date), "", permit_data$finalized_date),
      main_parcel = ifelse(is.na(permit_data$main_parcel), "", permit_data$main_parcel),
      address = ifelse(is.na(permit_data$address), "", permit_data$address),
      description = ifelse(is.na(permit_data$description), "", permit_data$description),
      stringsAsFactors = FALSE
    )
    
    all_permits <- rbind(all_permits, permit_df)
  }
  
  return(all_permits)
}

debug_scrape <- function(url) {
  b <- ChromoteSession$new()
  
  # Navigate
  b$Page$navigate(url)
  b$Page$loadEventFired()
  Sys.sleep(10)
  
  # Check what we actually got
  current_url <- b$Runtime$evaluate("window.location.href")$result$value
  title <- b$Runtime$evaluate("document.title")$result$value
  body_text <- b$Runtime$evaluate("document.body.innerText.substring(0, 500)")$result$value
  
  message("URL: ", current_url)
  message("Title: ", title)
  message("Body preview: ", body_text)
  
  # Check for specific elements
  search_elements <- b$Runtime$evaluate("document.querySelectorAll('div[name*=\"label-\"]').length")$result$value
  message("Found label elements: ", search_elements)
  
  b$close()
}

#### Checking if workflow works
# Set up
# Create two test dataframes to run through each website. Using the same two addresses
# which will return some positive number of permits for one address and zero for
# the other (and vice versa depending on which permit site is used)
unincorporated_parcels <- data.frame(address=c("2204 Grand Oaks Avenue", "1630 Carriage House Rd"),
                                     expected_permits=c(5, 0)) 
pasadena_parcels <- data.frame(address=c("2204 Grand Oaks Avenue", "1630 Carriage House Rd"),
                               expected_permits=c(0, 19))
# Website #1: EPIC LA - LA County building permits (unincorporated cities only)
lac_permits_url <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st="
# Website #2:https://mypermits.cityofpasadena.net/EnerGov_Prod/SelfService#/search?m=1&fm=1&ps=10&pn=1&em=true&st=[example:1630%20Carriage%20House]
# Details: Pasadena building permits 
pasadena_permits_url <- "https://mypermits.cityofpasadena.net/EnerGov_Prod/SelfService#/search?m=2&ps=10&pn=1&em=true&st="


# === UPDATED USAGE ===
# 1. First run: 
quick_test_chromote()
# 1A. If 2 failed, run:
debug_scrape(lac_permits_url) 

# Based on debug_scrape should try giving a full url (with search terms)
for(row_ in 1:nrow(unincorporated_parcels)) {
  url <- paste0(lac_permits_url, gsub(" ", "%20", unincorporated_parcels[row_, "address"]))
  message(paste("Testing URL:", url))
  debug_scrape(url)
  Sys.sleep(2)
}

# I'm getting 0 for both results (expecintg 5 for Grand Oaks address)

inspect_search_form <- function() {
  b <- ChromoteSession$new()
  b$Page$navigate("https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st=2204%20Grand%20Oaks%20Avenue")
  b$Page$loadEventFired()
  Sys.sleep(5)
  
  # Get all input elements
  inputs <- b$Runtime$evaluate("
    Array.from(document.querySelectorAll('input')).map(input => ({
      type: input.type,
      name: input.name,
      id: input.id,
      placeholder: input.placeholder,
      className: input.className
    }))
  ")$result$value
  
  # Get all buttons
  buttons <- b$Runtime$evaluate("
    Array.from(document.querySelectorAll('button')).map(btn => ({
      text: btn.textContent.trim(),
      type: btn.type,
      className: btn.className
    }))
  ")$result$value
  
  message("Input elements found:")
  print(inputs)
  message("Button elements found:")
  print(buttons)
  
  b$close()
}
inspect_search_form()

# The website is a single page application (SPA) will try a longer wait time
wait_for_spa_load <- function(url, max_wait = 45) {
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


# test fn: wait_for_spa_load(url="https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st=2204%20Grand%20Oaks%20Avenue")



# the above now works
# Complete workflow would be:
# add url column to the dataframe(s) of addresses
unincorporated_parcels <- unincorporated_parcels %>%
  mutate(search_url=paste0(lac_permits_url, gsub(" ", "%20", address)))
for(row_ in 1:nrow(unincorporated_parcels)) {
  url_ <- unincorporated_parcels[row_, "search_url"]
  message(paste("Current search URL:", url_))
  html_response <- wait_for_spa_load(url=url_)
  permits <- extract_permit_data(html_response)  # Extract the data
  Sys.sleep(5)
}

url_ <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st=2204%20Grand%20Oaks%20Avenue"
message(paste("Current search URL:", url_))
html_response <- wait_for_spa_load(url=url_)
permits <- extract_permit_data_specific(html_response)  # Extract the data
Sys.sleep(5)

# 2. If successful: 
result <- scrape_permits_chromote(lac_permits_url)

# Test with your URLs:
lac_permits_url <- "https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=2&ps=10&pn=1&em=true&st="
pasadena_permits_url <- "https://mypermits.cityofpasadena.net/EnerGov_Prod/SelfService#/search?m=2&ps=10&pn=1&em=true&st="

# Example usage:
# result <- scrape_permits_chromote(paste0(pasadena_permits_url, "1630%20Carriage%20House"))





#### Setting aside for later

# # Alternative function for more targeted extraction based on HTML structure
# extract_permit_data_targeted <- function(url) {
#   
#   page <- read_html(url)
#   
#   # Find all permit record containers
#   permit_containers <- page %>%
#     html_nodes("div[name^='label-']")
#   
#   permit_data <- list()
#   
#   for(container in permit_containers) {
#     
#     # Get the field name from the 'name' attribute
#     field_name <- container %>%
#       html_attr("name") %>%
#       gsub("label-", "", .)
#     
#     # Get the label text
#     label <- container %>%
#       html_node("label") %>%
#       html_text(trim = TRUE)
#     
#     # Get the value - try multiple methods
#     value <- container %>%
#       html_node("span.margin-md-left") %>%
#       html_text(trim = TRUE)
#     
#     # If no value found, try looking for highlighted content
#     if(is.na(value) || value == "") {
#       value <- container %>%
#         html_node("tyler-highlight span") %>%
#         html_text(trim = TRUE)
#     }
#     
#     # If still no value, try looking for links
#     if(is.na(value) || value == "") {
#       value <- container %>%
#         html_node("a") %>%
#         html_text(trim = TRUE)
#     }
#     
#     permit_data[[field_name]] <- value
#   }
#   
#   # Convert to dataframe
#   df <- data.frame(
#     field = names(permit_data),
#     value = unlist(permit_data),
#     stringsAsFactors = FALSE
#   )
#   
#   return(df)
# }


# Function for specific html structure
extract_specific_permit_fields <- function(url) {
  
  # Method 3: Includes error handling
  tryCatch({
    # First try with httr2
    response <- request(url) %>%
      req_headers(
        "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language" = "en-US,en;q=0.9",
        "Accept-Encoding" = "gzip, deflate, br",
        "DNT" = "1",
        "Connection" = "keep-alive",
        "Upgrade-Insecure-Requests" = "1",
        "Sec-Fetch-Dest" = "document",
        "Sec-Fetch-Mode" = "navigate",
        "Sec-Fetch-Site" = "none",
        "Cache-Control" = "max-age=0"
      ) %>%
      req_retry(max_tries = 3) %>%
      req_timeout(30) %>%
      req_perform()
    
    page <- response %>%
      resp_body_html()
    
  }, error = function(e) {
    message("httr2 failed, trying rvest with headers...")
    
    # Fallback to rvest with session
    session <- session(url,
                       user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    page <- session %>% read_html()
    return(page)
  })
  
  # Define the specific fields we want to extract based on your HTML
  fields_to_extract <- list(
    permit_number = "div[name='label-CaseNumber'] a",
    applied_date = "div[name='label-ApplyDate'] span.margin-md-left",
    type = "div[name='label-CaseType'] tyler-highlight span",
    issued_date = "div[name='label-IssuedDate'] span.margin-md-left",
    project_name = "div[name='label-Project'] tyler-highlight span",
    expiration_date = "div[name='label-ExpiredDate'] span.margin-md-left",
    status = "div[name='label-Status'] tyler-highlight span",
    finalized_date = "div[name='label-FinalizedDate'] span.margin-md-left",
    main_parcel = "div[name='label-MainParcel'] tyler-highlight span",
    address = "div[name='label-Address'] tyler-highlight span",
    description = "div[name='label-Description'] tyler-highlight span"
  )
  
  # Extract each field
  permit_info <- list()
  
  for(field_name in names(fields_to_extract)) {
    selector <- fields_to_extract[[field_name]]
    
    value <- page %>%
      html_node(selector) %>%
      html_text(trim = TRUE)
    
    permit_info[[field_name]] <- ifelse(is.na(value), "", value)
  }
  
  # Convert to dataframe
  df <- data.frame(
    field = names(permit_info),
    value = unlist(permit_info),
    stringsAsFactors = FALSE
  )
  
  return(df)
}

# # Example usage with error handling:
# # Replace 'your_url_here' with the actual URL you want to scrape
# 
# # Safe wrapper function
# safe_scrape <- function(url, delay = 2) {
#   message(paste("Scraping:", url))
#   
#   # Add delay between requests
#   Sys.sleep(delay)
#   
#   tryCatch({
#     df_permit_data <- extract_specific_permit_fields(url)
#     return(df_permit_data)
#   }, error = function(e) {
#     message(paste("Error scraping", url, ":", e$message))
#     return(NULL)
#   })
# }

# Additional helper functions for common 403 issues:

# Function to check if URL is accessible
check_url_access <- function(url) {
  tryCatch({
    response <- request(url) %>%
      req_headers("User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36") %>%
      req_perform()
    
    status <- resp_status(response)
    message(paste("Status code:", status))
    return(status == 200)
    
  }, error = function(e) {
    message(paste("Cannot access URL:", e$message))
    return(FALSE)
  })
}

# # Function for batch scraping with proper delays
# batch_scrape <- function(urls, delay = 3) {
#   results <- list()
#   
#   for(i in 1:length(urls)) {
#     message(paste("Processing", i, "of", length(urls)))
#     
#     result <- safe_scrape(urls[i], delay)
#     
#     if(!is.null(result)) {
#       results[[i]] <- result
#     }
#     
#     # Longer delay every 10 requests
#     if(i %% 10 == 0) {
#       message("Taking longer break...")
#       Sys.sleep(10)
#     }
#   }
#   
#   return(results)
# }

# Example usage:
# Replace 'your_url_here' with the actual URL you want to scrape
# df_permit_data <- extract_specific_permit_fields("https://mypermits.cityofpasadena.net/EnerGov_Prod/SelfService#/search?m=2&fm=1&ps=10&pn=1&em=true&st=1630%20Carriage%20House")


# trying new code to see if we can even access the page with results
url <- "https://mypermits.cityofpasadena.net/EnerGov_Prod/SelfService#/search?m=2&fm=1&ps=10&pn=1&em=true&st=1630%20Carriage%20House"
check_url_access(url = url)