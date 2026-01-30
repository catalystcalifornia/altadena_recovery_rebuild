## PURPOSE: The purpose of this script is to produce the rel_assessor_sales table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_assessor_sales.docx ##
## SCRIPT OUTPUT: rel_assessor_sales_YYYY_MM

#### STEP 1: SET UP (Update year and month) ####
library(sf)
library(rmapshaper)
library(leaflet)
library(htmlwidgets)
library(stringr)
library(readxl)
library(tidyverse)
library(janitor)
library(mapview)
library(writexl)

options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")

year <- "2025"
month <- "12"

#### STEP 2: PULL DATA AND FILTER (Update to latest data) ####
# get xwalk for PREVIOUS MONTH and CURRENT MONTH
xwalk <- st_read(con_alt, query="SELECT * FROM dashboard.crosswalk_assessor_2025_09_12")
# get assessor data for CURRENT MONTH and filter with xwalk for just AINs we are evaluating for
assessor_data <- st_read(con_alt, query="Select * from dashboard.assessor_data_universe_2025_12") %>%
  filter(ain %in% xwalk$ain_2025_12)

# get universe of distinct current ains to join assessor data to -- so we don't drop those without records in the data table (parcel shapes update faster than parcel data)
curr_ain_universe <- xwalk %>% distinct(ain_2025_12) %>% rename(ain=ain_2025_12)

sales <- curr_ain_universe %>% 
  left_join(assessor_data) %>% 
  select(ain, use_code, contains("owner"),
         recording_date,
         year_sold_to_state,
         last_sale_verif_key, last_sale_date, last_sale_amount, 
         sale_two_verif_key, sale_two_date, sale_two_amount, 
         sale_three_verif_key, sale_three_date, sale_three_amount,
         doc_reason_code,
         land_reason_key,
         tax_stat_key,
         impairment_key,
         partial_interest,
         exemption_type,
         ownership_code
  ) 

#### STEP 3: MUTATE DATES for SALES (NO UPDATES) ####

sales <- sales %>%
  mutate(last_sale_date_orig=last_sale_date,
         last_sale_char=as.character(last_sale_date),
         last_sale_date=as.Date(last_sale_char, format = "%Y%m%d"),
         sale_two_char=as.character(sale_two_date),
         sale_two_date=as.Date(sale_two_char, format = "%Y%m%d"),
         sale_three_char=as.character(sale_three_date),
         sale_three_date=as.Date(sale_three_char, format = "%Y%m%d")
  )

View(sales) # looks good
## extract year and month of most recent sale and flag for if sale took place after Eaton
# what date to use for flag of being sold after Eaton?
# first sale occurred in February according to news sources - https://www.cbsnews.com/losangeles/news/first-altadena-property-with-home-destroyed-by-eaton-fire-hits-market-sells-within-days/
# fire started on January 7th, 2025  https://www.fire.ca.gov/incidents/2025/1/7/eaton-fire
# LASD allowed all residents to at least visit their properties on 1/21-25 -- use this date https://www.instagram.com/p/DFGeGEOBbw5/?hl=en

#### STEP 4: CREATE T/F IF SOLD AFTER EATON (NO UPDATES) ####
# based on news article and data, we use date of 2-8-25 for sale after fire -- escrow takes 30 days to close anyways typically
sales <- sales %>%
  mutate(last_sale_year=format(last_sale_date,"%Y"),
         last_sale_month=format(last_sale_date,"%m"),
         sold_after_eaton=ifelse(last_sale_date>="2025-02-08", TRUE, FALSE))

#### STEP 5: CHECK YOUR DATA (NO UPDATES) ####

# quick check
sales%>%select(last_sale_date, sold_after_eaton)%>%View() # Looks good

# check for nulls
check_sales <- sales %>%
  group_by(last_sale_year,sold_after_eaton) %>%
  summarise(count=n())

check_sales %>% filter(is.na(last_sale_year) & is.na(sold_after_eaton))
# 30 with NA in check

# check most recent sales date
max(sales$last_sale_date,na.rm=TRUE)
# "2025-09-26" - if this doesnt increase flag to Elycia

na_sale_date <- sales %>%
  select(last_sale_date_orig, last_sale_year,recording_date, doc_reason_code, land_reason_key, everything()) %>%
  filter(is.na(sold_after_eaton))
# those with a sales date originally have errors in the sales date, but sold prior to 2025
# looking at recording date, only one had a recording date in 2025, but the recording was due to perfection of title not a sale, make nulls a FALSE flag
# land reason key 6 for this property but key 6 isn't in data dictionary

#### STEP 6A: CLEAN UP NAs for POSTGRES (Update AIN column names) ####

# clean up for postgres
sales_final <- sales %>%
  mutate(sold_after_eaton=ifelse(is.na(sold_after_eaton), FALSE, sold_after_eaton))

sales_final <- sales_final %>%
  select(ain,
         sold_after_eaton, last_sale_year, last_sale_month, 
         first_owner_name, first_owner_name_overflow, second_owner_name,
         recording_date, 
         ownership_code, doc_reason_code, land_reason_key, partial_interest,
         tax_stat_key, year_sold_to_state, impairment_key,
         last_sale_date, last_sale_verif_key, last_sale_amount,
         sale_two_date, sale_two_verif_key, sale_two_amount,
         sale_three_date, sale_three_verif_key, sale_three_amount) 

# check for duplicates or gaps
nrow(sales_final) - length(unique(sales_final$ain)) # should be 0
nrow(sales_final) - length(unique(xwalk$ain_2025_12)) # should be 0

# check total sales that it increased
table(sales_final$sold_after_eaton,useNA='always')
# 271 in December 2025

#### STEP 6B: Add sales data from "Altadena Not for Sale" database ####

#first pull most recent sales data and clean up columns to match la county sales data
anfs_sales <- st_read(con_alt, query="SELECT * FROM dashboard.anfs_sales_data_01092026") 

#second, add column anfs_sale TRUE or FALSE for record and strip data to just parcel and anfs_sold
anfs_record <- anfs_sales %>%
  mutate(
    anfs_sold = as.Date(sold_date) > as.Date("2025-01-07"),
    anfs_ain = parcel
  ) %>%
  select(anfs_ain, anfs_sold) 

#third, before merging anfs to lac, add older AINs to lac data
sales_record <- sales_final %>% select(ain, sold_after_eaton) %>% 
  mutate(lac_ain = ain) %>%
  left_join(xwalk, by = c("ain" = "ain_2025_12")) %>% # merge to get older ains jic anfs data is recording from prior ains
  select(lac_ain, sold_after_eaton, ain_2025_01, ain_2025_09)

#fourth, merge to anfs and add source column
sales_merged <- sales_record %>%
  left_join(anfs_record, by = c("lac_ain" = "anfs_ain"), suffix = c("", "_c")) %>%
  left_join(anfs_record, by = c("ain_2025_01" = "anfs_ain"), suffix = c("", "_c")) %>%
  left_join(anfs_record, by = c("ain_2025_09" = "anfs_ain"), suffix = c("", "_c")) %>%
  # coalesce all anfs columns
  mutate(
    across(
      ends_with(c("_a", "_b", "_c")),
      ~ coalesce(.x, get(sub("_[abc]$", "", cur_column())))
    )
  ) %>%
  # drop extra suffixed columns
  select(-ends_with(c("_a", "_b", "_c"))) %>%
  mutate(anfs_sold = coalesce(anfs_sold, FALSE)) %>%
  mutate(sold_source = ifelse(sold_after_eaton == TRUE & anfs_sold == TRUE, "both",
                              ifelse(sold_after_eaton == TRUE & anfs_sold == FALSE, "lac",
                                     ifelse(sold_after_eaton == FALSE & anfs_sold == TRUE, "anfs",
                                            "neither"))))
# table(sales_merged$sold_source)


#fifth, first clean up sales data tables from lac and anfs to merge later
anfs_sales_clean <- anfs_sales %>%
  mutate(last_sale_year_anfs = year(as.Date(sold_date)),
         last_sale_month_anfs = month(as.Date(sold_date)),
         last_sale_date_anfs = sold_date,
         sold_amount_anfs = contract_amt,
         owner_name_anfs = new_owner,
         address = property) %>%
  select(parcel, 
         last_sale_year_anfs,
         last_sale_month_anfs,
         last_sale_date_anfs,
         sold_amount_anfs,
         list_price, #unique to anfs, not in lac
         owner_name_anfs,
         address) #unique to anfs, not in lac
  
lac_sales_clean <- sales_final %>%
  mutate(owner_name_lac = first_owner_name,
         sold_amount_lac = last_sale_amount) %>%
  rename(last_sale_year_lac = last_sale_year,
         last_sale_month_lac = last_sale_month,
         last_sale_date_lac = last_sale_date) %>%
  select(ain, 
         last_sale_year_lac,
         last_sale_month_lac,
         last_sale_date_lac,
         sold_amount_lac,
         recording_date, #unique to lac, not in anfs
         owner_name_lac)

#sixth, merge to final dataset to get merged information needed. 

sales_updated <- sales_merged %>% 
  left_join(lac_sales_clean, by = c("lac_ain" = "ain")) %>%
  left_join(anfs_sales_clean, by = c("lac_ain" = "parcel"))

sales_updated_final <- sales_updated %>%
  mutate(
    last_sale_year = ifelse(sold_source == "anfs", 
                            last_sale_year_anfs,
                            last_sale_year_lac),
    last_sale_month = ifelse(sold_source == "anfs", 
                             last_sale_month_anfs,
                             last_sale_month_lac),
    last_sale_date = ifelse(sold_source == "anfs", 
                            last_sale_date_anfs,
                            last_sale_date_lac),
    sold_amount = ifelse(sold_source == "anfs", 
                         sold_amount_anfs,
                         sold_amount_lac),
    owner_name = ifelse(sold_source == "anfs", 
                        owner_name_anfs,
                        owner_name_lac)
  ) %>% 
  select(lac_ain, sold_after_eaton, anfs_sold, sold_source, 
         last_sale_year,
         last_sale_month, 
         last_sale_date,
         sold_amount,
         owner_name,
         recording_date,
         list_price,
         address)

# check total sales that it increased from before ANFS data was added
table(sales_updated_final$sold_source,useNA='always')
# 104+239+32 = 375 TOTAL SALES in December 2025

#### STEP 7: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_sales_", year, "_", month)
schema <- "dashboard"
indicator <- "Relational table with information on sales date and ownership/documentation changes. Includes a flag for sales of properties that were likely listed after the fire"
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_sales.R "
qa_filepath<-"  QA_sheet_rel_assessor_sales.docx "

dbWriteTable(con_alt, Id(schema, table_label), sales_final,
                         overwrite = FALSE, row.names = FALSE)


# Add metadata
column_names <- colnames(sales_final) # Get column names

column_comments <- c('ain for current month- use to match to other tables',
        ' true false field for if sale took place after 2-8-25--likely to have been listed after eaton fire',
         'year of last sale',
         'month of last sale in number format',
         'owner name',
         "owner name overflow",
         "second owner name",
         " This is the date of last change or correction of ownership",
         "This element contains a code that describes the relationships between the recording and valuation dates",
         "This element contains a one digit code which identifies the specific reason for a reappraisable or  non reappraisable status",
        "Reason key for the last land value change",
         "The percentage of property involved in a transfer of ownership. First two digits are percentage of property being transferred rounded to nearest whole. Third digit indicates the specific interest being transferred",
         "A one-digit code that indicates whether or not property taxes are delinquent",
         "If parcel is delinquent, this indicates the four digits of the year in which taxes first became delinquent",
        "A key indicating whether the parcel value has been impaired and describing the impairment",
         "This is the last sale date - Present for both verified and unverified sales",
         "Verification key of last sale - only unverified sales appear on the Secured Basic File Abstract",
         "Last unverified sale amount - If the sale is a verified sale (non-numeric character as indicated on the verifications key), the sale amount will not show",
        "This is the second to last sale date - Present for both verified and unverified sales",
        "Verification key of second to last sale - Only unverified sales appear on the Secured Basic File Abstract",
        "Second to Last unverified sale amount - If the sale is a verified sale (non-numeric character as indicated on the verifications key), the sale amount will not show",
        "This is the third to last sale date - Present for both verified and unverified sales",
        "Verification key of third to last sale - Only unverified sales appear on the Secured Basic File Abstract",
        "Third to last unverified sale amount - If the sale is a verified sale (non-numeric character as indicated on the verifications key), the sale amount will not show")

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 7B: Add updates sales data from "Altadena Not for Sale" to pgadmin database ####
# Export to postgres
table_label <- paste0("rel_assessor_sales_updated_", year, "_", month)
schema <- "dashboard"
indicator <- "Relational table with information on sales date and ownership/documentation changes combined with sales data from Altadena Not For Sale. "
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_sales.R "
qa_filepath<-"  QA_Sheet_anfs_sales_data.docx "

dbWriteTable(con_alt, Id(schema, table_label), sales_updated_final,
             overwrite = FALSE, row.names = FALSE)

# Add metadata
column_names <- colnames(sales_updated_final) # Get column names

column_comments <- c('ain for current month- use to match to other tables',
                     ' true false field for if sale took place after 1-8-25--likely to have been listed after eaton fire',
                     ' true false field for if sale is recorded by group: Altadena Not For Sale', 
                     ' flag if la county, altadena not for sale, both, or neither recorded the sale. Default was to use relative data from la county but anfs if only they recorded the information.',
                     'year of last sale',
                     'month of last sale in number format',
                     " This is the date of last change or correction of ownership",
                     'amount sold for',
                     'owner name',
                     "This is the last sale date - Present for both verified and unverified sales",
                     "price the property is originally listed for",
                     'address of property')

add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)


#### STEP 8: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)
