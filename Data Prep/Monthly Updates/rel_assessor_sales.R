## PURPOSE: The purpose of this script is to produce the rel_assessor_sales table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_tables_2026_04.docx ##
## SCRIPT OUTPUT: rel_assessor_sales_YYYY_MM
## Script combines data from LAC assessor with sales data collected from Altadena Not For Sale
## LAC sales data used as base and Altadena not for sale data supplements it

#### STEP 1: *UPDATE YEAR AND MONTH* SET UP  ####
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

year <- "2026"
month <- "04"

#### STEP 2: *UPDATE* PULL DATA AND FILTER (update to latest data) ####
# get xwalk for PREVIOUS MONTH and CURRENT MONTH
xwalk <- dbGetQuery(con_alt, "SELECT * FROM dashboard.crosswalk_assessor_2026_12_04")
# get assessor data for CURRENT MONTH and filter with xwalk for just AINs we are evaluating for
assessor_data <- dbGetQuery(con_alt, "Select * from dashboard.assessor_data_universe_2026_04") %>%
  filter(ain %in% xwalk$ain_2026_04)

# get universe of distinct current ains to join assessor data to -- 
# so we don't drop those without records in the data table 
# (parcel shapes update faster than parcel data)
curr_ain_universe <- xwalk %>% distinct(ain_2026_04) %>% rename(ain=ain_2026_04)

lac_sales <- curr_ain_universe %>% 
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
## PART 1 - LAC SALES FROM ASSESSOR -------
##### STEP 3: MUTATE DATES for SALES (NO UPDATES) #####

## extract year and month of most recent sale
lac_sales <- lac_sales %>%
  mutate(last_sale_date_orig=last_sale_date,
         last_sale_char=as.character(last_sale_date),
         last_sale_date=as.Date(last_sale_char, format = "%Y%m%d"),
         sale_two_char=as.character(sale_two_date),
         sale_two_date=as.Date(sale_two_char, format = "%Y%m%d"),
         sale_three_char=as.character(sale_three_date),
         sale_three_date=as.Date(sale_three_char, format = "%Y%m%d")
  )

View(lac_sales) # looks good

##### STEP 4: LAC SALES  - CREATE T/F IF SOLD AFTER EATON BASED ON LAC (NO UPDATES) #####
# based on news article and data, we use date of 2-8-25 for sale after fire -- 
# escrow takes 30 days to close anyways typically
# flag for if sale took place after Eaton
lac_sales <- lac_sales %>%
  mutate(last_sale_year=format(last_sale_date,"%Y"),
         last_sale_month=format(last_sale_date,"%m"),
         sold_after_eaton=case_when(
            last_sale_date>="2025-02-08" ~ TRUE, 
            last_sale_date<"2025-02-08" ~ FALSE,
            .default = FALSE)) # see line 108-111 for explanation

# quick check
lac_sales%>%select(last_sale_char,last_sale_date, sold_after_eaton)%>%View() # Looks good
table(lac_sales$sold_after_eaton, useNA = "always")
# FALSE  TRUE  <NA> 
# 5405   383    0 

##### STEP 5: *UPDATE* CHECK LAC DATA EACH  #####
# check most recent sales date
max(lac_sales$last_sale_date,na.rm=TRUE)
# "2025-12-31" - if this doesnt increase flag to Elycia

# check for nulls
check_sales <- lac_sales %>%
  group_by(last_sale_year,sold_after_eaton) %>%
  summarise(count=n())

check_sales %>% filter(is.na(last_sale_year))
# 30 with NA in check same as last update, assumes not sold in line 81
# April - 25 with NA, assumes not sold

na_sale_date <- lac_sales %>%
  select(last_sale_date_orig, last_sale_year,recording_date, doc_reason_code, land_reason_key, everything()) %>%
  filter(is.na(last_sale_year))
# those with a sales date originally have errors in the sales date or are missing a date, but sold prior to 2025
# looking at recording date, only one had a recording date in 2025, 
# but the recording was due to perfection of title not a sale, make nulls a FALSE flag
# land reason key 6 for this property but key 6 isn't in data dictionary
# checked 5839011007 which had 0 sale date and no record online of sale so assume false for 0 or missing

lac_sales_final <- lac_sales %>%
  # specify for LAC
  rename(sold_after_eaton_lac=sold_after_eaton) %>%
  select(ain,
         sold_after_eaton_lac, last_sale_year, last_sale_month, 
         first_owner_name, first_owner_name_overflow, second_owner_name,
         recording_date, 
         ownership_code, doc_reason_code, land_reason_key, partial_interest,
         tax_stat_key, year_sold_to_state, impairment_key,
         last_sale_date, last_sale_verif_key, last_sale_amount,
         sale_two_date, sale_two_verif_key, sale_two_amount,
         sale_three_date, sale_three_verif_key, sale_three_amount) 

# check for duplicates or gaps
nrow(lac_sales_final) - length(unique(lac_sales_final$ain)) # should be 0
nrow(lac_sales_final) - length(unique(xwalk$ain_2026_04)) # should be 0

# check total sales that it increased - flag to Elycia if it doesn't increase
table(lac_sales_final$sold_after_eaton_lac, useNA='always')
# 271 in December 2025
# 383 in March/April 2026

### PART 2 - ADD ALTADENA NOT FOR SALE DATA ####
##### STEP 6: *UPDATE* Add sales data from "Altadena Not for Sale" database #####
# first pull most recent sales data and clean up columns to match la county sales data
# update with most recent dataset
anfs_sales <- dbGetQuery(con_alt, "SELECT * FROM dashboard.anfs_sales_data_2026_04")

# future check for anymore combined AINs
check <- anfs_sales %>%
  filter(grepl(",\\s*", parcel))
# same 1 - 5844012018,19

anfs_sales <- anfs_sales %>%
  # there's 2 AINs in one sales row - split so each is one row and we can match
  # properly to other rel_ tables
  separate_rows(parcel, sep=",\\s*") %>%
  mutate(parcel = ifelse(parcel == "19", "5844012019", parcel)) %>%
  # Update anfs data in cases where there is a known typo or AIN from pre-2025
  mutate(parcel = case_when(
    parcel == "5842020011" ~ "5842022011", # typo
    parcel == "5844018036" ~ "5846018036", # typo
    parcel == "5482015020" ~ "5842015020", # typo
    parcel == "5843022001" ~ "5843022058", # anfs has outdated AIN (pre-2025)
    .default = parcel))


# check for duplicates
length(unique(anfs_sales$parcel))-nrow(anfs_sales)
# 3 duplicates remove by taking latest sale

# remove duplicates
anfs_sales_dup <- anfs_sales

anfs_sales <- anfs_sales_dup %>%
  arrange(parcel, desc(sold_date)) %>%
  distinct(parcel, .keep_all = TRUE)

length(unique(anfs_sales$parcel))-nrow(anfs_sales)
# no duplicates

#### PART 3 - *UPDATE* MERGE SALES FROM LAC ASSESSOR AND ANFS#####
##### STEP 7: *UPDATE* PREP DATA FOR MERGE #####
# before merging anfs to lac, add older AINs to lac data so we can match sales to prior ains if necessary
lac_sales_records <- lac_sales_final %>% 
  select(ain, sold_after_eaton_lac) %>% 
  mutate(lac_ain = ain) %>%
  left_join(xwalk, by = c("ain" = "ain_2026_04")) %>% # merge to get older ains jic anfs data is recording from prior ains
  select(lac_ain, sold_after_eaton_lac, ain_2025_01, ain_2025_12)

# check on duplicates after adding crosswalk
nrow(lac_sales_records) - nrow(lac_sales_final)
dup_check <- lac_sales_records %>% count(lac_ain) %>% filter(n>1)
print(dup_check)
# checked in the xwalk
xwalk %>% filter(ain_2026_04 %in% dup_check$lac_ain) %>% View()
# makes sense to have duplicate here, take care of later to ensure no more dups at the end

# check to make sure if any anfs parcels will get dropped and find no match
anfs_missing <- anfs_sales %>% 
  filter(!parcel %in% c(lac_sales_records$lac_ain,lac_sales_records$ain_2025_01,lac_sales_records$ain_2025_12))
# check against prior damage records
damage <- dbGetQuery(con_alt, "SELECT * FROM data.rel_assessor_damage_level_sept2025")
residential <- dbGetQuery(con_alt, "SELECT ain_sept, residential FROM data.rel_assessor_residential_sept2025") 
anfs_missing <- anfs_missing %>% 
  left_join(damage,by=c("parcel"="ain_sept")) %>%
  left_join(residential,by=c("parcel"="ain_sept"))

###### *UPDATE running log of ANFS issues to fix #####
# explore anfs ains that get no match in our crosswalks - 
# likely commercial or deleted parcels or in some cases typos
anfs_missing %>% filter(is.na(damage_category)) %>% View() 

# update log of parcels that don't apply (e.g., commercial) or that have typos
## Don't apply because commercial or public land or vacant (in jan25) properties
# 5845002015 - commercial (doesn't apply)
# 5841032019 - commercial (doesn't apply)
# 5835038003 - commercial (doesn't apply)
# 5862007300 - public land
# 5841001014 - vacant land - was vacant in jan 2025 based on 'SELECT * FROM dashboard.assessor_data_universe_2025_01 where ain='5841001014'' so doesn't apply to universe

## Don't join because of typo or outdated AIN - All are manually
## addressed when loading in anfs in lines 147-152
# 5843022001 - deleted (old ain from 2021) - should be: 5843022058 (manually fixed)
# 5842020011  - 5842022011 - typo (manually fixed)
# 5844018036 - 5846018036 - typo (manually fixed)
# 5844012018,19 - 5844012018 - significant damage split (manually fixed)
# 5844012018,19 - 5844012019 - no damage split (manually fixed)
# 5482015020 - 5842015020 typo (manually fixed)

## Don't apply due to damage level
# # 5751009007 - residential no damage on CalFire database - unclear why didn't match our september data
# 5831005008 Some damage (doesn't apply) / Misfortune & Calamity Status: APPROVED (looks ok in photo) https://portal.assessor.lacounty.gov/parceldetail/5831005008
# 5839002014 No Damage (doesn't apply) / Misfortune & Calamity Status: UNDER REVIEW (looks ok in photo) https://portal.assessor.lacounty.gov/parceldetail/5839002014
# 5841008021 No Damage (doesn't apply) / Misfortune & Calamity Status:N/A (just land, no improvements) https://portal.assessor.lacounty.gov/parceldetail/5841008021
# 5847012011 No Damage (doesn't apply) / Misfortune & Calamity Status: UNDER REVIEW (hard to tell in photo, seems mostly ok but lost a lot of trees) https://portal.assessor.lacounty.gov/parceldetail/5847012011
# 5833001041 No Damage (doesn't apply) / Misfortune & Calamity Status:N/A (house visibly destroyed but split across this parcel and 5833001042) - should probably keep?
# 5846018036 No Damage (doesn't apply) / Misfortune & Calamity Status: UNDER REVIEW (looks ok in photo) https://portal.assessor.lacounty.gov/parceldetail/5846018036 


##### STEP 8: Flag ANFS PARCELS SOLD AFTER EATON #####
# add column anfs_sale TRUE or FALSE for record and strip data to just parcel and anfs_sold
anfs_sales_records <- anfs_sales %>%
  mutate(
    anfs_sold = as.Date(sold_date) >= as.Date("2025-02-08"),
    anfs_ain = parcel) %>%
  select(anfs_ain, anfs_sold) 

##### STEP 9: *UPDATE* MERGE DATASET AND ADD SOURCE COLUMN #####
# merge to anfs and add source column
sales_merged <- lac_sales_records %>%
  # join anfs records based on each ain field date in the crosswalk, add suffix after 2nd join
  # add prior ains from previous xwalks each update
  left_join(anfs_sales_records, by = c("lac_ain" = "anfs_ain")) %>%
  left_join(anfs_sales_records, by = c("ain_2025_01" = "anfs_ain"), suffix = c("", "_b")) %>%
  left_join(anfs_sales_records, by = c("ain_2025_12" = "anfs_ain"), suffix = c("", "_c")) %>%
  # coalesce anfs sales columns into one field
  mutate(anfs_sold_combined = coalesce(anfs_sold,
                                 anfs_sold_b,
                                 anfs_sold_c, FALSE)) %>%
  # add source column
  mutate(
    sold_source = case_when(
      sold_after_eaton_lac == TRUE & anfs_sold_combined == TRUE ~ "both",
      sold_after_eaton_lac == TRUE & anfs_sold_combined == FALSE ~ "lac",
      sold_after_eaton_lac == FALSE & anfs_sold_combined == TRUE ~ "anfs",
      .default = "neither"),
    # create final sold after eaton column
    sold_after_eaton=ifelse(sold_source!='neither', TRUE, FALSE))

# check recoding worked
sales_merged %>% filter(is.na(anfs_sold) & !is.na(anfs_sold_b)) %>% View()
table(sales_merged$sold_source)
table(sales_merged$sold_after_eaton)
# 73 + 340 + 43 = 456
sales_merged %>% group_by(sold_source,anfs_sold_combined,sold_after_eaton_lac) %>% summarise(count=n())

# check for dups again
nrow(sales_merged) - nrow(lac_sales_records)
# none extra

### PART 4 - CREATE FINAL DATAFRAME AND ADD OWNER NAME ####

##### STEP 10: Clean up ANFS sales data with owner and sales info ######
anfs_sales_clean <- anfs_sales %>%
  mutate(
    last_sale_date_anfs=as.Date(sold_date, format = "%Y-%m-%d"),
    last_sale_year_anfs = format(last_sale_date_anfs,"%Y"),
    last_sale_month_anfs = format(last_sale_date_anfs,"%m"),
    sold_amount_anfs = contract,
    owner_name_anfs = new_owner,
    address = property) %>%
  select(
    parcel,
    last_sale_year_anfs,
    last_sale_month_anfs,
    last_sale_date_anfs,
    sold_amount_anfs,
    owner_name_anfs) 

##### STEP 11: Clean up LAC sales data with owner and sales info ######

lac_sales_clean <- lac_sales_final %>%
  # clean up name columns in sales_final
  mutate(
    second_owner_name = na_if(lac_sales_final$second_owner_name, ""),
    first_owner_name_overflow = na_if(lac_sales_final$first_owner_name_overflow, ""),
    second_owner_name = na_if(lac_sales_final$second_owner_name, "                                "),
    first_owner_name_overflow = na_if(lac_sales_final$first_owner_name_overflow, "                                "),
    ) %>%
  # clean owner name so it's in one field
  mutate(
    owner_name_lac = case_when(
      !is.na(first_owner_name_overflow) & is.na(second_owner_name) ~ sprintf("%s (%s)", first_owner_name, first_owner_name_overflow),
      is.na(first_owner_name_overflow) & !is.na(second_owner_name) ~ sprintf("%s & %s",first_owner_name, second_owner_name),
      !is.na(first_owner_name_overflow) & !is.na(second_owner_name) ~ sprintf("%s (%s) & %s", first_owner_name, first_owner_name_overflow, second_owner_name),
      is.na(first_owner_name_overflow) & is.na(second_owner_name) ~ first_owner_name,
      .default=NA)) %>%
  # clean up extra spaces
  mutate(
    owner_name_lac=gsub("\\s+", " ", owner_name_lac),
    owner_name_lac=gsub("\\( ","(", owner_name_lac),
    owner_name_lac=gsub("\\ )",")", owner_name_lac)) %>%
  # clean up extra ands
  mutate(
    owner_name_lac=gsub(" AND) &", ") &",owner_name_lac),
    owner_name_lac=gsub(" AND &", " &",owner_name_lac))

# check
lac_sales_clean %>% select(contains("owner_name")) %>% View()
         
# date rename
lac_sales_clean <- lac_sales_clean %>%                                                                                                                                              
  rename(sold_amount_lac = last_sale_amount,
         last_sale_year_lac = last_sale_year,
         last_sale_month_lac = last_sale_month,
         last_sale_date_lac = last_sale_date) %>%
  select(ain, 
         last_sale_year_lac,
         last_sale_month_lac,
         last_sale_date_lac,
         sold_amount_lac,
         owner_name_lac)

##### STEP 12: MERGE FINAL DATASET ######
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
    last_sale_date = if_else(sold_source == "anfs", 
                            as.Date(last_sale_date_anfs),
                            as.Date(last_sale_date_lac)),
    sold_amount = ifelse(sold_source == "anfs", 
                         sold_amount_anfs,
                         sold_amount_lac),
    owner_name = ifelse(sold_source == "anfs", 
                        owner_name_anfs,
                        owner_name_lac)
  ) %>% 
  select(lac_ain, 
         sold_after_eaton, 
         sold_source, 
         last_sale_year,
         last_sale_month, 
         last_sale_date,
         sold_amount,
         owner_name)

# check total sales that it increased from before ANFS data was added
table(sales_updated_final$sold_source,useNA='always')
# 106+243+28 = 377 TOTAL SALES 
# 73   +  340    +  43 = 456 sales March/April 2026

table(sales_updated_final$sold_after_eaton,useNA='always')
# 377 - Dec/Jan flag to Elycia if doesn't increase
# 456 - March/April

##### STEP 13: *UPDATE* REMOVE DUPS AND CLEAN #####
# check on duplicates
dup_check <- sales_updated_final %>% count(lac_ain) %>% filter(n>1)
# view duplicate
sales_updated_final %>% filter(lac_ain %in% dup_check$lac_ain) %>% View() 
# case of a merged parcel where half of parcel was sold after January into merged parcel, think okay to count as sold
# generally keep the instance where parcel marked as sold
final_df <- sales_updated_final %>%
  group_by(lac_ain) %>%
  slice_max(sold_after_eaton,n=1, with_ties=FALSE) %>%
  ungroup()
# check
nrow(final_df) - length(unique(final_df$lac_ain)) # no duplicates
table(final_df$sold_after_eaton) # sold decreases by 1 because 	5842008010 was double counted
# 455 sold March/April

# rename column
final_df <- final_df %>%
  rename(ain=lac_ain)

#### PART 5: PUSH TO PGADMIN ####
# Export to postgres
table_label <- paste0("rel_assessor_sales_", year, "_", month) 
schema <- "dashboard"
indicator <- "Relational table with information on sales date and owner information using a combination of LAC assessor data and Altadena not for sale data
We mark a property as sold if sale date was after 2-8-25 in either source. In cases where property is only marked as sold in ANFS data then we use the owner information and sales data from that file. In all other cases, we use LAC assessor"
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/MK/altadena_recovery_rebuild/altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_sales.R "
qa_filepath<-"  QA_Sheet_rel_tables_update_2026_04.docx "

# dbWriteTable(con_alt, Id(schema, table_label), final_df,
#                          overwrite = FALSE, row.names = FALSE)


# Add metadata
column_names <- colnames(final_df) # Get column names

column_comments <- c('ain for current month- use to match to other tables',
        ' true false field for if sale took place after 2-8-25--likely to have been listed after eaton fire',
        'source of sale information if sold, e.g., from LAC assessor, Altadena not for sale (ANFS) or both',
         'year of last sale',
         'month of last sale in number format',
        'date of last sale',
        'sold amount - note sold amount from LAC assessor might be inaccurate - unverified sales are recorded, meaning final sale price could be different in other sources',
         'owner name - from lac assessor if source is lac assessor or both and from anfs if source of sale is anfs')

# add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)


#### PART 8: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)
