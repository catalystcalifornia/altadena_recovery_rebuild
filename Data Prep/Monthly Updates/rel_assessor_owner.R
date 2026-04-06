## PURPOSE: The purpose of this script is to produce the rel_assessor_owner table for the Monthly Dashboard Updates ##
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_rel_tables_update_2026_04.docx ##
## SCRIPT OUTPUT: rel_assessor_owner_YYYY_MM

#### STEP 1: SET UP (*UPDATE* year and month) ####
library(sf)
library(rmapshaper)
library(stringr)
library(readxl)
library(tidyverse)
library(janitor)
library(writexl)

options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")

year <- "2026"
month <- "04"

#### STEP 2: PULL XWALKS AND DATA (*UPDATE* to latest data and xwalks) ####
# get for CURRENT MONTH
xwalk <- st_read(con_alt, query="SELECT * FROM dashboard.crosswalk_assessor_2026_12_04")

# get assessor data for CURRENT MONTH and filter with xwalk for just AINs we are evaluating for
assessor_data <- st_read(con_alt, query="SELECT * FROM dashboard.assessor_data_universe_2026_04") %>%
  filter(ain %in% xwalk$ain_2026_04)

# get sales data for current month 
sales_data <- st_read(con_alt, query="select * from dashboard.rel_assessor_sales_2026_04") %>%
  filter(ain %in% xwalk$ain_2026_04)

# get residential data for current month 
residential_data <- st_read(con_alt, query="select * from dashboard.rel_assessor_residential_2026_04")

# join sales data to assessor data keeping columns for owner type
owner_info <- sales_data %>%
  left_join(residential_data %>% select(ain_2026_04,total_units,landlord_units), by=c("ain"="ain_2026_04")) %>%
  left_join(assessor_data %>% 
              select(ain,exemption_type,tax_stat_key,year_sold_to_state,
                     contains("owner"),mail_house_no,contains("m_")), 
            by=c("ain"="ain")
            )

#### STEP 1: CREATING OWNER_RENTER COLUMN (QA AND *UPDATE* IF NEEDED) ####
# check exemption types and fix if needed
table(owner_info$exemption_type,useNA='always')
table(owner_info$num_howmowner_exemption,useNA='always')
table(owner_info$tax_stat_key,useNA='always')

# confirm sold source
table(owner_info$sold_source,useNA='always')
# if sold source is anfs then we can't rely on the assessor data for owner type, and must only rely on the owner_name

data_owner <- owner_info  %>%
  mutate(
    # fix exemption type if needed
    # exemption_type=ifelse(exemption_type=='ÿ', NA, exemption_type), 
         owner_renter=case_when(
           sold_source!='anfs' & num_howmowner_exemption>=1 & landlord_units==0 ~ "Owner occupied",
           sold_source!='anfs' & num_howmowner_exemption>=1 & landlord_units>=1 ~ "Owner & Renter occupied",
           sold_source!='anfs' & num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter occupied",
           TRUE ~ "Other")) 

# check
table(data_owner$owner_renter,useNA='always')
xtabs(~ sold_source + owner_renter, data = data_owner, na.action=na.pass, addNA=TRUE)
xtabs(~ num_howmowner_exemption + owner_renter, data = data_owner, na.action=na.pass, addNA=TRUE)

data_owner <- data_owner %>%
  # owner occupied or church/welfare exemption
  mutate(
    owner_renter = case_when(
      # veterans count as owner occupied
      sold_source!='anfs' & exemption_type %in% "1" & owner_renter == "Other" ~ "Owner occupied",
      # welfare, full religious, or church exemption
      sold_source!='anfs' & exemption_type %in% c("3","4", "5", "6","7","8") & owner_renter == "Other" ~ "Church/Welfare exemption",
      TRUE ~ owner_renter )) %>% # keeps existing value if none of the above conditions are met 
  # top code corporate owned
  mutate(
    owner_renter = ifelse(
      (grepl("LLC| LP| Inc| Investment| Corp", owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "LLC owned",owner_renter))%>%
  # trust owned
  mutate(
    owner_renter = ifelse(
      (grepl("TRUST|TRST| TR | TRS", owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Trust owned",owner_renter)) %>%
  # sold to state/govt owned
  mutate(
    owner_renter = case_when(
      sold_source!='anfs' & tax_stat_key %in% c("1","2") ~ "Sold to state",
      sold_source!='anfs' & tax_stat_key %in% "3" ~ "SBE or Government owned",
      TRUE ~ owner_renter  # keeps existing value if none of the above conditions are met
    )
  ) %>%
  # other church or charity owned based on name
  mutate(
    owner_renter = ifelse(
      (grepl("CHURCH|FRATERNAL|SERVICES", owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Church/Welfare exemption",owner_renter)) %>%
  # recoding other church or charity owned based on specific name
  mutate(
    owner_renter = ifelse(
      grepl("SUBSIDIZED HOUSING CORP|PASADENA CEMETRY ASSN CORP|Pasadena Cemetery Association|PUBLIC WORKS GROUP CORP|ARROYOS AND FOOTHILLS \\(CONSERVANCY\\)", owner_name, ignore.case = TRUE),
      "Church/Welfare exemption",owner_renter),
    # adding land trusts
    owner_renter = ifelse(
      grepl("GREENLINE HOUSING FOUNDATION|NHS NEIGHBORHOOD REDEVELOPMENT|NHS Nbrhd Redevelopment Corp", owner_name, ignore.case = TRUE),
      "Land Trust",owner_renter),
    # Misc owner type
    owner_renter = ifelse(
      grepl("LA VINA HOMEOWNERS|CAMERON,MARGARET K|CAMERON,JOHN K|LINCOLN AVENUE WATER CO", owner_name, ignore.case = TRUE) & owner_renter == "Other",
      "Other or Unknown Owner",
      ifelse(owner_renter=="Other","Likely owner-occupied, no exemption", owner_renter)),
  # Misc owner type for missing owner name
  owner_renter = ifelse(
    is.na(owner_name) & owner_renter=="Likely owner-occupied, no exemption", "Other or Unknown Owner",
    owner_renter)) %>%
  
      # clean up final names
  mutate(owner_renter_orig=owner_renter,
         owner_renter=case_when(
           owner_renter_orig=="Church/Welfare exemption" ~ "Church, charity, or nonprofit owned",
           owner_renter_orig=="Likely owner-occupied, no exemption" ~ "Likely owner occupied, no exemption",
           owner_renter_orig=="LLC owned" ~ "Corporation owned",
           owner_renter_orig=="Other" ~ "Other or unknown ownership",
           owner_renter_orig=="Owner & Renter occupied" ~ "Owner & renter occupied",
           owner_renter_orig=="SBE or Government owned" ~ "Government owned",
           owner_renter_orig=="Owner occupied" ~ "Owner occupied, homeowner exemption",
           TRUE ~ owner_renter_orig
         ))

# Check owner and renter recoding
sum(is.na(data_owner$owner_renter)) 
# 0

data_owner %>% count(owner_renter)
# March pre-update QA
# owner_renter    n
# 1  Church, charity, or nonprofit owned   13 # went down - added land trust
# 2                    Corporation owned  221 # went up
# 3                     Government owned    2 # no change
# 4                           Land Trust    4 # added
# 5  Likely owner occupied, no exemption 1250 # went up
# 6               Other or Unknown Owner   54 # went up
# 7              Owner & renter occupied  384 # down
# 8  Owner occupied, homeowner exemption 2517 # went down
# 9                      Renter occupied  512 # went down
# 10                       Sold to state   31 # same
# 11                         Trust owned  688 # went down

# March/April 2026 Update
# owner_renter    n
# 1  Church, charity, or nonprofit owned   11 # down
# 2                    Corporation owned  253 # up
# 3                     Government owned    2 # no change
# 4                           Land Trust    5 # no change
# 5  Likely owner occupied, no exemption 1255 # up
# 6               Other or Unknown Owner   48 # down
# 7              Owner & renter occupied  385 # no change
# 8  Owner occupied, homeowner exemption 2479 # down
# 9                      Renter occupied  517 # up
# 10                       Sold to state   28 # down
# 11                         Trust owned  693 # up

###### *QA and Update* - Review likely owner occupied and update recoding as needed ----------
likely_homeowner <-data_owner %>% filter(owner_renter=="Likely owner occupied, no exemption") %>%
  select(ain, owner_renter, tax_stat_key, year_sold_to_state, owner_name, exemption_type, num_howmowner_exemption, total_units, landlord_units, owner_renter)
View(likely_homeowner)

likely_homeowner_table <- likely_homeowner %>%
  group_by(owner_name) %>%
  summarise(count=n())
View(likely_homeowner_table) # sort desc by count
# looks okay

# check NA owner
na_owner_name <- data_owner %>% filter(is.na(owner_name))
View(na_owner_name)
na_owner_name_assessor <- assessor_data %>% filter(ain %in% na_owner_name$ain)
# ANFS Test - most parcels are just missing assessor data, one has been sold but has no updated owner name
# na owner name should be other/unknown

# check others by filtering dataframe view for other property types
data_owner %>% select(owner_renter,owner_name, everything()) %>% View()

#### STEP 7: CLEAN UP DF AND ADD PARCELS WITH MISSING DATA: UPDATE ####
final_df<- data_owner %>% 
  # add owner address field for private dashboard
  mutate(owner_zip=gsub("0000","",m_zip)) %>%
  mutate(owner_city_state=gsub(" CA", ", CA", m_city_state)) %>% 
  # convert fractional address back to normal character
  mutate(m_fraction_re=format(as.Date(m_fraction, format = "%d-%b"), "%m/%d"),
         m_fraction_re=gsub("0","",m_fraction_re)) %>%
  # make blanks na
  mutate(m_direction=na_if(m_direction,""),
         m_unit=na_if(m_unit,"")) %>%
  mutate(owner_address=paste(mail_house_no, m_fraction_re, m_direction, m_street_name, m_unit, owner_city_state, owner_zip) %>%
           # remove extra NAs
           str_replace_all("\\bNA\\b", "") %>%
           str_squish()) %>%
  # remove 0 from start of PO Boxes
  mutate(owner_address=str_remove(owner_address, "^0 ")) %>%
  select(ain, owner_name, owner_renter, owner_address, sold_source) %>%
  rename(ain_2026_04 = ain) %>%
  # make owner address unavailable when sold source is anfs, we only have site address from anfs not owner contact
  mutate(owner_address=case_when(
    sold_source=='anfs' ~ 'Not Available',
    TRUE ~ owner_address
  ))

# check for duplicates
nrow(final_df)-length(unique(final_df$ain_2026_04)) # should be 0 difference

# check for same number of rows as xwalk
nrow(final_df)-length(unique(xwalk$ain_2026_04)) # should be 0 difference

# check for NA owner type - should be 0
table(final_df$owner_renter,useNA='always')

#### STEP 8: PUSH TO PGADMIN (NO UPDATES NEEDED) ####

# Export to postgres
table_label <- paste0("rel_assessor_owner_", year, "_", month)
schema <- "dashboard"
indicator <- "Relational table with summarized information owner type and owner address recorded for current month residential parcels that were in Altadena in january 2025, selected based on crosswalk and keeping only the january 2025 parcels in Altadena. 
Owner type created based on combination of rental units, exemptions on property, tax status, and owner name. 
For recent sales just recorded in Altadena not for sale, only owner name is used to create owner renter type, not the assessor data which may not be associated with most recent sale"
source <- "Script: altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assessor_residential.R "
qa_filepath<-"  QA_sheet_rel_tables_update_2026_04.docx "

# dbWriteTable(con_alt, Id(schema, table_label), final_df,
#              overwrite = FALSE, row.names = FALSE)

# Add metadata
column_names <- colnames(final_df) # Get column names

column_comments <- c('Assessor ID number for current month- use this to match to other relational tables ',
                     'owner name based on combination of assessor data and altadena not for sale data - owner name from altadena not for sale for recently sold parcels',
                     'Housing tenure-ownerships -- either homeowner (indicated by homeowner exemption), renter, trust owned, corporation owned, sold to state, government owned, land trust, other or unknown ownership, or owner likely but with no exemption. For more detailed methodology of choices see R script',
                     'Owner mailing address - different from property site address - only for private dashboard',
                     'sold source - lac, anfs, both - indicators where owner data came from')

# add_table_comments(con_alt, schema, table_label, indicator, source, qa_filepath, column_names, column_comments)

#### STEP 9: close dbconnection (NO UPDATES NEEDED) ####
dbDisconnect(con_alt)
