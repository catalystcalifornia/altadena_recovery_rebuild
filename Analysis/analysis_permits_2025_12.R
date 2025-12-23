## Producing tables for characteristics of properties that have started permitting - for January release write-up

#### Step 0: Set Up ####
#load libraries
library(dplyr)
library(purrr)
library(stringr)
library(RPostgres)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)


#### Step 1: Pull in relational datasets ####
housing_dec <- st_read(con, query="SELECT * FROM dashboard.rel_assessor_residential_2025_12") 

permits <- st_read(con, query="SELECT ain, rebuild_status, dashboard_label FROM dashboard.rel_parcel_rebuild_status_2025_12")

#### Step 2: Explore permit data -----
duplicate_rows <- permits[(duplicated(permits$ain, fromLast = FALSE) | duplicated(permits$ain, fromLast = TRUE)), ]
# no duplicates

# select just the residential parcels - should be the same in december as it only includes residential
permits_res <- permits %>%
  filter(ain %in% housing_dec$ain)

nrow(permits_res)
length(unique(permits_res$ain))
# all unique

table(permits_res$rebuild_status,useNA='always') # confirm no NA's

# join data together
all_df <- housing_dec %>% 
  left_join(permits_res, by=c("ain_2025_12"="ain"))

# check
table(all_df$res_type,useNA='always')
table(all_df$rebuild_status,useNA='always') 
table(all_df$owner_renter,useNA='always')
nrow(all_df)
length(unique(all_df$ain))

#### Step 3: Overall ANALYSIS- [analysis_permits_damage_dec2025] ####
## what % of significantly damaged properties have started permits?
analysis_damage <- all_df %>% 
  mutate(area_name="Altadena",
         total=n()) %>% # total properties--all are significantly damaged in our universe now
  ungroup() %>%
  group_by(area_name,rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

# Upload tables to postgres and add table/column comments
dbWriteTable(con, name = "analysis_permits_sigdamage_dec2025", value = analysis_damage, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_permits_sigdamage_dec2025"
indicator <- "Distribution of permitting stages for significantly damaged properties as of December 2025, e.g., what % of significantly damaged residential properties in Altadena are awaiting construction"
source <- "Source: LA County Assessor Data, Dec 2025 & Scraped data Dec 2025."
qa_filepath <- "  "
column_names <- colnames(analysis_damage) # Get column names
column_names
column_comments <- c(
  "area",
  "permit status",
  "count of properties - numerator",
  "percent of properties ",
  "total residential properties - denominator")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#### Step 4: ANALYSIS  by ownership [analysis_permits_owner_renter_dec2025] ####
# At what rate are properties rebuilding by ownership type for significantly damaged properties
analysis_permits_owner <- all_df %>% 
  mutate(area_name="Altadena") %>%
  group_by(area_name,owner_renter) %>% 
  mutate(total=n()) %>% # total in owner category
  ungroup() %>%
  group_by(area_name,owner_renter,rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

# sum prc that have at least applied for permits
analysis_permits_owner <- analysis_permits_owner %>%
  group_by(area_name, owner_renter) %>%
  mutate(prc_permit_min=sum(prc[!rebuild_status %in% c("Permit Application Not Received","Fire Debris Removal Incomplete")],na.rm=TRUE),
         prc_no_permit=sum(prc[rebuild_status %in% c("Permit Application Not Received","Fire Debris Removal Incomplete")],na.rm=TRUE)
         )

distinct(analysis_permits_owner, owner_renter, prc_permit_min, prc_no_permit)

# Upload tables to postgres and add table/column comments
dbWriteTable(con, name = "analysis_permits_owner_renter_dec2025", value = analysis_permits_owner, overwrite = FALSE)
schema <- "data"
table_name <- "analysis_permits_owner_renter_dec2025"
indicator <- "Distribution of permitting stages by ownership type in Altadena for significantly damaged residential properties only, e.g., what % of significantly damaged owner-occupied properties have applied for permits but are awaiting construction"
source <- "Source: LA County Assessor Data, December 2025 & Scraped data."
qa_filepath <- " "
column_names <- colnames(analysis_permits_owner) # Get column names
column_names
column_comments <- c(
  "area",
  "ownership type",
  "permit status",
  "count of properties - numerator",
  "percent of properties ",
  "total residential properties in that owner type that were significantly damaged - denominator",
  "prc of properties in that owner type that have at least applied for permits, started construction, or are completed repairs",
  "prc of properties in that owner type that have not applied for permits or have not completed fire debris removal")
add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 5: ANALYSIS by res type  - didnt push to postgres ####
# At what rate are properties rebuilding by residential type for significantly damaged properties
analysis_permits_restype <- all_df %>% 
  mutate(area_name="Altadena") %>%
  group_by(area_name,res_type) %>% 
  mutate(total=n()) %>% # total in that residential type
  ungroup() %>%
  group_by(area_name,res_type,rebuild_status) %>%
  summarise(count=n(),
            prc=n()/min(total)*100,
            total=min(total))

# sum prc that have at least applied for permits
analysis_permits_restype  <- analysis_permits_restype  %>%
  group_by(area_name, res_type) %>%
  mutate(prc_permit_min=sum(prc[!rebuild_status %in% c("Permit Application Not Received","Fire Debris Removal Incomplete")],na.rm=TRUE))

distinct(analysis_permits_restype, res_type, prc_permit_min)


#### Step 8: close connection ####
dbDisconnect(con)
