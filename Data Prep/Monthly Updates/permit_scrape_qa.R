
##### Step 0: Set Up #####
#load libraries
library(dplyr)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")

curr_xwalk_year <- "2026" # year
curr_xwalk_month <- "08"
prev_xwalk_month <- "04"
schema <- "dashboard"
curr_year <- "2026" # current update year
curr_month <- "08" # current update month
prev_year <- "2026" # prev update year
prev_month <- "04" # prev update month


# get list of permit counts per ain from last update or scrape
con <- connect_to_db("altadena_recovery_rebuild")
prev_general_scrape <- dbGetQuery(con, sprintf("SELECT * FROM %s.scraped_general_permit_data_%s_%s;", 
                                               schema, prev_year, prev_month)) %>%
  group_by(ain) %>%
  summarise(total_permits_prev = sum(!is.na(record_id)))

prev_permit_counts <- dbGetQuery(con, sprintf("SELECT ain, total_permits FROM %s.rel_parcel_rebuild_status_%s_%s;", 
                                              schema, prev_year, prev_month))

curr_general_scrape <- dbGetQuery(con, sprintf("SELECT * FROM %s.scraped_general_permit_data_%s_%s;", 
                                               schema, curr_year, curr_month)) %>%
  group_by(ain) %>%
  summarise(total_permits_curr = sum(!is.na(record_id)))

curr_permit_counts <- dbGetQuery(con, sprintf("SELECT ain, total_permits FROM %s.rel_parcel_rebuild_status_%s_%s;", 
                                           schema, curr_year, curr_month))

xwalk <- dbGetQuery(con, sprintf("SELECT * FROM %s.crosswalk_assessor_%s_%s_%s;",
                                                  schema, curr_year, prev_month, curr_month)) 
dbDisconnect(con)

# compare to permit counts from this update or scrape
nrow(prev_general_scrape)
nrow(curr_general_scrape)

qa_scrape <- prev_general_scrape %>%
  left_join(select(xwalk, ain_2025_01, ain_2026_04, ain_2026_08), by=c("ain"="ain_2025_01")) %>%
  left_join(select(xwalk, ain_2026_04, ain_2026_08), by=c("ain"="ain_2026_04")) %>%
  mutate(ain_2026_08 = coalesce(ain_2026_08.y, ain_2026_08.x)) %>%
  select(-c(ain_2026_08.y, ain_2026_08.x)) %>%
           
  left_join(curr_general_scrape, by = c("ain_2026_08"="ain"), suffix = c("_prev", "_curr")) %>%
  mutate(
    diff = ifelse(is.na(ain_2026_08), 0-total_permits_prev, total_permits_curr - total_permits_prev),
    # flag AINs that have fewer permits than last time.
    red_flag = ifelse(diff < 0, 1, 0),
    missing_ain = ifelse(is.na(ain_2026_08), 1, 0))

sum(qa_scrape$red_flag) # 4
sum(qa_scrape$missing_ain) # 4

# Review flags


# qa_df <- prev_permit_counts %>%
#   left_join(select(xwalk, ain_2026_04, ain_2026_08), by=c("ain"="ain_2026_04")) %>%
#   left_join(curr_permit_counts, by = c("ain_2026_08"="ain"), suffix = c("_prev", "_curr")) %>%
#   mutate(
#     diff = total_permits_curr - total_permits_prev,
#     # flag AINs that have fewer permits than last time.
#     red_flag = ifelse(diff < 0, 1, 0))
# 
# sum(qa_df$red_flag) # 651 of them have fewer current permits than prev, rest are 0 in both
