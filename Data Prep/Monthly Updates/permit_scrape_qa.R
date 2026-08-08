
##### Step 0: Set Up #####
#load libraries
library(dplyr)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\Monthly Updates\\functions.R")

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
  summarise(total_permits_prev = sum(!is.na(record_id))) %>%
  # convert ain to current ain vintage
  mutate(transformed_ain = transform_ain_to_curr(ain, xwalk_df = xwalk, curr_ain = "ain_2026_08"))

prev_permits <- dbGetQuery(con, sprintf("SELECT * FROM %s.scraped_general_permit_data_%s_%s;", 
                                               schema, prev_year, prev_month))

prev_permit_counts <- dbGetQuery(con, sprintf("SELECT ain, total_permits FROM %s.rel_parcel_rebuild_status_%s_%s;", 
                                              schema, prev_year, prev_month))

curr_general_scrape <- dbGetQuery(con, sprintf("SELECT * FROM %s.scraped_general_permit_data_%s_%s;", 
                                               schema, curr_year, curr_month)) %>%
  group_by(ain) %>%
  summarise(total_permits_curr = sum(!is.na(record_id))) %>%
  # convert ain to current ain vintage
  mutate(transformed_ain = transform_ain_to_curr(ain, xwalk_df = xwalk, curr_ain = "ain_2026_08"))
  

curr_permit_counts <- dbGetQuery(con, sprintf("SELECT ain, total_permits FROM %s.rel_parcel_rebuild_status_%s_%s;", 
                                           schema, curr_year, curr_month))

xwalk <- dbGetQuery(con, sprintf("SELECT * FROM %s.crosswalk_assessor_%s_%s_%s;",
                                                  schema, curr_year, prev_month, curr_month)) 
dbDisconnect(con)

# compare to permit counts from this update or scrape
nrow(prev_general_scrape)
nrow(curr_general_scrape)

qa_scrape <- prev_general_scrape %>%
  full_join(curr_general_scrape, by = "transformed_ain", suffix = c("_prev", "_curr")) %>%
  group_by(transformed_ain) %>%
  summarise(
    total_permits_prev = max(total_permits_prev, na.rm=T),
    total_permits_curr = max(total_permits_curr, na.rm=T)
  ) %>%
  mutate(
    diff = ifelse(is.infinite(total_permits_curr), 0 - total_permits_prev, total_permits_curr-total_permits_prev),
    # flag AINs that have fewer permits than last time.
    red_flag = ifelse(diff < 0, 1, 0),
    missing_ain = ifelse(is.infinite(total_permits_curr), 1, 0))

sum(qa_scrape$red_flag) # 1
sum(qa_scrape$missing_ain) # 4

# Review scrape flags
# Red flags
red_flags <- qa_scrape %>% filter(red_flag==1)
check <- final_data %>% 
  mutate(transformed_ain = transform_ain_to_curr(ain, xwalk_df = xwalk, curr_ain = "ain_2026_08")) %>%
  filter(ain %in% red_flags$transformed_ain)
table(check$ain)
# 5846005002 
# 1 
# Reviewed epic la and only 1 permit exists this time. In previous scrape we pulled a second permit with status Cancelled - this is ok

# Missing
missing_flags <- qa_scrape %>% filter(missing_ain==1)
check <- final_data %>% 
  mutate(transformed_ain = transform_ain_to_curr(ain, xwalk_df = xwalk, curr_ain = "ain_2026_08")) %>%
  filter(ain %in% missing_flags$transformed_ain)
table(check$ain) # 0
table(missing_flags$transformed_ain)
# 5841007024 5842008018 5843023069 5843023070 
# 1          1          1          1
# Reviewed our xwalk and these AINS do not exist
# Reviewed assessor portal and EPIC LA
# 5841007024 - No response / No permits - Nothing to do
# 5842008018 - Shell that is being updated to 5842008017/ No permits - Nothing to do
# 5843023069 - Shell being updated to 5843023068 / No permits - Nothing to do
# 5843023070 - No response - should be 5843023037/ Has 1 permit under 5843023070 and 9 under 5843023037 which is captured - Nothing to do the 1 permit is minor/misc the other permits are explicitly about Eaton fire damage.

# qa_df <- prev_permit_counts %>%
#   left_join(select(xwalk, ain_2026_04, ain_2026_08), by=c("ain"="ain_2026_04")) %>%
#   left_join(curr_permit_counts, by = c("ain_2026_08"="ain"), suffix = c("_prev", "_curr")) %>%
#   mutate(
#     diff = total_permits_curr - total_permits_prev,
#     # flag AINs that have fewer permits than last time.
#     red_flag = ifelse(diff < 0, 1, 0))
# 
# sum(qa_df$red_flag) # 651 of them have fewer current permits than prev, rest are 0 in both
