# Script to assign permit stages for Altadena parcels with Some/Severe damage

##### Typology Outline #####
### 1. Fire Debris Removal (Is the site cleaned up?)
## Phase 1 Result:
# Yes (EPA clean up, parcel progressed to phase 2)
# No (Not completed)
# Deferred (Clean up deferred to Army Corps, parcel progressed to phase 2)
## Phase 2 Result:
# Yes (Army Corps completed cleanup)
# Yes (Private contractor completed clean up - Assumed if parcel has "^FDR" permit with a "Finaled" status)
# No (Not completed)
## Overall Yes if Phase 2 Result is Yes, For now assume yes also if parcel qualifies for Bucket 2 (below)  
## Overall No otherwise (could specify at what stage but will not implement now)

### 2. Permit Application Received (Have folks asked for permits to rebuild?)
## Yes
# Sub-bucket: Permanent Housing (permit_number "^CREB|UNC-BLDR|UNC-BLDF" AND project_name or description.detailed contains "ADU|SFR|SFD|SB9|story|duplex|dwelling|rebuild burned house|main house|residence|dwelling|pre-approved standard plan|rebuild house|mfr|)
# Sub-bucket: Temporary Housing (permit_number "^UNC-EXPR") AND project_name or description.detailed contains "temp hous|temporary hous"
# Sub-bucket: Garages, Pools, Etc. (if none of the others, then default to this bucket)
# Sub-bucket: Commercial (permit_number "^UNC-BLDC")
## No - Has no flags

### 3. In Construction (Are folks in construction process?)
## Yes
# Parcel has Yes for Bucket 2 AND workflow.item that contains "inspection" (excl. "debris removal inspection")
## No (default if not yes)

### 4. Construction Complete (Are folks moved back in to their homes)
## Yes: Qualifies for bucket 3 AND
# Sub-bucket 4.1: Permanent Housing Complete - Bucket 2.1 == Yes AND permit_number starts with “CREB”, “UNC-BLDR”, or “UNC-BLDF” and with status.detailed = “Finaled” 
# Sub-bucket 4.2: Temporary Housing Complete - Bucket 2.2 == Yes AND permit_number starts with “UNC-EXPR” And status.detailed = “Finaled” 
# Sub-bucket 4.3: Garages, Pools, etc., Complete (Alt name like Misc./Utility/Non housing?) - Bucket 2.3 == Yes And status.detailed = “Finaled” for ALL permits associated with this AIN (may be less accurate; possibly exclude?)
# Sub-bucket 4.4: Commercial - Bucket 2.4 == Yes AND permit_number starts with “UNC-BLDC” AND status.detailed = “Finaled”

##### Step 0: Set Up #####
#load libraries
library(dplyr)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
source("Data Prep\\Monthly Updates\\functions.R")
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)
date_ran <- as.character(Sys.Date())
curr_xwalk_year <- "2026" # year
curr_xwalk_month <- "04"
prev_xwalk_month <- "12"
schema <- "dashboard"
curr_year <- "2026" # current update year
curr_month <- "04" # current update month
prev_year <- "2025" # prev update year
prev_month <- "12" # prev update month

ain_curr <- sprintf("ain_%s_%s", curr_year, curr_month)

# load data
xwalk_parcels <- dbGetQuery(con, sprintf("SELECT * FROM %s.crosswalk_assessor_%s_%s_%s;",
                                         schema, curr_xwalk_year, prev_xwalk_month, curr_xwalk_month)) 

# get debris removal data
debris_usace <- dbGetQuery(con, "SELECT apn, ain, epa_status, roe_status, fso_pkg_received, fso_pkg_approved FROM data.usace_debris_removal_parcels_2025;")

# get jan universe
jan_universe <- dbGetQuery(con, "SELECT * FROM dashboard.parcel_universe_2025_01;")

permits_orig <- dbGetQuery(con, sprintf("SELECT gen.ain, gen.permit_number, gen.record_id, gen.applied_date, 
gen.type, gen.issued_date, gen.project_name, gen.expiration_date, 
gen.status as gen_status,  gen.finalized_date, gen.main_parcel, gen.address, 
det.description, det.completed_percent, 
det.in_progress_percent, det.not_started_percent, 
wf.workflow_item, 
wf.status as wf_status, 
wf.status_date as wf_status_date 
FROM %s.scraped_general_permit_data_%s_%s gen 
LEFT JOIN %s.scraped_detailed_permit_data_%s_%s det
ON gen.ain = det.ain AND gen.permit_number = det.permit_number 
LEFT JOIN %s.scraped_workflow_permit_data_%s_%s wf
ON gen.ain = wf.ain AND gen.permit_number = wf.permit_number;", 
                                        schema, curr_year, curr_month,
                                        schema, curr_year, curr_month,
                                        schema, curr_year, curr_month))

table(permits_orig$gen_status, useNA="ifany")

# look at permit numbers
permits_substring <- permits_orig %>%
  mutate(permit_sub=substring(permit_number, 1,4), # if you look at first 8 you'll see the building codes under UNC- noted below
         permit_sub_unc=ifelse(permit_sub=='UNC-', substring(permit_number, 1,8), NA)) # pull UNC types
         
table(permits_substring$permit_sub)
# CREB County Disaster recovery Permit Rebuild Project
# FCD Flood access/construction permit
# FCR Construction & Demolition Final Compliance
# FIRE tree removal
# PROP Property report
# PWRP Road permits
# RRP Construction & Demolition Deposit
# SWRC Sewer Tap & Saddle Installation
# UNC- Mechanical, Plumbing, Electrical permits (and building permits for SFR, MFR, Temporary Housing, Commercial buildings, etc.)

table(permits_substring$permit_sub_unc)
# April 2026
# UNC-BLDC UNC-BLDF UNC-BLDG UNC-BLDM UNC-BLDR UNC-ELEC UNC-EXPR UNC-GRAD UNC-MECH UNC-PLMB UNC-PLSP UNC-SEWR UNC-SOLR 
# 49      231     1350       26    36138    10245      442      305     5695     6718      418     4182      816 

# UNC-BLDC UNC-BLDF UNC-BLDG UNC-BLDM UNC-BLDR UNC-ELEC UNC-EXPR UNC-GRAD UNC-MECH UNC-PLMB UNC-PLSP UNC-SEWR UNC-SOLR 
# 25      206      805       20    26859     6801      337      211     3882     4388      218     2668      764 

# https://permits.lacounty.gov/permits/building-and-safety/
# building permit types
# UNC-BLDC - Commercial
# UNC-BLDF - Multifamily
# UNC-BLDG - Retaining Wall or Fence Permit
# UNC-BLDM - Mixed use
# UNC-BLDR - Residential
# UNC-ELEC - Electrical
# UNC-EXPR - Express - electrical, plumbing, temporary housing
# UNC-GRAD - Grading
# UNC-MECH - Mechanical
# UNC-PLMB - Plumbing
# UNC-PLSP - Pool/spa permit
# UNC-SEWR - Sewer
# UNC-SOLR - Solar

# based on QA, flag ones that are just repairs or express permits -- if these are the only repairs completed on a property they likely have more work to do
# UNC-BLDG UNC-ELEC UNC-EXPR UNC-GRAD UNC-MECH UNC-PLMB UNC-PLSP UNC-SEWR UNC-SOLR


##### 1. Prep data #####
debris_transformed <- debris_usace %>% 
  select(ain, fso_pkg_approved) %>%
  # filter for target universe
  filter(ain %in% jan_universe$ain_2025_01) %>%
  # transform to dec ain
  left_join(xwalk_parcels %>% select(starts_with("ain_")), by=c("ain"="ain_2025_01")) 
  

# check length and NAs
length(unique(debris_transformed$ain))
length(unique(debris_transformed$ain_2025_12))
debris_transformed %>% group_by(ain) %>% filter(n()>1)
# one duplicate ain 5842008010

# get army corps fire debris removal status
debris_status <- debris_transformed %>%
  mutate(
    # bucket 1 helper: does the parcel have full sign off (fso) from army corps
    b1_has_ace_fso = ifelse(!is.na(fso_pkg_approved), 1, 0)) %>%
  # 5841023010 and 5841023009 merge into dec ain 5841023022 and have same status
  select(-ain) %>%
  group_by(.data[[ain_curr]]) %>%
  summarise(b1_has_ace_fso=max(b1_has_ace_fso)) %>%
  rename(ain=all_of(ain_curr))

length(unique(debris_status$ain)) # 5673
debris_status %>% group_by(ain)%>% filter(n()>1)
table(debris_status$b1_has_ace_fso, useNA="ifany")
# Dec Update 2025: 0    1 
#             396 5277 
# March 2026 Prelim: 0    1 
#               396 5277 
# April 2026
# same - can probably make this a permanent pg table?

# Filter permits for applied date after Jan 7, 2025
permits_filtered <- permits_orig %>%
  # note: this filter happens in the general permit scraping script - we can remove but leaving for now while we prep the December update
  filter(as.Date(applied_date, format = "%m/%d/%Y") > as.Date("2025-01-07")) %>%
  # filter out voided, canceled, denied permits - also applied in permit scraping
  filter(!(gen_status %in% c("Void", "Canceled", "Denied"))) %>%
  mutate(is_creb = ifelse(grepl("^CREB", permit_number), 1, 0)) 

check_creb <- permits_filtered %>% select(ain, is_creb) %>%
  group_by(ain) %>%
  mutate(has_creb = ifelse(sum(is_creb, na.rm=TRUE)>0, 1, 0))

parcels_creb <- check_creb %>% select(ain, has_creb) %>% unique()

# check number of creb permits
table(permits_filtered$is_creb,useNA='always')
# Dec 2025 update 2081 CREB permits
# March 2026 Prelim 5342 CREB permits
# April 2026 update 7196 CREBs

# check status to make sure no new statuses to filter out
table(permits_filtered$gen_status, useNA="ifany")

nrow(permits_filtered) 
# 48360
# March 2026 prelim: 65122
# April 2026: 74829

# permit xwalk - need to have the same ain across datasets to group by the correct current parcel
permit_xwalk <- permits_filtered %>% 
  distinct(ain) %>% # 3250
  mutate(ain_curr = transform_ain_to_curr(ain, xwalk_parcels, curr_ain = ain_curr))
  # # transform to dec ains
  # # first join to jan ains to get a match in the xwalk that way
  # left_join(xwalk_parcels %>% select(starts_with("ain_")), by=c("ain"="ain_2025_01")) %>%
  # # some ains may be the new december or september ain so try a match that way next
  # left_join(xwalk_parcels %>% select(starts_with("ain_")), by=c("ain"="ain_2025_12")) %>%
  # # recode everything to december
  # mutate(ain_2025_12_orig=ain_2025_12,
  #   ain_2025_12=case_when(!is.na(ain_2025_12_orig) ~ ain_2025_12_orig,
  #                      is.na(ain_2025_12_orig) ~ ain,
  #                      TRUE ~ NA))

# now add xwalk to permits filtered
permits_filtered_curr_ains <- permits_filtered %>%
  left_join(permit_xwalk %>% select(ain, ain_curr), by=c("ain"="ain"))

# march prelim: above returns many to many warnings - makes sense since one ain can have multiple permits?
check <- permits_filtered_curr_ains %>%
  group_by(ain) %>%
  filter(n()>1)

# check
sum(is.na(permits_filtered_curr_ains$ain_curr))
# no NAs
# March prelim: no NAs
# April 2026: No NAs

length(unique(permits_filtered_curr_ains$ain_curr))
# 2640
# March prelim: 3149
# April 2026: 3249

permits_filtered_curr_ains <- permits_filtered_curr_ains %>%
  rename(ain_scrape=ain,
         ain=ain_curr)

# check for NAs in WF status data
sum(is.na(permits_filtered_curr_ains$wf_status_date))
sum(is.na(permits_filtered_curr_ains$wf_status))

# get workflow items and add has_inspection (will use for the bucket 3 check - construction has started)
workflow_all <- permits_filtered_curr_ains %>% 
  select(ain, permit_number, workflow_item, wf_status, wf_status_date) %>%
  unique() %>%
  # filter out FDR permits (those are pre-build and "debris removal inspection" is not sufficient to exclude)
  mutate(is_fdr = ifelse(grepl("^FDR", permit_number), 1, 0)) %>%
  filter(is_fdr==0) %>%
  # filter out those that do not have a wf status date AND wf status, if at least one exists it will indicate if inspection is planned or in some stage of passing (indicating construction has begun)
  filter(wf_status_date != "" | wf_status != "") %>%
  # flag that a workflow status or date exists
  mutate(has_status_or_date = case_when(wf_status != "" ~1,
                                        wf_status_date != "" ~1,
                                        .default=0)) %>%
  # add bucket 3 helper columns - has an inspection occurred based on workflow description (excluding debris removal)
  mutate(b3_has_inspection = ifelse(grepl("inspection", workflow_item, ignore.case = TRUE), 1, 0)) 

# check recode
check <- workflow_all %>% group_by(b3_has_inspection,workflow_item,wf_status) %>% summarise(count=n())
View(workflow_all)
# confirmed NAs are not interrupting code result

# summarize to the permit level
workflow <- workflow_all %>%
  group_by(ain, permit_number) %>%
  mutate(b3_has_inspection = ifelse(sum(b3_has_inspection, na.rm = TRUE)>0, 1, 0)) %>%
  select(ain, permit_number, b3_has_inspection) %>%
  unique() 

# 7679 
# march prelim: 10097
# april 2026: 11134

# check counts and recoding
check <- workflow %>% group_by(ain,permit_number) %>% summarise(count=n())
workflow %>% distinct(ain,permit_number) %>% nrow() 
# 7679 # should match number of rows in workflow df
# april 2026: 11134
length(unique(workflow$permit_number)) 
#7610, permit numbers are not necessarily unique - a permit can be associated be associated with multiple ains and have multiple workflow items (inspections, etc.)
# March prelim: 10097 unique permit-ain combos; 10029 unique permit numbers 
# April 2026:  11064

# check result
table(workflow$b3_has_inspection, useNA = "ifany")
# Dec Update
# 0    1 
# 5724 1955 

# March prelim
# 0    1 
# 6833 3264

# April 2026
# 0    1 
# 7212 3922

# permit level data
# key words used to determine permits related to residential permanent housing
keyword_list <- c("ADU", "SFR", "SFD", "SB9", "story", "duplex", "dwelling", 
                  "rebuild burned house", "main house", "residence", "pre-approved standard plan",
                  "rebuild house", "mfr", "mfd")

# create a primary permit df of helper columns for each bucket classification (section 2. Apply Typology)
# this df is at permit-level and in section 2 will be aggregated to a parcel-level table called final_types
permits <- permits_filtered_curr_ains %>%
  # remove workflow items to get a dataframe of just permit-level cols
  select(-c(workflow_item, wf_status, wf_status_date,ain_scrape)) %>%
  # keep unique permits - drop workflow items 
  unique() %>%
  
  # add helper cols
  mutate(
    # is this an FDR (Fire Debris Removal) permit
    b1_has_fdr=ifelse(grepl("^FDR", permit_number), 1, 0),
    # is this permit's status finaled - will use for other buckets too
    b4_has_finaled = ifelse(gen_status=="Finaled", 1, 0)) %>% 
  # bucket 1 helper column: check is fdr is finaled
  mutate(
    b1_has_fdr_finaled = case_when(
      (b1_has_fdr==1 & b4_has_finaled==1) ~ 1,
      .default = 0)) %>%
  # add bucket 2 helper columns
  mutate(
    # add flag for key word for reference and QA
    is_keyword=ifelse(grepl(paste(keyword_list, collapse="|"), project_name, ignore.case = TRUE) |
                        grepl(paste(keyword_list, collapse="|"), description, ignore.case = TRUE),1,0),
    is_tempword=ifelse( grepl("temp hous|temporary hous", description, ignore.case = TRUE),1,0),
    # is this permit associated with rebuilding structures (e.g., perm, temp, comm, misc)
    b2_has_build_permit = ifelse(grepl("^(CREB|UNC-)", permit_number), 1, 0),
    # if the permit is for permanent housing, saves permit_number here
    b2_perm = ifelse((grepl("^(CREB|UNC-BLDR|UNC-BLDF)", permit_number) & 
                        (grepl(paste(keyword_list, collapse="|"), project_name, ignore.case = TRUE) |
                           grepl(paste(keyword_list, collapse="|"), description, ignore.case = TRUE))), permit_number, ""),
    # if the permit is for temp housing, saves permit_number here
    b2_temp = ifelse((grepl("^UNC-EXPR", permit_number)& 
                        (grepl("temp hous|temporary hous", project_name, ignore.case = TRUE) |
                           grepl("temp hous|temporary hous", description, ignore.case = TRUE))), permit_number, ""),
    # if the permit is for commercial building, saves permit_number here
    b2_comm = ifelse(grepl("^UNC-BLDC", permit_number), permit_number, ""), 
    # if the permit does not have a known building permit prefix, saves permit_number here
    b2_other = ifelse(!grepl("^(CREB|UNC-)", permit_number), permit_number, "")) %>% # this includes fire debris and removal, but is kept for reference and not used in estimates
  mutate(
    # if the permit has a known rebuild prefix but doesn't belong under permanent, temporary, or commercial, put here
    b2_misc = ifelse((b2_has_build_permit==1 & paste0(b2_perm, b2_temp, b2_comm)==""), permit_number, ""),
    # if permit is a UNC (build permit), but its just related to mechanical, plumbing, etc. and other minor repairs put here
    b2_misc_minor = ifelse(grepl("^(UNC-BLDG|UNC-ELEC|UNC-EXPR|UNC-GRAD|UNC-MECH|UNC-PLMB|UNC-PLSP|UNC-SEWR|UNC-SOLR)", permit_number), permit_number, "") 
  ) %>%
  # bucket 4 helper cols
  mutate(
    # specifically is this a finaled permit for permanent housing
    b4_has_finaled_perm = ifelse((b2_perm != "" &
                                    b4_has_finaled==1), 1, 0),
    # specifically is this a finaled permit for temp housing
    b4_has_finaled_temp = ifelse((b2_temp != "" &
                                    b4_has_finaled==1), 1, 0),
    # specifically is this a finaled permit for commercial building
    b4_has_finaled_comm = ifelse((b2_comm !="" &
                                    b4_has_finaled==1), 1, 0),
    # specifically is this a finaled permit for misc construction related to rebuilding or repairs
    b4_has_finaled_misc = ifelse((b2_misc != "" &
                                    b4_has_finaled==1), 1, 0),
    # specifically is this a finaled permit for misc construction related to MINOR just mechanical, electrical, solar, plumbing, grading, pool repairs
    b4_has_finaled_misc_minor = ifelse((b2_misc_minor != "" &
                                    b4_has_finaled==1), 1, 0)) 

# check counts of variables
table(permits$gen_status, useNA = "ifany") 
table(permits$b1_has_fdr, useNA = "ifany")
table(permits$b1_has_fdr_finaled, useNA = "ifany")
table(permits$b2_has_build_permit, useNA = "ifany")
table(permits$b4_has_finaled, useNA = "ifany")
table(permits$b4_has_finaled_perm, useNA = "ifany")
table(permits$b4_has_finaled_temp, useNA = "ifany")
table(permits$b4_has_finaled_comm, useNA = "ifany")
table(permits$b4_has_finaled_misc, useNA = "ifany")
table(permits$b4_has_finaled_misc_minor, useNA = "ifany")


# check recoding of variables
## fdr
permits %>%
  filter(b1_has_fdr==1) %>%
  View()

## finaled
permits %>%
  filter(b4_has_finaled==1) %>%
  View()

## finaled perm
permits %>%
  filter(b4_has_finaled_perm==1) %>%
  View()

## finaled temp
permits %>%
  filter(b4_has_finaled_temp==1) %>%
  View()

## finaled misc
permits %>%
  filter(b4_has_finaled_misc==1) %>%
  View()
# Creb permit in here, but for garage, consider how we mark garages complete? All complete rebuilds of garages have type - 	
# Residential New Construction Building Permit - County

## misc repairs that may not be substantial to count as completed repair
permits %>%
  filter(b4_has_finaled_misc_minor==1) %>%
  View()

# check finaled against status
check <- permits %>% group_by(gen_status,b4_has_finaled) %>% summarise(count=n())
# looks good
check_canc_den <- permits %>% filter(gen_status %in% c("Denied","Canceled") & b4_has_finaled==1) # confirms no canceled/denied statuses
length(unique(check_canc_den$ain)) 
check <- permits_filtered %>% filter(gen_status=='') # none with blank status now

# check exempt permits
check <- permits %>% filter(gen_status %in% c("Exempt") & b4_has_finaled==1) # none
check <- permits %>% filter(gen_status %in% c("Exempt")) %>%
  left_join(select(permits_substring, ain, permit_number, permit_sub), by=c("ain", "permit_number")) %>%
  group_by(permit_sub) %>%
  summarize(count=n()) 
View(check) # 1 UNC- is UNC-GRAD (misc permit), all remaining are "other" permits (e.g., RRP - Construction/Demolition deposit)
permits %>% filter(gen_status %in% c("Exempt")) %>% View()
# march 2026 prelim note: one of above is FRP (fire debris removal permit for properties that opted out of government-run program)
# april 2026 4 unc permits that are for grading and they have a finaled date - will these count against a finaled construction?

# check counts for dups
# permit number and ain combos
nrow(permits)  # 8743 # march 2026: 11573 # april 2026: 12358
n_distinct(permits$permit_number, permits$ain) # 8715 unique ain/permit pairs # march 2026: 11570 # april 2026: 12355 - should match above
n_distinct(permits_filtered_curr_ains$permit_number, permits_filtered_curr_ains$ain) 
# 8715 unique ain/permit pairs # march 2026: 11570 # april 2026: 12355
# unique permits
length(unique(permits$permit_number)) # 8642 multiple rows per permit # march 2026: 11496 # april 2026: 12280
length(unique(permits_filtered_curr_ains$permit_number)) # 8642 # march 2026: 11496 # april 2026: 12280
length(unique(permits$permit_number)) # 8642 # march 2026: 11496 # april 2026: 12280

# length(unique(permits$ain)) # 2640 multiple rows per ain which makes sense # march 2026:3149 # april 2026: 3249 - delete?

# explore duplicates
duplicate <- permits %>% group_by(permit_number,ain) %>% filter(n()>1) # - 56 # march 2026: 6 # april 2026: 6
# includes duplicates where some permits have a blank description 
# now (march 2026): duplicates are just 3 pairs of permits for the same pair of AIN: 5841023022 (active, created 5/23/25) and 5841023010 (deleted 5/23/25)
# https://portal.assessor.lacounty.gov/parceldetail/5841023010
# https://portal.assessor.lacounty.gov/parceldetail/5841023022
# april 2026: same as from march 2026

dupe_ain_permits <- permits %>% filter(ain %in% duplicate$ain)
# I think we can drop where main_parcel = 5841023010 because it only has the 3 duplicated permits (and is deleted) whereas 5841023022 has more than those 3 permits (and is active)
# would just want to confirm which AIN we have a shape for re: dashboard map

dupes_to_keep <- permits  %>%
  filter(ain %in% duplicate$ain & permit_number %in% duplicate$permit_number) %>%
  filter(main_parcel=="5841023022") 
  # prev dec 2025: filter(!is.na(description)) 

# check deduped work
n_distinct(duplicate$permit_number, duplicate$ain) # 28 unique ain/permit pairs; march 2026: 3 # april 2026: 3
n_distinct(dupes_to_keep$permit_number, dupes_to_keep$ain) # 28 unique ain/permit pairs ; march 2026: 3 # april 2026: 3
# checks out

permits_deduped <- permits %>%
  # remove duplicates first
  anti_join(dupes_to_keep, by = c("permit_number", "ain"))

# add records we are keeping
permits_deduped <- rbind(permits_deduped,dupes_to_keep)

nrow(permits_deduped) #8716; march 2026: 11570 # april 2026: 12355
n_distinct(permits_deduped$permit_number, permits_deduped$ain) # 8715 unique ain/permit pairs; march 2026: 11570 # april 2026: 12355
duplicate <- permits_deduped %>% group_by(permit_number,ain) %>% filter(n()>1) # - 56; ; march 2026: 0 # april 2026: 0
# Jan note - RRP permit won't matter later, and due to 5841023022 which merged 2 parcels, original permit from original parcel

# get distinct parcels from xwalk
parcels_df <- xwalk_parcels %>%
  select(all_of(ain_curr)) %>%
  rename(ain=all_of(ain_curr)) %>% 
  unique()


# summarize workflow df to parcel level, apply flag to any parcel with at least one inspection
combined_wf <- parcels_df %>%
  left_join(workflow, by="ain") %>%
  group_by(ain) %>%
  mutate(b3_has_inspection = ifelse(sum(b3_has_inspection, na.rm=TRUE)>0, 1, 0)) %>%
  ungroup() %>%
  select(ain, b3_has_inspection) %>%
  unique()

table(combined_wf$b3_has_inspection, useNA = "ifany")
# Dec Update
# 0    1 
# 4811  865 
# March 2026 Update
# 0    1 
# 4431 1245 
# April 2026 Update
# 0    1 
# 4263 1413


# check
nrow(parcels_df) 
# 5676 - should be 5676 (number of distinct 12/2025 ains); 
# same for 03/2026 (expected because parcels not updated yet)
# april 2026 5676 - same as n_distinct(xwalk_parcels$ain_2026_04)
nrow(combined_wf) # 5676
length(unique(combined_wf$ain)) # 5676 distinct row per ain, no dups


# summarize helper cols to the parcel level, add debris here
# save df before grouping by ain
combined_parcels_all <- parcels_df %>%
  left_join(permits_deduped, by="ain") %>%
  left_join(debris_status, by="ain") %>%
  left_join(combined_wf, by="ain") %>%
  select(ain, permit_number, starts_with("b"), -b1_has_fdr) # exclude permit level flag we no longer need

combined_parcels <- combined_parcels_all %>%
  # summarize helper cols to the parcel level (bucket 1, 2, and 4)
  group_by(ain) %>%
  # bucket 1 - fire debris removal
  mutate(b1_has_fdr_finaled = ifelse(sum(b1_has_fdr_finaled, na.rm=TRUE)>0, 1, 0),
         b1_has_ace_fso = ifelse(sum(b1_has_ace_fso, na.rm=TRUE)>0, 1, 0)) %>%
  # bucket 2 - types of build permits
  mutate(
    # get number of ALL permits per parcel
    total_permits = ifelse(n()==1 & is.na(permit_number), 0, n()),
    # keep flag for rebuild permit application
    b2_has_build_permit = ifelse(sum(b2_has_build_permit, na.rm=TRUE)>0, 1, 0),
    # summarize in a list all permanent housing permits for that parcel
    b2_perm = paste(b2_perm[b2_perm != ""], collapse = ";"),
    # summarize in a list all temp housing permits for that parcel
    b2_temp = paste(b2_temp[b2_temp != ""], collapse = ";"),
    # summarize in a list all commercial permits for that parcel
    b2_comm = paste(b2_comm[b2_comm != ""], collapse = ";"),
    # summarize in a list all misc building permits for that parcel
    b2_misc = paste(b2_misc[b2_misc != ""], collapse = ";"),
    b2_misc_minor = paste(b2_misc_minor[b2_misc_minor != ""], collapse = ";"),
    # summarize in a list all other permits (e.g. PROP, FCR, RRP, SWRC)
    b2_other = paste(b2_other[b2_other != ""], collapse = ";")) %>%
  mutate(
    # get count of permanent housing permits for the parcel
    b2_perm_count = ifelse(b2_perm=="NA", 0, lengths(strsplit(b2_perm,";"))),
    # get count of temp housing permits for the parcel
    b2_temp_count = ifelse(b2_temp=="NA", 0, lengths(strsplit(b2_temp,";"))),
    # get count of commercial building permits for the parcel
    b2_comm_count = ifelse(b2_comm=="NA", 0, lengths(strsplit(b2_comm,";"))),
    # get count of misc building permits for the parcel
    b2_misc_count = ifelse(b2_misc=="NA", 0, lengths(strsplit(b2_misc,";"))),
    # get count of misc building permits for minor repairs for the parcel
    b2_misc_minor_count = ifelse(b2_misc_minor=="NA", 0, lengths(strsplit(b2_misc_minor,";"))),
    # get count of all other permits for the parcel
    b2_other_count = ifelse(b2_other=="NA", 0, lengths(strsplit(b2_other,";")))) %>%
  # bucket 3 - has at least one inspection
  mutate(
    b3_has_inspection = ifelse(sum(b3_has_inspection, na.rm=TRUE)>0, 1, 0)) %>%
  # get count of how many finaled permits exist for parcel
  mutate(b4_has_finaled_count = sum(b4_has_finaled, na.rm=TRUE)) %>% # should this exclude fire debris removal finaled? - no
  # bucket 4 
  mutate(
    # get count of how many finaled permits exist for each type (permanent, temp, misc, commercial)
    b4_finaled_perm_count = sum(b4_has_finaled_perm, na.rm=TRUE),
    b4_finaled_temp_count = sum(b4_has_finaled_temp, na.rm=TRUE),
    b4_finaled_comm_count = sum(b4_has_finaled_comm, na.rm=TRUE),
    b4_finaled_misc_count = sum(b4_has_finaled_misc, na.rm=TRUE),
    b4_finaled_misc_minor_count = sum(b4_has_finaled_misc_minor, na.rm=TRUE)
    ) %>%
  select(-permit_number) %>%
  unique() %>%
  rowwise() %>% 
  mutate(
    b4_is_housing = ifelse(b2_perm_count>0, 1, 0),
    b4_has_temp = ifelse(b2_temp_count>0, 1, 0),
    b4_is_temp_only = ifelse(b2_temp_count > 0 & b2_perm_count==0, 1, 0),
    b4_has_misc = ifelse(b2_misc_count > 0, 1, 0),
    b4_is_misc_minor_only=ifelse(
      # if misc repair is greater than zero and the same count as misc
      b2_misc_minor_count>0 &
      b2_misc_minor_count>=b2_misc_count & 
        # no permanent housing
        b2_perm_count==0 & 
        # no temporary housing
        b2_temp_count==0, 1, 0),
    b4_is_misc_only = ifelse(
      b2_misc_count > 0 & 
        b2_perm_count==0 & 
        b2_temp_count==0, 1, 0),
    b4_is_perm_only = ifelse(
      b2_perm_count > 0 & 
        b2_misc_count==0 & 
        b2_temp_count==0, 1, 0),
    b4_is_perm_temp = ifelse(
      b2_perm_count > 0 & 
        b2_misc_count==0 & 
        b2_temp_count>0, 1, 0),
    b4_is_perm_misc = ifelse(
      b2_perm_count > 0 & 
        b2_misc_count > 0 & 
        b2_temp_count==0, 1, 0),
    b4_is_all = ifelse(
      b2_perm_count > 0 & 
        b2_misc_count>0 & 
        b2_temp_count>0, 1, 0)) %>%
  ungroup() %>%
  mutate(b4_perm_finaled = ifelse((b2_perm_count>0 & b4_finaled_perm_count==b2_perm_count), 1, 0),
         b4_temp_finaled = ifelse((b2_temp_count>0 & b4_finaled_temp_count==b2_temp_count), 1, 0),
         b4_misc_finaled = ifelse((b2_misc_count>0 & b4_finaled_misc_count==b2_misc_count), 1, 0),
         b4_misc_minor_finaled = ifelse((b2_misc_minor_count>0 & b4_finaled_misc_minor_count==b2_misc_minor_count), 1, 0)
         ) %>%
  # need to drop flags that we summed to _count cols - keeping introduces duplicates
  select(-c(b4_has_finaled, b4_has_finaled_perm, b4_has_finaled_temp, b4_has_finaled_comm, b4_has_finaled_misc,b4_has_finaled_misc_minor)) %>%
  select(sort(colnames(.))) %>%
  select(ain, everything()) %>%
  unique() 

# For QA: AIN == 5833025005 is a great test case for spot checking because it has multiple different types of permits including misc repair, misc, and perm 
# permit_check <- combined_parcels_all%>%filter(ain=="5833025005") # compare this against:
# combined_check <- combined_parcels%>%filter(ain=="5833025005") 

length(unique(combined_parcels$ain)) # 5676
nrow(combined_parcels) # 5676
# no more dupes

dups <- combined_parcels %>%
  group_by(ain) %>%
  filter(n()>1) %>%
  ungroup() # 0

# check column sums
cols_sums <- combined_parcels %>% select(starts_with("b"),"total_permits") %>% select(where(is.numeric)) %>% colSums(na.rm=TRUE) %>% as.data.frame()

qa_view <- combined_parcels %>% select(starts_with("b"),"total_permits") %>% select(sort(names(.)))

# april 2026: two ains have commercial permits but are still residential in Assessor portal
# both seem to be related to getting new addresses for a structure on the parcel
# https://portal.assessor.lacounty.gov/parceldetail/5845020008
# https://portal.assessor.lacounty.gov/parceldetail/5828018003

##### Step 2: Apply Typology #####
# create table to store final results
final_types <- parcels_df %>%
  left_join(combined_parcels, by="ain") %>%
  # replace NAs that arise from parcels with no permits
  mutate(across(starts_with("b") & where(is.numeric), ~replace_na(., 0))) %>%
  mutate(across(starts_with("b") & where(is.character), ~na_if(., "NA"))) %>%
  mutate(total_permits=ifelse(is.na(total_permits),0,total_permits)) %>%
  # Bucket 1 status: Is fire debris cleared?
  mutate(
    bucket_1_status = case_when(
      b1_has_ace_fso==1 | b1_has_fdr_finaled== 1 ~ "Fire Debris Cleared",
      # we infer properties with a build permit have Fire Debris Cleared 
      b2_has_build_permit==1 ~ "Fire Debris Cleared",
      b1_has_ace_fso==0 & b1_has_fdr_finaled == 0 ~ "Fire Debris Removal Incomplete",
      .default = "Something else - please QA")) %>% # nothing left in the default
  # Bucket 2 status: Build Permit Application Received
  mutate(
    bucket_2_status = 
      case_when(
        (bucket_1_status=="Fire Debris Cleared" & b2_has_build_permit==1) ~ "Permit Application Received", 
        (bucket_1_status=="Fire Debris Cleared" & b2_has_build_permit==0) ~ "Permit Application Not Received",
        .default=bucket_1_status)) %>%
  # Bucket 3 status: Construction progress
  mutate(bucket_3_status = 
           case_when((
             bucket_2_status == "Permit Application Received" & b3_has_inspection==1) ~ "Construction In Progress",
             (bucket_2_status == "Permit Application Received" & b3_has_inspection==0) ~ "Construction Not Started",
             .default=bucket_2_status)) %>%
  # Bucket 4 Status: Rebuild complete
  mutate(bucket_4_status = case_when(
    # if Construction In Progress and has perm housing permits: all perm and temp housing permits are finaled then rebuild complete
    # If above but temp housing permits only, Construction in progress 
    # if above but no housing rebuilds and ONLY misc rebuild where all MISC permits are finaled then rebuild complete
    # else whatever bucket_3_status is
    
    # If only temp, construction in progress
    (bucket_3_status == "Construction In Progress" & 
       b4_is_temp_only==1) ~ "Construction In Progress",
    # if only misc minor, construction in progress
    (bucket_3_status == "Construction In Progress" & 
       b4_is_misc_minor_only==1) ~ "Construction In Progress",    
    # if only perm and perm is finaled then complete,
    (bucket_3_status == "Construction In Progress" &
       b4_is_perm_only==1 & b4_perm_finaled==1)  ~ "Repairs or Rebuild Complete",
    # if perm + temp, then perm and temp finaled
    (bucket_3_status == "Construction In Progress" &
       b4_is_perm_temp==1 & b4_perm_finaled==1 & b4_temp_finaled==1)  ~ "Repairs or Rebuild Complete",
    # if all (perm + misc + temp) then all have to be complete
    (bucket_3_status == "Construction In Progress" & b4_is_all==1 &
       b4_perm_finaled==1 & b4_temp_finaled==1 & b4_misc_finaled==1)  ~ "Repairs or Rebuild Complete",
    # if perm + misc then both have to be complete
    (bucket_3_status == "Construction In Progress" & b4_is_perm_misc==1 &
       b4_perm_finaled==1 & b4_misc_finaled==1)  ~ "Repairs or Rebuild Complete",
    # if only misc and misc is finaled then complete
    (bucket_3_status == "Construction In Progress" & 
       b4_is_misc_only==1 & 
       b4_misc_finaled==1) ~ "Repairs or Rebuild Complete",
    .default = bucket_3_status)) %>%
  mutate(
    rebuild_status=bucket_4_status
  ) %>%
  mutate(
    dashboard_label = case_when(
      rebuild_status == "Construction Not Started" ~ "With Permit Applications",
      rebuild_status == "Permit Application Not Received" ~ "Without Permit Applications",
      rebuild_status == "Construction In Progress" ~ "In Construction",
      .default = rebuild_status
    )
  )

# check column sums - compare to cols_sums dataframe to see if NA's were handled right
cols_sums_check_2 <- final_types %>% select(starts_with("b"),"total_permits") %>% select(where(is.numeric)) %>% colSums(na.rm=TRUE) %>% as.data.frame()

table(final_types$bucket_1_status, useNA="ifany")
table(final_types$bucket_2_status, useNA="ifany")
table(final_types$bucket_3_status, useNA="ifany")
table(final_types$bucket_4_status, useNA="ifany")
table(final_types$rebuild_status, useNA="ifany")
check_final_creb <- final_types %>%  left_join(parcels_creb, by="ain") 
check <- as.data.frame(table(check_final_creb$has_creb, check_final_creb$rebuild_status))
# see above - parcels with CREBs are only associated with construction phase (none are "Rebuild Complete")

table(final_types$rebuild_status, useNA = "ifany")

# Apr 2026
# Construction In Progress        Construction Not Started  Fire Debris Removal Incomplete Permit Application Not Received 
# 1338                            1698                              16                            2587 
# Repairs or Rebuild Complete 
# 37

# Mar 2026 (prelim)
# Construction In Progress        Construction Not Started  Fire Debris Removal Incomplete Permit Application Not Received 
# 1181                            1765                             17                            2686
# Rebuild Complete 
# 27

# Dec 2025
# Construction In Progress        Construction Not Started  Fire Debris Removal Incomplete Permit Application Not Received 
# 798                            1610                             19                            3226
# Rebuild Complete 
# 23 


table(final_types$dashboard_label, useNA = "ifany")

# Apr 2026
# Fire Debris Removal Incomplete                In Construction    Repairs or Rebuild Complete       With Permit Applications    Without Permit Applications
# 16                                                  1338                             37                           1698                         2587

# Mar 2026
# Fire Debris Removal Incomplete                In Construction    Repairs or Rebuild Complete       With Permit Applications    Without Permit Applications 
# 17                                                  1181                            27                          1765                           2686 

# Dec 2025
# Fire Debris Removal Incomplete                In Construction    Repairs or Rebuild Complete       With Permit Applications    Without Permit Applications 
# 19                                                  798                             23                          1610                           3226 

# final row and duplicate check
nrow(final_types)
nrow(parcels_df)
length(unique(final_types$ain))

# check those with fire debris removal still incomplete
permits_deduped %>% filter(ain %in% (final_types %>% 
                                       filter(rebuild_status == "Fire Debris Removal Incomplete") %>%
                                       pull(ain))) %>% View() # looks fine

debris_usace  %>% filter(ain %in% (final_types %>% 
                                      filter(rebuild_status == "Fire Debris Removal Incomplete") %>%
                                      pull(ain))) %>% View() # looks fine

# review repairs or rebuild complete
completed <- final_types %>% filter(dashboard_label=='Repairs or Rebuild Complete')

rebuild_check <- combined_parcels_all %>% filter(ain %in% completed$ain)

rebuild_check_full <- permits_deduped %>% filter(ain %in% completed$ain) %>% select(ain, permit_number, description, everything())
### QA - SKIM THESE TWO VIEWS REBUILD_CHECK and REBUILD_CHECK_FULL to make sure 

# # check against old labels for changes-finish updating this here so it works before export
# prev_labels <- dbGetQuery(con, sprintf("SELECT * FROM %s.rel_parcel_rebuild_status_%s_%s;",
#                                          schema, prev_year, prev_month)) 
# 
# check_rebuild_changes <- final_types %>% rename(curr_label=dashboard_label) %>%
#   left_join(prev_labels %>% rename(prev_label=dashboard_label),by=c("ain"="ain")) %>%
#   mutate(change_summary=case_when(
#     curr_label==prev_label THEN 'unchanged'
#            WHEN curr.dashboard_label = 'Repairs or Rebuild Complete' AND prev.dashboard_label != 'Repairs or Rebuild Complete' THEN 'new rebuild complete'
#            WHEN curr.dashboard_label != 'Repairs or Rebuild Complete' AND prev.dashboard_label = 'Repairs or Rebuild Complete' THEN 'reverted - QA to figure out why'
#            ELSE 'something else?'
#            END AS change_summary
#   ) %>%
#   filter(prev.dashboard_label = 'Repairs or Rebuild Complete' OR curr.dashboard_label = 'Repairs or Rebuild Complete';", 


### those flagged as completed make sense based on types of permits or further refinement might be needed
# April 2026

# looks better one parcel questionable still 	5846008016
# HK 3/5/26: Agreed that 5846008016 looks questionable - doesn't seem related to fires but not sure if we can address 
# HK 3/5/26: It falls under the case where there is only a misc permit and its finaled - don't notice a clear way to reclassify other than manually
# HK 3/5/26: 4 new "complete" ains are: EMG- missing note?

### EMG--we had 60 repairs/rebuild complete originally which included these instances-minor repairs just starting working like plumbing and electrical completed but not full construction yet, needed to bump these back to construction in progress
# instances where the only permit is electrical or plumbing and rebuild not complete, e.g.,
# https://gis.data.cnra.ca.gov/datasets/CALFIRE-Forestry::cal-fire-damage-inspection-dins-data/explore?filters=eyJJTkNJREVOVE5BTUUiOlsiRWF0b24iXSwiQVBOIjpbIjU4NTcwMTEwMjMiXX0%3D&showTable=true
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5857011023

# and
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5751002005
#   https://gis.data.cnra.ca.gov/datasets/CALFIRE-Forestry::cal-fire-damage-inspection-dins-data/explore?filters=eyJJTkNJREVOVE5BTUUiOlsiRWF0b24iXSwiQVBOIjpbIjU3NTEwMDIwMDUiXX0%3D&showTable=true 
# These shouldnt count if only completed
# UNC-PLMB
# UNC-MECH
# UNC-ELEC
# UNC-SEWR
# UNC-GRAD
# UNC-EXPR
# UNC-SOLR
# UNC-BLDG



##### Export to postgres #####
con <- connect_to_db("altadena_recovery_rebuild")
schema <- "dashboard"
table_name <- paste("rel_parcel_rebuild_status", curr_year, curr_month, sep="_")
date_ran <- as.character(Sys.Date())
indicator <- "Rebuild status for residential Altadena parcels based on scraped permit data from _2025_10 tables."
source <- paste("Data imported on", date_ran, "- Multiple sources - see QA doc.")
qa_filepath <- sprintf("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\monthly_updates\\QA_permit_scrape_and_rebuild_%s_%s.docx", curr_year, curr_month)
column_names <- colnames(final_types)
column_comments <- c(
  "AIN - current",
  "1/0 Flag - has Full Sign Off (fso) from Army Corps of Engineers (ace)",
  "1/0 Flag - has FDR permit that is finaled",
  "List of commerical building permit numbers associated with this parcel",
  "Count of commerical permit numbers associated with this parcel",
  "1/0 Flag - property has permit associated with building structures (e.g., commercial, housing, miscellaneous)",
  "List of miscellaneous building permit numbers associated with this parcel",
  "Count of misc permit numbers associated with this parcel",
  "List of minor misc permit numbers (i.e. not sufficient to fully rebuild or repair) associated with this parcel",
  "Count of mionr misc permit numbers associated with this parcel",
  "List of other permit numbers associated with this parcel",
  "Count of other permit numbers associated with this parcel",
  "List of permanent housing permit numbers associated with this parcel",
  "Count of permanent housing permit numbers associated with this parcel",
  "List of temporary housing permit numbers associated with this parcel",
  "Count of temporary housing permit numbers associated with this parcel",
  "1/0 Flag - property has permit with a relevant inspection to signal construction has started",
  "Count of commercial permits with a Finaled status",
  "Count of misc permits with a Finaled status",
  "Count of minor misc permits with a Finaled status",
  "Count of permanent housing permits with a Finaled status",
  "Count of temporary housing permits with a Finaled status",
  "Count of ALL permits (commercial, housing, misc, and other) with a Finaled status",
  "1/0 Flag - has misc building permits",
  "1/0 Flag - has temporary housing building permits",
  "1/0 Flag - has misc., permanent and temporary housing building permits",
  "1/0 Flag - has temp or permanent housing permits",
  "1/0 Flag - has ONLY misc building permits",
  "1/0 Flag - has ONLY minor misc building permits",
  "1/0 Flag - has perm and misc building permits",
  "1/0 Flag - has ONLY permanent housing building permits",
  "1/0 Flag - has permanent and temporary building permits",
  "1/0 Flag - has ONLY temporary housing building permits",
  "1/0 Flag - ALL misc (incl. minor misc) permits are finaled",
  "1/0 Flag - ALL minor misc ONLY (excl. misc) permits are finaled",
  "1/0 Flag - ALL permanent housing building permits are finaled",
  "1/0 Flag - ALL temporary housing building permits are finaled",
  "Count of all permits associated with this parcel (includes commercial, housing, misc., and other)",
  "Rebuild Status related to fire debris removal",
  "Rebuild Status related to permit application if applicable (else bucket_1_status)",
  "Rebuild Status related to construction progress if applicable (else bucket_2_status)",
  "Rebuild Status related to rebuild/repair completion if applicable (else bucket_3_status)",
  "Current status - same as bucket_4_status",
  "Label for dashboard")

# # Now write the table
# dbWriteTable(con, Id(schema=schema, table=table_name), final_types,
#              overwrite = FALSE, row.names = FALSE)
# 

# add_table_comments(con, schema=schema, table_name = table_name, indicator = indicator, source = source, qa_filepath = qa_filepath, column_names = column_names, column_comments = column_comments)


dbDisconnect(con)


### Additional QA
# Adding to see why some rebuild/repair statuses have reverted
con <- connect_to_db("altadena_recovery_rebuild")
sql_query <- sprintf("SELECT curr.*, prev.dashboard_label as prev_dashboard_label,
CASE 
        WHEN curr.dashboard_label = prev.dashboard_label THEN 'unchanged'
        WHEN curr.dashboard_label = 'Repairs or Rebuild Complete' AND prev.dashboard_label != 'Repairs or Rebuild Complete' THEN 'new rebuild complete'
        WHEN curr.dashboard_label != 'Repairs or Rebuild Complete' AND prev.dashboard_label = 'Repairs or Rebuild Complete' THEN 'reverted - QA to figure out why'
        ELSE 'something else?'
    END AS change_summary
FROM dashboard.rel_parcel_rebuild_status_%s_%s curr
LEFT JOIN dashboard.rel_parcel_rebuild_status_%s_%s prev ON curr.ain = prev.ain
WHERE prev.dashboard_label = 'Repairs or Rebuild Complete' OR curr.dashboard_label = 'Repairs or Rebuild Complete';", 
                     curr_year, curr_month,
                     prev_year, prev_month)

check_rebuild_changes <- dbGetQuery(con=con, sql_query)
calfire <- st_read(con, query="SELECT * FROM data.eaton_fire_dmg_insp_3310")
dbDisconnect(con)

qa_new_suspicious <- check_rebuild_changes %>% filter(change_summary=='new rebuild complete' & prev_dashboard_label != "In Construction")
qa_new <- check_rebuild_changes %>% filter(change_summary=='new rebuild complete' & !(ain %in% qa_new_suspicious$ain)) 
qa_reverted <- check_rebuild_changes %>% filter(change_summary=='reverted - QA to figure out why') 
qa_unchanged <- check_rebuild_changes %>% filter(change_summary=='unchanged') 

# April 2026 compared to Dec 2025
## There are 18 new rebuilds (16 previously In Construction and 2 that were not)
# Focusing on 2 not previously In Construction
qa_new_suspicious$ain
calfire %>% filter(apn_parcel %in% qa_new_suspicious$ain) %>% select(apn_parcel, everything()) %>% View()
permits_deduped %>% filter(ain %in% qa_new_suspicious$ain) %>% select(ain, everything())  %>% View()
# 5845014026 (prev. with permit): https://portal.assessor.lacounty.gov/parceldetail/5845014026 
## has two structures and 2 permits: one had major damage (looks like main residence), one with no damage; 
## From the aerial it doesn't appear to have 26-50% damage
## In the permits it sounds like there was damage to the detached garage. In that case this is probably repaired 
## Conclusion: Keep as is. # EMG AGREED 

# 5845015007 (prev. without permit): https://portal.assessor.lacounty.gov/parceldetail/5845015007
## has 3 structures and 1 permit: 2 of 3 structures were destroyed and each structure is confirmed residence
## Has one misc permit (not sufficient) arguably this permit should be misc_minor (e.g., repair/replacement of roof)
## Conclusion: MANUAL REASSIGN to In Construction at least. I would argue this was minor repair for the 3rd structure (other two will require full rebuilds) # EMG AGREED

# the 16 non-suspicious new rebuilt/repaired
# used below to check for build permit types, there are many New Builds which I think is a good sign at a glance
# might be worth it to check the AINs that only have repair/replacement or addition build types
# not sure if that's needed now though
structure_count <- calfire %>% filter(apn_parcel %in% qa_new$ain) %>% 
  select(apn_parcel, everything()) %>% group_by(apn_parcel) %>% summarise(structure_count=n()) %>% ungroup()
calfire %>% filter(apn_parcel %in% qa_new$ain) %>% select(apn_parcel, everything()) %>% View()
permits_deduped %>% filter(ain %in% qa_new$ain) %>% select(ain, everything())  %>% View()
structure_count %>% left_join(final_types, by = c("apn_parcel"="ain")) %>% View()

## There are 4 AINs that reverted to In Construction
qa_reverted$ain
qa_reverted_calfire <- calfire %>% filter(apn_parcel %in% qa_reverted$ain) %>% select(apn_parcel, everything())
qa_reverted_permits <- permits_deduped %>% filter(ain %in% qa_reverted$ain) %>% select(ain, everything())

# "5846021004": https://portal.assessor.lacounty.gov/parceldetail/5846021004
## Has two structures and 4 permits: 1 Destroyed and 1 Affected
## Latest permit is explicitly for an Eaton Fire Like For Like Rebuild (garage), 
## Previous permits were repair/replacements so previous status was likely Repair and next will be Rebuilt
## Conclusion: Keep as is

# "5846017025": https://portal.assessor.lacounty.gov/parceldetail/5846017025
## Has two structures and 2 permits: 1 Destroyed and 1 Affected
## Last permit was repair/replacement for reroof, new one is electrical permit
## Previous was probably repair of residence but destroyed structure is not rebuilt
## Conclusion: Keep as is

# "5829020023": https://portal.assessor.lacounty.gov/parceldetail/5829020023
## Has 14 structures and 20 permits: 2 destroyed and 12 no damage
## Last permits were repair/replacements likely for the 12 other structures (some are multiunit)
## New permits are CREBs presumably for 2 destroyed structures - associated with destroyed property at 3056
# note calfire has a 3052 property that is marked as destroyed but no associated permit data - sometimes calfire addresses can be inaccurate
## Conclusion: Keep as is # EMG AGREED

# "5828018005": https://portal.assessor.lacounty.gov/parceldetail/5828018005
## Has 2 structures and 3 permits: 1 destroyed, 1 affected
## Last permits were repair/replacements - likely for affected structure
## New permit is CREB presumably for destroyed structure
## Conclusion: Keep as is

## There are 19 that stayed rebuilt/repaired - will not manually review
## However noting that MOST have multiple structures with varying damage levels so some could still revert
calfire %>% filter(apn_parcel %in% qa_unchanged$ain) %>% select(apn_parcel, everything()) %>% View()
permits_deduped %>% filter(ain %in% qa_unchanged$ain) %>% select(ain, everything()) %>% View()


# March 2026 compared to Dec 2025
## There are 10 new rebuilds (8 previously "In Construction", 2 not)
## Taking a closer look at those 2 not previously in construction:
# 5845014026 (prev. with permit)
# 5845015007 (prev. without permit)

## There are 6 AINs that reverted 
# 5844028003 (now With Permit)
# 5846017025 (now In Construction)
# 5846021004 (now In Construction)
# 5828018005 (now In Construction)
# 5829018039 (now In Construction)
# 5829020023 (now In Construction)


## There are 17 that remained unchanged



# # compare to list of finaled perm housing permits for ones not included
# finaled_perm_check_2 <- finaled_perm_check %>% filter(!ain %in% rebuild_complete$ain) %>% filter(damage_category=="Significant Damage")
# # look up their permit details
# check_permit_details <- combined_parcels_all %>% filter(ain %in% finaled_perm_check_2$ain)
# # look up their final types
# check_final_type_details <- final_types %>% filter(ain %in% finaled_perm_check_2$ain)
# # looks good


##### QA note on permit type uses and how they could help with keyword search #####
# interesting one here that had one finaled permit for roof and then subsequent permits for demo, etc.
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5833016044
# but note that UNC-BLDR250519005667 got flagged as permanent housing when it was a reroof, consider using type categories instead
# also a situation where we'd want all the miscellaneous permits finaled before marking complete


# missing CREC permit for this one? - this is a CREC plan, not permit - will have ECI advise
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5829015003

# 	5846021031 looks accurate
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5846021031

# 5835017027 looks accurate
# https://portal.assessor.lacounty.gov/parceldetail/5835017027
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5835017027


# 5829034019 looks accurate minor damage to house and house roof repaired
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5829034019 
# https://portal.assessor.lacounty.gov/parceldetail/5829034019
## Check significantly damaged parcels clean up
fdr_incomplete <- final_types %>%
  filter(damage_category=="Significant Damage" & rebuild_status=="Fire Debris Removal Incomplete")

table(fdr_incomplete$damage_type_list) # majority are all destroyed check for these ains again

check_fdr_incomplete <- debris_status %>%
  inner_join(fdr_incomplete %>% select(ain,damage_category,damage_type_list))

View(check_fdr_incomplete)
table(check_fdr_incomplete$roe_status) # mostly opt out, but should returned ineligible be marked incomplete if they completed phase 1? no - confirmed by ECI

check_fdr_incomplete_permits <- permits %>%
  filter(ain %in% fdr_incomplete$ain)
# several in progress of clean up

View(check_fdr_incomplete_permits)

check_fdr_incomplete_permits_missing <- fdr_incomplete %>% select(ain,damage_category,damage_type_list) %>% 
  anti_join(permits) %>%
  left_join(debris_status)

# 2 parcels not in the army corps data
check_fdr_incomplete_permits_missing %>% filter(is.na(apn))

