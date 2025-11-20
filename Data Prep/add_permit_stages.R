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
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)

# load data
jan_parcels <- dbGetQuery(con, "SELECT * FROM data.rel_assessor_residential_jan2025 where residential=TRUE;")
xwalk_parcels <- dbGetQuery(con, "SELECT * FROM data.crosswalk_assessor_jan_sept_2025;")
jan_damage <- dbGetQuery(con, "SELECT ain, damage_category, mixed_damage, structure_count, damage_type_list FROM data.rel_assessor_damage_level;")
# damage <- dbGetQuery(con, "SELECT ain_sept, damage_category FROM data.rel_assessor_damage_level_sept2025;")

# get debris removal data
debris_status <- dbGetQuery(con, "SELECT apn, ain, epa_status, roe_status, fso_pkg_received, fso_pkg_approved FROM data.usace_debris_removal_parcels_2025;")

permits_orig <- dbGetQuery(con, "SELECT gen.ain, gen.permit_number, gen.record_id, gen.applied_date, 
gen.type, gen.issued_date, gen.project_name, gen.expiration_date, 
det.status,  gen.finalized_date, gen.main_parcel, gen.address, 
det.description, det.completed_percent, 
det.in_progress_percent, det.not_started_percent, 
wf.workflow_item, 
wf.status as wf_status, 
wf.status_date as wf_status_date 
FROM data.scraped_general_permit_data_2025_10 gen 
LEFT JOIN data.scraped_detailed_permit_data_2025_10 det
ON gen.ain = det.ain AND gen.permit_number = det.permit_number 
LEFT JOIN data.scraped_workflow_permit_data_2025_10 wf
ON gen.ain = wf.ain AND gen.permit_number = wf.permit_number;")

# look at permit numbers
permits_substring <- permits_orig %>%
  mutate(permit_sub=substring(permit_number, 1,4)) # if you look at first 8 you'll see the building codes under UNC- noted below

table(permits_substring$permit_sub)
# CREB County Disaster recovery Permit Rebuild Project
# FCD Flood access/construction permit
# FDR Construction & Demolition Final Compliance
# FIRE tree removal
# PROP Property report
# PWRP Road permits
# RRP Construction & Demolition Deposit
# SWRC Sewer Tap & Saddle Installation
# UNC- Mechanical, Plumbing, Electrical permits (and building permits for SFR, MFR, Temporary Housing, Commercial buildings, etc.)

##### 1. Prep data #####
### Note update these in the scraping script: (leaving in 0ct 2025 for now)
# Extend permits to include: CREB, FCR, PROP, RRP, SWRC, UNC- # everything but UNC and CREB go under other
# Remove Some Damage parcels (only keep Significant Damage)

# Filter permits for applied date after Jan 7, 2025
permits_filtered <- permits_orig %>%
  filter(as.Date(applied_date, format = "%m/%d/%Y") > as.Date("2025-01-07")) %>%
  # filter out voided permits
  filter(status != "Void")

# Minor clean up, filter, add helper columns
# get parcel-level data for some and significant damage parcels
parcels <- jan_parcels %>%
  left_join(xwalk_parcels, by=c("ain"="ain_jan")) %>%
  select(ain, ain_sept, residential, use_code, use_code_sept, address_jan, address_sept, source, status) %>%
  left_join(jan_damage, by="ain") %>%
  rename(xwalk_status = status)

# check
length(unique(jan_parcels$ain)) # 12938
length(unique(parcels$ain))     # 12938
length(unique(jan_damage$ain))  # 12958 - 20 extra here; 5 are significant damage

extra_ains_list <- setdiff(jan_damage$ain, jan_parcels$ain)
extra_ains_df <- jan_damage %>% filter(ain %in% extra_ains_list)

debris <- debris_status %>%
  mutate(
    # bucket 1 helper: does the parcel have full sign off (fso) from army corps
    b1_has_ace_fso = ifelse(!is.na(fso_pkg_approved), 1, 0))

length(unique(debris$ain)) # 7004
table(debris$b1_has_ace_fso, useNA="ifany")

# does not have a filter for wf_status_date-some wf_status_date are NA, probably just want to filter for permit date here too
# get workflow items and add has_inspection (will use for the bucket 3 check - construction has started)
workflow <- permits_filtered %>% 
  select(ain, permit_number, workflow_item, wf_status, wf_status_date) %>%
  unique() %>%
  # filter out FDR permits (those are pre-build and "debris removal inspection" is not sufficient to exclude)
  mutate(is_fdr = ifelse(grepl("^FDR", permit_number), 1, 0)) %>%
  filter(is_fdr==0) %>%
  filter(wf_status_date != "" | wf_status != "") %>%
  mutate(has_status_or_date = case_when(wf_status != "" ~1,
                                        wf_status_date != "" ~1,
                                        .default=0)) %>%
  # add bucket 3 helper columns - has an inspection occurred (excl. debris removal inspection)
  mutate(b3_has_inspection = ifelse(grepl("inspection", workflow_item, ignore.case = TRUE), 1, 0)) %>%
  # summarize to the permit level
  group_by(ain, permit_number) %>%
  mutate(b3_has_inspection = ifelse(sum(b3_has_inspection, na.rm = TRUE)>0, 1, 0)) %>%
  select(ain, permit_number, b3_has_inspection) %>%
  unique() # 4669

table(workflow$b3_has_inspection, useNA = "ifany")

# permit level data
# key words used to determine permits related to residential permanent housing
keyword_list <- c("ADU", "SFR", "SFD", "SB9", "story", "duplex", "dwelling", 
                  "rebuild burned house", "main house", "residence", "pre-approved standard plan",
                  "rebuild house", "mfr", "mfd")

# create a primary permit df of helper columns for each bucket classification (section 2. Apply Typology)
# this df is at permit-level and in section 2 will be aggregated to a parcel-level table called final_types
permits <- permits_filtered %>%
  # remove workflow items to get a dataframe of just permit-level cols
  select(-c(workflow_item, wf_status, wf_status_date)) %>%
  unique() %>%
  # add helper cols
  mutate(
    # is this an FDR (Fire Debris Removal) permit
    b1_has_fdr=ifelse(grepl("^FDR", permit_number), 1, 0),
    # is this permit's status finaled - will use for other buckets too
    b4_has_finaled = ifelse(status=="Finaled" | finalized_date != "", 1, 0)) %>% # expanding to include finalized date where some statuses are exempt
  # bucket 1 helper column: check is fdr is finaled
  mutate(
    b1_has_fdr_finaled = case_when(
      (b1_has_fdr==1 & b4_has_finaled==1) ~ 1,
      .default = 0)) %>%
  # add bucket 2 helper columns
  mutate(
    # is this permit associated with rebuilding structures 
    b2_has_build_permit = ifelse(grepl("^(CREB|UNC-)", permit_number), 1, 0),
    # if the permit is for permanent housing, saves permit_number here
    b2_perm = ifelse((grepl("^(CREB|UNC-BLDR|UNC-BLDF)", permit_number) & # UNC-BLDF doesn't exist
                        (grepl(paste(keyword_list, collapse="|"), project_name, ignore.case = TRUE) |
                           grepl(paste(keyword_list, collapse="|"), description, ignore.case = TRUE))), permit_number, ""),
    # if the permit is for temp housing, saves permit_number here
    b2_temp = ifelse((grepl("^UNC-EXPR", permit_number)& 
                        (grepl("temp hous|temporary hous", project_name, ignore.case = TRUE) |
                           grepl("temp hous|temporary hous", description, ignore.case = TRUE))), permit_number, ""),
    # if the permit is for commercial building, saves permit_number here
    b2_comm = ifelse(grepl("^UNC-BLDC", permit_number), permit_number, ""), # why do we have this if no commercial properties?
    # if the permit does not have a known building permit prefix, saves permit_number here
    b2_other = ifelse((!is.na(permit_number) & !grepl("^(CREB|UNC-)", permit_number)), permit_number, "")) %>% # this would include fire debris and removal
  rowwise() %>%
  mutate(
    # if the permit has a known rebuild prefix but doesn't belong under permanent, temporary, or commercial, put here
    b2_misc = ifelse((b2_has_build_permit==1 & paste0(b2_perm, b2_temp, b2_comm)==""), permit_number, "")) %>%
  ungroup() %>%
  
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
    # specifically is this a finaled permit for misc building
    b4_has_finaled_misc = ifelse((b2_misc != "" &
                                    b4_has_finaled==1), 1, 0)) 

# checks
table(permits$status, useNA = "ifany") # should we drop Canceled and Denied? What does Exempt mean?
table(permits$b1_has_fdr, useNA = "ifany")
table(permits$b1_has_fdr_finaled, useNA = "ifany")
table(permits$b2_has_build_permit, useNA = "ifany")
table(permits$b4_has_finaled, useNA = "ifany")
table(permits$b4_has_finaled_perm, useNA = "ifany")
table(permits$b4_has_finaled_temp, useNA = "ifany")
table(permits$b4_has_finaled_comm, useNA = "ifany")
table(permits$b4_has_finaled_misc, useNA = "ifany")

permits %>%
  filter(b1_has_fdr==1) %>%
  View()
# some have finalized date but not finaled status - HK: Other statuses with finalized date are Cancelled and Exempt
# some have applied date but no issued date

permits %>%
  filter(b4_has_finaled==1) %>%
  View()

nrow(permits)  # 5397

length(unique(permits$permit_number)) # multiple rows per permit 
length(unique(permits$ain)) # multiple rows per ain
n_distinct(permits$permit_number, permits$ain) # 5396 unique ain/permit pairs 
permits %>% group_by(permit_number,ain) %>% filter(n()>1) # - 1 duplicate: 5840015019 

# includes permits that apply to multiple parcels and 1 duplicate of ain/permit pair (one version has a project name)
dup_matches <- permits %>%
  group_by(permit_number) %>%
  filter(n() > 1) %>%
  ungroup()

permits_check <- permits %>%
  left_join(parcels %>% select(ain,damage_category,damage_type_list))

finaled_perm_check <- permits_check %>% 
  filter(b4_has_finaled_perm==1) %>% 
  select (ain, main_parcel,permit_number, type, description, damage_category, 
          damage_type_list, completed_percent, applied_date, issued_date, 
          finalized_date, status,record_id ) 
# check these in next step

finaled_temp_check <- permits_check %>% 
  filter(b4_has_finaled_temp==1) %>% 
  select (ain, main_parcel,permit_number, type, description, damage_category, 
          damage_type_list, completed_percent, applied_date, issued_date, 
          finalized_date, status,record_id )

finaled_comm_check <- permits_check %>% 
  filter(b4_has_finaled_comm==1) %>% 
  select (ain, main_parcel,permit_number, type, description, damage_category, 
          damage_type_list, completed_percent, applied_date, issued_date, 
          finalized_date, status,record_id )
# empty

# check recoding of main fields
permits_check_table <- permits %>%
  left_join(parcels %>% select(ain,damage_category,damage_type_list)) %>%
  select(ain,permit_number, type, description, project_name, b2_has_build_permit, 
         b2_perm, b2_temp, b2_comm, b2_other, b2_misc, damage_category,
         damage_type_list)

permits_check_ain <- permits_check_table %>%
  group_by(ain, damage_category, damage_type_list) %>%
  summarise(b2_has_build_permit=sum(!is.na(b2_has_build_permit)),
            b2_perm=sum(!is.na(b2_perm[b2_perm != ""])), 
            b2_temp=sum(!is.na(b2_temp[b2_temp != ""])), 
            b2_comm=sum(!is.na(b2_comm[b2_comm != ""])), 
            b2_other=sum(!is.na(b2_other[b2_other != ""])), 
            b2_misc=sum(!is.na(b2_misc[b2_misc != ""]))) %>%
  filter(damage_category=="Significant Damage")


# summarize workflow df to parcel level, apply flag to any parcel with at least one inspection
combined_wf <- parcels %>%
  left_join(workflow, by="ain") %>%
  group_by(ain) %>%
  mutate(b3_has_inspection = ifelse(sum(b3_has_inspection, na.rm=TRUE)>0, 1, 0)) %>%
  ungroup() %>%
  select(ain, b3_has_inspection) %>%
  unique()

table(combined_wf$b3_has_inspection, useNA = "ifany")

# check
nrow(workflow)
length(unique(workflow$ain)) # multiple rows per ain
length(unique(workflow$permit_number)) # multiple rows per permit

workflow %>% distinct(ain, permit_number) %>% nrow()

        
# summarize helper cols to the parcel level, add debris here
# save df before grouping by ain
combined_parcels_all <- parcels %>%
  left_join(permits, by="ain") %>%
  left_join(debris, by="ain") %>%
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
    b4_finaled_misc_count = sum(b4_has_finaled_misc, na.rm=TRUE)) %>%
  select(-permit_number) %>%
  unique() %>%
  rowwise() %>% 
  mutate(
    b4_is_housing = ifelse(b2_perm_count>0, 1, 0),
    b4_has_temp = ifelse(b2_temp_count>0, 1, 0),
    b4_is_temp_only = ifelse(b2_temp_count > 0 & b2_perm_count==0, 1, 0),
    b4_is_misc_only = ifelse(
      b2_misc_count > 0 & 
      b2_perm_count==0 & 
      b2_temp_count==0, 1, 0)) %>%
  ungroup() %>%
  mutate(b4_perm_finaled = ifelse((b2_perm_count>0 & b4_finaled_perm_count==b2_perm_count), 1, 0),
         b4_temp_finaled = ifelse((b2_temp_count>0 & b4_finaled_temp_count==b2_temp_count), 1, 0),
         b4_misc_finaled = ifelse((b2_misc_count>0 & b4_finaled_misc_count==b2_misc_count), 1, 0)) %>%
  # need to drop flags that we summed to _count cols
  select(-c(b4_has_finaled, b4_has_finaled_perm, b4_has_finaled_temp, b4_has_finaled_comm, b4_has_finaled_misc)) %>%
  select(sort(colnames(.))) %>%
  select(ain, everything()) %>%
  unique() 

length(unique(combined_parcels$ain)) # 12938
nrow(combined_parcels) # 12938
# no more dupes

# different results in b4_has_finaled_
dups <- combined_parcels %>%
  group_by(ain) %>%
  filter(n()>1) %>%
  ungroup()
                   

combined_parcels_qa <- combined_parcels_all %>%
  # summarize helper cols to the parcel level (bucket 1, 2, 3, and 4)
  group_by(ain) %>%
  # bucket 1 - fire debris removal
 summarise(b1_has_fdr_finaled = sum(b1_has_fdr_finaled,na.rm=TRUE),
         b1_has_ace_fso = sum(b1_has_ace_fso,na.rm=TRUE),
    # get number of ALL permits per parcel
    total_permits = sum(!is.na(permit_number)),
    # keep flag for rebuild permit application
    b2_has_build_permit = sum(b2_has_build_permit, na.rm=TRUE),
    # summarize in a list all permanent housing permits for that parcel
    b2_perm = paste(b2_perm[b2_perm != ""], collapse = ";"),
    # summarize in a list all temp housing permits for that parcel
    b2_temp = paste(b2_temp[b2_temp != ""], collapse = ";"),
    # summarize in a list all commercial permits for that parcel
    b2_comm = paste(b2_comm[b2_comm != ""], collapse = ";"),
    # summarize in a list all misc building permits for that parcel
    b2_misc = paste(b2_misc[b2_misc != ""], collapse = ";"),
    # summarize in a list all other permits (e.g. PROP, FCR, RRP, SWRC)
    b2_other = paste(b2_other[b2_other != ""], collapse = ";"),
    # bucket 3 - has at least one inspection
    b3_has_inspection = sum(b3_has_inspection, na.rm=TRUE),
    # get count of how many finaled permits exist for parcel
    b4_has_finaled_count = sum(b4_has_finaled, na.rm=TRUE), # should this exclude fire debris removal finaled?
    # bucket 4 
    # get count of how many finaled permits exist for each type (permanent, temp, misc, commercial)
    b4_finaled_perm_count = sum(b4_has_finaled_perm, na.rm=TRUE),
    b4_finaled_temp_count = sum(b4_has_finaled_temp, na.rm=TRUE),
    b4_finaled_comm_count = sum(b4_has_finaled_comm, na.rm=TRUE),
    b4_finaled_misc_count = sum(b4_has_finaled_misc, na.rm=TRUE)
    ) %>%
  ungroup() %>%
  # for each ain row now count the permits for the parcel based on recoded column
      mutate(
    # get count of permanent housing permits for the parcel
    b2_perm_count = ifelse(b2_perm=="NA",NA,lengths(strsplit(b2_perm,";"))),
    # get count of temp housing permits for the parcel
    b2_temp_count = ifelse(b2_temp=="NA",NA,lengths(strsplit(b2_temp,";"))),
    # get count of commercial building permits for the parcel
    b2_comm_count = ifelse(b2_comm=="NA",NA,lengths(strsplit(b2_comm,";"))),
    # get count of misc building permits for the parcel
    b2_misc_count = ifelse(b2_misc=="NA",NA,lengths(strsplit(b2_misc,";"))),
    # get count of all other permits for the parcel
    b2_other_count = ifelse(b2_other=="NA",NA,lengths(strsplit(b2_other,";")))) %>%
  # based on counts create flags for b4 buckets
  mutate(
    b4_is_housing = ifelse(b2_perm_count>0, 1, 0),
    b4_has_temp = ifelse(b2_temp_count>0, 1, 0),
    b4_is_temp_only = ifelse(b2_temp_count > 0 & b2_perm_count==0, 1, 0),
    b4_is_misc_only = ifelse(
      b2_misc_count > 0 & 
        b2_perm_count==0 & 
        b2_temp_count==0, 1, 0)) %>%
  # flag finalized housing, temp housing, and misc permits
  mutate(b4_perm_finaled = ifelse((b2_perm != "" & b4_finaled_perm_count==b2_perm_count), 1, 0), # check this
         b4_temp_finaled = ifelse((b2_temp != "" & b4_finaled_temp_count==b2_temp_count), 1, 0),
         b4_misc_finaled = ifelse((b2_misc != "" & b4_finaled_misc_count==b2_misc_count), 1, 0)) %>%
  select(sort(colnames(.))) %>%
  select(ain, everything()) 


length(unique(combined_parcels_qa$ain))
nrow(combined_parcels_qa)

##### Step 2: Apply Typology #####
# create table to store final results
final_types <- parcels %>%
  left_join(combined_parcels_qa, by="ain") %>%
  # replace NAs that arise from parcels with no permits
  mutate(across(starts_with("b") & where(is.numeric), ~replace_na(., 0))) %>%
  mutate(across(starts_with("b") & where(is.character), ~na_if(., "NA"))) %>%
  mutate(total_permits=replace_na(0)) %>%
  # Bucket 1 status: Is fire debris cleared?
  mutate(
    bucket_1_status = case_when(
      b1_has_ace_fso>=1 | b1_has_fdr_finaled>= 1 ~ "Fire Debris Cleared",
      # we infer properties with a build permit have Fire Debris Cleared 
      b2_has_build_permit>=1 ~ "Fire Debris Cleared",
      b1_has_ace_fso==0 & b1_has_fdr_finaled == 0 & damage_category == "No Damage"  ~ "Fire Debris Removal Not Applicable",
      b1_has_ace_fso==0 & b1_has_fdr_finaled == 0 & damage_category !="No Damage" ~ "Fire Debris Removal Incomplete",
      .default = "Something else")) %>%
  # Bucket 2 status: Build Permit Application Received
  mutate(
    bucket_2_status = 
      case_when(
        (bucket_1_status=="Fire Debris Cleared" & b2_has_build_permit>=1) ~ "Permit Application Received", 
        (bucket_1_status=="Fire Debris Cleared" & b2_has_build_permit==0) ~ "Permit Application Not Received",
        .default=bucket_1_status)) %>%
  # Bucket 3 status: Construction progress
  mutate(bucket_3_status = 
           case_when((
             bucket_2_status == "Permit Application Received" & b3_has_inspection>=1) ~ "Construction In Progress",
             (bucket_2_status == "Permit Application Received" & b3_has_inspection==0) ~ "Construction Not Started",
             .default=bucket_2_status)) %>%
  # Bucket 4 Status: Rebuild complete
  mutate(bucket_4_status = case_when(
    # if Construction In Progress and has perm housing permits: all perm and temp housing permits are finaled then rebuild complete
    # If above but temp housing permits only, Construction in progress 
    # if above but no housing rebuilds and ONLY misc rebuild where all MISC permits are finaled then rebuild complete
    # else whatever bucket_3_status is
    (bucket_3_status == "Construction In Progress" & 
       b4_is_temp_only==1) ~ "Construction In Progress",
    (bucket_3_status == "Construction In Progress" & b4_has_temp==1 &
       b4_is_housing==1 & b4_perm_finaled==1 & b4_temp_finaled==1)  ~ "Rebuild Complete",
    (bucket_3_status == "Construction In Progress" & b4_has_temp==0 &
       b4_is_housing==1 & b4_perm_finaled==1)  ~ "Rebuild Complete",
    (bucket_3_status == "Construction In Progress" & 
       b4_is_misc_only==1 & 
       b4_misc_finaled==1) ~ "Rebuild Complete",
    .default = bucket_3_status)) %>%
  mutate(
    rebuild_status=bucket_4_status
  )
  

table(final_types$bucket_1_status, useNA="ifany")
table(final_types$bucket_2_status, useNA="ifany")
table(final_types$bucket_3_status, useNA="ifany")
table(final_types$bucket_4_status, useNA="ifany")
table(final_types$rebuild_status, useNA="ifany")


# Check rebuild status
check <- as.data.frame(table(final_types$damage_category, final_types$rebuild_status))

sig_dmg <- final_types %>% filter(damage_category=="Significant Damage")

# table(sig_dmg$rebuild_status, useNA = "ifany")
# 
# Construction In Progress        Construction Not Started  Fire Debris Removal Incomplete Permit Application Not Received 
# 344                            1484                             79                            3754
# Rebuild Complete 
# 15 

# QA: See if the some damage/significant damage parcels have any NAs
sum(is.na(check)) # 0 NAs

## Check significantly damaged parcels
fdr_incomplete <- final_types %>%
  filter(damage_category=="Significant Damage" & rebuild_status=="Fire Debris Removal Incomplete")

table(fdr_incomplete$damage_type_list) # majority are all destroyed check for these ains again

check_fdr_incomplete <- debris_status %>%
  filter(ain %in% fdr_incomplete$ain)

View(check_fdr_incomplete)
table(check_fdr_incomplete$roe_status) # mostly opt out

check_fdr_incomplete_permits <- permits %>%
  filter(ain %in% fdr_incomplete$ain)

View(check_fdr_incomplete_permits)
# some have finalized date but no finalized status
# these might be finaled https://epicla.lacounty.gov/energov_prod/SelfService/#/permit/48d0b250-3b50-451b-958a-25ff185643c3
# if finaled date assumed finaled?
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5833020004
  # why didnt this one show permits 5833020004?
  
rebuild_complete <- final_types %>%
  filter(damage_category=="Significant Damage" & rebuild_status=="Rebuild Complete")

View(rebuild_complete)
# missing CREC permit for this one?
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5829015003
  
# 	5846021031 looks accurate
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5846021031

# 5835017027 looks accurate
# https://portal.assessor.lacounty.gov/parceldetail/5835017027
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5835017027
 

# 5829034019 looks accurate minor damage to house and house roof repaired
# https://epicla.lacounty.gov/energov_prod/SelfService/#/search?m=1&fm=1&ps=10&pn=1&em=true&st=5829034019 
# https://portal.assessor.lacounty.gov/parceldetail/5829034019


##### Export to postgres #####
con <- connect_to_db("altadena_recovery_rebuild")
table_name <- "rel_parcel_rebuild_status_2025_10"
date_ran <- as.character(Sys.Date())
  
# # Now write the table
# dbWriteTable(con, Id(schema="data", table=table_name), final_types,
#              overwrite = FALSE, row.names = FALSE)
# 
# dbSendQuery(con, paste0("COMMENT ON TABLE data.", table_name, " IS
#             'Rebuild status for Altadena parcels based on scraped permit data from _2025_10 tables,
#             Data imported on ",date_ran, "
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_permit_typology.docx
#             Source: Multiple sources - see QA doc'"))

dbDisconnect(con)