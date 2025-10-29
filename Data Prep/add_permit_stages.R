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

### 2. Applied for Rebuild Permits (Have folks asked for permits to rebuild?)
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
jan_parcels <- dbGetQuery(con, "SELECT * FROM data.rel_assessor_residential_jan2025;")
sept_parcels <- dbGetQuery(con, "SELECT * FROM data.rel_assessor_residential_sept2025;") %>% 
  rename(ain=ain_sept) %>%
  select(-zoning_code)
sept_damage <- dbGetQuery(con, "SELECT * FROM data.rel_assessor_damage_level_sept2025;")
ains <- rbind(select(jan_parcels,ain), select(sept_parcels,ain)) %>% unique() %>%
  left_join(jan_parcels, by="ain", suffix=c("", "_jan"))
ains <- ains %>% left_join(sept_parcels, by="ain", suffix=c("", "_sept")) %>%
  left_join(sept_damage, by=c("ain"="ain_sept"))
parcels <- ains %>% select(ain, damage_category, use_code, use_code_sept)
check <- parcels %>% group_by(ain) %>% filter(n()>1)
permits <- dbGetQuery(con, "SELECT gen.ain, gen.permit_number, gen.record_id, gen.applied_date, 
gen.type, gen.issued_date, gen.project_name, gen.expiration_date, 
det.status,  gen.finalized_date, gen.main_parcel, gen.address, 
det.description, gen.response_status, gen.retried, det.completed_percent, 
det.in_progress_percent, det.not_started_percent, 
det.response_status as response_status_det, 
det.retried as retried_det, 
wf.workflow_item, 
wf.status as wf_status, 
wf.status_date as wf_status_date 
FROM data.scraped_general_permit_data_2025_10 gen 
LEFT JOIN data.scraped_detailed_permit_data_2025_10 det
ON gen.ain = det.ain AND gen.permit_number = det.permit_number 
LEFT JOIN data.scraped_workflow_permit_data_2025_10 wf
ON gen.ain = wf.ain AND gen.permit_number = wf.permit_number;") 

# get debris removal data
debris_status <- dbGetQuery(con, "SELECT apn, ain, epa_status, roe_status, fso_pkg_received, fso_pkg_approved FROM data.usace_debris_removal_parcels_2025;")


final_types <- parcels # 12959

##### Prep data #####
### Note update these in the scraping script: (leaving in 0ct 2025 for now)
# Remove permits where applied_date.general is before 2025
# Remove permits that start with: FCDP (flood control), FILM, FIRE (related to trees mostly), PWRP (Public Works))
# Extend permits to include: CREB, FCR, PROP, RRP, SWRC, UNC- 

# Filter out 2025 and permits with status Void
permits_filtered <- permits %>% # 5477
  # filter out permits from before 2025
  filter(grepl("(2025|2026)$", applied_date)) %>%
  # filter out voided permits?
  filter(status != "Void") %>%
  select(-c(workflow_item, wf_status, wf_status_date)) %>%
  unique()

workflow <- permits %>% # 46436
  select(ain, permit_number, workflow_item, wf_status, wf_status_date) %>%
  unique()



# combine into one reference table and use that to define each bucket (result should be left joined to final_types)
keyword_list <- c("ADU", "SFR", "SFD", "SB9", "story", "duplex", "dwelling", 
                  "rebuild burned house", "main house", "residence", "pre-approved standard plan",
                  "rebuild house", "mfr", "mfd")
combined_wf <- parcels %>%
  left_join(workflow, by="ain") %>%
  # add bucket 3 helper columns
  mutate(has_inspection = ifelse((grepl("inspection", workflow_item, ignore.case = TRUE) & 
                                    !grepl("debris removal inspection", workflow_item, ignore.case = TRUE)), 1, 0)) %>%
  group_by(ain) %>%
  mutate(has_inspection = ifelse(sum(has_inspection, na.rm=TRUE)>0, 1, 0)) %>%
  ungroup() %>%
  select(ain, has_inspection) %>%
  unique()

final_types <- final_types %>%
  left_join(combined_wf, by="ain")
  
combined_permits <- parcels %>%
  left_join(permits_filtered, by="ain") %>% # 16468
  left_join(debris_status, by="ain") %>%
  # add bucket 1 helper columns
  mutate(has_ace_fso = ifelse(!is.na(fso_pkg_approved), 1, 0),
         has_fdr=ifelse(grepl("^FDR", permit_number), 1, 0),
         has_fdr_finaled = ifelse((grepl("^FDR", permit_number) & status=="Finaled"), 1, 0)) %>%
  mutate(phase_2_result = case_when((has_ace_fso==1)|(has_fdr==1 & has_fdr_finaled==1) ~ "Debris Removal Completed",
                                    (has_ace_fso==0 & damage_category == "No Damage") ~ "Debris Removal Not Applicable",
                                    .default = "Debris Removal Incomplete")) %>%
  # add bucket 2 helper columns
  mutate(has_rebuild_app = ifelse(grepl("^(CREB|UNC-)", permit_number), 1, 0)) %>%
  mutate(
    bucket_2_perm = ifelse((grepl("^(CREB|UNC-BLDR|UNC-BLDF)", permit_number) & 
                              (grepl(paste(keyword_list, collapse="|"), project_name, ignore.case = TRUE) |
                                 grepl(paste(keyword_list, collapse="|"), description, ignore.case = TRUE))), permit_number, ""),
    bucket_2_temp = ifelse((grepl("^UNC-EXPR", permit_number)& 
                              (grepl("temp hous|temporary hous", project_name, ignore.case = TRUE) |
                                 grepl("temp hous|temporary hous", description, ignore.case = TRUE))), permit_number, ""),
    bucket_2_comm = ifelse(grepl("^UNC-BLDC", permit_number), permit_number, ""),
    other_permits = ifelse((!is.na(permit_number) & !grepl("^(CREB|UNC-)", permit_number)), permit_number, "")) %>%
  rowwise() %>%
  mutate(
    bucket_2_misc = ifelse((has_rebuild_app==1 & paste0(bucket_2_perm, bucket_2_temp, bucket_2_comm)==""), permit_number, "")) %>%
  ungroup() %>%
  
  # add bucket 4 helper columns
  mutate(has_finaled = case_when(status=="Finaled" ~ 1,
                                 is.na(permit_number) ~ 0,
                                 .default = 0)) %>%
  mutate(
    has_finaled_perm = ifelse((bucket_2_perm != "" &
                                  has_finaled==1), 1, 0),
    has_finaled_temp = ifelse((bucket_2_temp != "" &
                                  has_finaled==1), 1, 0),
    has_finaled_comm = ifelse((bucket_2_comm !="" &
                                  has_finaled==1), 1, 0),
    has_finaled_misc = ifelse((bucket_2_misc != "" &
                                  has_finaled==1), 1, 0)) %>%
  # summarize helper cols to the permit level (bucket 4)
  group_by(ain, permit_number) %>%
  mutate(has_finaled = ifelse(sum(has_finaled, na.rm=TRUE)>0, 1,0)) %>%
  ungroup() %>%
  # summarize helper cols to the parcel level (bucket 1, 2, and 4)
  group_by(ain) %>%
  mutate(has_fdr = ifelse(sum(has_fdr, na.rm=TRUE)>0, 1, 0),
         has_fdr_finaled = ifelse(sum(has_fdr_finaled, na.rm=TRUE)>0, 1, 0)) %>%
  mutate(
    total_permits = ifelse((n()==1 & is.na(permit_number)), 0, n()),
    has_rebuild_app = ifelse(sum(has_rebuild_app, na.rm=TRUE)>0, 1, 0),
    bucket_2_perm = paste(bucket_2_perm[bucket_2_perm != ""], collapse = ";"),
    bucket_2_temp = paste(bucket_2_temp[bucket_2_temp != ""], collapse = ";"),
    bucket_2_comm = paste(bucket_2_comm[bucket_2_comm != ""], collapse = ";"),
    bucket_2_misc = paste(bucket_2_misc[bucket_2_misc != ""], collapse = ";"),
    other_permits = paste(other_permits[other_permits != ""], collapse = ";")
  ) %>%
  mutate(
    perm_count = lengths(strsplit(bucket_2_perm,";")),
    temp_count = lengths(strsplit(bucket_2_temp,";")),
    comm_count = lengths(strsplit(bucket_2_comm,";")),
    misc_count = lengths(strsplit(bucket_2_misc,";")),
    other_count = lengths(strsplit(other_permits,";"))
  ) %>%
  mutate(has_finaled = sum(has_finaled, na.rm=TRUE)) %>%
  mutate(finaled_perm_count = sum(has_finaled_perm, na.rm=TRUE),
         finaled_temp_count = sum(has_finaled_temp, na.rm=TRUE),
         finaled_comm_count = sum(has_finaled_comm, na.rm=TRUE),
         finaled_misc_count = sum(has_finaled_misc, na.rm=TRUE)) %>% 
  ungroup() %>%
  left_join(combined_wf, by="ain")
  

##### Bucket 1: Debris Removal Completed ##### 
### check if there's an fdr permit or army corps full sign off "fso" to indicate debris removal complete (phase_2_result)
### may need to refine further
check_debris <- combined_permits %>%
  select(ain, epa_status, roe_status, fso_pkg_received, fso_pkg_approved,
         has_ace_fso, has_fdr, has_fdr_finaled, phase_2_result) %>% 
  unique() 


### Add what we have to final_types and create bucket_1_status
final_types <- final_types %>% 
  left_join(check_debris, by="ain") %>%
  # add bucket 1 with assumption (note assumption is for any permit status)
  mutate(bucket_1_status = phase_2_result)
  
# check
table(final_types$bucket_1_status, useNA="ifany")

##### Bucket 2: Applied for Rebuild Permits #####
### Yes 
# Sub-bucket: Permanent Housing (permit_number "^CREB|UNC-BLDR|UNC-BLDF" AND project_name or description.detailed contains "ADU|SFR|SFD|SB9|story|duplex|dwelling|rebuild burned house|main house|residence|dwelling|pre-approved standard plan|rebuild house|mfr|mfd)
# Sub-bucket: Temporary Housing (permit_number "^UNC-EXPR") AND project_name or description.detailed contains "temp hous|temporary hous"
# Sub-bucket: Garages, Pools, Etc. (if none of the others, then default to this bucket)
# Sub-bucket: Commercial (permit_number "^UNC-BLDC")
### No if none of above

check_rebuild <- combined_permits %>%
  select(ain, has_rebuild_app, total_permits, 
         bucket_2_perm, bucket_2_temp, bucket_2_comm, bucket_2_misc, other_permits,
         perm_count, temp_count, comm_count, misc_count, other_count) %>%
  unique() 

### Add to final_types
final_types <- final_types %>%
  left_join(check_rebuild, by="ain") %>%
  # using an assumption that is the parcel has a rebuild app, it is probably finished with debris removal
  mutate(bucket_1_status = case_when(has_rebuild_app==1 ~ "Debris Removal Completed",
                                     .default=bucket_1_status)) %>%
  mutate(bucket_2_status = case_when((bucket_1_status=="Debris Removal Completed" & has_rebuild_app==1 & (damage_category=="Some Damage"|damage_category=="Significant Damage")) ~ "Applied for rebuild permits", 
                                     (bucket_1_status=="Debris Removal Completed" & has_rebuild_app==0 & (damage_category=="Some Damage"|damage_category=="Significant Damage")) ~ "Awaiting permit application",
                                     .default=bucket_1_status))

# check
table(final_types$has_rebuild_app, useNA = "ifany")
table(final_types$bucket_1_status, useNA = "ifany")
table(final_types$bucket_2_status, useNA = "ifany")
table(final_types$damage_category, final_types$bucket_1_status, useNA = "ifany")
table(final_types$damage_category, final_types$bucket_2_status, useNA = "ifany")

##### Bucket 3: Rebuild/Construction in progress #####
## Yes
# Parcel has Yes for Bucket 2 AND workflow.item that contains "inspection" (excl. "debris removal inspection")
## No (default if not yes)

# # check what values appear in workflow_item
# check <- as.data.frame(table(combined$workflow_item, useNA = "ifany")) %>%
#   filter(grepl("inspection", Var1, ignore.case=TRUE))
check_construction <- combined_permits %>%
  mutate(has_1_inspection = ifelse((has_rebuild_app==1 & has_inspection==1), 1, 0)) %>%
  group_by(ain) %>%
  mutate(has_1_inspection = ifelse(sum(has_1_inspection, na.rm=TRUE)>0, 1, 0)) %>%
  select(ain, has_1_inspection) %>%
  unique()

# join to final_types, create bucket_3_status
final_types <- final_types %>%
  left_join(check_construction, by="ain") %>%
  mutate(bucket_3_status = case_when((bucket_2_status == "Applied for rebuild permits" & has_1_inspection==1) ~ "Construction in progress", 
                                     (bucket_2_status == "Applied for rebuild permits" & has_1_inspection==0) ~ "Awaiting construction start", 
                                     .default=bucket_2_status))
  

table(final_types$damage_category, final_types$bucket_3_status, useNA = "ifany")
##### Bucket 4: Construction Complete #####
## Yes: Qualifies for bucket 3 AND
# Sub-bucket 4.1: Permanent Housing Complete - Bucket 2.1 == Yes AND permit_number starts with “CREB”, “UNC-BLDR”, or “UNC-BLDF” and with status.detailed = “Finaled” 
# Sub-bucket 4.2: Temporary Housing Complete - Bucket 2.2 == Yes AND permit_number starts with “UNC-EXPR” And status.detailed = “Finaled” 
# Sub-bucket 4.3: Garages, Pools, etc., Complete (Alt name like Misc./Utility/Non housing?) - Bucket 2.3 == Yes And status.detailed = “Finaled” for ALL permits associated with this AIN (may be less accurate; possibly exclude?)
# Sub-bucket 4.4: Commercial - Bucket 2.4 == Yes AND permit_number starts with “UNC-BLDC” AND status.detailed = “Finaled”

check_complete <- combined_permits %>%
  
  # summarize to parcel
  select(ain, damage_category, use_code, use_code_sept, 
         has_finaled, has_rebuild_app, 
         bucket_2_perm, bucket_2_temp, bucket_2_comm, bucket_2_misc,
         total_permits, 
         perm_count, temp_count, comm_count, misc_count, other_count,
         has_inspection, 
         finaled_perm_count, finaled_temp_count, finaled_comm_count, finaled_misc_count) %>%
  unique() %>%
  # right now we'll say that as long as one permanent/temporary housing is finaled, folks are moved in, 
  mutate(bucket_4_perm_comp = ifelse((bucket_2_perm != "" & finaled_perm_count>0), 1, 0),
         bucket_4_temp_comp = ifelse((bucket_2_temp != "" & finaled_temp_count>0), 1, 0)) %>%
  select(ain, bucket_4_perm_comp, bucket_4_temp_comp) %>%
  unique()
           
# join to final_types, create bucket_4_status
final_types <- final_types %>%
  left_join(check_complete, by="ain") %>%
  mutate(bucket_4_status = case_when((bucket_3_status == "Construction in progress" & sum(bucket_4_perm_comp, bucket_4_temp_comp, na.rm=TRUE)>=1) ~ "Move-in available", 
                                     .default=bucket_3_status)) %>%
  mutate(rebuild_status = bucket_4_status)

# Check rebuild status
check <- as.data.frame(table(final_types$damage_category, final_types$rebuild_status))


##### Export to postgres #####
con <- connect_to_db("altadena_recovery_rebuild")
table_name <- "rel_parcel_rebuild_status_2025_10"
date_ran <- as.character(Sys.Date())
  
# Now write the table
dbWriteTable(con, Id(schema="data", table=table_name), final_types,
             overwrite = FALSE, row.names = FALSE)

# dbSendQuery(con, paste0("COMMENT ON TABLE data.", table_name, " IS
#             'Rebuild status for Altadena parcels based on scraped permit data from _2025_10 tables,
#             Data imported on ",date_ran, "
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_permit_typology.docx
#             Source: Multiple sources - see QA doc'"))

dbDisconnect(con)