# Script to assign permit stages for Altadena parcels with Some/Severe damage

##### Typology Outline #####
### 1. Fire Debris Removal (Is the site cleaned up?)
## Phase 1: EPA (Required by All)
# Yes (Clean up done by EPA, parcel progressed to phase 2)
# No (Not completed)
# Deferred (EPA deferred to Army Corps, parcel progressed to phase 2)
## Phase 2A: Army Corps of Engineers (can Opt out)
# Yes (Army Corps completed cleanup)
# No (Not completed)
# Opt-out (Army Corps did not clean up, to be completed via private contractor)
## Phase 2B: Own cleanup (Private Contractor)
# Yes (has "^FDR" permit with a "Finaled" status)

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
jan_parcels <- dbGetQuery(con, "SELECT * FROM data.rel_assessor_residential_jan2025;") %>%
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
gen.status,  gen.finalized_date, gen.main_parcel, gen.address, 
gen.description, gen.response_status, gen.retried, det.completed_percent, 
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
ON gen.ain = wf.ain AND gen.permit_number = wf.permit_number;") %>%


##### Prep data #####
# Note update these in the scraping script
# Remove permits where applied_date.general is before 2025
# Remove permits that start with: FCDP (flood control), FILM, FIRE (related to trees mostly), PWRP (Public Works))
# Extend permits to include: CREB, FCR, PROP, RRP, SWRC, UNC- 
permits_filtered <- permits %>%
  # filter out permits from before 2025
  filter(grepl("(2025|2026)$", applied_date)) %>%
  # filter out voided permits?
  filter(status != "Void")

combined <- parcels %>%
  left_join(permits_filtered, by="ain") 
   


# check if there's an fdr permit (indicates private contractor used for Phase 2 clean up)
check_fdr <- combined %>%
  mutate(has_fdr=ifelse(grepl("^FDR", permit_number), 1, 0),
         has_fdr_finaled = ifelse((grepl("^FDR", permit_number) & status=="Finaled"), 1, 0)) %>%
  select(ain, has_fdr, has_fdr_finaled) %>%
  group_by(ain) %>%
  mutate(has_fdr = ifelse(sum(has_fdr, na.rm=TRUE)>0, 1, 0),
         has_fdr_finaled = ifelse(sum(has_fdr_finaled, na.rm=TRUE)>0, 1, 0)) %>%
  select(ain, has_fdr, has_fdr_finaled) %>% 
  unique()

combined <- combined %>% left_join(check_fdr, by="ain") %>%
  mutate(bucket1 = case_when(has_fdr==1 & has_fdr_finaled==1 ~ "Debris Removal Completed",
                             has_fdr==1 & has_fdr_finaled==0 ~ "Debris Removal In Progress",
                             .default = "Need more info"))

# # check if any building permits exist
# check_permit <- combined %>%
#   mutate(has_permit = ifelse(is.na(permit_number), 0, 1)) %>%
#   select(ain, has_permit) %>%
#   group_by(ain) %>%
#   summarise(count=sum(has_permit, na.rm=TRUE)) %>%
#   mutate(has_permit = ifelse(count>0, 1, 0)) %>%
#   select(ain, has_permit)
# 
# combined <- combined %>% left_join(check_permit, by="ain")
