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



##### Prep data #####
# Note update these in the scraping script
# Remove permits where applied_date.general is before 2025
# Remove permits that start with: FCDP (flood control), FILM, FIRE (related to trees mostly), PWRP (Public Works))
# Extend permits to include: CREB, FCR, PROP, RRP, SWRC, UNC- 