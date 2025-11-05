# Create the following relational tables for analysis
# 1: Residential properties
## properties with residential use codes
## properties with units (total units)
## total square footage
## Type of residential property
## homeowner/renter


# 2: West/East Altadena relational table (residential properties)

# 3: Damage type for residential properties

# Library and environment set up ----

library(sf)
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

# STEP 1: Load in data/shapes ----
# eaton fire shape
eaton_fire <- st_read(con_alt, query="SELECT * FROM data.eaton_fire_prmtr_3310", geom="geom")

# get xwalk for damage inspection data
dins_xwalk <- st_read(con_alt, query="SELECT * FROM data.crosswalk_dins_assessor_jan2025", geom="geom")

# get assessor parcels
assessor_parcels <- st_read(con_alt, query="Select * from data.assessor_parcels_universe_jan2025", geom="geom")

# get assessor data
assessor_data <- st_read(con_alt, query="Select * from data.assessor_data_universe_jan2025")

# get west and east altadena
east <- st_read(con_alt, query="SELECT * FROM data.east_altadena_3310", geom="geom")

west <- st_read(con_alt, query="SELECT * FROM data.west_altadena_3310", geom="geom")

# STEP 2: Prep parcels in Altadena ------
## Select parcels in West or East Altadena st_join ----
st_crs(assessor_parcels)
st_crs(east)
st_crs(west)

# parcels within east altadena
parcels_east <- st_join(assessor_parcels, east %>% select(name,label), join=st_within, left=FALSE)
# parcels_east <- st_join(assessor_parcels, east %>% select(name), join=st_intersects, left=FALSE) # tried intersects but I think the st_within will be better for mapping and most applicable for altadena
# for st_intersects -- 2 assessor parcels overlapping 5862005304 and 5843005901

# check
# mapview(parcels_east) +
#   mapview(east) 
# looks good

# parcels within west altadena
parcels_west <- st_join(assessor_parcels, west %>% select(name,label), join=st_within, left=FALSE)

# check
# mapview(parcels_west) +
#   mapview(west)
# looks good

# join together
parcels_altadena <- rbind(parcels_west,parcels_east)
table(parcels_altadena$name,useNA='always')

# check for duplicates
nrow(parcels_altadena)
length(unique(parcels_altadena$ain))
# looks good 13940
# this is our universe moving forward until we reduce to residential

# filter assessor data for same ains
data_altadena <- assessor_data %>%
  filter(ain %in% parcels_altadena$ain)

nrow(data_altadena)
length(unique(data_altadena$ain))
# no difference

## Prep additional fields for units and square feet -----
# Add total units and total square feet for us to see throughout
data_altadena <- data_altadena %>%
  mutate(total_units = rowSums(across(ends_with("_units"))),
       total_square_feet = rowSums(across(ends_with("_square_feet"))),
       total_bedrooms = rowSums(across(ends_with("_bedrooms")))) %>%
         mutate(total_units=total_units-landlord_units) # so we don't double count rental units

# check
data_altadena %>%
  select(ends_with("_units"),ends_with("_bedrooms")) %>%
  View()
# looks good

# Just to check consistency in units and square footage -- calculate units and square footage with bedrooms
# create units column list--remove totals

unit_cols <- names(data_altadena)[str_detect(names(data_altadena), "_units")]
unit_cols <- unit_cols[unit_cols != "landlord_units"]
unit_cols <- unit_cols[unit_cols != "total_units"]

# QA view result
View(as.data.frame(unit_cols))

# create bedroom columns list
bed_cols  <- str_replace(unit_cols, "_units", "_bedrooms")

# QA view result
View(as.data.frame(bed_cols))

# create square feet columns list--remove total
square_feet_cols <- names(data_altadena)[str_detect(names(data_altadena), "_square_feet")]
square_feet_cols <- square_feet_cols[square_feet_cols != "total_square_feet"]

# QA view result
View(as.data.frame(square_feet_cols))

# sum unit and square feet columns when corresponding building column is greater than 0, e.g., to sum b1_units, b1_bedrooms has to be >0
data_altadena <- data_altadena %>%
  rowwise() %>%
  mutate(
    units_with_bedrooms = sum(map2_dbl( # sum units if bedroom column is > 0
      unit_cols, bed_cols,
      ~ ifelse(get(.y) > 0, get(.x), 0)
    )),
    square_feet_with_bedrooms = sum(map2_dbl( # sum square feet if bedroom column is > 0
      square_feet_cols, bed_cols,
      ~ ifelse(get(.y) > 0, get(.x), 0)
    ))
  ) %>%
  ungroup()

# check units and total square footage again look at discrepancies to see what to use moving forward
data_altadena %>%
  filter(units_with_bedrooms!=total_units)%>%
  select(units_with_bedrooms, square_feet_with_bedrooms, ends_with("_units"),ends_with("_square_feet"), ends_with("_bedrooms"),
         # res_type,
         everything()) %>%
  View()

# See https://portal.assessor.lacounty.gov/parceldetail/5743003001, seems like total_units and total_square_feet will be a more accurate match

# STEP 3: TABLE 1: RESIDENTIAL PROPERTIES INFO ------
# We need to analyze all residential properties in Altadena and know what is single family, multi-family, condos, number of units, square footage, owner/renter
# this is our basis for all other tables
# Use codes: Ws01data\Project\RDA Team\Altadena Recovery and Rebuild\Data\Assessor Data Extract\Use Code 2023.pdf
## Residential properties ----
data_altadena_res_temp <- data_altadena %>%
  filter(str_detect(use_code, "^0")) 

table(data_altadena_res_temp$use_code)

# pull out x and v suffixes which are vacant or vacant parcel with improvements that are non-structural

# QA: Looking at the Use Code 2023.pdf do we also want to include the suffix U:  Vacant parcel under UAIZ contract? I believe UAIZ contract =Urban Agriculture Incentive Zone
# Check if there are any U suffix values
data_altadena_res_temp_u<-data_altadena_res_temp %>%
  filter(str_detect(use_code, "U$") ) # there are none so we can move forward

# Proceed with other residential types:

data_altadena_res_vacant <- data_altadena_res_temp %>%
  filter(str_detect(use_code, "V$") | str_detect(use_code, "X$") ) 

table(data_altadena_res_vacant$use_code)

data_altadena_res_vacant %>%
  select(total_units, total_square_feet, total_bedrooms, use_code, everything()) %>%
  View()
# sort by units descending

View(as.data.frame((unique(data_altadena_res_vacant$ain))))

# one has incorrect use code 5841019007 https://portal.assessor.lacounty.gov/parceldetail/5841019007
# remove from vacant land

# get a list of vacant residential properties we want to exclude
data_altadena_res_vacant <- data_altadena_res_vacant %>%
  filter(ain!="5841019007")

# now get the final list of residential properties, excluding vacant parcels
data_altadena_res <- data_altadena_res_temp %>%
  filter(!ain %in% data_altadena_res_vacant$ain) %>%
  # recode use code for one property
  mutate(use_code=ifelse(ain=="5841019007","0100",use_code))

## Mixed use properties ----
## Types can be
# STORE COMBINATION (WITH OFFICE OR RESIDENTIAL) - 121
## OFFICE BUILDING (office & residential) 172
data_altadena_mixed_temp <- data_altadena %>%
  filter(str_detect(use_code, "^121") | str_detect(use_code, "^172")) 

# check
table(data_altadena_mixed_temp$use_code)

data_altadena_mixed_temp %>%
  select(total_units, landlord_units, total_square_feet, total_bedrooms, use_code, everything()) %>%
  View()

# landlord units can include non-living units, e.g., commercial rental units (also see FIELD DEF pdf in Assessor Data Extract folder), see https://portal.assessor.lacounty.gov/parceldetail/5825002062, 1 unit but 0 beds or baths
# only keep records where there are total_units
# no units or bedrooms, but landlord units https://portal.assessor.lacounty.gov/parceldetail/5845010018

data_altadena_mixed <- data_altadena_mixed_temp  %>%
  filter(total_units>0)
  
# check
data_altadena_mixed %>%
  select(total_units, landlord_units, total_square_feet, total_bedrooms, use_code, everything()) %>%
  View()
# https://portal.assessor.lacounty.gov/parceldetail/5845018003 looks okay
# https://portal.assessor.lacounty.gov/parceldetail/5825002062 larger than other properties and doesn't look like there is residential, drop

data_altadena_mixed <- data_altadena_mixed  %>%
  filter(ain!="5825002062")

# QA: just noting for extra context that AIN 5825002062 is a disaster recovery center https://recovery.lacounty.gov/2025/01/24/new-altadena-disaster-recovery-center-opens-monday/
# Makes sense to exclude

# QA: I also double checked AIN 5845018003 because it had 0 bedrooms but looks residental and OK to keep: https://www.zillow.com/homedetails/848-Marcheta-St-Altadena-CA-91001/82875015_zpid/

## Add column flags and then merge two property types ----
use_codes <- table(data_altadena_res$use_code) %>% as.data.frame()
View(use_codes)
# one room/boarding house

use_codes_mixed <- table(data_altadena_mixed$use_code) %>% as.data.frame()
View(use_codes_mixed)

### Flags for mixed use or residential ------
data_altadena_res <- data_altadena_res %>%
  mutate(residential=TRUE, # residential column flag
         mixed_use=FALSE) # mixed use flag

data_altadena_mixed <- data_altadena_mixed %>%
  mutate(residential=FALSE, # residential column flag
         mixed_use=TRUE) # mixed use flag

# bind residential and mixed use         
rel_res_df <- rbind(data_altadena_res, data_altadena_mixed)

### Flags for type of residential property
rel_res_df  <- rel_res_df  %>%
  mutate(
         res_type=case_when( # type of residential property
           # condos first
          str_detect(use_code, "C$|E$") ~ "Condominium",
          str_detect(use_code, "^01") ~ "Single-family",
          str_detect(use_code, "^02|^03|^04|^05") ~ "Multifamily",
          str_detect(use_code,"^08") ~ "Boarding house",
          str_detect(use_code,"^12|^17") ~ "Mixed use", 
          TRUE ~NA))

table(rel_res_df$res_type,useNA='always')

# QA run some checks on the flag results

qa_01<-use_codes%>%filter(str_detect(Var1, "^01"))%>%mutate(total=sum(Freq))%>%View() # 11987 ----this minus the 181 from condominiums gives us the 11806 we see in rel_res_df so checks out and there is no double counting happening
qa_ce<-use_codes%>%filter(str_detect(Var1, "C$|E$"))%>%mutate(total=sum(Freq))%>%View() # 181 --checks out with rel_res_df


### Flags for homeowner vs. renter -----
check <- rel_res_df  %>%
  select(num_howmowner_exemption, homeowner_exemption_val,landlord_units, total_units, use_code, total_bedrooms, everything())

View(check)
# some inconsistencies but let's count at least one homeowner exemptions as homeowner, rental as if landlord units are reported, if homeowners and rental then combined
rel_res_df <- rel_res_df %>%
  mutate(owner_renter=case_when(
    num_howmowner_exemption>=1 & landlord_units==0 ~ "Owner occupied",
    num_howmowner_exemption>=1 & landlord_units>=1 ~ "Owner occupied",
    num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter",
  TRUE ~ NA))

check <- rel_res_df %>%
  select(owner_renter,num_howmowner_exemption, homeowner_exemption_val,landlord_units, total_units, use_code, total_bedrooms, everything())

check %>%
  filter(is.na(owner_renter)) %>%
  View()
# owned by trusts, churches, etc. count as other?

rel_res_df <- rel_res_df %>%
  mutate(owner_renter=case_when(
    num_howmowner_exemption>=1 & landlord_units==0 ~ "Owner occupied",
    num_howmowner_exemption>=1 & landlord_units>=1 ~ "Owner occupied",
    num_howmowner_exemption==0 & landlord_units>=1 ~ "Renter occupied",
    TRUE ~ "Other"))

table(rel_res_df$owner_renter, useNA='always')
# might want to count these as homeowner, but let's leave for now as another homeowner type, could be family homes, but not primary residences

## Recode the remaining "OTHER" category

## For more details on how we came up with these groups, see the script: Data Prep/resident_exemption_explore.R

# Categories: 
# Veteran exemption: move to 'Owner occupied'
# New category: LLC-owned
# New category: Trust-owned
# New category: Church / Welfare exemption
# New category: Sold to state (tax delinquent)
# New category: SCE or Government owned


# Start with exemption type recoding:

other<-rel_res_df%>%filter(owner_renter=="Other") # separate out 'Other' so I can perform checks throughout

table(other$exemption_type) 

# Start with recoding by exemption types

rel_res_df<-rel_res_df%>%
  mutate(
    owner_renter = case_when(
      exemption_type %in% "1" & owner_renter == "Other" ~ "Owner occupied",
      exemption_type %in% c("4", "5", "7") & owner_renter == "Other" ~ "Church/Welfare exemption",
      TRUE ~ owner_renter  # keeps existing value if none of the above conditions are met
    )
  )


# check
table(rel_res_df$owner_renter) # this looks like what I expect. OTHER category went from 4461 to 4433 and we have 18 new observations under-owner occupied

# move on to trusts and LLCs
test <- rel_res_df%>%
  # top code corporate
  mutate(
    owner_renter = ifelse(
      (grepl("LLC", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC", first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "LLC owned",owner_renter))%>%
  mutate(
    owner_renter = ifelse(
      (grepl("TRUST", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Trust owned",owner_renter))%>%
  mutate(owner_renter=ifelse(ain %in% "5829032026", "LLC owned", owner_renter)) %>% # manually recoded after original result returned a property with a LLC and trust
filter(owner_renter %in% c("LLC owned","Trust owned"))

test_2 <- rel_res_df%>%
  # top code corporate
  mutate(
    owner_renter = ifelse(
      (grepl("LLC| LP| Inc", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc", first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC| LP| Inc", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "LLC owned",owner_renter))%>%
  mutate(
    owner_renter = ifelse(
      (grepl("TRUST|TRST| TR ", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR ",first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST|TRST| TR ", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Trust owned",owner_renter))%>%
  mutate(owner_renter=ifelse(ain %in% "5829032026", "LLC owned", owner_renter)) %>% # manually recoded after original result returned a property with a LLC and trust
  filter(owner_renter %in% c("LLC owned","Trust owned"))
# check:
table(test$owner_renter) # this has the number of Trusts I expect -originally had only 115 LLCs instead of 116 --but after exploring and manually adjusting for the single AIN case that had Trust AND LLC in the name, now the numbers are as I expect

trust_llc_check<-test_2 %>%
  filter(!ain %in% test$ain) %>%
  select(ain,owner_renter,contains("_owner"),exemption_type, num_howmowner_exemption,landlord_units) %>%
  View()

# further test result
other_trust_llc <- other %>%
  filter(
    grepl("TRUST|LLC", first_owner_name, ignore.case = TRUE) |
      grepl("TRUST|LLC", first_owner_name_overflow, ignore.case = TRUE) |
      grepl("TRUST|LLC", second_owner_name, ignore.case = TRUE)
  ) %>%
  mutate(
    flag_trust = ifelse(
      grepl("TRUST", first_owner_name, ignore.case = TRUE) |
        grepl("TRUST", first_owner_name_overflow, ignore.case = TRUE) |
        grepl("TRUST", second_owner_name, ignore.case = TRUE),
      1, 0
    ),
    flag_llc = ifelse(
      grepl("LLC", first_owner_name, ignore.case = TRUE) |
        grepl("LLC", first_owner_name_overflow, ignore.case = TRUE) |
        grepl("LLC", second_owner_name, ignore.case = TRUE),
      1, 0
    )
  )

other_trust_llc%>%
  summarise(llc_tot=sum(flag_llc), # this is 116 LLCs
            trust_to=sum(flag_trust)) # this is 1498 trusts - 10 more trusts assuming these are all owner occupied or verified rentals

# find out which AIN is not getting captured as an LLC in my recoding -- now null since manual fix
other_llc_ain<-other_trust_llc$ain[other_trust_llc$flag_llc==1]

test_ain<-test$ain[test$owner_renter=="LLC owned"]

not_in_test <- setdiff(other_llc_ain, test_ain)
print(not_in_test) # AIN 5829032026

missing_llc<-other_trust_llc%>%select(ain, flag_llc, flag_trust)%>%filter(ain=="5829032026")

# The reason for the original discrepency is because there is one parcel with LLC & Trust in the name
# first_owner_name=="MOOGAR GROUP LLC TRUSTEE  2908"
# first_owner_name_overflow=="ASITAS AVENUE TRUST"

# Decided this should be coded as LLC owned, and fixed code above
# trust check
trust_check <- other_trust_llc %>% group_by(flag_trust, flag_llc, exemption_type, owner_renter) %>% summarise(count=n())
# 9 more trusts that also have veteran's exemption that's fine, keep recoding

rel_res_df <- rel_res_df%>%
  mutate(
    owner_renter = ifelse(
      (grepl("TRUST", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST", first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("TRUST", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "Trust owned",owner_renter))%>%
  mutate(
    owner_renter = ifelse(
      (grepl("LLC", first_owner_name, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC", first_owner_name_overflow, ignore.case = TRUE) & owner_renter == "Other") |
        (grepl("LLC", second_owner_name, ignore.case = TRUE) & owner_renter == "Other"),
      "LLC owned",owner_renter))%>%
  mutate(owner_renter=ifelse(ain %in% "5829032026", "LLC owned", owner_renter)) # manually recoded after original result returned a property with a LLC and trust

# check:
table(rel_res_df$owner_renter) # this has the number of Trusts I expect -originally had only 115 LLCs instead of 116 --but after exploring and manually adjusting for the single AIN case that had Trust AND LLC in the name, now the numbers are as I expect

## Now lets recode based on tax status:
table(rel_res_df$tax_stat_key)

sold_to_state <- rel_res_df %>% filter(tax_stat_key %in% c("1","2")) %>%
  select(ain, owner_renter, tax_stat_key, year_sold_to_state, first_owner_name, second_owner_name, first_owner_name_overflow, exemption_type, num_howmowner_exemption, landlord_units, owner_renter)
table(sold_to_state$owner_renter)
# some are other owner types, prioritizing tax defaulted status as this could affect rebuilding

rel_res_df<-rel_res_df%>%
  mutate(
    owner_renter = case_when(
      tax_stat_key %in% c("1","2") ~ "Sold to state",
      tax_stat_key %in% "3" ~ "SBE or Government owned",
      TRUE ~ owner_renter  # keeps existing value if none of the above conditions are met
    )
  )

# check
table(rel_res_df$owner_renter) # numbers check out

# Now lets recode the remaining as  "likely owner-occupied, no exemption'
# check remaining other
remaining_other <-rel_res_df %>% filter(owner_renter=="Other") %>%
  select(ain, owner_renter, tax_stat_key, year_sold_to_state, first_owner_name, second_owner_name, first_owner_name_overflow, exemption_type, num_howmowner_exemption, total_units, landlord_units, owner_renter)
# some with church in title, recode as church? could also recode 'TR ' or 'TRST' as trust

rel_res_df<-rel_res_df%>%
  mutate(
    owner_renter = case_when(
      res_type %in% c("Condominium","Single-family", "Multifamily") & owner_renter == "Other" ~ "Likely owner-occupied, no exemption",
      TRUE ~ owner_renter  # keeps existing value if none of the above conditions are met
    )
  )

# check
table(rel_res_df$owner_renter,useNA='always') # numbers check out


## Clean up and export to postgres ----
rel_res_df_final <- rel_res_df %>%
  select(ain,residential,mixed_use,res_type,owner_renter,total_units,landlord_units,total_square_feet,total_bedrooms,use_code)

# check for duplicates
length(unique(rel_res_df_final$ain))
nrow(rel_res_df_final) #same count

# table_name <- "rel_assessor_residential_jan2025"
# schema <- "data"
# indicator <- "Relational data table with summarized information and flags for residential and mixed use properties in Altadena as of January 2025. Only includes properties in either West or East Altadena proper"
# source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_jan25.R "
# qa_filepath<-"  QA_sheet_relational_tables.docx "
# dbWriteTable(con_alt, Id(schema, table_name), rel_res_df_final,
#              overwrite = FALSE, row.names = FALSE)

# # Add metadata
# column_names <- colnames(rel_res_df_final) # Get column names
# column_names
# column_comments <- c('Assessor ID number - use this to match to other relational tables',
#                      'Flag for whether property is a residential use (e.g., use code starting with 0)',
#                      'Flag for whether property is mixed residential-commercial (use code starting with 1 but with a residential combo indicated in 3rd character)',
#                      'Residential type -- either single-family, multifamily, mixed use, condominium, boarding house',
#                      'Housing tenure-ownerships -- either homeowner (indicated by homeowner exemption), renter, trust owned, LLC owned, sold to state, SBE or government owned, or owner likely but with no exemption. For more detailed methodology of choices see R script: Data Prep/resident_exemption_explore.R',
#                      'Total residential units on the property -- use caution when interpreting for mixed use - - can include commercial',
#                      'Total rental units on the property -- use caution when interpreting for mixed use - can include commercial',
#                      'Total square feet of buildings on property',
#                      'Total bedrooms on property',
#                      'Original use code for reference')
# 
 # add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

# STEP 4: TABLE 2: Table to indicate West or East Altadena with geometry ------
rel_area_geom_df <- parcels_altadena %>%
  select(ain,name,label) %>%
  rename(area_name=name,
         area_label=label)

# select just the residential/mixed uses that we found in the assessor data
rel_area_geom_df <- rel_area_geom_df %>%
  filter(ain %in% rel_res_df_final$ain)

# QA quick check---checks out we got the same AINs

# sum(as.numeric(unique(rel_area_geom_df$ain))) # 75678334063382
# sum(as.numeric(unique(rel_res_df_final$ain))) # 75678334063382


# table_name <- "rel_assessor_altadena_parcels_jan2025"
# schema <- "data"
# indicator <- "Relational spatial table with geometries and area flags for residential/mixed use properties in either West or East Altadena proper"
# source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_jan25.R "
# qa_filepath<-"  QA_sheet_relational_tables.docx "
# 
# export_shpfile(con=con_alt, df=rel_area_geom_df, schema="data",
#                table_name= "rel_assessor_altadena_parcels_jan2025",
#                geometry_column = "geom")


# # Add metadata
# column_names <- colnames(rel_area_geom_df) # Get column names
# column_names
# column_comments <- c('Assessor ID number - use this to match to other relational tables',
#                      'West or East Altadena shortened label',
#                      'West or East Altadena long label',
#                      'geometry')
# 
# add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)



# STEP 5: TABLE 3: Damage categories ------
# We need to summarise at assessor ID number the damage categories based on our definitions
# any parcel with at least one building damaged or majorly destroyed >25% is counted as significant damage
# any parcel with maximum of an affecting or minor <25% damage is counted as some damage
# no damage or inaccessible is counted as no damage

# I want the following columns: overall damage category based on groups, binary columns with count of structures in each damage level, list of damage types, count of structures assessed

# keep just AINs in our residential or mixed used base
residential_ains <- st_read(con_alt, query="SELECT * FROM data.rel_assessor_residential_jan2025")

dins_xwalk_res <- dins_xwalk %>% 
  filter(ain %in% residential_ains$ain)

length(unique(dins_xwalk_res$ain))
length(unique(residential_ains$ain))
# some parcels aren't assessed if they aren't in the fire perimeter
# mapview(dins_xwalk_res) +
#   mapview(eaton_fire,col.regions="red") +
#   mapview(parcels_altadena)
# looks pretty good, some holes are from commercial, parks, schools, etc.

## Create binary columns for each damage level -----
table(dins_xwalk_res$damage)

dins_xwalk_res <- dins_xwalk_res %>%
  mutate(destroyed_damage=ifelse(damage=="Destroyed (>50%)", 1,0),
         major_damage=ifelse(damage=="Major (26-50%)", 1,0),
         minor_damage=ifelse(damage=="Minor (10-25%)",1,0),
         affected_damage=ifelse(damage=="Affected (1-9%)",1,0),
         no_damage=ifelse(damage=="No Damage",1,0),
         inaccessible_damage=ifelse(damage=="Inaccessible",1,0))

# check
dins_xwalk_res %>%
  select(damage,ends_with("_damage")) %>%
  View()
# looks good

rel_assessor_dins <- dins_xwalk_res %>%
  st_drop_geometry() %>%
  group_by(ain) %>%
  summarise(
    structure_count=n(),
    destroyed_damage_count=sum(destroyed_damage),
    major_damage_count=sum(major_damage),
    minor_damage_count=sum(minor_damage),
    affected_damage_count=sum(affected_damage),
    no_damage_count=sum(no_damage),
    inaccessible_damage_count=sum(inaccessible_damage),
    damage_type_list=list(unique(damage))
  )

# check counts
nrow(dins_xwalk_res)
sum(rel_assessor_dins$structure_count)
# checks out

sum(rel_assessor_dins$destroyed_damage_count)
# checks out

## Create a single damage category based on highest damage level -----
rel_assessor_dins <- rel_assessor_dins %>%
  ungroup() %>%
  mutate(damage_category = case_when(
    destroyed_damage_count>=1 ~ "Significant Damage", # top code so destroyed goes first
    major_damage_count>=1 ~ "Significant Damage", # same coding for highest damage of major
    minor_damage_count>=1 ~ "Some Damage", # next category
    affected_damage_count>=1 ~ "Some Damage",
    no_damage_count>=1 ~ "No Damage",
    inaccessible_damage_count>=1 ~ "No Damage",
    TRUE ~ NA))
    
# check
rel_assessor_dins %>%
  select(damage_category,everything()) %>%
  View()
# looks good

check <- rel_assessor_dins %>%
  group_by(damage_category,damage_type_list) %>%
  count()
# looks good

# sort list of damage for easier viewing
rel_assessor_dins$damage_type_list <- lapply(rel_assessor_dins$damage_type_list,sort)

check <- rel_assessor_dins %>%
  group_by(damage_category,damage_type_list) %>%
  count()

View(check)
# looks good

# add a column with the count of unique damage types
rel_assessor_dins<- rel_assessor_dins %>%
  group_by(ain) %>%
  mutate(damage_type_count=length(unique(damage_type_list[[1]]))) %>%
  ungroup() %>%
  mutate(mixed_damage=ifelse(damage_type_count<2, "One Damage Type",
                             "Two or More"))

rel_assessor_dins %>%
  select(damage_type_count,damage_type_list,mixed_damage) %>%
  View()
# looks good

## Clean up and push to postgres----
rel_assessor_dins_final <- rel_assessor_dins %>%
  rowwise() %>%
  mutate(damage_type_list=paste(damage_type_list,collapse=', '), # make list column easier to read
         source="CalFire DINS") %>% # add a source column so we know that it came from CalFire
  select(ain, damage_category,source,structure_count,mixed_damage,everything()) 

View(rel_assessor_dins_final)
nrow(rel_assessor_dins_final)
length(unique(rel_assessor_dins_final$ain))
# final check of unique AINs, looks good

# I want to add the unassessed properties too so we can easily loop over this relational table for analysis, so any properties not in the database is labeled as no damage
residential_ains_no_damage <- residential_ains %>%
  filter(!ain %in% rel_assessor_dins_final$ain) %>%
  select(ain) %>%
  mutate(damage_category="No Damage",
         source="Assessor Parcel Outside of Fire Area")

rel_assessor_dins_final_final <- bind_rows(rel_assessor_dins_final,residential_ains_no_damage)


# table_name <- "rel_assessor_damage_level"
# schema <- "data"
# indicator <- "Relational table that contains summarised damage levels for assessor IDS assessed by CalFire post the Eaton Fire, Unlike the damage inspection database, this table is at the unique parcel level rather than building level
# Table includes all residential parcels in Altadena. If they werent assessed then No Damage is assumed. The source column indicates if the parcel was assessed or we are assuming No Damage because outside of fire area and/or not assessed"
# source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/crosswalks_relational_tables/assessor_relational_tables_jan25.R "
# qa_filepath<-"  QA_sheet_relational_tables.docx "

# dbWriteTable(con_alt, Id(schema, table_name), rel_assessor_dins_final_final,
#              overwrite = FALSE, row.names = FALSE)


# # Add metadata
# column_names <- colnames(rel_assessor_dins_final_final) # Get column names
# column_names
# column_comments <- c('Assessor ID number - use this to match to other relational tables',
#                      'Overall damage category level -- top coded so highest damage level of a building on the property takes precedent',
#                      'Whether damage category is based on CalFire DINs or assumed because not in the database',
#                      'Number of structures assessed by CalFire associated with AIN',
#                      'Indicator for whether there was a one type of damage or two or more',
#                      'total count for destroyed structures on property',
#                      'total count for major damage structures on property',
#                      'total count for minor damage structures on property',
#                      'total count for affected damage structures on property',
#                      'total count for no damage structures on property',
#                      'total count for inaccessible structures on property',
#                      'list of unique damage types assigned to property by CalFire',
#                      'Total unique types of damage')
# 
# add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

