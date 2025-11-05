# Join the assessor data to the damage inspection data
# create a crosswalk of the damage inspection data to the january assessor data


# Library and environment set up ----

library(sf)
library(rmapshaper)
library(leaflet)
library(htmlwidgets)
library(tigris)
library(stringr)
library(readxl)
library(tidyverse)
library(janitor)
library(mapview)
library(writexl)

options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")
con_rda <- connect_to_db("rda_shared_data")
con_fires <- connect_to_db("la_fires")

# Load in data ----

# get assessor data
assessor_data <- st_read(con_alt, query="Select * from data.assessor_data_universe_jan2025")

# get EMG's rel table
emg_rel<-dbGetQuery(con_alt, "SELECT * FROM rel_assessor_residential_jan2025")

# get column names
assessor_cols<-as.data.frame(colnames(assessor_data))

# Step 0: Explore exemption column in Jan assessor data--------------------------------------------

table(assessor_data$exemption_type)
sum(is.na(assessor_data$exemption_type)) # 54159 that used to be the weird symbol are now NA

# Look at how many people in the relational table are not homeowner or renter
table(emg_rel$owner_renter) # 4461 classified as OTHER

# From assessor data extract pdf: "W:\Project\RDA Team\Altadena Recovery and Rebuild\Data\Assessor Data Extract\FIELD DEF -- SBF.pdf"
# Exemption Claim Type
# This is an alphanumeric key which indicates the type of exemption processed. It may be any of the following:
# 0 - Veteran number on file, no claim
# 1 - Veteran
# 2 - Delete veteran exemption
# 3 - Church, wholly exempt
# 4 - Welfare, wholly exempt
# 5 - Full religious
# 6 - Church, partially exempt
# 7 - Welfare, partially exempt
# 8 - Religious, partially exempt
# 9 - Delete real estate exemption

# Recode and explore

exemption_re<-assessor_data%>%
  select(exemption_type)%>%
  mutate(exemption_type_re=ifelse(exemption_type %in% "0", "Veteran number on file, no claim",
                                  ifelse(exemption_type %in% "1", "Veteran",
                                         ifelse(exemption_type %in% "2", "Delete veteran exemption",
                                                ifelse(exemption_type %in% "3", "Church, wholly exempt",
                                                       ifelse(exemption_type %in% "4", "Welfare, wholly exempt",
                                                              ifelse(exemption_type %in% "5", "Full religious",
                                                                     ifelse(exemption_type %in% "6", "Church, partially exempt",
                                                                            ifelse(exemption_type %in% "7", "Welfare, partially exempt",
                                                                                   ifelse(exemption_type %in% "8", "Religious, partially exempt",
                                                                                          ifelse(exemption_type %in% "9", "Delete real estate exemption",
                                                                                                 "No exemption status")))))))))))%>%
  group_by(exemption_type_re)%>%
  mutate(count=n())%>%
  slice(1)

# Step 1: Look at the exemption status type for all the AINs in the EMG rel table that are 'Other' (not renter or homwowner)---------

other_step1<-emg_rel%>%
  filter(owner_renter=="Other")

other_assessor_step1<-assessor_data%>%
  filter(ain %in% other_step1$ain)

table(other_assessor_step1$exemption_type) 

# 1 - Veteran: 18 counts
# 4 - Welfare, wholly exempt: 7 counts
# 5 - Full religious: 1 count
# 7 - Welfare, partially exempt: 2 counts

# Check landlord_unit==0 real quick:

other_assessor_step1%>%filter(landlord_units>0) # 0 values

# check res_type on emg_rel table:

table(other_step1$res_type)

# Lets look at res_type for each exemption status type
other_assessor_step1%>%
  mutate(
    res_type=case_when( # type of residential property
      # condos first
      str_detect(use_code, "C$|E$") ~ "Condominium",
      str_detect(use_code, "^01") ~ "Single-family",
      str_detect(use_code, "^02|^03|^04|^05") ~ "Multifamily",
      str_detect(use_code,"^08") ~ "Boarding house",
      str_detect(use_code,"^12|^17") ~ "Mixed use", 
      TRUE ~NA))%>%
  select(exemption_type, res_type)%>%
  filter(!is.na(exemption_type))%>%
  count(exemption_type, res_type)

# exemption_type      res_type  n
# 1                 Single-family 18
# 4                 Multifamily  1
# 4                 Single-family  6
# 5                 Single-family  1
# 7                 Single-family  2

# We can see all are single or multi-family homes. All of the exemption_type==1 (Veteran exemption) are single-family homes
# I think these can be coded as HOMEOWNER 

# More info about veterans exemption: https://assessor.lacounty.gov/exemptions/veterans

# For now let us recode OTHER where exemption_type==1 as HOMEOWNER

emg_rel_step1<-emg_rel%>%
  mutate(owner_renter=ifelse(ain %in% emg_rel_other_exemption$ain[emg_rel_other_exemption$exemption_type=="1"], "Homeowner", 
                             owner_renter))

table(emg_rel_step1$owner_renter) # reduces the 'OTHER' category from 4461 to 4443 - what we would expect that reduces it by 18

# Step 2: Lets explore the remaining 4433 values that are coded as OTHER: FOcusing on TRUSTS/LLCs----------------------------------

other_step2<-emg_rel_step1%>%filter(owner_renter=='Other')

other_assessor_step2<-assessor_data%>%filter(ain %in% other_step2$ain)

### Look into how many are TRUST OR LLCs ####

trust_llc <- other_assessor_step2 %>%
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
# summarise results:

# 116 LLCs and #1489 Trusts 

trust_llc%>%
  summarise(llc_tot=sum(flag_llc),
            trust_to=sum(flag_trust))

# check if any have rentals

trust_llc%>%filter(landlord_units>0) # none


table(trust_llc$exemption_type) # none of these have exemptions

# Lets recode these as LLCs and Trusts

emg_rel_step2<-emg_rel_step1%>%
  mutate(owner_renter=ifelse(ain %in% trust_llc$ain[trust_llc$flag_llc=="1"], "LLC-Likely not a residence", 
                         ifelse(ain %in% trust_llc$ain[trust_llc$flag_trust=="1"], "Homeowner-Trust",
                             owner_renter)))

table(emg_rel_step2$owner_renter) # Now other is down to 2839

# Step 3: CONTINUE exploring remaining 2839: TAX STATUS-------------

other_assessor_step3<-other_assessor_step2%>%filter(ain %in% emg_rel_step2$ain[emg_rel_step2$owner_renter=="Other"])

# Lets look at tax status

table(other_assessor_step3$tax_stat_key)

# 0 value: 2771 observations -taxes paid not delinquent
# 1 value: 49 observations -Sold to state (delinquent 1-5 years)
# 2 value: 1 observation = deeded to state (delinquent 6+ years)
# 3 value: 18 observations: SBE or government owned

# I think we can recode 50 observations as -OWNED BY STATE and the 18 other observations as -SBE OR GOVERNMENT OWNED 

emg_rel_step3<-emg_rel_step2%>%
  mutate(owner_renter=ifelse(ain %in% other_assessor_step3$ain[other_assessor_step3$tax_stat_key=="1"], "Sold to state (tax delinquent)", 
                             ifelse(ain %in% other_assessor_step3$ain[other_assessor_step3$tax_stat_key=="2"], "Sold to state (tax delinquent)",
                                    ifelse(ain %in% other_assessor_step3$ain[other_assessor_step3$tax_stat_key=="3"], "SBE or Government owned",
                                    owner_renter))))

## QUESTION for EMG: Do we want one lumped category that is just 'Government owned' for all the tax_stat_key values (1,2,3)? 

# check: we see 'Other' category reduced by 68 so went from to 2839 to 2771
table(emg_rel_step3$owner_renter)

# Step 4: CONTINUE exploring remaining 2771 'OTHER' observations-----------------------------

other_assessor_step4<-other_assessor_step3%>%filter(ain %in% emg_rel_step3$ain[emg_rel_step3$owner_renter=="Other"])

# Lets look at zone codes really quick:

# Filter out any zoning_code values where the third character !=R (this would be Residential) based on data dictionary "W:\Project\RDA Team\Altadena Recovery and Rebuild\Data\Assessor Data Extract\FIELD DEF -- SBF.pdf"

# From data dictionary:
# Zone Code
# This identifies the incorporated or unincorporated cities in Los Angeles County in which the property is located, the zone of the property, and the minimum lot size and/or height limit. It may include from 3 to 15 characters. The first two characters which are always alphabetic, represent zoning jurisdictions or cities, (e.g., LB - Long Beach, LA - Los Angeles, LC - Los Angeles County). See appendix for complete list.
# The third character is always alphabetic too. It may be any of the following:
#   A = Agricultural (Note: For Long Beach only, A = Amusement)
# C = Commercial
# M = Industrial
# R = Residential
# The fourth character is alphanumeric and generally represents either the intensity or limit of a property's use; or, in the case of a dash, it is used to separate multiple zones or height districts.
# The fifth through the fifteenth characters represent data supplied and used by the Real Estate Division of the L.A. Office of Assessor.

zoning<-other_assessor_step4%>%
  filter(substr(zoning_code, 3, 3) != "R")

zoning%>%filter(landlord_units>0)

table(zoning$zoning_code)

# LCC2*           LCC2YY          LCC3*           LCC3YY          LCM1*           LCM30000*       LCP*            
#   3               2               2               2               5               3               1               1 
# LCPYYY          
# 1 

# Third character==C means "Commercial" (9 observations)
# Third character==M means "Industrial" (8 observations)
# Third character==P -nothing in data dictionary (2 observations)

# All of these have bedroom units so they are probably mix-used and should be left as it


# I am spot checking some of the AINs of the remaining 2771 OTHER observations and it is likely they are all probably homeowners who forgot to file their exemption
# Going to recode any remaining that also have a SINGLE/MULTIFAMILY use code as 'Likely homeowner -No exemption'

# First recode use codes (taken from Elycia's original relational table script and already QAed)

other_assessor_step4<-other_assessor_step4%>%
  mutate(
    res_type=case_when( # type of residential property
      # condos first
      str_detect(use_code, "C$|E$") ~ "Condominium",
      str_detect(use_code, "^01") ~ "Single-family",
      str_detect(use_code, "^02|^03|^04|^05") ~ "Multifamily",
      str_detect(use_code,"^08") ~ "Boarding house",
      str_detect(use_code,"^12|^17") ~ "Mixed use", 
      TRUE ~NA))

table(other_assessor_step4$res_type) # all condos, multilfamily or single famiyl residences

# should also be no landlord units

other_assessor_step4%>%filter(landlord_units>0) # none

# Recode

emg_rel_step4<-emg_rel_step3%>%
  mutate(owner_renter=ifelse(ain %in% other_assessor_step4$ain[other_assessor_step4$res_type=="Condominium"], "Likely Homeowner -No exemption", 
                             ifelse(ain %in% other_assessor_step4$ain[other_assessor_step4$res_type=="Single-family"], "Likely Homeowner -No exemption", 
                                    ifelse(ain %in% other_assessor_step4$ain[other_assessor_step4$res_type=="Multifamily"], "Likely Homeowner -No exemption", 
                                           owner_renter))))


# check
table(emg_rel_step4$owner_renter) # no more 'OTHER" category

# Push table to postgres--------------------------------------

df <- emg_rel_step4%>%
  rename("owner_renter_re"="owner_renter")%>%
  left_join(emg_rel%>%select(ain, owner_renter))%>%
  rename("owner_renter_original"="owner_renter")%>%
  select(ain, residential, mixed_use, res_type, owner_renter_re, owner_renter_original, everything())

# check for duplicates
length(unique(df$ain))
nrow(df) #same count

table_name <- "rel_assessor_residential_jan2025_recode"
schema <- "data"
indicator <- "Relational data table with summarized information and flags for residential and mixed use properties in Altadena as of January 2025. Only includes properties in either West or East Altadena proper. Includes a owner_renter_original flag for what was originally created in the assessor_relational_tables_jan25.R script and a column for a recoded version of that flag called owner_renter_re"
source <- "Script: W:/Project/RDA Team/Altadena Recovery and Rebuild/GitHub/EMG/altadena_recovery_rebuild/Data Prep/resident_exemption_explore.R "
qa_filepath<-"  QA_resident_exemption_explore.docx "
dbWriteTable(con_alt, Id(schema, table_name), df,
             overwrite = TRUE, row.names = FALSE)

# Add metadata
column_names <- colnames(df) # Get column names
column_names
column_comments <- c('Assessor ID number - use this to match to other relational tables',
                     'Flag for whether property is a residential use (e.g., use code starting with 0)',
                     'Flag for whether property is mixed residential-commercial (use code starting with 1 but with a residential combo indicated in 3rd character)',
                     'Residential type -- either single-family, multifamily, mixed use, condominium, boarding house',
                     'Housing tenure-ownerships recoded-- Includes more categories and recodes all properties that were originally the OTHER category from the owner_renter_original column',
                     'Housing tenure-ownerships -- either homeowner (indicated by homeowner exemption), renter, homeowner-renter combo, or other (no homeowner exemption or rental units indicated',
                     'Total residential units on the property -- use caution when interpreting for mixed use - - can include commercial',
                     'Total rental units on the property -- use caution when interpreting for mixed use - can include commercial',
                     'Total square feet of buildings on property',
                     'Total bedrooms on property',
                     'Original use code for reference')

 add_table_comments(con_alt, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
      