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

emg_rel_other<-emg_rel%>%
  filter(owner_renter=="Other")

emg_rel_other_exemption<-assessor_data%>%
  filter(ain %in% emg_rel_other$ain)

table(emg_rel_other_exemption$exemption_type) 

# 1 - Veteran: 18 counts
# 4 - Welfare, wholly exempt: 7 counts
# 5 - Full religious: 1 count
# 7 - Welfare, partially exempt: 2 counts

# Check landlord_unit==0 real quick:

emg_rel_other_exemption%>%filter(landlord_units>0) # 0 values

# check res_type on emg_rel table:

table(emg_rel_other$res_type)

# Lets look at res_type for each exemption status type
emg_rel_other_exemption%>%
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

# For now let us recode OTHER where exemption_type==1 as HOMEOWNER-VETERAN

emg_rel_step1<-emg_rel%>%
  mutate(owner_renter=ifelse(ain %in% emg_rel_other_exemption$ain[emg_rel_other_exemption$exemption_type=="1"], "Homeowner-Veteran", 
                             owner_renter))

table(emg_rel_step1$owner_renter) # reduces the 'OTHER' category from 4461 to 4443 - what we would expect that reduces it by 18

# Step 2: Lets explore the remaining 4433 values that are coded as OTHER: TRUSTS/LLCs----------------------------------

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
  mutate(owner_renter=ifelse(ain %in% trust_llc$ain[trust_llc$flag_llc=="1"], "LLC", 
                         ifelse(ain %in% trust_llc$ain[trust_llc$flag_trust=="1"], "Trust",
                             owner_renter)))

table(emg_rel_step2$owner_renter) # Now other is down to 2839




# Step 3: CONTINUE exploring remaining 2839-------------

other_assessor_step3<-other_assessor_step2%>%filter(ain %in% emg_rel_step2$ain[emg_rel_step2$owner_renter=="Other"])

## Lets look at use codes:
# Data dictionary for use codes: "W:\Project\RDA Team\Altadena Recovery and Rebuild\Data\Assessor Data Extract\Use Code 2023.pdf"

table(other_assessor_step3$use_code)

# 384 rows have usecode ==0101 which I think means it is a POOL ----wondering if these should just be excluded
# 1 row has usecode == 0102 which is "Estate Guest House"
      
      