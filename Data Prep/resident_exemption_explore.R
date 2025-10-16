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

# Explore exemption column--------------------------------------------

table(assessor_data$exemption_type)

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
                                                                     "Delete real estate exemption"))))))))))%>%
  group_by(exemption_type_re)%>%
  mutate(count=n())%>%
  slice(1)

# Notes:
# Seems like an encoding issue in the initial import of the assessor data which is why we see Ã¿ as one of the exemption type codes
# But based on the data dictionary of the assessor data and if we assume Ã¿ should be 9, which means 'Delete real estate exemption'
# I am wondering if this is a potential source of why there are so many exemptions. Because these are supposed to be 'Delete real estate exemption'
# Meaning (I think) that the exemption no longer exists. And given how many records have this exemption code. 

# Explore where exemption_type (maybe) == 9 ---------

exemption_re_9<-assessor_data%>%
  mutate(exemption_type_re=ifelse(exemption_type %in% "0", "Veteran number on file, no claim",
                                  ifelse(exemption_type %in% "1", "Veteran",
                                         ifelse(exemption_type %in% "2", "Delete veteran exemption",
                                                ifelse(exemption_type %in% "3", "Church, wholly exempt",
                                                       ifelse(exemption_type %in% "4", "Welfare, wholly exempt",
                                                              ifelse(exemption_type %in% "5", "Full religious",
                                                                     ifelse(exemption_type %in% "6", "Church, partially exempt",
                                                                            ifelse(exemption_type %in% "7", "Welfare, partially exempt",
                                                                                   ifelse(exemption_type %in% "8", "Religious, partially exempt",
                                                                                          "Delete real estate exemption"))))))))))%>%
  filter(exemption_type_re=="Delete real estate exemption")


# pull out ains
ain9<-exemption_re_9$ain

# pull out emg xwalk ains
emg_ain<-emg_rel$ain

# See where there is overlap and not overlap in AINS

ains_in_emg <- as.data.frame(unique(ain9[ain9 %in% emg_ain])) # 12,919 of these made it into the emg rel table
ains_not_in_emg <- as.data.frame(unique(ain9[!ain9 %in% emg_ain])) #41,240 not in the rel table

# Isolate those AINs and look deeper at them, first see how these AINs were recoded in the rel table

rel_ain9<-emg_rel%>%
  filter(ain %in% ain9)

table(rel_ain9$owner_renter) # only 4433 recoded as 'Other' and 1078 recoded as 'Homeowner/Renter'

# confirm these also don't have landlord_units

rel_ain9<-emg_rel%>%
  filter(ain %in% ain9)%>%
  filter(owner_renter=="Other")%>%
  mutate(rental_flag=ifelse(landlord_units<1, "No rentals", "Rentals"))

# There are 4433 rows recoded as 'Other' that are the AINS where exemption_type=="Delete Real Estate Exemption"
table(rel_ain9$rental_flag) # None of them have rentals (as we'd expect)

# Notes:
# I am not entirely sure if there is anything here. I think this is mostly dependent on really understanding what 
# "Delete Real Estate Exemption" means. Online it looks like it just means that property is no longer exempt
# But the reason why can vary from failure to file, house sale, death etc.
# The question here then is should these 4433 rows be recoded from 'Other' to 'Homeowner' ?

# Lets also look at their usecodes

table(rel_ain9$res_type) 

# Notes:
# There is one boarding house that should NOT be recoded as 'Homeowner'
# But there are 4,260 Single-family res type that might need to be recoded as 'Homeowner' 

# Condos and Multifamily I am not as sure
