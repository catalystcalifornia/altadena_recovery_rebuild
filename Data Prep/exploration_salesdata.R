## Exploring Sales Data from LA County Assessor Portal and collected by group, 'Altadena Not for Sale'

## Sales Data from 'Altadena Not for Sale' https://docs.google.com/spreadsheets/d/1qDFwOTnUq87o12n-EeFIZ0xfcqpGOEoNIGFbDJe33a8/edit?gid=846804396#gid=846804396

## QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_anfs_sales_data.docx

## The goal for this exploratory script is to do the following: 
# 1. Evaluating where their higher # of sales come from - is this from sales that occurred after October 7th or from them tracking undamaged homes?
# 2. For additional sales they have tracked of significantly damaged properties, QA their data against a public source of current sales (e.g., redfin or other site) to make sure they aren't over identifying sales
# 3. A script for merging the assessor sales and their sales, e.g., use assessor through october 7th and fill in remaining sales with their data.

#### set up and download current data being used for sales for dashboard ####
#load libraries
library(dplyr)
library(purrr)
library(stringr)
library(sf)
library(RPostgres)
library(leaflet)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)

lac_sales <-  st_read(con, query="SELECT * FROM dashboard.rel_assessor_sales_2025_12")

lac_res <- st_read(con, query = "SELECT * FROM dashboard.rel_assessor_residential_2025_12")

xwalk <- st_read(con, query = "SELECT * FROM dashboard.crosswalk_assessor_2025_09_12")


#### download and import new sales data ####
#First, I downloaded to the data from the URL link to here as a csv file: W:\Project\RDA Team\Altadena Recovery and Rebuild\Data\Altadena Not for Sale Data

#Second, I am reading it into this script. 
anfs_sales <- read.csv("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Altadena Not for Sale Data\\Altadena Not for Sale_ property sold through Jan 7, 2025-Jan.9, 2026 - Final stats _ SOLD (Jan 7, 2025 - Jan 9, 2026).csv")

#Third, clean up the dataset 
anfs_sales <- anfs_sales %>% 
  rename_with(~ .x %>%
                tolower() %>%
                str_replace_all("\\.", "_")) %>%
  rename(notes = x,
         contract_amt = contract__,
         parcel = parcel__) %>% 
  relocate(notes, .after = last_col()) %>%
  filter(!is.na(sold_date), sold_date != "") #filter out empty rows at the bottom. 


# dbWriteTable(con, DBI::Id(schema = "dashboard", table = "anfs_sales_data_01092026"), anfs_sales,
#   overwrite = FALSE,  
#   row.names = FALSE
# )

# Comment on table and columns
schema <- "dashboard"
table_name <- "anfs_sales_data_01092026"
indicator <- "Data on sales in Altadena from January 7, 2025 to January 9, 2026. Reported from Altadena No for Sale."
source <- "Source: https://docs.google.com/spreadsheets/d/1qDFwOTnUq87o12n-EeFIZ0xfcqpGOEoNIGFbDJe33a8/edit?gid=846804396#gid=846804396"
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_anfs_sales_data.docx"
column_names <- colnames(anfs_sales) # Get column names
column_comments <- c(
  "New Owner",
  "address of property",
  "days on market",
  "first day on market",
  "contract/sale amount?",
  "price listed",
  "contact final amount?",
  "date sold", 
  "square footage of lot", 
  "parcel aka ain", 
  "year built", 
  "notes"
)

# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#### Goal 1: Evaluating where the higher # of sales comes from ####
#Question: Do they have sales documented after October 7th? 

#checking how many we have identified as sold
lac_sales %>%
  count(sold_after_eaton)
# Our LA County Assessor data has identified 271 properties sold. 

anfs_afteroct7 <- anfs_sales %>%
  mutate(sold_date = as.Date(sold_date)) %>%
  filter(sold_date > as.Date("2025-10-07"))

nrow(anfs_afteroct7)
nrow(anfs_sales)
# A total of 358 are identified as sold and 85 of those properties are identified as sold after October 7th
# 358- 85 = 273 (there are still 2 additional properties they have marked as sold even if accounting for Oct 7th as a cut off for data)

#Question: Are they tracked undamaged properties? 

dmg_lac_data <- st_read(con, query="SELECT * FROM data.rel_assessor_damage_level_sept2025")

dmg_anfs_sales <- anfs_sales %>%
  left_join(dmg_lac_data, by = c("parcel" = "ain_sept"))

dmg_anfs_sales %>%
  count(damage_category)
#The count reveals that only 343 of the properties marked as sold in the anfs data are significantly damaged so yes, they are including other properties but also have reported more properties sold that are significantly damaged than we have with assesor data

# 10 NA properties for damage
dmg_anfs_na <- dmg_anfs_sales %>% filter(is.na(damage_category)) 
View(dmg_anfs_na) # one with double ain, fix that
# create list with these
dmg_anfs_na_list <- dmg_anfs_na$parcel
dmg_anfs_na_list <- dmg_anfs_na_list[dmg_anfs_na_list != "5844012018,19"]
dmg_anfs_na_list  <- c(dmg_anfs_na_list , "5844012018")
dmg_anfs_na_list  <- c(dmg_anfs_na_list , "5844012019")
# check these
lac_sales_xwalk <- lac_sales %>% filter(sold_after_eaton == TRUE) %>%
  left_join(xwalk, by=c("ain"="ain_2025_12")) %>%
  filter(ain %in% dmg_anfs_na_list | ain_2025_01 %in% dmg_anfs_na_list | ain_2025_09 %in% dmg_anfs_na_list)
# only one found a match
# ones that don't merge are either commercial, deleted or typos in parcel number
# https://portal.assessor.lacounty.gov/parceldetail/5841032019 # sold date recorded after 10/07 on portal and its commercial
# 5842020011 does not turn up a record on assessor should be 5842022011 # we mark as sold
# deleted parcel https://portal.assessor.lacounty.gov/parceldetail/5843022001 now 5843-022-058 which we mark as sold as well
# 5845002015 commercial
# 5841023009 deleted and now 5841023022 - we dont mark as sold, parcel could have been merged and we dont have the sales data for it as a result - should we correct this on our end and mark as sold?
# 5844018036 is a bad AIN should be 5846018036 - sold in November
# 5847020011 is a deleted parcel now 5847020027 - sold in October
# 5482015020 should 5842015020 # we mark as sold
# 5835038003 commercial https://portal.assessor.lacounty.gov/parceldetail/5835038003

#Question: Are there any significantly damaged properties they've identified as sold before October 7th that we have not? 
anfs_beforeoct7 <- dmg_anfs_sales %>% 
  filter(damage_category == 'Significant Damage')%>%
  mutate(sold_date = as.Date(sold_date)) %>%
  filter(sold_date < as.Date("2025-10-07"))

#ANFS identified a total of 261 significantly damaged properties sold before October 7th and we have identified 10 more sold than they have. 
#Which ones did we identify as sold and they did not? 
discrepency_lac <- anti_join(lac_sales %>% filter(sold_after_eaton == TRUE), 
                             anfs_beforeoct7, by = c("ain" = "parcel"))

discrepency_anfs <- anti_join(anfs_beforeoct7,
                              lac_sales %>% filter(sold_after_eaton == TRUE), 
                               by = c("parcel" = "ain"))
#32 properties identified for discrepency by lac.. may be due to private sales that are in assessor data and not on public sites
#22 properties identified for discrepency by anfs..

#check discrepency_anfs parcel numbers for addresses, maybe we have a different parcel number for an address than they do. And if it does say the ain parcel has been sold from a public sale site
# 5829014016 | https://portal.assessor.lacounty.gov/parceldetail/5829014016 address: 509 W ALTADENA DR AND discrepency_anfs says the address as 509 Altadena Dr so there is a discrepency in address
## searching to see if address match but first I need sales data that has address
## View(lac_sales %>% filter(ain == "5829014016")) #we report as not sold
## View(lac_res %>% filter(ain_2025_12 == "5829014016")) #address we associated with this ain: 509 W Altadena so address DOES NOT MATCH
# recording date on portal is 10-03-25 so likely a delay in the LAC assessor data
## public sale site says 509 W ALTADENA DR has been sold: https://www.zillow.com/homedetails/509-W-Altadena-Dr-Altadena-CA-91001/20909445_zpid/
## public sale site also says 509 ALTADENA DR has been sold: https://www.realtor.com/realestateandhomes-detail/509-W-Altadena-Dr_Altadena_CA_91001_M18287-76316

# 5843012005 | https://portal.assessor.lacounty.gov/parceldetail/5843012005 address: 3273 ALEGRE LN
## View(lac_sales %>% filter(ain == "5843012005")) #we report as not sold
## View(lac_res %>% filter(ain_2025_12 == "5843012005")) # address matches 3273 ALEGRE LN
## public sale site says 3273 ALEGRE LN has been sold: https://www.zillow.com/homedetails/3273-Alegre-Ln-Altadena-CA-91001/20916352_zpid/

# 5843024014 | https://portal.assessor.lacounty.gov/parceldetail/5843024014 address: 1695 E Loma Alta Drive (matches with anfs)
## View(lac_sales %>% filter(ain == "5843024014")) #we report as not sold
## View(lac_res %>% filter(ain_2025_12 == "5843024014")) # address matches 
## public site says it has been sold: https://www.realtor.com/realestateandhomes-detail/1695-E-Loma-Alta-Dr_Altadena_CA_91001_M21621-20172


# View(lac_sales %>% filter(sold_after_eaton == TRUE))
#My follow up questions is what geographic area are ANFS monitoring? It may be a smaller area than we are? I cannot identify what else is contributing to the discrepency?
geom_lac <- st_read(con, query="SELECT * FROM dashboard.rel_assessor_parcels_2025_12")
geom_disrepency_lac <- right_join(geom_lac,
                                 discrepency_lac, by = c("ain_2025_12" = "ain"))
geom_disrepency_lac %>%
  st_transform(4326) %>%   
  leaflet() %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  addPolygons(
    weight = 1,
    color = "blue",
    fillOpacity = 0.6
  )

#Assessment: area is not a factor. Likely it is a discrepency of different reporting to la county and some AIN variations

# check our parcels that sold but aren't marked as sold by anfs
discrepancy_lac_address <- lac_res %>% filter(ain_2025_12 %in% discrepency_lac$ain)
# 5847028002 sold and listed again https://www.zillow.com/homedetails/1578-Morada-Pl-Altadena-CA-91001/20918644_zpid/
# 5846022029 sold https://www.zillow.com/homedetails/1678-Midwick-Dr-Altadena-CA-91001/20918041_zpid/
# 	5846011031 sold https://www.zillow.com/homedetails/2577-Boulder-Rd-Altadena-CA-91001/20917766_zpid/
# our extra sales seem accurate

#### Goal 2: QA ANFS data for significantly damaged properties sold ####
#start with filtering for the properties that are significantly damaged AND have not been identified as sold by us 
qa_filter_anfs <- dmg_anfs_sales %>% 
  filter(damage_category == 'Significant Damage') %>% 
  inner_join(lac_sales %>% filter(sold_after_eaton == FALSE), by = c("parcel" = "ain"))

# check date of sales
qa_filter_anfs_sales_data <- table(qa_filter_anfs$sold_date) %>% as.data.frame()
# from the qa list, I randomly picked the following 5 properties and checked if they are reported as sold on redfin, etc. 
# most after October

# 1145 E Altadena Dr | 5844015012
# Zillow reports it has been sold on 10/31/25 | https://www.zillow.com/homedetails/1145-E-Altadena-Dr-Altadena-CA-91001/20916907_zpid/
# Redfin also reports that it has been sold 
# sales date a little different - difference of 3 days

# 416 E Altadena Drive | 5840005027
# Zillow reports it has been sold on 12/10/25 | https://www.zillow.com/homedetails/416-E-Altadena-Dr-Altadena-CA-91001/20914934_zpid/
# Realtor.com reports the same | https://www.realtor.com/realestateandhomes-detail/416-E-Altadena-Dr_Altadena_CA_91001_M20505-08674
# sales date a little different by a day but sold

# 2962 Emerson Way | 5833022015
# Redfin reports sold on 11/14/25 | https://www.redfin.com/CA/Altadena/2962-Emerson-Way-91001/home/7254923
# Zillow reports the same | https://www.zillow.com/homedetails/2962-Emerson-Way-Altadena-CA-91001/20911781_zpid/

# 3234 Raymond Ave | 5833006011
# Zillow reports it was sold on 10/20/25 | https://www.zillow.com/homedetails/3234-Raymond-Ave-Altadena-CA-91001/20911355_zpid/
# Realtor.com reports the same | https://www.realtor.com/realestateandhomes-detail/3234-Raymond-Ave_Altadena_CA_91001_M19331-20956

# 900 E Mount Curve Ave | 5842016028
# Zillow reports it has been sold 10/27/25 | https://www.zillow.com/homedetails/900-E-Mount-Curve-Ave-Altadena-CA-91001/20916105_zpid/
# Berkshire reports the same | https://www.bhhsdrysdale.com/ca/900-e-mount-curve-ave-altadena-91001/pid-407491796

# check sold off market
# 5846011033 sold - https://www.zillow.com/homedetails/Altadena-CA-91001/20917768_zpid/
# 5846001005 - sold - https://www.zillow.com/homedetails/1085-E-Mendocino-St-Altadena-CA-91001/20917500_zpid/
#Overall the conclusion is that the public sites report the same sales that the ANFS data does. 

#### Goal 3: Code for a script to merge our sales data with their sales data ####

# In order to add their sales data to our sales data, we would need to edit our script altadena_recovery_rebuild/Data Prep/Monthly Updates/rel_assesor_sales.R
## Please see the changes I've made in that script under STEP 6B. 
