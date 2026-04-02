## Copied from Data Prep/exploration_salesdata.R and refined to include only import steps
# Script imports 'Altadena Not for Sale' data that is typically shared as spreadsheet via .xlsx or google spreadsheet link
# Previous source example is listed in QA doc. 
## QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_anfs_sales_data.docx

#### set up and download current data being used for sales for dashboard ####
#load libraries
library(dplyr)
library(RPostgres)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)


#### download and import new sales data ####
# First, I downloaded data from Asana task attachment: https://app.asana.com/1/110506578179264/project/1211010946971619/task/1213899938055217?focus=true
# Opened the downloaded .xlsx file and saved as .csv
# Copied to W:\Project\RDA Team\Altadena Recovery and Rebuild\Data\Altadena Not for Sale Data
# NOTE: this file has NO HEADERS
# based on URL above I'm assuming these are the correct colnames - cross checked URL against row 1 of csv (address is 92 E Harriet Street)
# also compared against the pg table: dashboard.anfs_sales_data_01092026
anfs_cols <- c(
  "anfs_id", # basically row number
  "property_type", # land, single family home, residential 3 units
  "days_on_market", 
  "begin_date",
  "sold_date",
  "contract_date",
  "list_price",
  "contract_amt",
  "contract",
  "property",
  "city",
  "yr_built",
  "sq_ft_lot",
  "parcel",
  "new_owner"
)

#Second, I am reading it into this script.
# MUST UPDATE FILENAME, DATA VINTAGE YEAR/MONTH/DAY, DATA UPDATE YEAR/MONTH
anfs_sales_filename <- "Altadena LOTS Sold Jan_07 March 30 2026.csv" 
data_vintage_year <- "2026"
data_vintage_month <- "03"
data_vintage_day <- "30"
data_update_year <- "2026"
data_update_month <- "04"
anfs_sales_filepath <- sprintf("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Altadena Not for Sale Data\\%s", anfs_sales_filename)
anfs_sales <- read.csv(anfs_sales_filepath, header=FALSE, col.names = anfs_cols, fileEncoding = "UTF-8-BOM") 

nrow(anfs_sales) # 428
colnames(anfs_sales)
head(anfs_sales)
tail(anfs_sales) # note: headers are at the end and there's a blank row - will convert date cols to date types and filter the NAs
View(anfs_sales)

 
#Third, clean up the dataset 
anfs_sales_clean <- anfs_sales %>% 
  mutate(across(ends_with("_date"), ~as.Date(., format = "%Y-%m-%d"))) %>%
  filter(!is.na(sold_date)) #filter out empty row and header row at the bottom. 
nrow(anfs_sales_clean) # 426 (dropped last 2 rows - good)

# Comment on table and columns
schema <- "dashboard"
table_name <- sprintf("anfs_sales_data_%s_%s", data_update_year, data_update_month)
data_vintage_date <- sprintf("%s-%s-%s", data_vintage_year, data_vintage_month, data_vintage_day)
indicator <- sprintf("Data on sales in Altadena from 2025-01-07 to %s. Reported from Altadena No for Sale.", data_vintage_date)
source <- sprintf("Source sent via email and saved here: %s", anfs_sales_filepath)
qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_anfs_sales_data.docx"
column_names <- colnames(anfs_sales_clean) # Get column names
column_comments <- c(
  "ANFS ID",
  "Property type - includes land, single family home, etc.",
  "days on market",
  "first day on market",
  "date sold", 
  "contract date?",
  "price listed",
  "contract/sale amount?",
  "contact final amount?",
  "address of property",
  "city",
  "year built",
  "square footage of lot", 
  "parcel aka ain", 
  "New Owner"
)

# dbWriteTable(con, DBI::Id(schema = schema, table = table_name), anfs_sales_clean,
#   overwrite = FALSE,
#   row.names = FALSE
# )

# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
