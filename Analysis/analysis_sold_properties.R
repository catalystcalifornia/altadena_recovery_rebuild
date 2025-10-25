## Producing tables for characteristics of properties that have sold since the fire
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_analysis_sold_propertiesdocx

#### Step 0: Set Up ####
#load libraries
library(dplyr)
library(purrr)
library(stringr)
library(RPostgres)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)


#### Step 1: Pull in relational datasets ####
housing_sept <- st_read(con, query="SELECT * FROM data.rel_assessor_residential_sept2025 WHERE residential = 'true'") 

damage <- st_read(con, query="SELECT * FROM data.rel_assessor_damage_level_sept2025") 

sales <- st_read(con, query="SELECT * FROM data.rel_assessor_sales_sept2025") 

altadena_e_w <- st_read(con, query="SELECT ain_sept, area_name, area_label FROM data.rel_assessor_altadena_parcels_sept2025") #dropping geom since it's not needed for analysis

#### Step 2: combine all three data frames and clean up ####
all_df<- housing_sept  %>%
  left_join(altadena_e_w, by= "ain_sept") %>%
  left_join(damage, by="ain_sept") %>%
  left_join(sales, by="ain_sept")

nrow(housing_sept)
nrow(all_df)
# looks good same number of rows, check the data now
View(all_df)
# looks good

# check sales
table(all_df$sold_after_eaton,useNA='always')
# assume if not sold, then false -- should correct in relational table

all_df <- all_df %>%
  mutate(sold_after_eaton=
           case_when(
             sold_after_eaton==TRUE ~ "Sold",
             sold_after_eaton==FALSE ~ "Not Sold",
             TRUE ~ "Not Sold"))

table(all_df$sold_after_eaton,useNA='always')

# filter for sales that occured
all_sales <- all_df %>%
  filter(sold_after_eaton=="Sold")

#### Step 3: First ANALYSIS- [analysis_sales_area_sept2025] ####
## What percentage of sales have occurred in each area
analysis_sales_area_prc <- all_df %>% 
  group_by(sold_after_eaton) %>%
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(sold_after_eaton,area_name) %>%
  summarise(sold_count=n(),
            sold_prc=n()/min(total)*100,
            total=min(total))
# maybe west properties make up greater share of sales by a margin

# # Upload tables to postgres and add table/column comments 
# dbWriteTable(con, name = "analysis_sales_area_sept2025", value = analysis_sales_area_prc, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_sales_area_sept2025"
# indicator <- "Distribution of properties by sold status and area, what percentage of properties sold were sold in West Altadena?"
# source <- "Source: LA County Assessor Data, September 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_sales.docx"
# column_names <- colnames(analysis_sales_area_prc) # Get column names
# column_names
# column_comments <- c(
#   "whether analysis is for properties sold or not sold",
#   "area",
#   "count of sold or not sold properties",
#   "percent of properties sold or not sold",
#   "total properties in altadena that were sold or not sold")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 4: Second ANALYSIS- [analysis_sales_damage_sept2025] ####
## At what rate are damaged or undamaged properties selling? -- what % of significantly damaged properties in each area have sold?
analysis_sales_damage_e_w <- all_df %>% 
  group_by(area_name,damage_category) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,damage_category,sold_after_eaton) %>%
  summarise(damage_level_count=n(),
            damage_level_prc=n()/min(total)*100,
            damage_level_total=min(total))

analysis_sales_damage_alt <- all_df %>% 
  group_by(damage_category) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(damage_category,sold_after_eaton) %>%
  summarise(damage_level_count=n(),
            damage_level_prc=n()/min(total)*100,
            damage_level_total=min(total))

analysis_sales_damage <- rbind(analysis_sales_damage_e_w,
                               analysis_sales_damage_alt %>% 
                                 mutate(area_name="Altadena") %>%
                                 select(area_name, everything()))


# Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_sales_damage_sept2025", value = analysis_sales_damage, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_sales_damage_sept2025"
# indicator <- "Rate of properties being sold by damage level and area --what percent of significantly damaged properties in West Altadena have been sold?"
# source <- "Source: LA County Assessor Data, September 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_sales.docx"
# column_names <- colnames(analysis_sales_damage) # Get column names
# column_names
# column_comments <- c(
#   "area name",
#   "damage category",
#   "whether analysis is for properties sold or not sold",
#   "count of sold or not sold properties in damage level",
#   "percent of properties sold or not sold in damage level and area",
#   "total properties in each damage level for each area")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 5: Third ANALYSIS- [analysis_sales_restype_sept2025] ####
# At what rate are properties selling by residential type
e_w <- all_df %>% 
  group_by(area_name,res_type) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,res_type,sold_after_eaton) %>%
  summarise(sold_count=n(),
            sold_prc=n()/min(total)*100,
            res_type_total=min(total))

alt<- all_df %>% 
  mutate(area_name="Altadena") %>%
  group_by(area_name,res_type) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,res_type,sold_after_eaton) %>%
  summarise(sold_count=n(),
            sold_prc=n()/min(total)*100,
            res_type_total=min(total))

analysis_sales_restype <- rbind(e_w,
                               alt) 

# Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_sales_restype_sept2025", value = analysis_sales_restype, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_sales_restype_sept2025"
# indicator <- "Rate of properties being sold by residential type and area --what percent of single family home properties in West Altadena have been sold?"
# source <- "Source: LA County Assessor Data, September 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_sales.docx"
# column_names <- colnames(analysis_sales_restype) # Get column names
# column_names
# column_comments <- c(
#   "area name",
#   "residential type",
#   "whether analysis is for properties sold or not sold",
#   "count of sold or not sold properties in residential type and area",
#   "percent of properties sold or not sold in residential type and area",
#   "total properties in each res type for each area")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 6: Fourth ANALYSIS- [analysis_sales_owner_renter_sept2025] ####
# At what rate are properties selling by owner renter status
e_w <- all_df %>% 
  group_by(area_name,owner_renter) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,owner_renter,sold_after_eaton) %>%
  summarise(sold_count=n(),
            sold_prc=n()/min(total)*100,
            owner_renter_total=min(total))

alt<- all_df %>% 
  mutate(area_name="Altadena") %>%
  group_by(area_name,owner_renter) %>% 
  mutate(total=n()) %>%
  ungroup() %>%
  group_by(area_name,owner_renter,sold_after_eaton) %>%
  summarise(sold_count=n(),
            sold_prc=n()/min(total)*100,
            owner_renter_total=min(total))

analysis_sales_owner_renter <- rbind(e_w,
                                alt) 

# Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_sales_owner_renter_sept2025", value = analysis_sales_owner_renter, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_sales_owner_renter_sept2025"
# indicator <- "Rate of properties being sold by owner type and area --what percent of owner-occupied properties in West Altadena have been sold?"
# source <- "Source: LA County Assessor Data, September 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_sales.docx"
# column_names <- colnames(analysis_sales_owner_renter) # Get column names
# column_names
# column_comments <- c(
#   "area name",
#   "owner renter type",
#   "whether analysis is for properties sold or not sold",
#   "count of sold or not sold properties in owner type and area",
#   "percent of properties sold or not sold in owner type and area",
#   "total properties in each owner type for each area")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 7: Fifth ANALYSIS- [analysis_sales_by_month_sept2025] ####
# Compare sales by month over the past 3 years
sales_by_month <- all_df %>%
  filter(last_sale_year>=2023) %>% # compare last three years
  filter(!last_sale_month %in% c("08","09","10","11","12")) %>% # don't have data for september on yet or complete data for August
  group_by(last_sale_year, last_sale_month) %>%
  summarise(sales_count=n())

# Upload tables to postgres and add table/column comments
# dbWriteTable(con, name = "analysis_sales_month_sept2025", value = sales_by_month, overwrite = FALSE)
# schema <- "data"
# table_name <- "analysis_sales_month_sept2025"
# indicator <- "Total properties being sold by month in last 3 years January-July"
# source <- "Source: LA County Assessor Data, September 2025."
# qa_filepath <- " QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_analysis_sales.docx"
# column_names <- colnames(sales_by_month) # Get column names
# column_names
# column_comments <- c(
#   "sale year",
#   " sale month - number",
#   "sales that occured")
# add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#### Step 8: close connection ####
dbDisconnect(con)
