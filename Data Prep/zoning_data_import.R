## Importing Zoning Perimeter Data 
## DATA SOURCE: City of Pasadena Zoning Data Dec 4, 2024 | Los Angeles County Unincorporated Zoning Data April 10, 2025
## QA DOC: W:\Project\RDA Team\Altadena Recovery and Rebuild\Documentation\QA_Sheet_import_zoning.docx

#### Step 0: Set Up ####
#load libraries
library(dplyr)
library(purrr)
library(stringr)
library(sf)
library(RPostgres)

#Add database connection and source script with functions for pushing to postgres
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")
options(scipen = 999)


#### Step 1: Pull in raw data and prep ####

#Raw Data Source URL Pasadena: https://data.cityofpasadena.net/datasets/17339081d410464a94d2df1ddd95d3d6_0/about
#Raw Data Source URL LA Unincorporated: https://egis-lacounty.hub.arcgis.com/datasets/3a3cf8766dca45e7a9fd0fbb8d0cd93c_3/explore 


#downloaded here
pas_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Zoning_CityofPasadena\\Zoning.shp")
lac_shp <- st_read("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\Zoning_LACounty_Unincorporated\\Zoning_(L.A._County_Unincorporated).shp")


#check SRID
st_crs(pas_shp) #2229
st_crs(lac_shp) #2229

#transform to 3310/ California Albers preferred for CA statistics/analysis
pas_shp_3310 <- st_transform(pas_shp, 3310) 

lac_shp_3310 <- st_transform(lac_shp, 3310) 
colnames(lac_shp_3310) <- tolower(colnames(lac_shp_3310))

#### Step 1A: Cleaning up column names for Pasadena ####
#rename columns
#1st, pull the column names from csv and view
step1_col_names <- names(pas_shp)
View(tibble::tibble(original = step1_col_names))

#2nd, clean column names and view
step2_col_names <- step1_col_names %>%
  # make lowercase
  str_to_lower() %>%
  # delete spaces and punctuation, add underscores but delete trailing underscores 
  str_replace_all("[^a-z0-9]+", "_") %>%
  str_replace_all("^_+|_+$", "")

#3rd, create df to see if cleaning is good and what needs to be renamed
step3_col_names <- as.data.frame(tibble(original = step1_col_names, cleaned = step2_col_names))
View(step3_col_names)

#4th, rename columns as needed
cols_rename <- c(
  "gen_code" = "general_code",
  "overlay_co" = "overlay_code",
  "overlay_de" = "overlay_label",
  "overlay_1" = "overlay_code2",
  "gen_plan" = "general_plan",
  "gen_plan_d" = "general_plan_label"
)

step4_col_names <- step3_col_names %>%
  mutate(
    cleaned = ifelse(cleaned %in% names(cols_rename),
                     cols_rename[cleaned],
                     cleaned)
  ) %>%
  filter(!cleaned %in% c("objectid", "x", "y")) 

#5th, apply to shapefile
names(pas_shp_3310)[1:ncol(pas_shp_3310)-1] <- step4_col_names$cleaned

#### Step 2: Use export function to push to postgres ####

# export_shpfile(con=con, df=pas_shp_3310, schema="data",
#                table_name= "cityofpasadena_zoning_3310",
#                geometry_column = "geometry")
# 
# dbSendQuery(con, "COMMENT ON TABLE data.cityofpasadena_zoning_3310 IS
#             'City of Pasadena Zoning Data, Dec 4, 2024 in SRID 3310
#             Data imported on 9-12-25
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_zoning.docx
#             Source:https://data.cityofpasadena.net/datasets/17339081d410464a94d2df1ddd95d3d6_0/about'")
# 
# export_shpfile(con=con, df=lac_shp_3310, schema="data",
#                table_name= "lac_unincorporated_zoning_3310",
#                geometry_column = "geometry")
# 
# dbSendQuery(con, "COMMENT ON TABLE data.lac_unincorporated_zoning_3310 IS
#             'LA County Unincorporated Zoning Data, April 10, 2025 in SRID 3310
#             Data imported on 9-12-25
#             QA DOC: W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_Sheet_import_zoning.docx
#             Source:https://egis-lacounty.hub.arcgis.com/datasets/3a3cf8766dca45e7a9fd0fbb8d0cd93c_3/explore'")
