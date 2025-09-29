## Script adds grid geoms for lead testing data from LACDPH and joins attribute data to geoms
## Sources: 
# https://storymaps.arcgis.com/stories/667412ef37ee4392bddb5c90da3480f1#ref-n-5niwaH
# https://www.arcgis.com/apps/mapviewer/index.html?url=https://services.arcgis.com/RmCCgQtiZLDCtblq/ArcGIS/rest/services/Eaton_Fire_Area_Reference_Grid_Roux_041725/FeatureServer/14/&source=sd 


# Get lead grid geoms from arcgis online REST API ----

library(httr) # GET()
library(jsonlite) # content()
library(sf) # st_read()
library(mapview)
library(readxl)
library(stringr)

source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("altadena_recovery_rebuild")

# define url for api endpoint
api_url <- "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Eaton_Fire_Area_Reference_Grid_Roux_041725/FeatureServer/14/query?where=1%3D1&objectIds=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=&returnGeometry=true&returnCentroid=false&returnEnvelope=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&collation=&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnTrueCurves=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token="

# request data from api endpoint
response <- GET(api_url)

# Check status code for request - 200 is successful
response$status_code

# extract json portion of response
json_content <- content(response, "text")

# convert json to spatial data (e.g., geojson, sf object, etc.) to view and confirm shapes
sf_object <- st_read(json_content)

# map to confirm shapes
mapview(sf_object)

# export to pg - rename columns, define schema, table names, etc.
colnames(sf_object) <- c("grid_name", "geometry")



# Get lead data (geometric mean and percent exceedances) for Eaton grids ----
# We are only doing lead because per Rouxs summary paper -- lead was the only contaminant that exceeded acceptable thresholds post-fire
# manually entered lead data
# source: http://publichealth.lacounty.gov/media/wildfire/docs/Roux_Community_Soil_Sampling_and_Human_Health_Screening_Report_Eaton_and_Palisades_Fire_Regions.pdf
# pg. 48 
# manually entered here: W:\Project\RDA Team\Altadena Recovery and Rebuild\Data\LACDPH Roux Soil Testing\Eaton Results Lead Geometric Mean August 2025.xlsx
geo_mean_df <- read_excel("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\LACDPH Roux Soil Testing\\Eaton Results Lead Geometric Mean August 2025.xlsx")


# percentage exceedances from external appendix
# source: https://storymaps.arcgis.com/stories/667412ef37ee4392bddb5c90da3480f1
# downloaded a subfolder of Appendix B here: W:\Project\RDA Team\Altadena Recovery and Rebuild\Data\LACDPH Roux Soil Testing\Summary Statistics (By Grid), For Specific COCs
pct_exceed_df <- read_excel("W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Data\\LACDPH Roux Soil Testing\\Summary Statistics (By Grid), For Specific COCs\\Eaton_Lead_data_20250826.xlsx")

# clean up column names
colnames(pct_exceed_df) <- tolower(colnames(pct_exceed_df))

pct_exceed_df <- pct_exceed_df %>%
  rename(lead_samples_total=total_samples_lead,
         lead_exceedances_count=total_exceedances_lead,
         lead_exceedances_percent=percent_exceedances_lead)

# clean up grid labels
start_char <- "-"
end_char <- "-"
pattern <- paste0("(?<=\\", start_char, ").*?(?=\\", end_char, ")")
pct_exceed_df$grid_name<- str_extract(pct_exceed_df$grid, pattern)
View(pct_exceed_df)
# F10 formatted differently, manually clean
pct_exceed_df <- pct_exceed_df %>%
  mutate(grid_name=case_when(is.na(grid_name) ~ "F10",
                             TRUE ~ grid_name))

View(pct_exceed_df)

## Match two dataframes
lead_data <- geo_mean_df %>% 
              left_join(pct_exceed_df %>% select(-grid), by=c("grid"="grid_name"))

View(lead_data) # looks good

# Join geoms to lead data -------
sf_object <- sf_object %>%
  left_join(lead_data,by=c("grid_name"="grid"))

sf_object <- st_transform(sf_object, 3310)

schema <- "data"
table_name <- "lacdph_lead_results_grid_2025"
indicator <- "Lead testing results and grid geoms for mapping lead results post the Eaton fire. Soil sample testing only done for intact homes - property had less than 26% damage (no damage, minor, affected damage)"
source <- "LA County Department of Public Health and Roux Associates
Read more here: https://storymaps.arcgis.com/stories/667412ef37ee4392bddb5c90da3480f1?cover=false"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_lead_data.docx"
column_names <- colnames(sf_object)
column_names <- column_names[column_names !="geometry"]
column_comments <- c("Grid label",
                     "lead geometric mean for the grid measured in mg/kg, 80 or more indicates above residential soil screening levels considered protective over a lifetime of exposure",
                     "total lead samples collected",
                     "total lead samples above the residential soil screening level (80 mg/kg)",
                     "percent of lead samples collected over the residential soil screening level")

export_shpfile(con=con, df=sf_object, schema=schema, table_name=table_name, geometry_column = "geometry")

add_table_comments(con=con, schema=schema, table_name=table_name, 
                   indicator=indicator, source=source, qa_filepath=qa_filepath, 
                   column_names=column_names, column_comments=column_comments)

