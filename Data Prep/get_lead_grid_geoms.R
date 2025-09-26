# Get lead grid geoms from arcgis online REST API

library(httr) # GET()
library(jsonlite) # content()
library(sf) # st_read()
library(mapview)

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

schema <- "data"
table_name <- "lacdph_lead_results_grid_2025"
indicator <- "Grid geoms used to map lead testing for Eaton fire"
source <- "LA County Department of Public Health"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_lead_data.docx"
column_names <- colnames(sf_object)[1]
column_comments <- c("Grid label")

export_shpfile(con=con, df=sf_object, schema=schema, table_name=table_name, geometry_column = "geometry")

add_table_comments(con=con, schema=schema, table_name=table_name, 
                   indicator=indicator, source=source, qa_filepath=qa_filepath, 
                   column_names=column_names, column_comments=column_comments)

