# Create universe of january residential parcels to track overtime

##### Step 0: Set up, initial prep #####
# Library and environment set up
source("W:\\RDA Team\\R\\credentials_source.R")
library(sf)
library(mapview)

options(scipen=999)

con <- connect_to_db("altadena_recovery_rebuild")

res<- st_read(con, query="SELECT * from data.rel_assessor_residential_jan2025")

dmg <- st_read(con, query="SELECT * from data.rel_assessor_damage_level")

# join dataframes
jan_universe <- res %>% left_join(dmg, by="ain")

# filter for only residential significantly damaged parcels
jan_universe <- jan_universe %>% filter(damage_category=="Significant Damage" & residential==TRUE)

# clean up columns to keep
jan_universe <- jan_universe %>%
  select(ain,use_code,res_type,owner_renter,total_units,total_square_feet,damage_category,mixed_damage,damage_type_list,structure_count)

# add january identifier to column names
colnames(jan_universe )[c(1:6)] <- paste0(colnames(jan_universe)[c(1:6)], "_2025_01")

# Export
indicator <- "January residential parcels with significant damage based on CalFire data and Jan Assessor Records"
qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\monthly_updates\\QA_parcel_xwalk.docx"

schema <- "dashboard"
table_name <-"parcel_universe_2025_01"
source <- "Data Prep\\Monthly Updates\\parcel_xwalk.R"
dbWriteTable(con, Id(schema, table_name), jan_universe,
             overwrite = FALSE, row.names = FALSE)

colnames(jan_universe)
col_comments <- c("ain, suffix denotes respective assessor data version",
                  "use code from january",
                  "residential type from january",
                  "ownership from january",
                  "total units from january",
                  "total square feet from january",
                  "our custom damage category",
                  "denotes if mixed damage or not",
                  "list of damage types on property",
                  "number of strucutres assessed by CalFire on property")


add_table_comments(con=con, schema=schema,table_name=table_name,indicator = indicator, qa_filepath = qa_filepath,
                   source=source,column_names = colnames(jan_universe), column_comments = col_comments)


