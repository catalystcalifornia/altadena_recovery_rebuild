# Create a crosswalk of Jan 2025 and Sept 2025 Altadena parcel data


# Library and environment set up ----

library(sf)
library(mapview)

options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")

con_alt <- connect_to_db("altadena_recovery_rebuild")
# con_rda <- connect_to_db("rda_shared_data")
# con_fires <- connect_to_db("la_fires")

# get assessor parcels and add an identifier column

parcels_jan <- st_read(con_alt, query="SELECT parcels.ain, parcels.geom, stats.use_code, stats.situs_house_no, stats.direction, stats.street_name, stats.unit, stats.city_state 
                       FROM data.assessor_parcels_universe_jan2025 parcels
                       LEFT JOIN data.assessor_data_universe_jan2025 stats
                       ON parcels.ain=stats.ain") %>%
  mutate(flag="jan") %>%
  mutate(area = st_area(geom))

parcels_sept <- st_read(con_alt, query="SELECT parcels.ain, parcels.geom, stats.use_code, stats.situs_house_no, stats.direction, stats.street_name, stats.unit, stats.city_state 
                       FROM data.assessor_parcels_universe_sept2025 parcels
                       LEFT JOIN data.assessor_data_universe_sept2025 stats
                       ON parcels.ain=stats.ain")%>%
  mutate(flag="sept") %>%
  mutate(area = st_area(geom))

# get assessor data: not sure if we need this though so commenting out for now

 assessor_jan <- st_read(con_alt, query="Select * from data.assessor_data_universe_jan2025")
 assessor_sept <- st_read(con_alt, query="Select * from data.assessor_data_universe_sept2025")

# double check CRS of both of parcel shapes
st_crs(parcels_jan)$epsg #3310 good
st_crs(parcels_sept)$epsg #3310 good


# # Intersect Jan and Sept parcel shapes-------------------------------------
# 
# parcels_join <- st_join(parcels_jan, parcels_sept, join = st_intersects,
#                         suffix = c("_jan", "_sept"))
# 
# 
# class(parcels_join) # check object type is sf data frame which is what we want
# 
# # Perform checks on the intersect------------------------------------------
# 
# # See how many unique AINs are there for jan and sept: same number
# 
# length(unique(parcels_join$ain_jan)) #54874
# length(unique(parcels_join$ain_sept)) #54874
# 
# # look at result just the AIN column:
# # This is definitely not a clean 1:1 for most of the AINs. It also seems there might be a many to many relationship
# # with the AINs as well. 
# 
# # For example, AIN 5317001003 in Jan matches with 5 different Sept AINs, one of which matches 5317001003
# 
# join_ain<-parcels_join%>%select(ain_jan, intersect_pct_jan, ain_sept, intersect_pct_sept)
# join_ain_filter <- join_ain %>%
#   filter(intersect_pct_jan > 80 & intersect_pct_sept > 80)
# 
# # See if any AINs are NA after the join 
# 
# sept_not_joined <- parcels_join %>%
#   filter(is.na(ain_sept))%>%View() # None
# 
# jan_not_joined <- parcels_join %>%
#   filter(is.na(ain_sept))%>%View() # None
# 
# # Pull out rows where the AINs are matching
# same_ain <- parcels_join %>%
#   filter(!is.na(ain_jan) & ain_jan == ain_sept) %>%
#   select(ain_jan, intersect_pct_jan, ain_sept, intersect_pct_sept)
# 
# # See how many AINs is that:
# length(unique(same_ain$ain_sept)) #54825 
# 
# # Now I want to extract the opposite: the rows where the jan ain and sept ain are not matching
# not_matching_ain <- parcels_join %>%
#   filter(!is.na(ain_jan) & !is.na(ain_sept) & ain_jan != ain_sept) %>%
#   select(ain_jan, intersect_pct_jan, ain_sept, intersect_pct_sept)
# 
# # See how many AINs is that:
# length(unique(not_matching_ain$ain_sept)) #54813 
# 
# # Pull out duplicate AINs and count
# duplicate_ain <- parcels_join %>%
#   add_count(ain_jan, name = "ain_count_jan") %>%
#   add_count(ain_sept, name = "ain_count_sept") %>%
#   filter(ain_count_jan > 1| ain_count_sept > 1) %>% 
#   select(ain_jan, ain_count_jan, ain_sept, ain_count_sept)
# 
# # see how many unique AINs are among duplicates
# length(unique(duplicate_ain$ain_jan)) #54817
# length(unique(duplicate_ain$ain_sept)) #54817

# # map of duplicates - not this is very large and takes long to load
# mapview(duplicate_ain)


# QA Notes: Recommending this approach 

##### Step 1: find out which shapes are the same in jan and sept #####
match_parcels <- rbind(parcels_jan, parcels_sept) %>%
  # add address to see if that helps with matching shapes with different ains later
  mutate(address=paste(situs_house_no, direction, street_name, unit, city_state)) %>%
  mutate(address=gsub("\\s+", " ", address)) %>%
  select(-c(situs_house_no, direction, street_name, unit, city_state)) %>%
  # group by shape and see how many duplicates there are - note: this takes a couple mins
  mutate(geom_wkt = st_as_text(geom)) %>%
  st_drop_geometry() %>%
  group_by(geom_wkt) %>%
  mutate(group_count = n()) %>%
  # shapes appearing more than once get a group id (dupe_id), if it's unique then NA
  mutate(dupe_id = ifelse(group_count>1,cur_group_id(), NA)) %>%
  ungroup() 
# 109748
check <- data.frame(table(match_parcels$dupe_id, useNA = "ifany"))
check_2 <- data.frame(table(match_parcels$group_count, useNA = "ifany"))
# number of dupe groups - 44524
# number of unique shapes - 354 --> 356?
# total unique shapes - 44878

# Make wider, clean up values to filter later
match_parcels_wide <- match_parcels %>%
  select(-c(area, use_code, address)) %>%
  pivot_wider(
    names_from = flag,
    values_from = flag,
    names_prefix = "flag_"
  ) %>%
  mutate(
    # Convert flags to binary (1/0) and handle NAs in one step
    flag_jan = as.integer(!is.na(flag_jan)),
    flag_sept = as.integer(!is.na(flag_sept)),
    # Flag for same AIN in both months -- would this be same geom?
    same_ain = as.integer(flag_jan == 1 & flag_sept == 1)
  ) %>%
  # Calculate totals by dupe_id
  group_by(dupe_id) %>%
  mutate(
    total_jan = sum(flag_jan),
    total_sept = sum(flag_sept),
    # Shape match: has dupe_id and both months present
    shape_match = as.integer(!is.na(dupe_id) & total_jan > 0 & total_sept > 0)
  ) %>%
  mutate(even_counts = ifelse(total_jan==total_sept, 1, 0)) %>%
  ungroup() %>%
  select(dupe_id, ain, shape_match, same_ain, even_counts, everything()) %>%
  mutate(
    status = case_when(
      shape_match==0 ~ "run intersect by month", # no shape or ain match
      shape_match==1 & same_ain == 0 & even_counts==1 & group_count==2 ~ "diff ain pair, simple xwalk",
      shape_match==1 & same_ain == 0 & even_counts==1 & group_count>2 ~ "ambiguous matches, needs closer look",
      shape_match==1 & same_ain == 0 & even_counts==0 & group_count>2 ~ "uneven matches, needs closer look",
      shape_match==1 & same_ain == 1 & even_counts==1 ~ "same ains, simple xwalk",
      .default = "undefined status, please review")
  )

table(match_parcels_wide$status)
## diff ain pair, simple xwalk ambiguous matches, needs closer look               run intersect by month              same ains, simple xwalk    uneven matches, needs closer look 
## 8                                    4                                  370                                54674                                   18 


##### Review each status type separately to create a xwalk, then combine at end #####
### Status: "same ains, simple xwalk"
same_ain <- match_parcels_wide %>%
  filter(status == "same ains, simple xwalk") 
#54674

 length(unique(same_ain$dupe_id)) # 44516

same_ain_xwalk <- same_ain %>%
  select(ain, dupe_id) %>%
  mutate(ain_jan = ain,
         ain_sept = ain) %>%
  select(-ain) %>%
  mutate(status = "same ains, simple xwalk")


### Status: "diff ain pair, simple xwalk"
diff_ain <- match_parcels_wide %>%
  filter(status == "diff ain pair, simple xwalk") 

 length(unique(diff_ain$dupe_id)) # 4

diff_ain_xwalk <- diff_ain %>%
  group_by(dupe_id) %>%
  summarise(
    ain_jan = ain[flag_jan == 1],
    ain_sept = ain[flag_sept == 1]) %>%
  mutate(status = "diff ain pair, simple xwalk")

### Status: "ambiguous matches, needs closer look"
ambiguous <- match_parcels_wide %>%
  filter(status == "ambiguous matches, needs closer look") 

 length(unique(ambiguous$dupe_id)) # 2

# note: these are two cases where the jan ain is for a parcel deleted in 2024 and the september ain is the correct parcel ain for that address
# https://portal.assessor.lacounty.gov/parceldetail/5722013039
# https://portal.assessor.lacounty.gov/parceldetail/5722013905

# https://portal.assessor.lacounty.gov/parceldetail/5739001072
# https://portal.assessor.lacounty.gov/parceldetail/5739001900
# looked these up in the assessor_jan and assessor_sept data to make sure they were the same house and unit numbers
 
# xwalk will include a jan_ain_revised
ambiguous_xwalk <- ambiguous %>%
  group_by(dupe_id) %>%
  summarise(
    ain_jan = ain[flag_jan == 1],
    ain_sept = ain[flag_sept == 1]) %>%
  mutate(
    ain_jan_revised = ain_sept,
    status = "jan ain deleted, new sept ain")


### Status: "uneven matches, needs closer look"
uneven <-  match_parcels_wide %>%
  filter(status == "uneven matches, needs closer look")

# length(unique(uneven$dupe_id)) # 1

# note: this shape is for a parcel deleted in 12/2024, does not match september data files
# does have valid assessor portal results, but deleted in 12/2024 the other parcels matched are in the sept shapefile but not in the assessor data
# ains 5734023084 to 5734023100 - 16 units, could be in development but no information on use codes
# https://portal.assessor.lacounty.gov/parceldetail/5734023022
# parcels_sept %>% filter(grepl('573402308',ain)) %>% select(ain,use_code)
# this was a commercial property
# address: 139 S OAK KNOLL AVE PASADENA CA 91101-2608
# Recommend revising so september records keep their ain, but the jan ain is the original 5734023022

uneven_xwalk <- uneven %>%
  group_by(dupe_id) %>%
  summarise(
    ain_jan = ain[flag_jan == 1],
    ain_sept = ain[flag_sept == 1]) %>%
  mutate(ain_jan_revised=ain_sept,
         status="single jan ain, multiple sept ains")


### Status: "run intersect by month"
intersect_jan <- match_parcels_wide %>%
  filter(status == "run intersect by month" & flag_jan==1) # 193

jan_parcels_filtered <- parcels_jan %>% filter(ain %in% intersect_jan$ain)

jan_join <- st_intersection(jan_parcels_filtered, parcels_sept) %>%
  mutate(area_intersect = st_area(geom)) %>%
  mutate(pct_jan_overlap = as.numeric(area_intersect)/as.numeric(area)*100,
         pct_sept_overlap = as.numeric(area_intersect)/as.numeric(area.1)*100) %>%
  mutate(address=paste(situs_house_no, direction, street_name, unit, city_state),
         address.1=paste(situs_house_no.1, direction.1, street_name.1, unit.1, city_state.1)) %>%
  mutate(address=gsub("\\s+", " ", address),
         address.1=gsub("\\s+", " ", address.1)) %>%
  select(-c(situs_house_no, direction, street_name, unit, city_state,
            situs_house_no.1, direction.1, street_name.1, unit.1, city_state.1)) %>%
  group_by(ain) %>%
  mutate(count= n()) %>%
  ungroup() %>%
  mutate(address_match = ifelse(address==address.1, 1, 0),
         sept_na = ifelse(is.na(flag.1), 1, 0), # if sept flag is na then not in sept file, use_code can be NA even if in file
         in_sept_shp = ifelse(ain %in% parcels_sept$ain, 1, 0),
         ain_match = ifelse(ain==ain.1, 1 ,0)
         ) %>%
  filter(pct_jan_overlap>0)

# explore duplicates

dup_matches <- jan_join[jan_join$ain %in% jan_join$ain[duplicated(jan_join$ain)], ]
# see https://portal.assessor.lacounty.gov/parceldetail/5327012023
# neither 5327012023 or 5327012026 are in the september file but online map suggests closest match to 5327012026

# get results where overlapping parcels have same ain -- for condos mostly, keep use code to see
jan_ain_match <-jan_join %>% 
  filter(ain_match==1) %>%
  st_drop_geometry() %>%
  select(ain, ain.1, pct_jan_overlap, pct_sept_overlap, use_code, use_code.1) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(status="spatial intersect, same ain, different shapes")

View(jan_ain_match)

# matches under a 70% jan overlap seem to be just slight changes to shapes looking at maps online and comparing to postgres
# https://portal.assessor.lacounty.gov/parceldetail/5832024005
# SELECT ain, st_transform(geom,4326) FROM data.assessor_parcels_universe_jan2025 where ain='5832024005'
 
length(unique(jan_ain_match$ain_jan)) # 151

jan_leftover <- jan_join %>% 
  st_drop_geometry() %>%
  filter(!(ain %in% jan_ain_match$ain_jan)) %>% 
  filter(pct_jan_overlap>10)

length(unique(jan_leftover$ain)) # 42

# look at duplicates
dup_matches <- jan_leftover[jan_leftover$ain %in% jan_leftover$ain[duplicated(jan_leftover$ain)], ]
# very few duplicates

# keep records where there is a 100% overlap with the original january file and there is a september record
jan_revise_ain_merged <- jan_leftover %>%
  select(ain, ain.1,pct_jan_overlap,pct_sept_overlap) %>%
  filter(pct_jan_overlap>=100) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(ain_jan_revised=ain_sept,
         status="spatial intersect, jan parcel merged or split")

View(jan_revise_ain_merged)

# see what's leftover
jan_leftover_v2 <- jan_leftover %>% 
  filter(!ain %in% jan_revise_ain_merged$ain_jan) 


# keep records where there is greater than a 90% overlap with the original january file and there is a september record
jan_revise_ain_split <- jan_leftover_v2 %>%
  filter(sept_na==0) %>%
  select(ain, ain.1,pct_jan_overlap,pct_sept_overlap) %>%
  filter(pct_jan_overlap>=90) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(ain_jan_revised=ain_sept,
         status="spatial intersect, jan parcel merged or split")

View(jan_revise_ain_split)


# see what's leftover
jan_leftover_v3 <- jan_leftover_v2 %>% 
  filter(!ain %in% jan_revise_ain_split$ain_jan) 

# https://portal.assessor.lacounty.gov/parceldetail/5757029054 split parcel
# https://portal.assessor.lacounty.gov/parceldetail/5709030009 parcel change - 5709030033 is a better match
# SELECT ain, st_transform(geom,4326) FROM data.assessor_parcels_universe_jan2025 where ain='5709030009'

jan_revise_ain_manual <- jan_leftover_v3 %>%
  filter(ain.1!='5709030034') %>%
  select(ain, ain.1,pct_jan_overlap,pct_sept_overlap) %>%
  rename(ain_jan=ain,
         ain_sept=ain.1) %>%
  mutate(ain_jan_revised=ain_sept,
         status="spatial intersect manual, jan parcel split")

intersect_jan_xwalk <- bind_rows(jan_ain_match, jan_revise_ain_merged, jan_revise_ain_split, jan_revise_ain_manual) %>%
  rename(use_code_jan=use_code,
         use_code_sept=use_code.1)

# look at duplicates and make sure they make sense
dup_matches <- intersect_jan_xwalk[intersect_jan_xwalk$ain_jan %in% intersect_jan_xwalk$ain_jan[duplicated(intersect_jan_xwalk$ain_jan)], ]
# looks fine, some were looked at manually
# https://portal.assessor.lacounty.gov/parceldetail/5719022111
# 5719022101 and 5719022108 are matching to 5719022111 and 5719022114 larger parcels they were merged to

# notes: some are matching to sept ains with no assessor details (e.g., use_codes and address fields are NA)
# pull below and spot checked and these are not returning valid assessor parcel details 
# didn't have use codes for january either

sept_data_nas <- intersect_jan_xwalk %>% filter(is.na(use_code_sept))
# sort(unique(sept_data_nas$ain_jan))
# length(unique(sept_data_nas$ain_jan)) #43
# I'm not going to deal with sept parcels at the moment, just putting it as a placeholder here.
intersect_sept <- match_parcels_wide %>%
  filter(status == "run intersect by month" & flag_sept==1)

##### combine xwalks and export #####
combined_xwalks <- bind_rows(intersect_jan_xwalk, uneven_xwalk, ambiguous_xwalk, diff_ain_xwalk, same_ain_xwalk) 

# check dups
dup_matches <- combined_xwalks[combined_xwalks$ain_jan %in% combined_xwalks$ain_jan[duplicated(combined_xwalks$ain_jan)], ]
# looks fine, includes one instance of a commercial property being split

# # # Export
# schema <- "data"
# table_name <- "crosswalk_assessor_jan_sept_2025"
# indicator <- "Crosswalk of 2025 Assessor AINs from January to September using the respective shp universe tables."
# qa_filepath <- "W:\\Project\\RDA Team\\Altadena Recovery and Rebuild\\Documentation\\QA_assessor_shp_xwalk.docx"
# 
# source <- "Data Prep\\jan_sept_parcel_xwalk.R"
# dbWriteTable(con_alt, Id(schema, table_name), combined_xwalks,
#              overwrite = FALSE, row.names = FALSE)
# dbSendQuery(con_alt, paste0("COMMENT ON TABLE data.",table_name, " IS '", indicator, "
#             Data imported on 10-15-25. ",
#                         "QA DOC: ", qa_filepath,
#                         " Source: ", source, "'"))
# colnames(combined_xwalks)
# col_comments <- c("ain in january shp data",
#                   "ain in september shp data",
#                   "qa column that refers to categories used to crosswalk",
#                   "includes an alternative ain if ain_jan is associated with a deleted parcel or 999 if ain_jan should be excluded from analysis",
#                   "includes an alternative ain if ain_sept is associated with a deleted parcel or 999 if ain_sept should be excluded from analysis",
#                   "qa column that tracks duplicate parcel shapes")
# 
# 
# add_table_comments(con=con_alt, schema=schema,table_name=table_name,indicator = indicator, qa_filepath = qa_filepath,
#                    source=source,column_names = colnames(combined_xwalks), column_comments = col_comments)

# JZ QA 10/20----------------------------------------------

# AINs in assessor_data_universe_jan2025 but NOT in combined_xwalk

missing_in_xwalk <- assessor_jan %>%
  anti_join(combined_xwalks, by = c("ain" = "ain_jan"))
# all ains accounted for

# AINs in combined_xwalk but NOT in assessor_data_universe_jan2025

missing_in_assessor <- combined_xwalks %>%
  anti_join(assessor_jan, by = c("ain_jan" = "ain")) # This produces 48 jan AINs that are in the xwalk but not in the assessor data

# I want to see of those 48 jan AINS from the xwalk if their corresponding sept AIN is in the jan and sept assessor data:

check_missing_sept_ain_in_jan_assessor <- assessor_jan %>%
  filter(ain %in% missing_in_assessor$ain_sept)

# Of the missing Jan AINs, one of the corresponding Sept AINs is in the Jan Assessor data --this one makes sense because it is the one with the updated jan AIN
# So really there are only 47 Jan AINs in the xwalk missing in the Jan assessor data

# Check missing ains in the sept assessor data:

check_missing_jan_ain_in_sept_assessor <- assessor_sept %>%
  filter(ain %in% missing_in_assessor$ain_jan)

# of the 47 missing Jan AINs, 13 of them are NOT missing in the Sept Assessor data

check_missing_sept_ain_in_sept_assessor <- assessor_sept %>%
  filter(ain %in% missing_in_assessor$ain_sept) 

# of the 47 missing Jan AINs, 15 of their corresponding Sept AINs are NOT missing in the Sept Assessor data

# Focus on the missing_in_assessor df and spot check some of those AINs against the assessor portal online
# ( Except the one with the revised jan AIN) :

## AINS with issues:

# AIN 5725-002-918: This looks like a commercial type, government owned: https://portal.assessor.lacounty.gov/parceldetail/5725002918
# AIN 5713037904: Looks like a DELETED Parcel status: https://portal.assessor.lacounty.gov/parceldetail/5713037904
# AIN 5736026047: Parcel status SHELL not sure what that means but seems like can be dropped https://portal.assessor.lacounty.gov/parceldetail/5736026047
# AIN 5746025908: This looks like a government owned parcel type: https://portal.assessor.lacounty.gov/parceldetail/5746025908
# AIN 5734025088: Another SHELL Type parcel: https://portal.assessor.lacounty.gov/parceldetail/5734025088 
# AIN 5728014060: This is a regular type parcel that is active but tax status = DELINQUENT: https://portal.assessor.lacounty.gov/parceldetail/5728014060
# AIN 5863003900: This is a SHELL parcel status: https://portal.assessor.lacounty.gov/parceldetail/5863003900
# AIN: 5863003901: This is a government owned type parcel: https://portal.assessor.lacounty.gov/parceldetail/5863003901
# AIN 5738005099: This is a SHELL parcel status: https://portal.assessor.lacounty.gov/parceldetail/5738005099
# AIN 5728014061: This is TAX STATUS==DELINQUENT: https://portal.assessor.lacounty.gov/parceldetail/5728014061 
# AIN 5738013906: This looks like a condominium with active parcel status.  But it does say government owned: https://portal.assessor.lacounty.gov/parceldetail/5738013906
# AIN 5327002089: This looks like it is an ACTIVE parcel status with a CURRENT tax status. However on the assessor map it says it is a commercial use parcel. Also no site address provided on the portal: https://portal.assessor.lacounty.gov/parceldetail/5327002089 
# AIN 5327002092: This one has active parcel status but use type == Vacant Land. But on the assessor satellite image it does look like there is a building. No site address provided:  https://portal.assessor.lacounty.gov/parceldetail/5327002092
# AIN 5327002090: Use type is vacant land also no site address provided on the portal: https://portal.assessor.lacounty.gov/parceldetail/5327002090
# AIN 5327002091: Use type is Vacant land also no site addressw provided on the protal. But satellite image on portal does look like there is a building? https://portal.assessor.lacounty.gov/parceldetail/5327002091


# JZ Notes:

# Feeling like these can be dropped because they all seem to have some sort of issue. 
# Wondering if we should be filtering out by Parcel Status == SHELL | DELETED in our jan assessor data
# Could also filter out parcels where type == government owned
