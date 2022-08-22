# Databricks notebook source
# MAGIC %python
# MAGIC storage_account_name = "stgrowthanalytics"
# MAGIC access_key1 = "btmsIBQv0t8HdR6/uH+C7jm7UQoFIuSKj8EztnzdCOOR41tiEQdqpQ6kQBtoqSCpOm9wd9FuBqA3gnpYBMuRlw=="
# MAGIC spark.conf.set("fs.azure.account.key.stgrowthanalytics.blob.core.windows.net",
# MAGIC                access_key1)
# MAGIC 
# MAGIC print("config has been set")

# COMMAND ----------

## Loading required libraries
library(dplyr)
library(stringr)
library(sparklyr)

# COMMAND ----------

sc <- spark_connect(method='databricks')

# COMMAND ----------

### NPD Incrementality Modelling Data Prep and Model Output Paths

## Data Prep Outputs
process_path = "wasbs://npd-incrementality@stgrowthanalytics.blob.core.windows.net/process/140222/"

output_path = "wasbs://npd-incrementality@stgrowthanalytics.blob.core.windows.net/NPD_incr_drivers/"

## Model Outputs
input_path = "wasbs://npd-incrementality@stgrowthanalytics.blob.core.windows.net/140222/lag_Q4_all_inno_PPA_0.4_bis/"

# COMMAND ----------

### Read NPD Incrementality files

### deep_dive
deep_dive <- spark_read_csv(sc, 'deep_dive', path=paste0(input_path, "deep_dive_all_categories.csv"), header=TRUE, overwrite=TRUE)

### all_incr
all_incr <- spark_read_csv(sc, 'all_incr', path=paste0(input_path, "all_incr_all_categories_long.csv"), header=TRUE, overwrite=TRUE)

### all_NPD_facts
all_NPD_facts <- spark_read_csv(sc, 'all_NPD_facts', path=paste0(input_path, "all_NPD_facts_all_categories.csv"), header=TRUE, overwrite=TRUE)

### all_NPD_Biscuits
all_NPD_Biscuits <- spark_read_csv(sc, 'all_NPD_Biscuits', path=paste0(process_path, "/BISCUITS_ALL_MARKET/all_NPD_facts_BISCUITS.csv"), header=TRUE, overwrite=TRUE)

### Country-BU Mapping
country_mapping <- spark_read_csv(sc, 'country_mapping', path=paste0(process_path, "MDLZ Region - BU - Country Mapping.csv"), header=TRUE, overwrite=TRUE)

### Emerging_Developed_mapping
EM_DM_mapping = spark_read_csv(sc, 'Emerging_Developed_mapping', path=paste0(process_path, "Emerging_Developed_mapping.csv"), header=TRUE, overwrite=TRUE)

# COMMAND ----------

all_incr <- all_incr %>% mutate('GLOBAL_SEGMENT'=toupper(GLOBAL_SEGMENT),
                         'GLOBAL_MANUFACTURER'=toupper(GLOBAL_MANUFACTURER))
all_NPD_facts <- all_NPD_facts %>% mutate('GLOBAL_SEGMENT'=toupper(GLOBAL_SEGMENT),
                         'GLOBAL_MANUFACTURER'=toupper(GLOBAL_MANUFACTURER))
deep_dive <- deep_dive %>% mutate('Value_Sales'=Value_Sales/1000,
                                 'Volume_Sales'=Volume_Sales/1000)

# COMMAND ----------

# Selecting for Biscuits Category in concerned BUs
all_incr_biscuits_BU <- all_incr %>% filter(GLOBAL_CATEGORY=="BISCUITS" & MDLZ_AREA %in% c('ANZJ','C. EUROPE','CANADA','CHINA','E. EUROPE','N. EUROPE','SEA','US','W. EUROPE') & GLOBAL_BRAND!='ALL OTHER' & GLOBAL_SEGMENT!='OTHER')

# COMMAND ----------

# Deselecting PPA, Flavor innovations >= 99 %
NPDS <- all_incr_biscuits_BU %>% filter(Time_period=="Overall" & (INNOVATION_TYPE %in% c("PPA","FLAVOR") & NPD_PB <0.99 | !INNOVATION_TYPE %in% c("PPA","FLAVOR"))) %>% pull('INITIATIVE_NAME')

# COMMAND ----------

# Filtering Yr_1 values for NPDs which have completed full year
all_incr_filtered <- all_incr_biscuits_BU %>% filter(INITIATIVE_NAME %in% NPDS & Time_Period=="Yr_1" & Number_of_months_considered_in==12)

# COMMAND ----------

# Selecting required columns
all_incr_req_cols <- all_incr_filtered %>% 
      select(-c('Incrementality_Performance', 'Brand_Value_Share', 'Mfr_Value_Share', 'MAPE_', 'RSquare', 'Total_NEW_LAUNCH_TDP', 'Total_PARENT_VOL', 'Total_PARENT_VAL', 'Number_of_months_considered_in', 'TIER', 'Survived')) %>%
      mutate('Total_Incremental_Value'=NPD_PB*Total_NEW_LAUNCH_VAL)

# COMMAND ----------

NPD_list <- all_incr_req_cols %>% pull('INITIATIVE_NAME')

# COMMAND ----------

# Filtering all_NPD_facts for NPDs from filtered all_incr 
all_NPD_facts_filtered <- all_NPD_facts %>% filter(INITIATIVE_NAME %in% NPD_list)

# COMMAND ----------

sdf_dim(all_NPD_facts_filtered)

# COMMAND ----------

# Creating new launch date column
mapping_all_NPD_filtered <- all_NPD_facts_filtered %>% group_by(INITIATIVE_NAME) %>% 
              filter(NEW_LAUNCH_ACV > 0) %>% 
              mutate(NEW_LAUNCH_DATE=min(Date))
mapping_old_new_in_dt <- mapping_all_NPD_filtered %>% select('INITIATIVE_NAME','NEW_LAUNCH_DATE','INITIATIVE_DT') %>% distinct()

# COMMAND ----------

# mapping new launch date for the NPDs
all_NPD_facts_modified <- left_join(all_NPD_facts_filtered,mapping_old_new_in_dt,by=c('INITIATIVE_NAME','INITIATIVE_DT')) %>%    
        mutate('Date'=as.Date(Date),'NEW_LAUNCH_DATE'=as.Date(NEW_LAUNCH_DATE))

# COMMAND ----------

# Filtering first 12 months for each NPD from launch date
all_NPD_12_months_filtered <- all_NPD_facts_modified %>% group_by(INITIATIVE_NAME) %>%  
     filter(Date>=NEW_LAUNCH_DATE) %>%
     arrange(Date) %>% 
     filter(row_number() <= 12) %>% 
     mutate('COMP_VAL_1'=COMP_PRICE_1*COMP_VOL_1,
            'COMP_VAL_2'=COMP_PRICE_2*COMP_VOL_2)

# COMMAND ----------

# Summarising the Value, Volume, TDP metrics for each NPD
all_NPD_Yr1 <- all_NPD_12_months_filtered %>% group_by(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT, INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE) %>% 
        summarise('Yr1_SEGMENT_VOL'=sum(CAT_VOL),
                  'Yr1_MANUFACTURER_VOL_WITHIN_SEGMENT'=sum(MONDELEZ_CAT_VOL),
                  'Yr1_MANUFACTURER_VALUE_WITHIN_SEGMENT'=sum(MONDELEZ_CAT_VALUE),
                  'Yr1_NEW_LAUNCH_VALUE'=sum(NEW_LAUNCH_VAL),
                  'Yr1_NEW_LAUNCH_VOL'=sum(NEW_LAUNCH_VOL),
                  'Yr1_NEW_LAUNCH_TDP'=mean(NEW_LAUNCH_TDPS),
                  'Yr1_PARENT_VOL'=sum(PARENT_VOL),
                  'Yr1_PARENT_VALUE'=sum(PARENT_VALUE),
                  'Yr1_PARENT_BRAND_TDP'=mean(PARENT_TDP),
                  'Yr1_COMP_VOL_1'=sum(COMP_VOL_1),
                  'Yr1_COMP_TDP_1'=mean(COMP_TDP_1),
                  'Yr1_COMP_VAL_1'=sum(COMP_VAL_1),
                  'Yr1_COMP_VOL_2'=sum(COMP_VOL_2),
                  'Yr1_COMP_TDP_2'=mean(COMP_TDP_2),
                  'Yr1_COMP_VAL_2'=sum(COMP_VAL_2)) %>%
        mutate('Yr1_VELOCITY_NPD'=(Yr1_NEW_LAUNCH_VALUE/Yr1_NEW_LAUNCH_TDP)*1000,
               'Yr1_VELOCITY_PB'=(Yr1_PARENT_VALUE/Yr1_PARENT_BRAND_TDP)*1000,
               'Yr1_VELOCITY_COMP_1'=(Yr1_COMP_VAL_1/Yr1_COMP_TDP_1)*1000,
               'Yr1_VELOCITY_COMP_2'=(Yr1_COMP_VAL_2/Yr1_COMP_TDP_2)*1000)

# COMMAND ----------

# Creating quarter and half year columns 
all_NPD_quarter_half_year <- all_NPD_12_months_filtered %>% group_by(INITIATIVE_NAME) %>% 
          mutate('Half_Year'=ifelse(row_number() <=6,"H1","H2"), 'Quarter'=ifelse(row_number()<=3,"Q1",ifelse(row_number() %in% 4:6,"Q2",ifelse(row_number() %in% 7:9,"Q3","Q4"))))

# COMMAND ----------

# Summarising the Value, Volume, TDP metrics for each NPD - Quarterly basis
all_NPD_Quarterly_long <- all_NPD_quarter_half_year %>% group_by(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT, INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE, Quarter) %>% 
        summarise('SEGMENT_VOL'=sum(CAT_VOL),
                  'NEW_LAUNCH_TDP'=mean(NEW_LAUNCH_TDPS),
                  'PARENT_VALUE'=sum(PARENT_VALUE),
                  'PARENT_VOL'=sum(PARENT_VOL),
                  'NEW_LAUNCH_VOL'=sum(NEW_LAUNCH_VOL),
                  'PARENT_TDP'=mean(PARENT_TDP),
                  'NEW_LAUNCH_VAL'=sum(NEW_LAUNCH_VAL),
                  'COMP_VOL_1'=sum(COMP_VOL_1),
                  'COMP_VOL_2'=sum(COMP_VOL_2),
                  'COMP_TDP_1'=mean(COMP_TDP_1),
                  'COMP_TDP_2'=mean(COMP_TDP_2))

# COMMAND ----------

sdf_gather <- function(tbl, gather_cols){
  other_cols <- colnames(tbl)[!colnames(tbl) %in% gather_cols]
  
  lapply(gather_cols, function(col_nm){
    tbl %>% 
      select(c(other_cols, col_nm)) %>%
      mutate(key = col_nm) %>%
      rename(value = col_nm)
  }) %>%
    sdf_bind_rows() %>%
    select(c(other_cols, 'key', 'value'))
}
fun.aggregate <- function(gdf){
    expr <- invoke_static(
      sc,
      "org.apache.spark.sql.functions",
      "expr",
      "sum(value)"
    )
  
    gdf %>% invoke("agg", expr, list())
  }

# COMMAND ----------

# Converting all_NPD_Quarterly from long to wide
all_NPD_Quarterly <- all_NPD_Quarterly_long %>%
    select(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT, INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE, Quarter, SEGMENT_VOL, NEW_LAUNCH_TDP, PARENT_VALUE, PARENT_VOL, NEW_LAUNCH_VOL, PARENT_TDP, NEW_LAUNCH_VAL, COMP_VOL_1, COMP_VOL_2, COMP_TDP_1, COMP_TDP_2) %>%
    sdf_gather(c("SEGMENT_VOL", "NEW_LAUNCH_TDP", "PARENT_VALUE", "PARENT_VOL", "NEW_LAUNCH_VOL", "PARENT_TDP", "NEW_LAUNCH_VAL", "COMP_VOL_1", "COMP_VOL_2", "COMP_TDP_1", "COMP_TDP_2")) %>%
    mutate('Quarter_data'=paste(Quarter, key, sep="_")) %>%
    select(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT,
             INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE, Quarter_data,value) %>%
    sdf_pivot(INITIATIVE_NAME + COUNTRY + INNOVATION_TYPE + GLOBAL_CATEGORY + FLAVOR + PACK_SIZE + MDLZ_Region + MDLZ_AREA + MARKET_BREAKDOWN + GLOBAL_SEGMENT + INNOVATION_SCOPE + GLOBAL_BRAND + GLOBAL_MANUFACTURER + INITIATIVE_DT + NEW_LAUNCH_DATE ~ Quarter_data, fun.aggregate = fun.aggregate)

# COMMAND ----------

# Growth rates for Quarterly data
all_NPD_Quarterly <- all_NPD_Quarterly %>% mutate('Q1-Q2_GROWTH_SEGMENT_VOL'=(Q2_SEGMENT_VOL-Q1_SEGMENT_VOL)/Q1_SEGMENT_VOL,
                                  'Q2-Q3_GROWTH_SEGMENT_VOL'=(Q3_SEGMENT_VOL-Q2_SEGMENT_VOL)/Q2_SEGMENT_VOL,
                                  'Q3-Q4_GROWTH_SEGMENT_VOL'=(Q4_SEGMENT_VOL-Q3_SEGMENT_VOL)/Q3_SEGMENT_VOL,
                                  'Q1-Q2_GROWTH_NEW_LAUNCH_TDP'=(Q2_NEW_LAUNCH_TDP-Q1_NEW_LAUNCH_TDP)/Q1_NEW_LAUNCH_TDP,
                                  'Q2-Q3_GROWTH_NEW_LAUNCH_TDP'=(Q3_NEW_LAUNCH_TDP-Q2_NEW_LAUNCH_TDP)/Q2_NEW_LAUNCH_TDP,
                                  'Q3-Q4_GROWTH_NEW_LAUNCH_TDP'=(Q4_NEW_LAUNCH_TDP-Q3_NEW_LAUNCH_TDP)/Q3_NEW_LAUNCH_TDP,
                                  'Q1-Q2_GROWTH_PARENT_VALUE'=(Q2_PARENT_VALUE-Q1_PARENT_VALUE)/Q1_PARENT_VALUE,
                                  'Q2-Q3_GROWTH_PARENT_VALUE'=(Q3_PARENT_VALUE-Q2_PARENT_VALUE)/Q2_PARENT_VALUE,
                                  'Q3-Q4_GROWTH_PARENT_VALUE'=(Q4_PARENT_VALUE-Q3_PARENT_VALUE)/Q3_PARENT_VALUE,
                                  'Q1-Q2_GROWTH_PARENT_VOL'=(Q2_PARENT_VOL-Q1_PARENT_VOL)/Q1_PARENT_VOL,
                                  'Q2-Q3_GROWTH_PARENT_VOL'=(Q3_PARENT_VOL-Q2_PARENT_VOL)/Q2_PARENT_VOL,
                                  'Q3-Q4_GROWTH_PARENT_VOL'=(Q4_PARENT_VOL-Q3_PARENT_VOL)/Q3_PARENT_VOL,
                                  'Q1-Q2_GROWTH_NEW_LAUNCH_VOL'=(Q2_NEW_LAUNCH_VOL-Q1_NEW_LAUNCH_VOL)/Q1_NEW_LAUNCH_VOL,
                                  'Q2-Q3_GROWTH_NEW_LAUNCH_VOL'=(Q3_NEW_LAUNCH_VOL-Q2_NEW_LAUNCH_VOL)/Q2_NEW_LAUNCH_VOL,
                                  'Q3-Q4_GROWTH_NEW_LAUNCH_VOL'=(Q4_NEW_LAUNCH_VOL-Q3_NEW_LAUNCH_VOL)/Q3_NEW_LAUNCH_VOL,
                                  'Q1-Q2_GROWTH_PARENT_TDP'=(Q2_PARENT_TDP-Q1_PARENT_TDP)/Q1_PARENT_TDP,
                                  'Q2-Q3_GROWTH_PARENT_TDP'=(Q3_PARENT_TDP-Q2_PARENT_TDP)/Q2_PARENT_TDP,
                                  'Q3-Q4_GROWTH_PARENT_TDP'=(Q4_PARENT_TDP-Q3_PARENT_TDP)/Q3_PARENT_TDP,
                                  'Q1-Q2_GROWTH_NEW_LAUNCH_VAL'=(Q2_NEW_LAUNCH_VAL-Q1_NEW_LAUNCH_VAL)/Q1_NEW_LAUNCH_VAL,
                                  'Q2-Q3_GROWTH_NEW_LAUNCH_VAL'=(Q3_NEW_LAUNCH_VAL-Q2_NEW_LAUNCH_VAL)/Q2_NEW_LAUNCH_VAL,
                                  'Q3-Q4_GROWTH_NEW_LAUNCH_VAL'=(Q4_NEW_LAUNCH_VAL-Q3_NEW_LAUNCH_VAL)/Q3_NEW_LAUNCH_VAL,
                                  'Q1-Q2_GROWTH_COMP_VOL_1'=(Q2_COMP_VOL_1-Q1_COMP_VOL_1)/Q1_COMP_VOL_1,
                                  'Q2-Q3_GROWTH_COMP_VOL_1'=(Q3_COMP_VOL_1-Q2_COMP_VOL_1)/Q2_COMP_VOL_1,
                                  'Q3-Q4_GROWTH_COMP_VOL_1'=(Q4_COMP_VOL_1-Q3_COMP_VOL_1)/Q3_COMP_VOL_1,
                                  'Q1-Q2_GROWTH_COMP_TDP_1'=(Q2_COMP_TDP_1-Q1_COMP_TDP_1)/Q1_COMP_TDP_1,
                                  'Q2-Q3_GROWTH_COMP_TDP_1'=(Q3_COMP_TDP_1-Q2_COMP_TDP_1)/Q2_COMP_TDP_1,
                                  'Q3-Q4_GROWTH_COMP_TDP_1'=(Q4_COMP_TDP_1-Q3_COMP_TDP_1)/Q3_COMP_TDP_1,
                                  'Q1-Q2_GROWTH_COMP_VOL_2'=(Q2_COMP_VOL_2-Q1_COMP_VOL_2)/Q1_COMP_VOL_2,
                                  'Q2-Q3_GROWTH_COMP_VOL_2'=(Q3_COMP_VOL_2-Q2_COMP_VOL_2)/Q2_COMP_VOL_2,
                                  'Q3-Q4_GROWTH_COMP_VOL_2'=(Q4_COMP_VOL_2-Q3_COMP_VOL_2)/Q3_COMP_VOL_2,
                                  'Q1-Q2_GROWTH_COMP_TDP_2'=(Q2_COMP_TDP_2-Q1_COMP_TDP_2)/Q1_COMP_TDP_2,
                                  'Q2-Q3_GROWTH_COMP_TDP_2'=(Q3_COMP_TDP_2-Q2_COMP_TDP_2)/Q2_COMP_TDP_2,
                                  'Q3-Q4_GROWTH_COMP_TDP_2'=(Q4_COMP_TDP_2-Q3_COMP_TDP_2)/Q3_COMP_TDP_2)

# COMMAND ----------

# Summarising the Value, Volume, TDP metrics for each NPD - Half yearly basis
all_NPD_half_yearly_long <- all_NPD_quarter_half_year %>% group_by(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT, INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE, Half_Year) %>% 
        summarise('SEGMENT_VOL'=sum(CAT_VOL),
                  'NEW_LAUNCH_TDP'=mean(NEW_LAUNCH_TDPS),
                  'PARENT_VALUE'=sum(PARENT_VALUE),
                  'PARENT_VOL'=sum(PARENT_VOL),
                  'NEW_LAUNCH_VOL'=sum(NEW_LAUNCH_VOL),
                  'PARENT_TDP'=mean(PARENT_TDP),
                  'NEW_LAUNCH_VAL'=sum(NEW_LAUNCH_VAL),
                  'COMP_VOL_1'=sum(COMP_VOL_1),
                  'COMP_VOL_2'=sum(COMP_VOL_2),
                  'COMP_TDP_1'=mean(COMP_TDP_1),
                  'COMP_TDP_2'=mean(COMP_TDP_2))

# COMMAND ----------

# Converting all_NPD_half_yearly from long to wide
all_NPD_half_yearly <- all_NPD_half_yearly_long %>%
    select(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT, INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE, Half_Year, SEGMENT_VOL, NEW_LAUNCH_TDP, PARENT_VALUE, PARENT_VOL, NEW_LAUNCH_VOL, PARENT_TDP, NEW_LAUNCH_VAL, COMP_VOL_1, COMP_VOL_2, COMP_TDP_1, COMP_TDP_2) %>%
    sdf_gather(c("SEGMENT_VOL", "NEW_LAUNCH_TDP", "PARENT_VALUE", "PARENT_VOL", "NEW_LAUNCH_VOL", "PARENT_TDP", "NEW_LAUNCH_VAL", "COMP_VOL_1", "COMP_VOL_2", "COMP_TDP_1", "COMP_TDP_2")) %>%
    mutate('Half_Year_data'=paste(Half_Year, key, sep="_")) %>%
    select(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT,
             INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE, Half_Year_data,value) %>%
    sdf_pivot(INITIATIVE_NAME + COUNTRY + INNOVATION_TYPE + GLOBAL_CATEGORY + FLAVOR + PACK_SIZE + MDLZ_Region + MDLZ_AREA + MARKET_BREAKDOWN + GLOBAL_SEGMENT + INNOVATION_SCOPE + GLOBAL_BRAND + GLOBAL_MANUFACTURER + INITIATIVE_DT + NEW_LAUNCH_DATE ~ Half_Year_data, fun.aggregate = fun.aggregate)

# COMMAND ----------

# Growth rates for half yearly data
all_NPD_half_yearly <- all_NPD_half_yearly %>% mutate('H1-H2_GROWTH_SEGMENT_VOL'=(H2_SEGMENT_VOL-H1_SEGMENT_VOL)/H1_SEGMENT_VOL,
                                  'H1-H2_GROWTH_NEW_LAUNCH_TDP'=(H2_NEW_LAUNCH_TDP-H1_NEW_LAUNCH_TDP)/H1_NEW_LAUNCH_TDP,
                                  'H1-H2_GROWTH_PARENT_VALUE'=(H2_PARENT_VALUE-H1_PARENT_VALUE)/H1_PARENT_VALUE,
                                  'H1-H2_GROWTH_PARENT_VOL'=(H2_PARENT_VOL-H1_PARENT_VOL)/H1_PARENT_VOL,
                                  'H1-H2_GROWTH_NEW_LAUNCH_VOL'=(H2_NEW_LAUNCH_VOL-H1_NEW_LAUNCH_VOL)/H1_NEW_LAUNCH_VOL,
                                  'H1-H2_GROWTH_PARENT_TDP'=(H2_PARENT_TDP-H1_PARENT_TDP)/H1_PARENT_TDP,
                                  'H1-H2_GROWTH_NEW_LAUNCH_VAL'=(H2_NEW_LAUNCH_VAL-H1_NEW_LAUNCH_VAL)/H1_NEW_LAUNCH_VAL,
                                  'H1-H2_GROWTH_COMP_VOL_1'=(H2_COMP_VOL_1-H1_COMP_VOL_1)/H1_COMP_VOL_1,
                                  'H1-H2_GROWTH_COMP_TDP_1'=(H2_COMP_TDP_1-H1_COMP_TDP_1)/H1_COMP_TDP_1,
                                  'H1-H2_GROWTH_COMP_VOL_2'=(H2_COMP_VOL_2-H1_COMP_VOL_2)/H1_COMP_VOL_2,
                                  'H1-H2_GROWTH_COMP_TDP_2'=(H2_COMP_TDP_2-H1_COMP_TDP_2)/H1_COMP_TDP_2)

# COMMAND ----------

# calculating price index 
all_NPD_Yr1 <- all_NPD_Yr1 %>% mutate('Price_Index'=(Yr1_NEW_LAUNCH_VALUE/Yr1_NEW_LAUNCH_VOL)/(Yr1_PARENT_VALUE/Yr1_PARENT_VOL),
                                      'Initiative_Size'=Yr1_NEW_LAUNCH_VALUE/Yr1_PARENT_VALUE,
                                      'NPD_TDP_relative_to_brand_TDP'=Yr1_NEW_LAUNCH_TDP/Yr1_PARENT_BRAND_TDP,
                                      'PB_TDP_relative_to_COMP_TDP_1'=Yr1_PARENT_BRAND_TDP/Yr1_COMP_TDP_1,
                                      'PB_TDP_relative_to_COMP_TDP_2'=Yr1_PARENT_BRAND_TDP/Yr1_COMP_TDP_2)

# COMMAND ----------

# Velocity of core products in Parent Brand
deep_dive_core_product <- deep_dive %>% filter(INNOVATION_TYPE=='EXISTING') %>%
    select(-c('INNOVATION_TYPE')) %>%
    mutate('Date'=as.Date(Date)) %>% 
    right_join(all_NPD_12_months_filtered,by=c('COUNTRY', 'MDLZ_Region', 'MDLZ_AREA', 'GLOBAL_CATEGORY', 'GLOBAL_SEGMENT', 'GLOBAL_MANUFACTURER', 'GLOBAL_BRAND', 'Date')) %>% 
    group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_SEGMENT, GLOBAL_MANUFACTURER, GLOBAL_BRAND,INITIATIVE_NAME) %>%
    summarise('Existing_PB_Value_Sales'=sum(Value_Sales,na.rm=TRUE),
              'Existing_PB_Volume_Sales'=sum(Volume_Sales,na.rm=TRUE),
              'Existing_PB_TDP'=mean(TDP,na.rm=TRUE),
              'Parent_Brand_Value_sales'=sum(PARENT_VALUE,na.rm=TRUE)) %>%
    mutate('Velocity_Core_Product'=(Existing_PB_Value_Sales/Existing_PB_TDP)*1000,
           'Total_Brand_innovation_sales'=Parent_Brand_Value_sales-Existing_PB_Value_Sales)

# COMMAND ----------

# Segment level data
deep_dive_segment_data <- deep_dive %>% mutate('Date'=as.Date(Date)) %>% 
    group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_SEGMENT, Date) %>%
    summarise('BTC_Value_Sales'=sum(Value_Sales[INNOVATION_TYPE=="BEYOND THE CORE"],na.rm=TRUE),
              'FLAVOR_Value_Sales'=sum(Value_Sales[INNOVATION_TYPE=="FLAVOR"],na.rm=TRUE),
              'PPA_Value_Sales'=sum(Value_Sales[INNOVATION_TYPE=="PPA"],na.rm=TRUE),
              'Total_Value_Sales'=sum(Value_Sales)) %>% 
    right_join(all_NPD_12_months_filtered,by=c('COUNTRY', 'MDLZ_Region', 'MDLZ_AREA', 'GLOBAL_CATEGORY', 'GLOBAL_SEGMENT', 'Date')) %>% 
    group_by(INITIATIVE_NAME) %>%
    summarise('Segment_BTC_Value_Sales'=sum(BTC_Value_Sales,na.rm=TRUE),
              'Segment_FLAVOR_Value_Sales'=sum(FLAVOR_Value_Sales,na.rm=TRUE),
              'Segment_PPA_Value_Sales'=sum(PPA_Value_Sales,na.rm=TRUE),
              'Segment_Value_sales'=sum(Total_Value_Sales,na.rm=TRUE)) %>%
    mutate('Segment_innovation_sales'=Segment_BTC_Value_Sales+Segment_FLAVOR_Value_Sales+Segment_PPA_Value_Sales)

# COMMAND ----------

# Brand level data
deep_dive_brand_data <- deep_dive %>% mutate('Date'=as.Date(Date)) %>% 
    group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_SEGMENT, GLOBAL_MANUFACTURER, GLOBAL_BRAND, Date) %>%
    summarise('BTC_Value_Sales'=sum(Value_Sales[INNOVATION_TYPE=="BEYOND THE CORE"],na.rm=TRUE),
              'FLAVOR_Value_Sales'=sum(Value_Sales[INNOVATION_TYPE=="FLAVOR"],na.rm=TRUE),
              'PPA_Value_Sales'=sum(Value_Sales[INNOVATION_TYPE=="PPA"],na.rm=TRUE),
              'Total_Value_Sales'=sum(Value_Sales)) %>% 
    right_join(all_NPD_12_months_filtered,by=c('COUNTRY', 'MDLZ_Region', 'MDLZ_AREA', 'GLOBAL_CATEGORY', 'GLOBAL_SEGMENT', 'GLOBAL_MANUFACTURER', 'GLOBAL_BRAND', 'Date')) %>% 
    group_by(INITIATIVE_NAME) %>%
    summarise('Brand_BTC_Value_Sales'=sum(BTC_Value_Sales,na.rm=TRUE),
              'Brand_FLAVOR_Value_Sales'=sum(FLAVOR_Value_Sales,na.rm=TRUE),
              'Brand_PPA_Value_Sales'=sum(PPA_Value_Sales,na.rm=TRUE))

# COMMAND ----------

# Calculating Innovation fair share index at PB level - year 1
deep_dive_combined <- deep_dive_segment_data %>% inner_join(deep_dive_core_product, by="INITIATIVE_NAME") %>%
      mutate('Fair_share_index'=(Total_Brand_innovation_sales/Segment_innovation_sales)/(Parent_Brand_Value_sales/Segment_Value_sales))
deep_dive_combined <- deep_dive_combined %>% left_join(deep_dive_brand_data, by="INITIATIVE_NAME")

# COMMAND ----------

# Survival of NPD with minimum 1 Year sales
survived_df <- all_NPD_quarter_half_year %>% filter(Quarter=="Q4") %>% 
    select("INITIATIVE_NAME", "Date", "NEW_LAUNCH_ACV") %>%
    group_by(INITIATIVE_NAME) %>%
    summarise(NEW_LAUNCH_ACV=mean(NEW_LAUNCH_ACV)) %>%
    mutate('Survived' = ifelse(NEW_LAUNCH_ACV >= 5, "1", "0")) %>%
    select(c('INITIATIVE_NAME', 'Survived')) %>%
    distinct()
all_NPD_Yr1 <- all_NPD_Yr1 %>% left_join(survived_df,by="INITIATIVE_NAME")

# COMMAND ----------

all_NPD_Yr1 <- all_NPD_Yr1 %>% left_join(all_NPD_Quarterly) %>% 
      left_join(all_NPD_half_yearly) %>% 
      left_join(deep_dive_combined)

# COMMAND ----------

# Mfr value sales
deep_dive_Mfr_value_sales <- deep_dive %>% mutate('Date'=as.Date(Date)) %>% 
    group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_SEGMENT, GLOBAL_MANUFACTURER, Date) %>%
    summarise(Mfr_value_sales=sum(Value_Sales)) %>% 
    right_join(all_NPD_12_months_filtered,by=c('COUNTRY', 'MDLZ_Region', 'MDLZ_AREA', 'GLOBAL_CATEGORY', 'GLOBAL_SEGMENT', 'GLOBAL_MANUFACTURER', 'Date')) %>% 
    group_by(INITIATIVE_NAME) %>%
    summarise(Mfr_Value_sales = sum(Mfr_value_sales))

# COMMAND ----------

# Brand value share and Mfr value share
all_NPD_Yr1 <- deep_dive_Mfr_value_sales %>% right_join(all_NPD_Yr1,by="INITIATIVE_NAME")
all_NPD_Yr1 <- all_NPD_Yr1 %>% mutate('Brand_value_share'=Yr1_PARENT_VALUE/Segment_Value_sales,
                                      'Mfr_value_share'=Mfr_Value_sales/Segment_Value_sales,
                                      'COMP_1_value_share'=Yr1_COMP_VAL_1/Segment_Value_sales,
                                      'COMP_2_value_share'=Yr1_COMP_VAL_2/Segment_Value_sales)

# COMMAND ----------

# Filtering deep dive file
deep_dive_filtered <- deep_dive %>% filter(GLOBAL_CATEGORY=="BISCUITS" & COUNTRY %in% c("AUSTRALIA", "BELGIUM", "CANADA", "CHINA", "CZECH REPUBLIC", "FRANCE", "GERMANY", "GREAT BRITAIN", "INDONESIA", "ITALY", "JAPAN", "MALAYSIA", "NETHERLANDS", "POLAND", "RUSSIA", "SPAIN", "UNITED STATES" ) & GLOBAL_BRAND!='ALL OTHER' & GLOBAL_SEGMENT!='OTHER')

# Mfr presence and Brand presence
mfr_presence <- deep_dive_filtered %>% group_by(GLOBAL_MANUFACTURER) %>% 
      summarise(Mfr_presence=n_distinct(COUNTRY)) %>% 
      distinct()
brand_presence <- deep_dive_filtered %>% group_by(GLOBAL_MANUFACTURER,GLOBAL_BRAND) %>% 
      summarise(Brand_presence=n_distinct(COUNTRY)) %>% 
      distinct()

# COMMAND ----------

# Country X Category level data
deep_dive_country <- deep_dive %>% group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY) %>%
    summarise('Country_Value_Sales'=sum(Value_Sales)) 

# Country X Category X Mfr value sales
deep_dive_country_Mfr <- deep_dive %>%
    group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_MANUFACTURER) %>%
    summarise(Country_Mfr_value_sales=sum(Value_Sales)) 

# COMMAND ----------

all_NPD_Yr1 <- all_NPD_Yr1 %>% left_join(mfr_presence) %>%
    left_join(brand_presence) %>% left_join(deep_dive_country) %>% left_join(deep_dive_country_Mfr) %>% mutate('Mfr_Country_Share'=Country_Mfr_value_sales/Country_Value_Sales)

# COMMAND ----------

# # Country X Category X Yr 1 level data
# #deep_dive_country_data <- deep_dive %>% mutate('Date'=as.Date(Date)) %>% 
#     group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, Date) %>%
#     summarise('Total_Value_Sales'=sum(Value_Sales)) %>% 
#     right_join(all_NPD_12_months_filtered,by=c('COUNTRY', 'MDLZ_Region', 'MDLZ_AREA', 'GLOBAL_CATEGORY', 'Date')) %>% 
#     group_by(INITIATIVE_NAME) %>%
#     summarise('Country_Yr1_Value_sales'=sum(Total_Value_Sales,na.rm=TRUE))

# # Country X Category X Mfr X Yr 1 value sales
# deep_dive_country_Mfr_sales <- deep_dive %>% mutate('Date'=as.Date(Date)) %>% 
#     group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_MANUFACTURER, Date) %>%
#     summarise(Country_Mfr_Yr1_sales=sum(Value_Sales)) %>% 
#     right_join(all_NPD_12_months_filtered,by=c('COUNTRY', 'MDLZ_Region', 'MDLZ_AREA', 'GLOBAL_CATEGORY', 'GLOBAL_MANUFACTURER', 'Date')) %>% 
#     group_by(INITIATIVE_NAME) %>%
#     summarise(Country_Mfr_Yr1_sales = sum(Country_Mfr_Yr1_sales))

# # Country X Category X Brand X Yr 1 value sales
# deep_dive_country_brand_sales <- deep_dive %>% mutate('Date'=as.Date(Date)) %>% 
#     group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_MANUFACTURER, GLOBAL_BRAND, Date) %>%
#     summarise(Country_brand_sales=sum(Value_Sales)) %>% 
#     right_join(all_NPD_12_months_filtered,by=c('COUNTRY', 'MDLZ_Region', 'MDLZ_AREA', 'GLOBAL_CATEGORY', 'GLOBAL_MANUFACTURER', 'GLOBAL_BRAND', 'Date')) %>% 
#     group_by(INITIATIVE_NAME) %>%
#     summarise(Country_brand_Yr1_sales = sum(Country_brand_sales))

# COMMAND ----------

# # Mfr_Country_share and Brand_Country_share
# Mfr_brand_Country_share <- left_join(deep_dive_country_data, deep_dive_country_Mfr_sales, by="INITIATIVE_NAME") %>% 
#     left_join(deep_dive_country_brand_sales) %>%
#     mutate('Mfr_Country_Yr1_share'=Country_Mfr_Yr1_sales/Country_Yr1_Value_sales,
#            'Brand_Country_Yr1_share'=Country_brand_Yr1_sales/Country_Yr1_Value_sales)

# COMMAND ----------

#all_NPD_Yr1 <- all_NPD_Yr1 %>% right_join(Mfr_brand_Country_share,by="INITIATIVE_NAME")

# COMMAND ----------

# AVERAGE PACK SIZE
all_incr_pack_size <- all_incr_req_cols %>% select(INITIATIVE_NAME,PACK_SIZE) %>% collect()
s <- strsplit(all_incr_pack_size$PACK_SIZE, split = "\\|")
all_incr_pack_list <- data.frame(INITIATIVE_NAME = rep(all_incr_pack_size$INITIATIVE_NAME, sapply(s, length)), PACK_SIZE = unlist(s))
regexp <- "\\.*\\d+\\.*\\d*"
regexp1 <- "[aA-zZ]+"
all_incr_pack_list$SIZE <- str_extract(all_incr_pack_list$PACK_SIZE, regexp)
all_incr_pack_list$UNIT <- str_extract(all_incr_pack_list$PACK_SIZE, regexp1)
all_incr_pack_list <- all_incr_pack_list %>% mutate(SIZE=as.numeric(SIZE),SIZE=ifelse(UNIT=="OUNCE",SIZE*28.35,ifelse(UNIT=="COUNT",NA,SIZE))) %>% 
            group_by(INITIATIVE_NAME) %>% summarise(MEAN_PACK_SIZE=mean(SIZE,na.rm=TRUE))
mean_pack_size <- sdf_copy_to(sc,all_incr_pack_list, overwrite = TRUE)

# COMMAND ----------

all_NPD_Yr1 <- all_NPD_Yr1 %>% left_join(mean_pack_size,by="INITIATIVE_NAME")

# COMMAND ----------

# 90th Quantile for Mfr_Country_Share
mfr_country_filtered <- collect(all_NPD_Yr1 %>% select(COUNTRY,GLOBAL_MANUFACTURER,Mfr_Country_Share) %>% distinct())
mfr_country_quantile <- mfr_country_filtered %>% group_by(COUNTRY) %>% mutate('top_decile_mfr_country_share' = quantile(Mfr_Country_Share, 0.90))
mfr_country_quantile <- sdf_copy_to(sc,mfr_country_quantile, overwrite = TRUE)

# COMMAND ----------

# Mfr Maturity level
all_NPD_Yr1 <- all_NPD_Yr1 %>% left_join(mfr_country_quantile,by=c( "COUNTRY", "GLOBAL_MANUFACTURER", "Mfr_Country_Share")) %>%    
            mutate('Mfr_Maturity_level'=ifelse((Mfr_presence>=5 | Mfr_Country_Share >= `top_decile_mfr_country_share`),"Mature","Not Mature"))

# COMMAND ----------

# Seasonality Index
deep_dive_parent <- deep_dive_filtered %>% group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_SEGMENT, GLOBAL_MANUFACTURER, GLOBAL_BRAND, Date) %>% summarise('parent_volume_sales'=sum(Volume_Sales)) %>% mutate('Date'=as.Date(Date),'year'=year(Date),'month'=month(Date))
seasonality_index <- deep_dive_parent %>% group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_SEGMENT, GLOBAL_MANUFACTURER, GLOBAL_BRAND, year) %>%
    mutate(yearly_scaled = parent_volume_sales/mean(parent_volume_sales,na.rm = T)) %>%
    group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_SEGMENT, GLOBAL_MANUFACTURER, GLOBAL_BRAND, month) %>%
    mutate(SI = mean(yearly_scaled, na.rm = T)) %>% 
    ungroup() %>%
    select(-c('year', 'month', 'yearly_scaled')) %>% 
    right_join(all_NPD_quarter_half_year,by=c('COUNTRY', 'MDLZ_Region', 'MDLZ_AREA', 'GLOBAL_CATEGORY', 'GLOBAL_SEGMENT', 'GLOBAL_MANUFACTURER', 'GLOBAL_BRAND', 'Date')) %>%
    filter(Quarter=="Q1") %>% 
    group_by(INITIATIVE_NAME) %>% summarise(SI=mean(SI))

# COMMAND ----------

all_incr_final <- all_incr_req_cols %>% select(INITIATIVE_NAME,UPC_COMBINATION,Total_Incremental_Vol,NPD_PB,Total_Incremental_Value) %>%
        na.replace(Total_Incremental_Value=0)

# COMMAND ----------

all_NPD_Yr1 <- all_NPD_Yr1 %>% left_join(seasonality_index,by="INITIATIVE_NAME") %>% 
        na.replace(0) %>% 
        left_join(all_incr_final, by="INITIATIVE_NAME")

# COMMAND ----------

sdf_dim(all_NPD_Yr1)

# COMMAND ----------

req_cols <- c("INITIATIVE_NAME", "COUNTRY", "INNOVATION_TYPE", "GLOBAL_CATEGORY", "FLAVOR", "UPC_COMBINATION", "PACK_SIZE", "MDLZ_Region",
              "MDLZ_AREA","MARKET_BREAKDOWN", "GLOBAL_SEGMENT", "INNOVATION_SCOPE", "GLOBAL_BRAND", "GLOBAL_MANUFACTURER", "INITIATIVE_DT",
              "NEW_LAUNCH_DATE", "Yr1_SEGMENT_VOL", "Yr1_MANUFACTURER_VOL_WITHIN_SEGMENT", "Yr1_MANUFACTURER_VALUE_WITHIN_SEGMENT", 
              "Yr1_NEW_LAUNCH_VALUE", "Yr1_NEW_LAUNCH_VOL", "Yr1_NEW_LAUNCH_TDP", "Yr1_PARENT_VOL", "Yr1_PARENT_VALUE", "Yr1_PARENT_BRAND_TDP", 
              "Yr1_COMP_VOL_1", "Yr1_COMP_TDP_1", "Yr1_COMP_VAL_1", "Yr1_COMP_VOL_2", "Yr1_COMP_TDP_2", "Yr1_COMP_VAL_2", "Yr1_VELOCITY_NPD", 
              "Yr1_VELOCITY_PB", "Yr1_VELOCITY_COMP_1", "Yr1_VELOCITY_COMP_2", "Price_Index", "Initiative_Size", "NPD_TDP_relative_to_brand_TDP", 
              "PB_TDP_relative_to_COMP_TDP_1", "PB_TDP_relative_to_COMP_TDP_2", "Survived", "Q1_COMP_TDP_1", 
              "Q1_COMP_TDP_2", "Q1_COMP_VOL_1", "Q1_COMP_VOL_2", "Q1_NEW_LAUNCH_TDP", "Q1_NEW_LAUNCH_VAL", "Q1_NEW_LAUNCH_VOL", "Q1_PARENT_TDP", 
              "Q1_PARENT_VALUE", "Q1_PARENT_VOL", "Q1_SEGMENT_VOL", "Q2_COMP_TDP_1", "Q2_COMP_TDP_2", "Q2_COMP_VOL_1", "Q2_COMP_VOL_2", 
              "Q2_NEW_LAUNCH_TDP", "Q2_NEW_LAUNCH_VAL", "Q2_NEW_LAUNCH_VOL", "Q2_PARENT_TDP", "Q2_PARENT_VALUE", "Q2_PARENT_VOL", "Q2_SEGMENT_VOL", 
              "Q3_COMP_TDP_1", "Q3_COMP_TDP_2", "Q3_COMP_VOL_1", "Q3_COMP_VOL_2", "Q3_NEW_LAUNCH_TDP", "Q3_NEW_LAUNCH_VAL", "Q3_NEW_LAUNCH_VOL", 
              "Q3_PARENT_TDP", "Q3_PARENT_VALUE", "Q3_PARENT_VOL", "Q3_SEGMENT_VOL", "Q4_COMP_TDP_1", "Q4_COMP_TDP_2", "Q4_COMP_VOL_1", "Q4_COMP_VOL_2", 
              "Q4_NEW_LAUNCH_TDP", "Q4_NEW_LAUNCH_VAL", "Q4_NEW_LAUNCH_VOL", "Q4_PARENT_TDP", "Q4_PARENT_VALUE", "Q4_PARENT_VOL", "Q4_SEGMENT_VOL", 
              "Q1-Q2_GROWTH_SEGMENT_VOL", "Q2-Q3_GROWTH_SEGMENT_VOL", "Q3-Q4_GROWTH_SEGMENT_VOL", "Q1-Q2_GROWTH_NEW_LAUNCH_TDP", 
              "Q2-Q3_GROWTH_NEW_LAUNCH_TDP", "Q3-Q4_GROWTH_NEW_LAUNCH_TDP", "Q1-Q2_GROWTH_PARENT_VALUE", "Q2-Q3_GROWTH_PARENT_VALUE", 
              "Q3-Q4_GROWTH_PARENT_VALUE", "Q1-Q2_GROWTH_PARENT_VOL", "Q2-Q3_GROWTH_PARENT_VOL", "Q3-Q4_GROWTH_PARENT_VOL", "Q1-Q2_GROWTH_NEW_LAUNCH_VOL", 
              "Q2-Q3_GROWTH_NEW_LAUNCH_VOL", "Q3-Q4_GROWTH_NEW_LAUNCH_VOL", "Q1-Q2_GROWTH_PARENT_TDP", "Q2-Q3_GROWTH_PARENT_TDP", 
              "Q3-Q4_GROWTH_PARENT_TDP", "Q1-Q2_GROWTH_NEW_LAUNCH_VAL", "Q2-Q3_GROWTH_NEW_LAUNCH_VAL", "Q3-Q4_GROWTH_NEW_LAUNCH_VAL", 
              "Q1-Q2_GROWTH_COMP_VOL_1", "Q2-Q3_GROWTH_COMP_VOL_1", "Q3-Q4_GROWTH_COMP_VOL_1", "Q1-Q2_GROWTH_COMP_TDP_1", "Q2-Q3_GROWTH_COMP_TDP_1", 
              "Q3-Q4_GROWTH_COMP_TDP_1", "Q1-Q2_GROWTH_COMP_VOL_2", "Q2-Q3_GROWTH_COMP_VOL_2", "Q3-Q4_GROWTH_COMP_VOL_2", "Q1-Q2_GROWTH_COMP_TDP_2", 
              "Q2-Q3_GROWTH_COMP_TDP_2", "Q3-Q4_GROWTH_COMP_TDP_2", "H1_COMP_TDP_1", "H1_COMP_TDP_2", "H1_COMP_VOL_1", "H1_COMP_VOL_2", 
              "H1_NEW_LAUNCH_TDP", "H1_NEW_LAUNCH_VAL", "H1_NEW_LAUNCH_VOL", "H1_PARENT_TDP", "H1_PARENT_VALUE", "H1_PARENT_VOL", "H1_SEGMENT_VOL", 
              "H2_COMP_TDP_1", "H2_COMP_TDP_2", "H2_COMP_VOL_1", "H2_COMP_VOL_2", "H2_NEW_LAUNCH_TDP", "H2_NEW_LAUNCH_VAL", "H2_NEW_LAUNCH_VOL", 
              "H2_PARENT_TDP", "H2_PARENT_VALUE", "H2_PARENT_VOL", "H2_SEGMENT_VOL", "H1-H2_GROWTH_SEGMENT_VOL", "H1-H2_GROWTH_NEW_LAUNCH_TDP", 
              "H1-H2_GROWTH_PARENT_VALUE", "H1-H2_GROWTH_PARENT_VOL", "H1-H2_GROWTH_NEW_LAUNCH_VOL", "H1-H2_GROWTH_PARENT_TDP", 
              "H1-H2_GROWTH_NEW_LAUNCH_VAL", "H1-H2_GROWTH_COMP_VOL_1", "H1-H2_GROWTH_COMP_TDP_1", "H1-H2_GROWTH_COMP_VOL_2", "H1-H2_GROWTH_COMP_TDP_2", 
              "Segment_BTC_Value_Sales", "Segment_FLAVOR_Value_Sales", "Segment_PPA_Value_Sales", "Segment_Value_sales", "Segment_innovation_sales", 
              "Existing_PB_Value_Sales", "Existing_PB_Volume_Sales", "Existing_PB_TDP", "Parent_Brand_Value_sales", "Velocity_Core_Product", 
              "Total_Brand_innovation_sales", "Fair_share_index", "Brand_BTC_Value_Sales", "Brand_FLAVOR_Value_Sales", "Brand_PPA_Value_Sales", 
              "Mfr_Value_sales", "Brand_value_share", "Mfr_value_share", "COMP_1_value_share", "COMP_2_value_share", "Mfr_presence", "Brand_presence", 
              "Country_Value_Sales", "Country_Mfr_value_sales", "Mfr_Country_Share", "MEAN_PACK_SIZE", "top_decile_mfr_country_share", 
              "Mfr_Maturity_level", "SI", "Total_Incremental_Vol", "NPD_PB", "Total_Incremental_Value")
all_NPD_Yr1 <- all_NPD_Yr1 %>% select(req_cols)

# COMMAND ----------

# Number of Innovations
no_of_innovations <- all_NPD_Biscuits %>% mutate(GLOBAL_CATEGORY="BISCUITS") %>% 
      group_by(COUNTRY, MDLZ_Region, MDLZ_AREA, GLOBAL_CATEGORY, GLOBAL_SEGMENT, GLOBAL_MANUFACTURER, GLOBAL_BRAND) %>%
      summarise('No_of_Innovations'=n_distinct(INITIATIVE_NAME)) %>% 
      distinct() %>% 
     mutate('GLOBAL_SEGMENT'=toupper(GLOBAL_SEGMENT),
            'GLOBAL_MANUFACTURER'=toupper(GLOBAL_MANUFACTURER))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS nielsen_biscuits;
# MAGIC CREATE TABLE nielsen_biscuits
# MAGIC USING parquet
# MAGIC -- OPTIONS (path "wasbs://npd-incremetality@stgrowthanalytics.blob.core.windows.net/marco_data_2/nielsen_biscuits.parquet")
# MAGIC OPTIONS (path "wasbs://npd-incremetality@stgrowthanalytics.blob.core.windows.net/marco_data_140222/nielsen_biscuits.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS nielsen_mkt_biscuits;
# MAGIC CREATE TABLE nielsen_mkt_biscuits
# MAGIC USING parquet
# MAGIC -- OPTIONS (path "wasbs://npd-incremetality@stgrowthanalytics.blob.core.windows.net/marco_data_2/nielsen_mkt_biscuits.parquet");
# MAGIC OPTIONS (path "wasbs://npd-incremetality@stgrowthanalytics.blob.core.windows.net/marco_data_140222/nielsen_mkt_biscuits.parquet");
# MAGIC 
# MAGIC DROP TABLE IF EXISTS nielsen_mkt;
# MAGIC CREATE TABLE nielsen_mkt
# MAGIC SELECT * FROM nielsen_mkt_biscuits 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS nielsen_prod_biscuits;
# MAGIC CREATE TABLE nielsen_prod_biscuits
# MAGIC USING parquet
# MAGIC -- OPTIONS (path "wasbs://npd-incremetality@stgrowthanalytics.blob.core.windows.net/marco_data_2/nielsen_prod_biscuits.parquet");
# MAGIC OPTIONS (path "wasbs://npd-incremetality@stgrowthanalytics.blob.core.windows.net/marco_data_140222/nielsen_prod_biscuits.parquet");
# MAGIC 
# MAGIC DROP TABLE IF EXISTS nielsen_prod_raw;
# MAGIC CREATE TABLE nielsen_prod_raw
# MAGIC SELECT * FROM nielsen_prod_biscuits 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS nielsen_prod;
# MAGIC CREATE OR REPLACE TABLE nielsen_prod
# MAGIC SELECT * FROM nielsen_prod_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE nielsen_prod
# MAGIC SET global_segment = 'Gum' WHERE global_category = 'GUM';
# MAGIC UPDATE nielsen_prod
# MAGIC SET global_segment = "Sweet Biscuits" WHERE global_sub_segment LIKE 'Sweet Biscuits%';
# MAGIC UPDATE nielsen_prod
# MAGIC SET global_segment = "Savory Biscuits" WHERE global_sub_segment LIKE 'Savory Biscuits%';
# MAGIC UPDATE nielsen_prod
# MAGIC SET global_segment = "Soft Cakes" WHERE global_sub_segment LIKE 'Soft Cakes%';
# MAGIC UPDATE nielsen_prod
# MAGIC SET global_segment = "Other" WHERE (super_category = 'BISCUITS') AND (global_sub_segment LIKE 'Other%');
# MAGIC UPDATE nielsen_prod
# MAGIC SET global_segment = "All Segments" WHERE (global_segment = 'Biscuits') AND (global_sub_segment LIKE 'All Segments%');

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM nielsen_prod WHERE global_segment == "All Segments"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS biscuits_dat_TOTAL;
# MAGIC CREATE TABLE biscuits_dat_TOTAL
# MAGIC 
# MAGIC SELECT
# MAGIC nmkt.mkt_country,
# MAGIC nprod.flavor,
# MAGIC nprod.flavor_group,
# MAGIC nprod.global_brand,
# MAGIC nprod.global_brand_family,
# MAGIC nprod.global_category,
# MAGIC nprod.global_manufacturer,
# MAGIC nprod.global_segment,
# MAGIC nprod.global_sub_brand,
# MAGIC nprod.global_sub_segment,
# MAGIC nprod.innovation_date,
# MAGIC nprod.innovation_scope,
# MAGIC nprod.innovation_type,
# MAGIC nprod.item,
# MAGIC nprod.introduction_type,
# MAGIC nmkt.market_breakdown,
# MAGIC nprod.pack_type,
# MAGIC nprod.pack_size_weight,
# MAGIC nc.obspricevolusd,
# MAGIC nc.promoanyvalusd,
# MAGIC nprod.promo_variant,
# MAGIC nmkt.retail_environment,
# MAGIC nprod.seasonal_pack,
# MAGIC nprod.super_category,
# MAGIC nc.tdp,
# MAGIC nc.per_short,
# MAGIC nc.unit,
# MAGIC nprod.upc,
# MAGIC nc.valusd,
# MAGIC nc.vol,
# MAGIC nc.distwtd,
# MAGIC nc.val,
# MAGIC nc.mkt_hier_level_name,
# MAGIC nmkt.mkt_long,
# MAGIC nmkt.mkt_tag,
# MAGIC nprod.prod_tag
# MAGIC 
# MAGIC FROM nielsen_biscuits as nc
# MAGIC 
# MAGIC JOIN nielsen_mkt nmkt ON nc.mkt_tag = nmkt.mkt_tag
# MAGIC JOIN nielsen_prod nprod ON nc.prod_tag = nprod.prod_tag
# MAGIC 
# MAGIC WHERE
# MAGIC 
# MAGIC nc.perd_typ = 'MONTHLY'
# MAGIC AND nc.prod_hier_level_name = 'ITEM'
# MAGIC AND nprod.prod_hier_name = 'GLOBAL_H1'
# MAGIC AND nc.mkt_hier_level_name = 'COUNTRY'
# MAGIC AND nmkt.super_category = 'BISCUITS'
# MAGIC AND nprod.super_category = 'BISCUITS'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE biscuits_dat_TOTAL
# MAGIC SET
# MAGIC   market_breakdown = "TOTAL MARKET"

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE biscuits_dat_TOTAL
# MAGIC SET innovation_scope = 'NOT DEFINED' WHERE innovation_scope IS NULL;
# MAGIC UPDATE biscuits_dat_TOTAL
# MAGIC SET innovation_date = 'NOT DEFINED' WHERE innovation_date IS NULL;
# MAGIC UPDATE biscuits_dat_TOTAL
# MAGIC SET flavor = 'UNIDENTIFIED' WHERE flavor IS NULL;
# MAGIC UPDATE biscuits_dat_TOTAL
# MAGIC SET pack_size_weight = 'UNIDENTIFIED' WHERE pack_size_weight IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE biscuits_dat_TOTAL
# MAGIC SET global_brand_family = CASE WHEN (mkt_country = 'CN' AND global_sub_brand = 'OREO MINI') THEN 'OREO MINI' 
# MAGIC                                WHEN (mkt_country = 'CN' AND global_sub_brand = 'OREO CLEVER KNOT') THEN 'OREO CLEVER KNOT'
# MAGIC                                WHEN (mkt_country = 'CN' AND global_sub_brand = 'OREO FLUTE') THEN 'OREO FLUTE'
# MAGIC                                WHEN (mkt_country = 'CN' AND global_sub_brand = 'OREO WAFER') THEN 'OREO WAFER'
# MAGIC                                ELSE global_brand_family
# MAGIC                           END
# MAGIC WHERE (mkt_country = 'CN' AND global_brand_family = 'OREO')                          

# COMMAND ----------

sc <- spark_connect(method='databricks')

# COMMAND ----------

biscuits_TM <- sdf_sql(sc, 'SELECT * FROM biscuits_dat_TOTAL')

# COMMAND ----------

# No_of_core_products
no_of_core_products <- biscuits_TM %>% filter(innovation_scope=="NOT DEFINED") %>% 
      group_by(mkt_country, global_category, global_segment, global_manufacturer, global_brand_family) %>%
      summarise('No_of_core_products'=n_distinct(prod_tag)) %>% 
      rename("COUNTRY_CODE"="mkt_country", 
             "GLOBAL_CATEGORY"="global_category", 
             "GLOBAL_SEGMENT"="global_segment", 
             "GLOBAL_MANUFACTURER"="global_manufacturer", 
             "GLOBAL_BRAND"="global_brand_family") %>% 
      left_join(country_mapping) %>%  
      mutate('GLOBAL_SEGMENT'=toupper(GLOBAL_SEGMENT),
             'GLOBAL_MANUFACTURER'=toupper(GLOBAL_MANUFACTURER)) %>% 
      distinct()

# COMMAND ----------

EM_DM_mapping <- EM_DM_mapping %>% rename("COUNTRY"='Country') %>% select(-c(COUNTRY_CODE))

# COMMAND ----------

all_NPD_Yr1 <- all_NPD_Yr1 %>% left_join(no_of_innovations, by = c("COUNTRY", "GLOBAL_CATEGORY", "MDLZ_Region", "MDLZ_AREA", "GLOBAL_SEGMENT", "GLOBAL_BRAND", "GLOBAL_MANUFACTURER")) %>% 
        left_join(no_of_core_products,by = c("COUNTRY", "GLOBAL_CATEGORY", "MDLZ_Region", "MDLZ_AREA", "GLOBAL_SEGMENT", "GLOBAL_BRAND", "GLOBAL_MANUFACTURER")) %>% 
        select(-c('COUNTRY_CODE')) %>%
            left_join(EM_DM_mapping) %>%
      na.replace(No_of_core_products = 0)

# COMMAND ----------

# Summarising the Value, Volume, TDP metrics for parent brand w/o NPD
all_NPD_Yr1 <- all_NPD_Yr1 %>% mutate('Yr1_PARENT_VOL_w/o_NPD'=Yr1_PARENT_VOL-Yr1_NEW_LAUNCH_VOL,
                                      'Yr1_PARENT_VALUE_w/o_NPD'=Yr1_PARENT_VALUE-Yr1_NEW_LAUNCH_VALUE,
                                      'Yr1_PARENT_BRAND_TDP_w/o_NPD'=Yr1_PARENT_BRAND_TDP-Yr1_NEW_LAUNCH_TDP,
                                      'Yr1_VELOCITY_PB_w/o_NPD'=(`Yr1_PARENT_VALUE_w/o_NPD`/`Yr1_PARENT_BRAND_TDP_w/o_NPD`)*1000)

# COMMAND ----------

# Filtering for NPD with launch dates from July 2019 - Jan 2021
all_NPD_Yr1 <- all_NPD_Yr1 %>% filter(NEW_LAUNCH_DATE>="2019-07-01" & NEW_LAUNCH_DATE<="2021-01-01")

# COMMAND ----------

# UPC_COMBINATION
UPC <- all_NPD_Yr1 %>% select(INITIATIVE_NAME,COUNTRY,GLOBAL_SEGMENT,GLOBAL_MANUFACTURER,GLOBAL_BRAND,UPC_COMBINATION) %>% collect()

upc_split <- strsplit(UPC$UPC_COMBINATION, split = "\\|")

upc_combination_split <- data.frame(INITIATIVE_NAME = rep(UPC$INITIATIVE_NAME, sapply(upc_split, length)),COUNTRY = rep(UPC$COUNTRY, sapply(upc_split, length)),GLOBAL_SEGMENT = rep(UPC$GLOBAL_SEGMENT, sapply(upc_split, length)), GLOBAL_MANUFACTURER = rep(UPC$GLOBAL_MANUFACTURER, sapply(upc_split, length)), GLOBAL_BRAND = rep(UPC$GLOBAL_BRAND, sapply(upc_split, length)), UPC_COMBINATION = unlist(upc_split))

upc_combination_split <- sdf_copy_to(sc,upc_combination_split, overwrite = TRUE)

upc_combination_split <- upc_combination_split %>% left_join(country_mapping,by="COUNTRY")

# COMMAND ----------

biscuits_TM <- biscuits_TM %>%
  mutate(item = ifelse((is.na(item))|(item=='null')|(item==''), 'NOT_AVAILABLE', item)) %>%
  mutate(upc = ifelse((is.na(upc))|(upc=='null')|(upc==''), item, upc)) 

# COMMAND ----------

# taking pack_type for required UPCs
upc_pack_type <- biscuits_TM %>% select(c(mkt_country,global_segment,global_manufacturer,global_brand_family,upc,pack_type)) %>% 
      rename("COUNTRY_CODE"="mkt_country",
                            "GLOBAL_SEGMENT"="global_segment",
                            "GLOBAL_MANUFACTURER"="global_manufacturer",
                            "GLOBAL_BRAND"="global_brand_family",
                            "UPC_COMBINATION"="upc") %>% 
      mutate('GLOBAL_SEGMENT'=toupper(GLOBAL_SEGMENT), 'GLOBAL_MANUFACTURER'=toupper(GLOBAL_MANUFACTURER)) %>%
      right_join(upc_combination_split,by=c("COUNTRY_CODE","GLOBAL_SEGMENT","GLOBAL_MANUFACTURER","GLOBAL_BRAND","UPC_COMBINATION")) %>% distinct()

all_NPD_pack <- upc_pack_type %>% group_by(INITIATIVE_NAME) %>% summarise('PACK_TYPE' = paste(collect_list(pack_type), sep = '|'))

# COMMAND ----------

all_NPD_Yr1 <- all_NPD_Yr1 %>% left_join(all_NPD_pack,by=c("INITIATIVE_NAME"))

# COMMAND ----------

# Filtering 3 months before launch date for each NPD
all_NPD_prelaunch_filter <- all_NPD_facts_modified %>% group_by(INITIATIVE_NAME) %>%  
     filter(Date<NEW_LAUNCH_DATE) %>%
     arrange(Date) %>% 
     filter(row_number() >= (n() - 5))

# COMMAND ----------

display(as.data.frame(all_NPD_prelaunch_filter))

# COMMAND ----------

# Creating pre launch quarter columns 
all_NPD_prelaunch_filter <- all_NPD_prelaunch_filter %>% group_by(INITIATIVE_NAME) %>% 
          mutate('Quarter'=ifelse(row_number() <=3,"Prelaunch_6months","Prelaunch_3months"))

# Summarising the Value, Volume, TDP metrics for each NPD - prelaunch basis
all_NPD_prelaunch_long <- all_NPD_prelaunch_filter %>% group_by(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT, INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE, Quarter) %>% 
        summarise('PARENT_VOL'=sum(PARENT_VOL),
                  'PARENT_TDP'=mean(PARENT_TDP),
                  'COMP_VOL_1'=sum(COMP_VOL_1),
                  'COMP_TDP_1'=mean(COMP_TDP_1)
                  
                 
                 )

# COMMAND ----------

# Converting all_NPD_prelaunch from long to wide
all_NPD_prelaunch <- all_NPD_prelaunch_long %>%
    select(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT, INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE, Quarter, PARENT_VOL, PARENT_TDP,COMP_VOL_1,COMP_TDP_1) %>%
    sdf_gather(c("PARENT_VOL", "PARENT_TDP","COMP_VOL_1","COMP_TDP_1")) %>%
    mutate('Quarter_data'=paste(Quarter, key, sep="_")) %>%
    select(INITIATIVE_NAME, COUNTRY, INNOVATION_TYPE, GLOBAL_CATEGORY, FLAVOR, PACK_SIZE, MDLZ_Region, MDLZ_AREA, MARKET_BREAKDOWN, GLOBAL_SEGMENT,
             INNOVATION_SCOPE, GLOBAL_BRAND, GLOBAL_MANUFACTURER, INITIATIVE_DT, NEW_LAUNCH_DATE, Quarter_data,value) %>%
    sdf_pivot(INITIATIVE_NAME + COUNTRY + INNOVATION_TYPE + GLOBAL_CATEGORY + FLAVOR + PACK_SIZE + MDLZ_Region + MDLZ_AREA + MARKET_BREAKDOWN + GLOBAL_SEGMENT + INNOVATION_SCOPE + GLOBAL_BRAND + GLOBAL_MANUFACTURER + INITIATIVE_DT + NEW_LAUNCH_DATE ~ Quarter_data, fun.aggregate = list(value = "sum"))

# COMMAND ----------

# Growth rates for pre launch data
all_NPD_prelaunch_growth <- all_NPD_prelaunch %>% 
    mutate('pre6M-pre3M_GROWTH_PARENT_VOL'=(Prelaunch_3months_PARENT_VOL-Prelaunch_6months_PARENT_VOL) /Prelaunch_6months_PARENT_VOL,
           'pre6M-pre3M_GROWTH_PARENT_TDP'=(Prelaunch_3months_PARENT_TDP-Prelaunch_6months_PARENT_TDP)/ Prelaunch_6months_PARENT_TDP)
#            'pre6M-pre3M_GROWTH_COMP_VOL'=(Prelaunch_3months_COMP_VOL_1-Prelaunch_6months_COMP_VOL_1) / Prelaunch_6months_COMP_VOL_1,
#            'pre6M-pre3M_GROWTH_COMP_TDP'=(Prelaunch_3months_COMP_TDP_1-Prelaunch_6months_COMP_TDP_1)/ Prelaunch_6months_COMP_TDP_1)


all_NPD_Yr1 <- all_NPD_Yr1 %>% left_join(all_NPD_prelaunch_growth)

# COMMAND ----------

all_NPD_Yr1 <- all_NPD_Yr1 %>% mutate('pre3M-post3M_GROWTH_PARENT_VOL'=(Q1_PARENT_VOL-Prelaunch_3months_PARENT_VOL)/Prelaunch_3months_PARENT_VOL,
                                      'pre3M-post3M_GROWTH_PARENT_TDP'=(Q1_PARENT_TDP-Prelaunch_3months_PARENT_TDP)/Prelaunch_3months_PARENT_TDP) %>% 
            na.replace(0)

# COMMAND ----------

sdf_dim(all_NPD_Yr1)

# COMMAND ----------

colnames(as.data.frame(all_NPD_Yr1))

# COMMAND ----------

all_NPD_Yr1 <- sdf_coalesce(all_NPD_Yr1, 1)
spark_write_csv(all_NPD_Yr1, paste0(output_path, "NPD_incr_drivers.csv"), mode='overwrite', header=TRUE)

# COMMAND ----------

req_cols <- c("INITIATIVE_NAME", "COUNTRY", "MDLZ_Region", "MDLZ_AREA", "GLOBAL_CATEGORY", "GLOBAL_SEGMENT", "GLOBAL_MANUFACTURER", "GLOBAL_BRAND", 
              "FLAVOR", "UPC_COMBINATION", "PACK_SIZE", "PACK_TYPE", "MARKET_BREAKDOWN", "INNOVATION_TYPE", "INNOVATION_SCOPE", "INITIATIVE_DT", 
              "NEW_LAUNCH_DATE", "Yr1_SEGMENT_VOL", "Yr1_MANUFACTURER_VOL_WITHIN_SEGMENT", "Yr1_MANUFACTURER_VALUE_WITHIN_SEGMENT", 
              "Yr1_NEW_LAUNCH_VALUE", "Yr1_NEW_LAUNCH_VOL", "Yr1_NEW_LAUNCH_TDP", "Yr1_PARENT_VOL", "Yr1_PARENT_VALUE", "Yr1_PARENT_BRAND_TDP", 
              "Yr1_COMP_VOL_1", "Yr1_COMP_TDP_1", "Yr1_COMP_VAL_1", "Yr1_COMP_VOL_2", "Yr1_COMP_TDP_2", "Yr1_COMP_VAL_2", "Yr1_VELOCITY_NPD", 
              "Yr1_VELOCITY_PB", "Yr1_VELOCITY_COMP_1", "Yr1_VELOCITY_COMP_2", "Price_Index", "Initiative_Size", "NPD_TDP_relative_to_brand_TDP", 
              "PB_TDP_relative_to_COMP_TDP_1", "PB_TDP_relative_to_COMP_TDP_2", "Survived", "Q1-Q2_GROWTH_SEGMENT_VOL", "Q2-Q3_GROWTH_SEGMENT_VOL", 
              "Q3-Q4_GROWTH_SEGMENT_VOL", "Q1-Q2_GROWTH_NEW_LAUNCH_TDP", "Q2-Q3_GROWTH_NEW_LAUNCH_TDP", "Q3-Q4_GROWTH_NEW_LAUNCH_TDP", 
              "Q1-Q2_GROWTH_PARENT_VALUE", "Q2-Q3_GROWTH_PARENT_VALUE", "Q3-Q4_GROWTH_PARENT_VALUE", "Q1-Q2_GROWTH_PARENT_VOL", "Q2-Q3_GROWTH_PARENT_VOL", 
              "Q3-Q4_GROWTH_PARENT_VOL", "Q1-Q2_GROWTH_NEW_LAUNCH_VOL", "Q2-Q3_GROWTH_NEW_LAUNCH_VOL", "Q3-Q4_GROWTH_NEW_LAUNCH_VOL", 
              "Q1-Q2_GROWTH_PARENT_TDP", "Q2-Q3_GROWTH_PARENT_TDP", "Q3-Q4_GROWTH_PARENT_TDP", "Q1-Q2_GROWTH_NEW_LAUNCH_VAL", 
              "Q2-Q3_GROWTH_NEW_LAUNCH_VAL", "Q3-Q4_GROWTH_NEW_LAUNCH_VAL", "Q1-Q2_GROWTH_COMP_VOL_1", "Q2-Q3_GROWTH_COMP_VOL_1", 
              "Q3-Q4_GROWTH_COMP_VOL_1", "Q1-Q2_GROWTH_COMP_TDP_1", "Q2-Q3_GROWTH_COMP_TDP_1", "Q3-Q4_GROWTH_COMP_TDP_1", "Q1-Q2_GROWTH_COMP_VOL_2", 
              "Q2-Q3_GROWTH_COMP_VOL_2", "Q3-Q4_GROWTH_COMP_VOL_2", "Q1-Q2_GROWTH_COMP_TDP_2", "Q2-Q3_GROWTH_COMP_TDP_2", "Q3-Q4_GROWTH_COMP_TDP_2", 
              "H1-H2_GROWTH_SEGMENT_VOL", "H1-H2_GROWTH_NEW_LAUNCH_TDP", "H1-H2_GROWTH_PARENT_VALUE", "H1-H2_GROWTH_PARENT_VOL", 
              "H1-H2_GROWTH_NEW_LAUNCH_VOL", "H1-H2_GROWTH_PARENT_TDP", "H1-H2_GROWTH_NEW_LAUNCH_VAL", "H1-H2_GROWTH_COMP_VOL_1", 
              "H1-H2_GROWTH_COMP_TDP_1", "H1-H2_GROWTH_COMP_VOL_2", "H1-H2_GROWTH_COMP_TDP_2", "Segment_BTC_Value_Sales", "Segment_FLAVOR_Value_Sales", 
              "Segment_PPA_Value_Sales", "Segment_Value_sales", "Segment_innovation_sales", "Existing_PB_Value_Sales", "Existing_PB_Volume_Sales", 
              "Existing_PB_TDP", "Parent_Brand_Value_sales", "Velocity_Core_Product", "Total_Brand_innovation_sales", "Fair_share_index", 
              "Brand_BTC_Value_Sales", "Brand_FLAVOR_Value_Sales", "Brand_PPA_Value_Sales", "Mfr_Value_sales", "Brand_value_share", "Mfr_value_share", 
              "COMP_1_value_share", "COMP_2_value_share", "Mfr_presence", "Brand_presence", "Country_Value_Sales", "Country_Mfr_value_sales", 
              "Mfr_Country_Share", "MEAN_PACK_SIZE", "top_decile_mfr_country_share", "Mfr_Maturity_level", "SI", "Total_Incremental_Vol", "NPD_PB", 
              "Total_Incremental_Value", "No_of_Innovations", "No_of_core_products", "EM_vs_DM", "Yr1_PARENT_VOL_w/o_NPD", "Yr1_PARENT_VALUE_w/o_NPD", 
              "Yr1_PARENT_BRAND_TDP_w/o_NPD", "Yr1_VELOCITY_PB_w/o_NPD", "pre6M-pre3M_GROWTH_PARENT_VOL", "pre6M-pre3M_GROWTH_PARENT_TDP", 
              "pre3M-post3M_GROWTH_PARENT_VOL", "pre3M-post3M_GROWTH_PARENT_TDP",
              "Prelaunch_3months_COMP_TDP_1","Prelaunch_3months_COMP_VOL_1",
              "Prelaunch_6months_COMP_TDP_1","Prelaunch_6months_COMP_VOL_1",
              
              "Prelaunch_3months_PARENT_TDP","Prelaunch_3months_PARENT_VOL",
              "Prelaunch_6months_PARENT_TDP","Prelaunch_6months_PARENT_VOL"
              
             
             )
all_NPD_Yr1_req_features <- all_NPD_Yr1 %>% select(req_cols)

# COMMAND ----------

sdf_dim(all_NPD_Yr1)

# COMMAND ----------

display(as.data.frame(all_NPD_Yr1))

# COMMAND ----------

