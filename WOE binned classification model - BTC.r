# Databricks notebook source
# DBTITLE 1,Install packages
install.packages("Information")
install.packages("fastDummies")
install.packages("caTools")    # For Logistic regression
install.packages("ROCR")
install.packages("klaR")
install.packages("dgof")
install.packages("stringr")
install.packages("rbin")
install.packages("naniar")


library("rbin")
library(fastDummies)
library(dplyr)
library(stringr)
library(sparklyr)
library(glmnet)
library("caTools")    # For Logistic regression
library("caret")
library(Information)
library(gridExtra)
library(pROC)
library(dgof)
library(stringr)
library(tidyr)
library(naniar)


# COMMAND ----------

library(dplyr)
library(stringr)
library(sparklyr)

# COMMAND ----------

# DBTITLE 1,Spark connect
sc <- spark_connect(method='databricks')

# COMMAND ----------

# DBTITLE 1,Read dataset
df <- spark_read_csv(sc, 'data', path="/FileStore/tables/NPD_drivers_all_features-1.csv", header=TRUE, overwrite=TRUE)

# COMMAND ----------

colnames(df)

# COMMAND ----------


df=as.data.frame(df)

# COMMAND ----------

colnames(df)

# COMMAND ----------

grep("MEAN_PACK_SIZE", colnames(df))

# COMMAND ----------

df[, 177][df[, 177] == 0] <- NA

# COMMAND ----------

summary(df$MEAN_PACK_SIZE)

# COMMAND ----------

df <- df[!(is.na(df$NPD_PB)), ]
df <- df[df$INNOVATION_TYPE=='BEYOND THE CORE', ]


# COMMAND ----------

# MAGIC %md ######Feature engineering - additional features

# COMMAND ----------

# DBTITLE 1,New features
df$Parent_TDP_relative_to_comp1_TDP_prelaunch=(df$Prelaunch_3months_PARENT_TDP)/replace(df$Prelaunch_3months_COMP_TDP_1, df$Prelaunch_3months_COMP_TDP_1==0, 1)
df$Parent_VOL_relative_to_comp1_VOL_prelaunch=(df$Prelaunch_3months_PARENT_VOL)/replace(df$Prelaunch_3months_COMP_VOL_1, df$Prelaunch_3months_COMP_VOL_1==0, 1)


# COMMAND ----------

# DBTITLE 1,Binary DV
df$NPD_PB_avg <- df$NPD_PB
df$NPD_PB<-ifelse(df$NPD_PB>=0.31, 1, 0)
df_2 <- df

# COMMAND ----------

features<-c(
'Fair_share_index',
'MEAN_PACK_SIZE',
'Brand_value_share',
'No_of_core_products',
'No_of_Innovations',
'NPD_PB',
'Price_Index',
'NPD_TDP_relative_to_brand_TDP',
'INNOVATION_TYPE',
'Mfr_Maturity_level',
'EM_vs_DM',
'Initiative_Size',
'pre6Mpre3M_GROWTH_PARENT_VOL',
'pre6Mpre3M_GROWTH_PARENT_TDP',
'H1H2_GROWTH_NEW_LAUNCH_TDP',
'H1H2_GROWTH_PARENT_TDP',
'Parent_TDP_relative_to_comp1_TDP_prelaunch',
'Parent_VOL_relative_to_comp1_VOL_prelaunch'
)

# COMMAND ----------

df<-df[features]

# COMMAND ----------

df <- dummy_cols(df, select_columns = c('Mfr_Maturity_level','EM_vs_DM'),
                remove_first_dummy=FALSE,remove_selected_columns=TRUE)



# COMMAND ----------

feature_list<-colnames(df)
feature_list<-feature_list[!feature_list %in% c('INNOVATION_TYPE','Mfr_Maturity_level_Not Mature','EM_vs_DM_DM')]
df<-df[feature_list]
feature_list

# COMMAND ----------

# MAGIC %md ######Remove correlated features

# COMMAND ----------

sum(is.na(df))

# COMMAND ----------

corr.df <- cor(df[feature_list])
corr.df[upper.tri(corr.df)] <- 0
diag(corr.df) <- 0
data <- 
  df[, !apply(corr.df, 2, function(x) any(abs(x) >=0.8, na.rm = TRUE))]
dim(data)

# COMMAND ----------

# MAGIC %md ######IV summary

# COMMAND ----------

table(data$NPD_PB)

# COMMAND ----------

set.seed(1)

smp_size <- floor(0.8 * nrow(data))

train_ind <- sample(seq_len(nrow(data)), size = smp_size)
train_reg <- data[train_ind, ]
test_reg <- data[-train_ind, ]

print(paste0("Shape of train data ",dim(train_reg)))
print(paste0("Shape of test data ",dim(test_reg)))

# COMMAND ----------

IV_5_bins <- create_infotables(data=train_reg,valid=test_reg, y="NPD_PB", ncore=2,bins=5)
IV_3_bins <- create_infotables(data=train_reg,valid=test_reg, y="NPD_PB", ncore=2,bins=3)

# COMMAND ----------

display(as.data.frame(IV_5_bins$Summary))

# COMMAND ----------

# features where iv>0.01

# COMMAND ----------

# MAGIC %md ######Binning IDVs

# COMMAND ----------


get_binned_data <- function(data,var,var_new,nbins=5){
if (var %in% c('No_of_core_products','No_of_Innovations'))
  {
  IV.data<-as.data.frame(IV_3_bins$Tables[[var]])  
}
  else
 {
  IV.data<-as.data.frame(IV_5_bins$Tables[[var]])  
}
  
  
df<-data.frame(x = c(IV.data[[var]]))
df<-df %>% extract(x, c("Low", "High"), ".([^,]+).([^)]+).")
IV.data$Low<-df$Low
IV.data$High<-df$High
woe_list<-c(IV.data$WOE)
bin_list<-c()
bin_list[1]<-c(IV.data$Low)
bin_list<-append(bin_list,c(IV.data$High))
print(bin_list[1:5])
print(woe_list[1])
print(woe_list[2])
print(woe_list[3])
print(woe_list[4])
print(woe_list[5])
  
if (var %in% c('No_of_core_products','No_of_Innovations'))
  {
data <- data %>% mutate(!!var_new
                        := case_when(
                          data[[var]]>= as.numeric(bin_list[1]) & (data[[var]]<=as.numeric(bin_list[2])) ~woe_list[1],
                          data[[var]]> as.numeric(bin_list[2]) & (data[[var]]<=as.numeric(bin_list[3])) ~woe_list[2],
                          data[[var]]> as.numeric(bin_list[3]) ~woe_list[3]
                                    ))
  
  }
  else {
    data <- data %>% mutate(!!var_new
                        := case_when(
                          data[[var]]>= as.numeric(bin_list[1]) & (data[[var]]<=as.numeric(bin_list[2])) ~woe_list[1],
                          data[[var]]> as.numeric(bin_list[2]) & (data[[var]]<=as.numeric(bin_list[3])) ~woe_list[2],
                          data[[var]]> as.numeric(bin_list[3]) & (data[[var]]<=as.numeric(bin_list[4])) ~woe_list[3],
                          data[[var]]> as.numeric(bin_list[4]) & (data[[var]]<=as.numeric(bin_list[5]) )~woe_list[4],
                          data[[var]] > as.numeric(bin_list[5])  ~woe_list[5]
                                    ))
  
    
  }
  return (data)}

# COMMAND ----------

iv_based_features<-c(IV_5_bins$Summary[IV_5_bins$Summary$AdjIV>=0.01,]$Variable)
iv_based_features<-append(iv_based_features,c('Parent_VOL_relative_to_comp1_VOL_prelaunch','Fair_share_index'))
iv_based_features

# COMMAND ----------

# iv_based_features
for (x in iv_based_features) {
  data<-get_binned_data(data,x,paste0(x,"_binned"))
  print(paste0(x,"_binned"))

}

for (x in iv_based_features) {
  train_reg<-get_binned_data(train_reg,x,paste0(x,"_binned"))
  print(paste0(x,"_binned"))

}

for (x in iv_based_features) {
  test_reg<-get_binned_data(test_reg,x,paste0(x,"_binned"))
  print(paste0(x,"_binned"))

}

# COMMAND ----------

# MAGIC %md ######Manual bins

# COMMAND ----------


bins <- rbin_manual(data=data,response=NPD_PB,predictor=No_of_Innovations,cut_points= c(4,8))
bins


# COMMAND ----------

data <- data %>% mutate('No_of_Innovations_binned'
                        := case_when(
                          data[['No_of_Innovations']]>0 & (data[['No_of_Innovations']]<=4) ~ -0.58357686,
                          data[['No_of_Innovations']]>4 & (data[['No_of_Innovations']]<=8) ~ 0.09662684,
                          data[['No_of_Innovations']]> 8 ~ 0.94272096
                          ))


train_reg <- train_reg %>% mutate('No_of_Innovations_binned'
                        := case_when(
                          train_reg[['No_of_Innovations']]>0 & (train_reg[['No_of_Innovations']]<=4) ~ -0.58357686,
                          train_reg[['No_of_Innovations']]>4 & (train_reg[['No_of_Innovations']]<=8) ~ 0.09662684,
                          train_reg[['No_of_Innovations']]> 8 ~ 0.94272096
                          ))


test_reg <- test_reg %>% mutate('No_of_Innovations_binned'
                        := case_when(
                          test_reg[['No_of_Innovations']]>0 & (test_reg[['No_of_Innovations']]<=4) ~ -0.58357686,
                          test_reg[['No_of_Innovations']]>4 & (test_reg[['No_of_Innovations']]<=8) ~ 0.09662684,
                          test_reg[['No_of_Innovations']]> 8 ~ 0.94272096
                          ))


# COMMAND ----------

bins <- rbin_manual(data=data,response=NPD_PB,predictor=Parent_VOL_relative_to_comp1_VOL_prelaunch,cut_points= c(1,5))
bins

# COMMAND ----------

data <- data %>% mutate('Parent_VOL_relative_to_comp1_VOL_prelaunch_binned'
                        := case_when(
                          data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<1 ~ -0.74721440,
                          data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=1 & (data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<5) ~ 0.03028952,
                          data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=5 ~ 0.47927727
                          ))

train_reg <- train_reg %>% mutate('Parent_VOL_relative_to_comp1_VOL_prelaunch_binned'
                        := case_when(
                          train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<1 ~ -0.74721440,
                          train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=1 & (train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<5) ~ 0.03028952,
                          train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=5 ~ 0.47927727
                          ))


test_reg <- test_reg %>% mutate('Parent_VOL_relative_to_comp1_VOL_prelaunch_binned'
                        := case_when(
                          test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<1 ~ -0.74721440,
                          test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=1 & (test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<5) ~ 0.03028952,
                          test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=5 ~ 0.47927727
                          ))




# COMMAND ----------

bins <- rbin_manual(data=data,response=NPD_PB,predictor=No_of_core_products,cut_points= c(20,40,60))
bins

# COMMAND ----------

data <- data %>% mutate('No_of_core_products_binned'
                        := case_when(
                          data[['No_of_core_products']]>=0 & (data[['No_of_core_products']]<=20) ~ -0.6856789,
                          data[['No_of_core_products']]>20 & (data[['No_of_core_products']]<=40) ~ -0.2451225,
                          data[['No_of_core_products']]> 40 & (data[['No_of_core_products']]<=60) ~ 0.5350361,
                          data[['No_of_core_products']]>60 ~ 0.6346107
                          ))


train_reg <- train_reg %>% mutate('No_of_core_products_binned'
                        := case_when(
                          train_reg[['No_of_core_products']]>=0 & (train_reg[['No_of_core_products']]<=20) ~ -0.6856789,
                          train_reg[['No_of_core_products']]>20 & (train_reg[['No_of_core_products']]<=40) ~ -0.2451225,
                          train_reg[['No_of_core_products']]> 40 & (train_reg[['No_of_core_products']]<=60) ~ 0.5350361,
                          train_reg[['No_of_core_products']]>60 ~ 0.6346107
                          ))

test_reg <- test_reg %>% mutate('No_of_core_products_binned'
                        := case_when(
                          test_reg[['No_of_core_products']]>=0 & (test_reg[['No_of_core_products']]<=20) ~ -0.6856789,
                          test_reg[['No_of_core_products']]>20 & (test_reg[['No_of_core_products']]<=40) ~ -0.2451225,
                          test_reg[['No_of_core_products']]> 40 & (test_reg[['No_of_core_products']]<=60) ~ 0.5350361,
                          test_reg[['No_of_core_products']]>60 ~ 0.6346107
                          ))


# COMMAND ----------

data <- data %>% mutate(NPD_TDP_relative_to_PB_TDP_h1h2_binned
                        := case_when(
                          (data[['H1H2_GROWTH_PARENT_TDP']]>=-0.77)  & (data[['H1H2_GROWTH_PARENT_TDP']]<=-0.02) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=-1)  & (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.18)
                          ~( -0.20668022112710804 * -0.7754377690206252),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]>=-0.77)  & (data[['H1H2_GROWTH_PARENT_TDP']]<=-0.02) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.18)  & (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.58)
                          ~( -0.10237734702741726 * -0.7754377690206252) ,
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]>=-0.77)  & (data[['H1H2_GROWTH_PARENT_TDP']]<=-0.02) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.58) 
                          ~ (0.3183552332115355 * -0.7754377690206252) ,
                          
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]>-0.02 ) & (data[['H1H2_GROWTH_PARENT_TDP']]<=0.07) & 
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=-1)  & (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.18)
                          ~ (-0.0496267816418884 * -0.20668022112710804),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]>-0.02 ) & (data[['H1H2_GROWTH_PARENT_TDP']]<=0.07) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.18)  & (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.58)
                          ~( -0.0496267816418884 * -0.10237734702741726),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]>-0.02 ) & (data[['H1H2_GROWTH_PARENT_TDP']]<=0.07) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.58) 
                          ~ (-0.0496267816418884 * 0.3183552332115355),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]> 0.07)  & 
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=-1)  & (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.18) 
                          ~( 0.9737620857886338 * -0.20668022112710804),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]> 0.07)  & 
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.18)  & (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.58) 
                          ~ (0.9737620857886338 * -0.10237734702741726),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]> 0.07)  & 
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.58) 
                          ~ (0.9737620857886338 * 0.3183552332115355)
                          
                          
                          
                          ))


# COMMAND ----------

train_reg <- train_reg %>% mutate(NPD_TDP_relative_to_PB_TDP_h1h2_binned
                        := case_when(
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.77)  & (train_reg[['H1H2_GROWTH_PARENT_TDP']]<=-0.02) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=-1)  & (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.18)
                          ~( -0.20668022112710804 * -0.7754377690206252),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.77)  & (train_reg[['H1H2_GROWTH_PARENT_TDP']]<=-0.02) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.18)  & (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.58)
                          ~( -0.10237734702741726 * -0.7754377690206252) ,
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.77)  & (train_reg[['H1H2_GROWTH_PARENT_TDP']]<=-0.02) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.58) 
                          ~ (0.3183552332115355 * -0.7754377690206252) ,
                          
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]>-0.02 ) & (train_reg[['H1H2_GROWTH_PARENT_TDP']]<=0.07) & 
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=-1)  & (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.18)
                          ~ (-0.0496267816418884 * -0.20668022112710804),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]>-0.02 ) & (train_reg[['H1H2_GROWTH_PARENT_TDP']]<=0.07) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.18)  & (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.58)
                          ~( -0.0496267816418884 * -0.10237734702741726),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]>-0.02 ) & (train_reg[['H1H2_GROWTH_PARENT_TDP']]<=0.07) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.58) 
                          ~ (-0.0496267816418884 * 0.3183552332115355),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]> 0.07)  & 
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=-1)  & (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.18) 
                          ~( 0.9737620857886338 * -0.20668022112710804),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]> 0.07)  & 
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.18)  & (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.58) 
                          ~ (0.9737620857886338 * -0.10237734702741726),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]> 0.07)  & 
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.58) 
                          ~ (0.9737620857886338 * 0.3183552332115355)
                          
                          
                          
                          ))

test_reg <- test_reg %>% mutate(NPD_TDP_relative_to_PB_TDP_h1h2_binned
                        := case_when(
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.77)  & (test_reg[['H1H2_GROWTH_PARENT_TDP']]<=-0.02) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=-1)  & (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.18)
                          ~( -0.20668022112710804 * -0.7754377690206252),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.77)  & (test_reg[['H1H2_GROWTH_PARENT_TDP']]<=-0.02) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.18)  & (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.58)
                          ~( -0.10237734702741726 * -0.7754377690206252) ,
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.77)  & (test_reg[['H1H2_GROWTH_PARENT_TDP']]<=-0.02) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.58) 
                          ~ (0.3183552332115355 * -0.7754377690206252) ,
                          
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]>-0.02 ) & (test_reg[['H1H2_GROWTH_PARENT_TDP']]<=0.07) & 
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=-1)  & (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.18)
                          ~ (-0.0496267816418884 * -0.20668022112710804),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]>-0.02 ) & (test_reg[['H1H2_GROWTH_PARENT_TDP']]<=0.07) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.18)  & (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.58)
                          ~( -0.0496267816418884 * -0.10237734702741726),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]>-0.02 ) & (test_reg[['H1H2_GROWTH_PARENT_TDP']]<=0.07) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.58) 
                          ~ (-0.0496267816418884 * 0.3183552332115355),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]> 0.07)  & 
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=-1)  & (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.18) 
                          ~( 0.9737620857886338 * -0.20668022112710804),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]> 0.07)  & 
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.18)  & (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<=0.58) 
                          ~ (0.9737620857886338 * -0.10237734702741726),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]> 0.07)  & 
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>0.58) 
                          ~ (0.9737620857886338 * 0.3183552332115355)
                          
                          
                          
                          ))


# COMMAND ----------

bins <- rbin_manual(data=data,response=NPD_PB,predictor=Initiative_Size,cut_points= c(0.05))
bins

# COMMAND ----------

data <- data %>% mutate('Initiative_Size_binned'
                        := case_when(
                          data[['Initiative_Size']]>=0 & (data[['Initiative_Size']]<=0.05) ~ 0.4622094,
                          data[['Initiative_Size']]>0.05  ~ -0.4682660
                          ))

train_reg <- train_reg %>% mutate('Initiative_Size_binned'
                        := case_when(
                          train_reg[['Initiative_Size']]>=0 & (train_reg[['Initiative_Size']]<=0.05) ~ 0.4622094,
                          train_reg[['Initiative_Size']]>0.05  ~ -0.4682660
                          ))

test_reg <- test_reg %>% mutate('Initiative_Size_binned'
                        := case_when(
                          test_reg[['Initiative_Size']]>=0 & (test_reg[['Initiative_Size']]<=0.05) ~ 0.4622094,
                          test_reg[['Initiative_Size']]>0.05  ~ -0.4682660
                          ))


# COMMAND ----------

bins <- rbin_manual(data=data,response=NPD_PB,predictor=Price_Index,cut_points= c(0.8,1.2))
bins

# COMMAND ----------

data <- data %>% mutate('Price_Index_binned'
                        := case_when(
                          data[['Price_Index']]<0.8 ~ 0.2248812,
                          data[['Price_Index']]>=0.8 & (data[['Price_Index']]<1.2) ~ -0.3102618,
                          data[['Price_Index']]>=1.2 ~ 0.3778205
                          ))

train_reg <- train_reg %>% mutate('Price_Index_binned'
                        := case_when(
                          train_reg[['Price_Index']]<0.8 ~ 0.2248812,
                          train_reg[['Price_Index']]>=0.8 & (train_reg[['Price_Index']]<1.2) ~ -0.3102618,
                          train_reg[['Price_Index']]>=1.2 ~ 0.3778205
                          ))

test_reg <- test_reg %>% mutate('Price_Index_binned'
                        := case_when(
                          test_reg[['Price_Index']]<0.8 ~ 0.2248812,
                          test_reg[['Price_Index']]>=0.8 & (test_reg[['Price_Index']]<1.2) ~ -0.3102618,
                          test_reg[['Price_Index']]>=1.2 ~ 0.3778205
                          ))




# COMMAND ----------

selected_features<-paste0(iv_based_features,"_binned")
selected_features

# COMMAND ----------

selected_features<-selected_features[selected_features!='H1H2_GROWTH_PARENT_TDP_binned']
selected_features<-append(selected_features,c('NPD_TDP_relative_to_PB_TDP_h1h2_binned'))
selected_features

# COMMAND ----------

sum(is.na(train_reg[selected_features]))
sum(is.na(data[selected_features]))

# COMMAND ----------

train_reg[is.na(train_reg)] <- 0
test_reg[is.na(test_reg)] <- 0
data[is.na(data)] <- 0

# COMMAND ----------

create_model_equation <- function(dv, fixed_effects, random_effects=c()){
  fixed_equation<-paste(fixed_effects, collapse="+")
  ranef_equation <- paste(random_effects, collapse="+")
  if(length(random_effects) > 0){
    rhs_eq <- paste(fixed_equation, ranef_equation, sep="+")
  }else {
    rhs_eq <- fixed_equation 
  }
  model_equation<-paste(dv,rhs_eq ,sep="~")
  return(model_equation)
}

# COMMAND ----------

model_eq<-create_model_equation(dv='NPD_PB', fixed_effects=selected_features)
model_eq

# COMMAND ----------

# MAGIC %md ######Variable bins

# COMMAND ----------



get_var_bins <- function(data,variable){

  bin_var<-paste0(variable,"_binned")
  bin_summary<-as.data.frame(data%>%group_by(!!sym(bin_var))
                             %>%summarise(no_of_NPDs=n(),
                                          goods=sum(NPD_PB),
                                          bads=no_of_NPDs-goods,
                                          perc_goods=goods/no_of_NPDs,
                                          perc_bads=bads/no_of_NPDs))
  
  woe.df<-as.data.frame(IV_5_bins$Tables[[variable]])
  
  
  var.bins<-merge(y=bin_summary,x=woe.df,by.y=bin_var,by.x='WOE',all.x=TRUE)
  return(var.bins)
  }
  

# COMMAND ----------

display(get_var_bins(data,'Fair_share_index'))

# COMMAND ----------

bin_summary<-data%>%group_by(NPD_TDP_relative_to_PB_TDP_h1h2_binned)%>%summarise(
                                                    no_of_NPDs=n(),
                                                    goods=sum(NPD_PB),
                                                    bads=no_of_NPDs-goods,
                                                    perc_goods=goods/no_of_NPDs,
                                                    perc_bads=bads/no_of_NPDs)
display(as.data.frame(bin_summary))

# COMMAND ----------

# MAGIC %md ######Model

# COMMAND ----------

logistic_model <- glm(model_eq, 
                      data = train_reg, 
                      family = "binomial")

# COMMAND ----------

summary(logistic_model)

# COMMAND ----------

# DBTITLE 1,Train metrics
# Train metrics

predict_reg <- predict(logistic_model, 
                       train_reg, type = "response")  
   
# Changing probabilities
predicted.classes <- ifelse(predict_reg > 0.5, 1, 0)
pred_tab <- table(predicted.classes, train_reg$NPD_PB)

print(auc(train_reg$NPD_PB, predict_reg))
confusionMatrix(pred_tab)

# COMMAND ----------

# Test metrics

predict_reg <- predict(logistic_model, 
                       test_reg, type = "response")

# Changing probabilities
predicted.classes <- ifelse(predict_reg > 0.5, 1, 0)
pred_tab <- table(predicted.classes, test_reg$NPD_PB)


print(auc(test_reg$NPD_PB, predict_reg))
confusionMatrix(pred_tab)

# COMMAND ----------

# MAGIC %md ######Overall

# COMMAND ----------

display(as.data.frame(cor(data)))

# COMMAND ----------

logistic_model_overall <- glm(model_eq, 
                      data = data, 
                      family = "binomial")

# COMMAND ----------

# Test metrics

predict_reg <- predict(logistic_model_overall, 
                       data, type = "response")
predict_reg  
   
# Changing probabilities
predicted.classes <- ifelse(predict_reg > 0.65, 1, 0)
pred_tab <- table(predicted.classes, data$NPD_PB)


print(auc(data$NPD_PB, predict_reg))
confusionMatrix(pred_tab)

# COMMAND ----------

data$predict_reg<-predict_reg

# COMMAND ----------

# MAGIC %md ######Lift

# COMMAND ----------

lift <- function(depvar, predcol, groups=10) {

if(is.factor(depvar)) depvar <- as.integer(as.character(depvar))
if(is.factor(predcol)) predcol <- as.integer(as.character(predcol))
helper = data.frame(cbind(depvar, predcol))
helper[,"bucket"] = ntile(-helper[,"predcol"], groups)
gaintable = helper %>% group_by(bucket)  %>%
  summarise_at(vars(depvar), funs(total = n(),
  totalresp=sum(., na.rm = TRUE))) %>%
  mutate(Cumresp = cumsum(totalresp),
  Gain=Cumresp/sum(totalresp)*100,
  Cumlift=Gain/(bucket*(100/groups)))
return(gaintable)
}

# COMMAND ----------

dt = lift(data$NPD_PB , data$predict_reg, groups = 10)

# COMMAND ----------

display(dt)

# COMMAND ----------

display(data[c('NPD_PB','predict_reg')])

# COMMAND ----------

# MAGIC %md ######Variable importance

# COMMAND ----------

varimp.df<-as.data.frame(varImp(logistic_model_overall))
varimp.df$Features<-rownames(varimp.df)

# COMMAND ----------

display(varimp.df)

# COMMAND ----------

# MAGIC %md ######Coefficients

# COMMAND ----------

coeff.df<-as.data.frame(summary(logistic_model_overall)$coef)
coeff.df$features<-rownames(coeff.df)

# COMMAND ----------

display(coeff.df)

# COMMAND ----------

display(data[c('NPD_PB','predict_reg')])

# COMMAND ----------

display(df_2[c('NPD_PB','NPD_PB_avg')])

# COMMAND ----------

# MAGIC %md ######Calculate Average incrementality

# COMMAND ----------

data$NPD_PB_avg <- df_2$NPD_PB_avg
data$NPD_PB_chk <- df_2$NPD_PB
data$INITIATIVE_NAME <- df_2$INITIATIVE_NAME

data$Yr1_NEW_LAUNCH_VALUE <- df_2$Yr1_NEW_LAUNCH_VALUE
data$Yr1_PARENT_VALUE <- df_2$Yr1_PARENT_VALUE
data$H1_PARENT_TDP <- df_2$H1_PARENT_TDP
data$H2_PARENT_TDP <- df_2$H2_PARENT_TDP
data$H1_NEW_LAUNCH_TDP <- df_2$H1_NEW_LAUNCH_TDP
data$H2_NEW_LAUNCH_TDP <- df_2$H2_NEW_LAUNCH_TDP
data$Innovation_market_share <- df_2$Total_Brand_innovation_sales/df_2$Segment_innovation_sales
data$PB_market_share <- df_2$Parent_Brand_Value_sales/df_2$Segment_Value_sales
data$Prelaunch_3months_COMP_VOL_1 <- df_2$Prelaunch_3months_COMP_VOL_1
data$Prelaunch_3months_PARENT_VOL <- df_2$Prelaunch_3months_PARENT_VOL
data$Prelaunch_3months_PARENT_TDP <- df_2$Prelaunch_3months_PARENT_TDP
data$Prelaunch_6months_PARENT_TDP <- df_2$Prelaunch_6months_PARENT_TDP
data$NPD_Price <- (df_2$Yr1_NEW_LAUNCH_VALUE/df_2$Yr1_NEW_LAUNCH_VOL)
data$PB_Price <- (df_2$Yr1_PARENT_VALUE/df_2$Yr1_PARENT_VOL)

# COMMAND ----------

display(df_2)

# COMMAND ----------

display(data)

# COMMAND ----------

display(data[c('NPD_PB','NPD_PB_chk','NPD_PB_avg','predict_reg')])

# COMMAND ----------

lm_model_data <- data 
# %>% filter(NPD_PB_avg!=0)
dim(lm_model_data)

# COMMAND ----------

lm_model <- lm(NPD_PB_avg~1+predict_reg,lm_model_data)

# COMMAND ----------

lm_model

# COMMAND ----------

lm_predictions <- predict(lm_model, lm_model_data)
lm_model_data$lm_predictions <- lm_predictions

# COMMAND ----------

display(lm_model_data[c('NPD_PB_avg','lm_predictions')])

# COMMAND ----------

wmape_fun <- function(actual_val, predicted_val) {
  #' Function to compute weighted mape
  #' @param actual_val vector of actual values
  #' @param predicted_val vector of predicted values
  wmape_val <- sum(abs(actual_val - predicted_val)) / sum(actual_val)
  return(wmape_val)
}

r2_general<-function(actual,preds){ 
  rss <- sum((preds - actual) ^ 2)
  tss <- sum((actual - mean(actual)) ^ 2)
  rsq <- 1 - rss/tss
  return(rsq)
}

# COMMAND ----------

wmape_fun(lm_model_data$NPD_PB_avg,lm_model_data$lm_predictions)

# COMMAND ----------

r2_general(lm_model_data$NPD_PB_avg,lm_model_data$lm_predictions)

# COMMAND ----------

hist(lm_model_data$NPD_PB_avg)

# COMMAND ----------

display(lm_model_data)

# COMMAND ----------

