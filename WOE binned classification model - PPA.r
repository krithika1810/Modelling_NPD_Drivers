# Databricks notebook source
# DBTITLE 1,Install packages
install.packages("Information")
install.packages("fastDummies")
install.packages("caTools")    # For Logistic regression
install.packages("ROCR")
install.packages("klaR")
install.packages("dgof")
install.packages("stringr")
# install.packages("rlang",version=1.0.2)
# install.packages("tidymodels")
install.packages("rbin")
install.packages("naniar")


library("rbin")
library(fastDummies)
library(dplyr)
library(stringr)
library(sparklyr)
library(glmnet)
library("caTools")    # For Logistic regression
# library("ROCR")
library("caret")
library(Information)
library(gridExtra)
# library(klaR)
library(pROC)
library(dgof)
library(stringr)
library(tidyr)
library(naniar)
# library("tidymodels")

# COMMAND ----------

# DBTITLE 1,Spark connect
sc <- spark_connect(method='databricks')

# COMMAND ----------

# DBTITLE 1,Read dataset
df_2 <- spark_read_csv(sc, 'data', path="/FileStore/tables/NPD_drivers_all_features-1.csv", header=TRUE, overwrite=TRUE)

# COMMAND ----------

df_2=as.data.frame(df_2)

# COMMAND ----------

dim(df_2[(df_2$MEAN_PACK_SIZE==0) & (df_2$INNOVATION_TYPE=='PPA'),])

# COMMAND ----------

colnames(df_2)

# COMMAND ----------

grep("MEAN_PACK_SIZE", colnames(df_2))

# COMMAND ----------

df_2 %>% replace_with_na(replace=list(MEAN_PACK_SIZE=0))
df_2[, 177][df_2[, 177] == 0] <- NA

# COMMAND ----------

row.has.na <- apply(df_2, 1, function(x){any(is.na(x))})

# COMMAND ----------

sum(row.has.na)

# COMMAND ----------

df_2 <- df_2[!row.has.na,]

# COMMAND ----------

summary(df_2$MEAN_PACK_SIZE)

# COMMAND ----------

df_2 <- df_2[!(is.na(df_2$NPD_PB)), ]
df_2 <- df_2[df_2$INNOVATION_TYPE=='PPA', ]


# COMMAND ----------

# MAGIC %md ######Feature engineering - additional features

# COMMAND ----------

# df$NPD_TDP_relative_to_PB_TDP_h1h2=(df$H1H2_GROWTH_NEW_LAUNCH_TDP-df$H1H2_GROWTH_PARENT_TDP)/replace(df$H1H2_GROWTH_PARENT_TDP, df$H1H2_GROWTH_PARENT_TDP==0, 1)
# df$NPD_VOL_relative_to_PB_VOL_h1h2=(df$H1H2_GROWTH_NEW_LAUNCH_VOL-df$H1H2_GROWTH_PARENT_VOL)/replace(df$H1H2_GROWTH_PARENT_VOL, df$H1H2_GROWTH_PARENT_VOL==0, 1)

# # df$Parent_TDP_relative_to_comp1_TDP_H1=(df$H1_PARENT_TDP-df$H1_COMP_TDP_1)/replace(df$H1_COMP_TDP_1,df$H1_COMP_TDP_1==0, 1)
# # df$Parent_TDP_relative_to_comp2_TDP_H1=(df$H1_PARENT_TDP-df$H1_COMP_TDP_2)/replace(df$H1_COMP_TDP_2,df$H1_COMP_TDP_2==0, 1)

# # df$Parent_TDP_relative_to_comp1_TDP_H2=(df$H2_PARENT_TDP-df$H2_COMP_TDP_1)/replace(df$H2_COMP_TDP_1,df$H2_COMP_TDP_1==0, 1)
# # df$Parent_TDP_relative_to_comp2_TDP_H2=(df$H2_PARENT_TDP-df$H2_COMP_TDP_2)/replace(df$H2_COMP_TDP_2,df$H2_COMP_TDP_2==0, 1)

# df$Change_in_PB_TDP_vs_comp1_h1h2=(df$H1H2_GROWTH_PARENT_TDP-df$H1H2_GROWTH_COMP_TDP_1)/replace(df$H1H2_GROWTH_COMP_TDP_1,df$H1H2_GROWTH_COMP_TDP_1==0, 1)
# df$Change_in_PB_TDP_vs_comp2_h1h2=(df$H1H2_GROWTH_PARENT_TDP-df$H1H2_GROWTH_COMP_TDP_2)/replace(df$H1H2_GROWTH_COMP_TDP_2, df$H1H2_GROWTH_COMP_TDP_2==0,1) 

# df$Change_in_PB_VOL_vs_comp1_h1h2=(df$H1H2_GROWTH_PARENT_VOL-df$H1H2_GROWTH_COMP_VOL_1)/replace(df$H1H2_GROWTH_COMP_VOL_1,df$H1H2_GROWTH_COMP_VOL_1==0, 1)
# df$Change_in_PB_VOL_vs_comp2_h1h2=(df$H1H2_GROWTH_PARENT_VOL-df$H1H2_GROWTH_COMP_VOL_2)/replace(df$H1H2_GROWTH_COMP_VOL_2,df$H1H2_GROWTH_COMP_VOL_2==0, 1)

# COMMAND ----------

df_2$NPD_TDP_relative_to_PB_TDP_h1h2=ifelse(df_2$H1H2_GROWTH_NEW_LAUNCH_TDP<0 & df_2$H1H2_GROWTH_PARENT_TDP<0,-(df_2$H1H2_GROWTH_NEW_LAUNCH_TDP*df_2$H1H2_GROWTH_PARENT_TDP), df_2$H1H2_GROWTH_NEW_LAUNCH_TDP*df_2$H1H2_GROWTH_PARENT_TDP)
df_2$NPD_VOL_relative_to_PB_VOL_h1h2=ifelse(df_2$H1H2_GROWTH_NEW_LAUNCH_VOL<0 & df_2$H1H2_GROWTH_PARENT_VOL<0, -(df_2$H1H2_GROWTH_NEW_LAUNCH_VOL*df_2$H1H2_GROWTH_PARENT_VOL),df_2$H1H2_GROWTH_NEW_LAUNCH_VOL*df_2$H1H2_GROWTH_PARENT_VOL)

# df_2$Parent_TDP_relative_to_comp1_TDP_H1=(df_2$H1_PARENT_TDP)/df_2$H1_COMP_TDP_1
# df_2$Parent_TDP_relative_to_comp2_TDP_H1=(df_2$H1_PARENT_TDP)/df_2$H1_COMP_TDP_2

df_2$Parent_TDP_relative_to_comp1_TDP_prelaunch=(df_2$Prelaunch_3months_PARENT_TDP)/replace(df_2$Prelaunch_3months_COMP_TDP_1, df_2$Prelaunch_3months_COMP_TDP_1==0, 1)
df_2$Parent_VOL_relative_to_comp1_VOL_prelaunch=(df_2$Prelaunch_3months_PARENT_VOL)/replace(df_2$Prelaunch_3months_COMP_VOL_1, df_2$Prelaunch_3months_COMP_VOL_1==0, 1)




# df_2$Parent_TDP_relative_to_comp1_TDP_H2= ifelse(df_2$H2_PARENT_TDP<0 & df_2$H2_COMP_TDP_1<0, -(df_2$H2_PARENT_TDP*df_2$H2_COMP_TDP_1),df_2$H2_PARENT_TDP*df_2$H2_COMP_TDP_1)
# df_2$Parent_TDP_relative_to_comp2_TDP_H2=ifelse(df_2$H2_PARENT_TDP<0 & df_2$H2_COMP_TDP_2<0, -(df_2$H2_PARENT_TDP*df_2$H2_COMP_TDP_2),df_2$H2_PARENT_TDP*df_2$H2_COMP_TDP_2)

df_2$Change_in_PB_TDP_vs_comp1_h1h2=ifelse(df_2$H1H2_GROWTH_PARENT_TDP<0 & df_2$H1H2_GROWTH_COMP_TDP_1<0, -(df_2$H1H2_GROWTH_PARENT_TDP*df_2$H1H2_GROWTH_COMP_TDP_1),df_2$H1H2_GROWTH_PARENT_TDP*df_2$H1H2_GROWTH_COMP_TDP_1)
df_2$Change_in_PB_TDP_vs_comp2_h1h2=ifelse(df_2$H1H2_GROWTH_PARENT_TDP<0 & df_2$H1H2_GROWTH_COMP_TDP_2<0, -(df_2$H1H2_GROWTH_PARENT_TDP*df_2$H1H2_GROWTH_COMP_TDP_2),df_2$H1H2_GROWTH_PARENT_TDP*df_2$H1H2_GROWTH_COMP_TDP_2)

df_2$Change_in_PB_VOL_vs_comp1_h1h2=ifelse(df_2$H1H2_GROWTH_PARENT_VOL<0 & df_2$H1H2_GROWTH_COMP_VOL_1<0, -(df_2$H1H2_GROWTH_PARENT_VOL*df_2$H1H2_GROWTH_COMP_VOL_1),df_2$H1H2_GROWTH_PARENT_VOL*df_2$H1H2_GROWTH_COMP_VOL_1)
df_2$Change_in_PB_VOL_vs_comp2_h1h2=ifelse(df_2$H1H2_GROWTH_PARENT_VOL<0 & df_2$H1H2_GROWTH_COMP_VOL_2<0, -(df_2$H1H2_GROWTH_PARENT_VOL*df_2$H1H2_GROWTH_COMP_VOL_2),df_2$H1H2_GROWTH_PARENT_VOL*df_2$H1H2_GROWTH_COMP_VOL_2)


df_2$Change_in_PB_TDP_vs_comp1_h1h2=(df_2$H1H2_GROWTH_PARENT_TDP - df_2$H1H2_GROWTH_COMP_TDP_1)
df_2$Change_in_PB_TDP_vs_comp2_h1h2=(df_2$H1H2_GROWTH_PARENT_TDP - df_2$H1H2_GROWTH_COMP_TDP_2)

df_2$Change_in_PB_VOL_vs_comp1_h1h2=(df_2$H1H2_GROWTH_PARENT_VOL - df_2$H1H2_GROWTH_COMP_VOL_1)
df_2$Change_in_PB_VOL_vs_comp2_h1h2=(df_2$H1H2_GROWTH_PARENT_VOL - df_2$H1H2_GROWTH_COMP_VOL_2)

# COMMAND ----------

# DBTITLE 1,Binary DV
df_2$NPD_PB_avg <- df_2$NPD_PB
df_2$NPD_PB<-ifelse(df_2$NPD_PB>=0.31, 1, 0)

# COMMAND ----------

features<-c(
'Fair_share_index',
'MEAN_PACK_SIZE',
# 'Brand_value_share',
'No_of_core_products',
'No_of_Innovations',
'NPD_PB',
'Price_Index',
# 'NPD_TDP_relative_to_brand_TDP',
# 'SI',
'INNOVATION_TYPE',
# 'Velocity_Core_Product',
# 'Mfr_Maturity_level',
'EM_vs_DM',
# 'Initiative_Size',
# 'pre6Mpre3M_GROWTH_PARENT_VOL',
'pre6Mpre3M_GROWTH_PARENT_TDP',
# 'NPD_TDP_relative_to_PB_TDP_h1h2',
  'H1H2_GROWTH_NEW_LAUNCH_TDP',
  'H1H2_GROWTH_PARENT_TDP',
# 'NPD_VOL_relative_to_PB_VOL_h1h2',
  
# 'Parent_TDP_relative_to_comp1_TDP_H1',
# 'Parent_TDP_relative_to_comp2_TDP_H1'
  
  'Parent_VOL_relative_to_comp1_VOL_prelaunch'
#   'Parent_TDP_relative_to_comp1_TDP_prelaunch'
  

)

# COMMAND ----------

df<-df_2[features]

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

# df$Parent_VOL_relative_to_comp1_VOL_prelaunch

# COMMAND ----------

corr.df <- cor(df[feature_list])
corr.df[upper.tri(corr.df)] <- 0
diag(corr.df) <- 0
data <- 
  df[, !apply(corr.df, 2, function(x) any(abs(x) >=0.7, na.rm = TRUE))]
dim(data)

# COMMAND ----------

# MAGIC %md ######IV summary

# COMMAND ----------

table(data$NPD_PB)

# COMMAND ----------

set.seed(1)

smp_size <- floor(0.8 * nrow(data))

# split <- sample.split(data, SplitRatio = 0.8)
train_ind <- sample(seq_len(nrow(data)), size = smp_size)
train_reg <- data[train_ind, ]
test_reg <- data[-train_ind, ]

# train_reg <- subset(data, split == "TRUE")
# test_reg <- subset(data, split == "FALSE")
print(paste0("Shape of train data ",dim(train_reg)))
print(paste0("Shape of test data ",dim(test_reg)))

# COMMAND ----------

IV <- create_infotables(data=train_reg,valid=test_reg, y="NPD_PB", ncore=2)
IV_5_bins <- create_infotables(data=train_reg,valid=test_reg, y="NPD_PB", ncore=2,bins=5)
IV_3_bins <- create_infotables(data=train_reg,valid=test_reg, y="NPD_PB", ncore=2,bins=3)

# COMMAND ----------

display(as.data.frame(IV_5_bins$Summary))

# COMMAND ----------

display(as.data.frame(IV_3_bins$Tables$H1H2_GROWTH_NEW_LAUNCH_TDP))

# COMMAND ----------

display(as.data.frame(IV_3_bins$Tables$H1H2_GROWTH_PARENT_TDP))

# COMMAND ----------


bins <- rbin_manual(data=data,response=NPD_PB,predictor=H1H2_GROWTH_NEW_LAUNCH_TDP,cut_points= c(0.04,0.42))
bins


# COMMAND ----------


bins <- rbin_manual(data=data,response=NPD_PB,predictor=H1H2_GROWTH_PARENT_TDP,cut_points= c(-0.05,0.02))
bins


# COMMAND ----------

data <- data %>% mutate(NPD_TDP_relative_to_PB_TDP_h1h2_binned
                        := case_when(
#                          
                          
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]>=-0.05) & (data[['H1H2_GROWTH_PARENT_TDP']]<0.02) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.04)  
                          ~( 0.1174856 * -0.2607811),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]>=-0.05) & (data[['H1H2_GROWTH_PARENT_TDP']]<0.02) & 
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.04)  & (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.42)  
                          ~( 0.1174856 * 0.4808664),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]>=-0.05) & (data[['H1H2_GROWTH_PARENT_TDP']]<0.02) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.42)   
                          ~( 0.1174856 * -0.1661396),
                          
                          
                            (data[['H1H2_GROWTH_PARENT_TDP']]>=0.02) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.04)  
                          ~( -0.3429120 * -0.2607811),
                          
                           (data[['H1H2_GROWTH_PARENT_TDP']]>=0.02) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.04)  & (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.42)  
                          ~( -0.3429120 * 0.4808664),
                          
                           (data[['H1H2_GROWTH_PARENT_TDP']]>=0.02) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.42)   
                          ~( -0.3429120 * -0.1661396),
                          
                           (data[['H1H2_GROWTH_PARENT_TDP']]<-0.05) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.04)  
                          ~( 0.2105761 * -0.2607811),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]<-0.05) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.04)  & (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.42)  
                          ~( 0.2105761 * 0.4808664),
                          
                          (data[['H1H2_GROWTH_PARENT_TDP']]<-0.05) &
                          (data[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.42)   
                          ~( 0.2105761 * -0.1661396)
                          
                          
                          
                          
                          
                          
                         
                          
                          ))


# COMMAND ----------

train_reg <- train_reg %>% mutate(NPD_TDP_relative_to_PB_TDP_h1h2_binned
                        := case_when(
#                          
                          
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.05) & (train_reg[['H1H2_GROWTH_PARENT_TDP']]<0.02) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.04)  
                          ~( 0.1174856 * -0.2607811),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.05) & (train_reg[['H1H2_GROWTH_PARENT_TDP']]<0.02) & 
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.04)  & (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.42)  
                          ~( 0.1174856 * 0.4808664),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.05) & (train_reg[['H1H2_GROWTH_PARENT_TDP']]<0.02) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.42)   
                          ~( 0.1174856 * -0.1661396),
                          
                          
                            (train_reg[['H1H2_GROWTH_PARENT_TDP']]>=0.02) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.04)  
                          ~( -0.3429120 * -0.2607811),
                          
                           (train_reg[['H1H2_GROWTH_PARENT_TDP']]>=0.02) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.04)  & (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.42)  
                          ~( -0.3429120 * 0.4808664),
                          
                           (train_reg[['H1H2_GROWTH_PARENT_TDP']]>=0.02) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.42)   
                          ~( -0.3429120 * -0.1661396),
                          
                           (train_reg[['H1H2_GROWTH_PARENT_TDP']]<-0.05) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.04)  
                          ~( 0.2105761 * -0.2607811),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]<-0.05) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.04)  & (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.42)  
                          ~( 0.2105761 * 0.4808664),
                          
                          (train_reg[['H1H2_GROWTH_PARENT_TDP']]<-0.05) &
                          (train_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.42)   
                          ~( 0.2105761 * -0.1661396)
                          
                          
                          
                          
                          
                          
                         
                          
                          ))

test_reg <- test_reg %>% mutate(NPD_TDP_relative_to_PB_TDP_h1h2_binned
                        := case_when(
#                          
                          
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.05) & (test_reg[['H1H2_GROWTH_PARENT_TDP']]<0.02) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.04)  
                          ~( 0.1174856 * -0.2607811),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.05) & (test_reg[['H1H2_GROWTH_PARENT_TDP']]<0.02) & 
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.04)  & (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.42)  
                          ~( 0.1174856 * 0.4808664),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]>=-0.05) & (test_reg[['H1H2_GROWTH_PARENT_TDP']]<0.02) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.42)   
                          ~( 0.1174856 * -0.1661396),
                          
                          
                            (test_reg[['H1H2_GROWTH_PARENT_TDP']]>=0.02) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.04)  
                          ~( -0.3429120 * -0.2607811),
                          
                           (test_reg[['H1H2_GROWTH_PARENT_TDP']]>=0.02) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.04)  & (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.42)  
                          ~( -0.3429120 * 0.4808664),
                          
                           (test_reg[['H1H2_GROWTH_PARENT_TDP']]>=0.02) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.42)   
                          ~( -0.3429120 * -0.1661396),
                          
                           (test_reg[['H1H2_GROWTH_PARENT_TDP']]<-0.05) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.04)  
                          ~( 0.2105761 * -0.2607811),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]<-0.05) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.04)  & (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]<0.42)  
                          ~( 0.2105761 * 0.4808664),
                          
                          (test_reg[['H1H2_GROWTH_PARENT_TDP']]<-0.05) &
                          (test_reg[['H1H2_GROWTH_NEW_LAUNCH_TDP']]>=0.42)   
                          ~( 0.2105761 * -0.1661396)
                          
                          
                          
                          
                          
                          
                         
                          
                          ))


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
#                           data[[var]]> as.numeric(bin_list[4]) & (data[[var]]<=as.numeric(bin_list[5]) )~woe_list[4],
#                           data[[var]] > as.numeric(bin_list[5])  ~woe_list[5]
#                           data[[var]]> as.numeric(bin_list[6]) & data[[var]]<=as.numeric(bin_list[7]) ~woe_list[6],
#                           data[[var]]> as.numeric(bin_list[7]) & data[[var]]<=as.numeric(bin_list[8]) ~woe_list[7],
#                           data[[var]]> as.numeric(bin_list[8]) & data[[var]]<=as.numeric(bin_list[9]) ~woe_list[8],
#                           data[[var]]> as.numeric(bin_list[9]) & data[[var]]<=as.numeric(bin_list[10]) ~woe_list[9],
#                           data[[var]]> as.numeric(bin_list[10]) ~woe_list[10]
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
#                           data[[var]]> as.numeric(bin_list[6]) & data[[var]]<=as.numeric(bin_list[7]) ~woe_list[6],
#                           data[[var]]> as.numeric(bin_list[7]) & data[[var]]<=as.numeric(bin_list[8]) ~woe_list[7],
#                           data[[var]]> as.numeric(bin_list[8]) & data[[var]]<=as.numeric(bin_list[9]) ~woe_list[8],
#                           data[[var]]> as.numeric(bin_list[9]) & data[[var]]<=as.numeric(bin_list[10]) ~woe_list[9],
#                           data[[var]]> as.numeric(bin_list[10]) ~woe_list[10]
                                    ))
  
    
  }
  return (data)}

# COMMAND ----------

iv_based_features<-c(IV_5_bins$Summary[IV_5_bins$Summary$AdjIV>-1,]$Variable)
iv_based_features

# COMMAND ----------

# iv_based_features

# data$MEAN_PACK_SIZE<-as.numeric(data$MEAN_PACK_SIZE)

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

# display(as.data.frame(IV_5_bins$Tables$Parent_TDP_relative_to_comp1_TDP_prelaunch))

# COMMAND ----------

# display(as.data.frame(IV_5_bins$Tables$Parent_VOL_relative_to_comp1_VOL_prelaunch))

# COMMAND ----------

# data<-get_binned_data(data,'NPD_VOL_relative_to_PB_VOL_h1h2','NPD_VOL_relative_to_PB_VOL_h1h2_binned')
# # data<-get_binned_data(data,'Initiative_Size','Initiative_Size_binned')
# # data<-get_binned_data(data,'No_of_core_products','No_of_core_products_binned')
# # data<-get_binned_data(data,'No_of_Innovations','No_of_Innovations_binned')
# data<-get_binned_data(data,'MEAN_PACK_SIZE','MEAN_PACK_SIZE_binned')
# # data<-get_binned_data(data,'Fair_share_index','Fair_share_index_binned')
# data<-get_binned_data(data,'PB_TDP_relative_to_COMP_TDP_1','PB_TDP_relative_to_COMP_TDP_1_binned')
# data<-get_binned_data(data,'pre6Mpre3M_GROWTH_PARENT_TDP','pre6Mpre3M_GROWTH_PARENT_TDP_binned')

# # data<-get_binned_data(data,'Change_in_PB_VOL_vs_comp1_h1h2','Change_in_PB_VOL_vs_comp1_h1h2_binned')
# data<-get_binned_data(data,'EM_vs_DM_EM','EM_vs_DM_EM_binned')
# # data<-get_binned_data(data,'Velocity_Core_Product','Velocity_Core_Product_binned')

# COMMAND ----------

# MAGIC %md ######Manual bins

# COMMAND ----------

bins <- rbin_manual(data=data,response=NPD_PB,predictor=No_of_core_products,cut_points= c(20,40,60))
bins

# COMMAND ----------

# data <- data %>% mutate('Fair_share_index_binned'
#                         := case_when(
#                           data[['Fair_share_index']]<1.4 ~ 0.3437909,
#                           data[['Fair_share_index']]>= 1.4 ~ -0.4872176
#                           ))

# train_reg <- train_reg %>% mutate('Fair_share_index_binned'
#                         := case_when(
#                           train_reg[['Fair_share_index']]<1.4 ~ 0.3437909,
#                           train_reg[['Fair_share_index']]>= 1.4 ~ -0.4872176
#                           ))

# test_reg <- test_reg %>% mutate('Fair_share_index_binned'
#                         := case_when(
#                           test_reg[['Fair_share_index']]<1.4 ~ 0.3437909,
#                           test_reg[['Fair_share_index']]>= 1.4 ~ -0.4872176
#                           ))


# COMMAND ----------

data <- data %>% mutate('No_of_core_products_binned'
                        := case_when(
                          data[['No_of_core_products']]>=0 & (data[['No_of_core_products']]<25) ~ -0.7029407,
                          data[['No_of_core_products']]>=25 & (data[['No_of_core_products']]<100) ~ 0.1009232,
                          data[['No_of_core_products']]>= 100 ~ 0.3652868
                          ))


train_reg <- train_reg %>% mutate('No_of_core_products_binned'
                        := case_when(
                          train_reg[['No_of_core_products']]>=0 & (train_reg[['No_of_core_products']]<25) ~ -0.7029407,
                          train_reg[['No_of_core_products']]>=25 & (train_reg[['No_of_core_products']]<100) ~ 0.1009232,
                          train_reg[['No_of_core_products']]>= 100 ~ 0.3652868
                          ))

test_reg <- test_reg %>% mutate('No_of_core_products_binned'
                        := case_when(
                          test_reg[['No_of_core_products']]>=0 & (test_reg[['No_of_core_products']]<25) ~ -0.7029407,
                          test_reg[['No_of_core_products']]>=25 & (test_reg[['No_of_core_products']]<100) ~ 0.1009232,
                          test_reg[['No_of_core_products']]>= 100 ~ 0.3652868
                          ))

# COMMAND ----------

bins <- rbin_manual(data=data,response=NPD_PB,predictor=No_of_Innovations,cut_points= c(8))
bins

# COMMAND ----------

data <- data %>% mutate('No_of_Innovations_binned'
                        := case_when(
                          data[['No_of_Innovations']]<8 ~ -0.3214058,
                          data[['No_of_Innovations']]>= 8 ~ 0.5964966
                          ))


train_reg <- train_reg %>% mutate('No_of_Innovations_binned'
                        := case_when(
                          train_reg[['No_of_Innovations']]<8 ~ -0.3214058,
                          train_reg[['No_of_Innovations']]>= 8 ~ 0.5964966
                          ))

test_reg <- test_reg %>% mutate('No_of_Innovations_binned'
                        := case_when(
                          test_reg[['No_of_Innovations']]<8 ~ -0.3214058,
                          test_reg[['No_of_Innovations']]>= 8 ~ 0.5964966
                          ))

# COMMAND ----------

bins <- rbin_manual(data=data,response=NPD_PB,predictor=pre6Mpre3M_GROWTH_PARENT_TDP,cut_points= c(0.1))
bins

# COMMAND ----------

# data <- data %>% mutate('Initiative_Size_binned'
#                         := case_when(
#                           data[['Initiative_Size']]>=0 & (data[['Initiative_Size']]<0.03) ~ 0.1468602,
#                           data[['Initiative_Size']]>=0.03  ~ -0.1816438
#                           ))

# train_reg <- train_reg %>% mutate('Initiative_Size_binned'
#                         := case_when(
#                           train_reg[['Initiative_Size']]>=0 & (train_reg[['Initiative_Size']]<0.03) ~ 0.1468602,
#                           train_reg[['Initiative_Size']]>=0.03  ~ -0.1816438
#                           ))

# test_reg <- test_reg %>% mutate('Initiative_Size_binned'
#                         := case_when(
#                           test_reg[['Initiative_Size']]>=0 & (test_reg[['Initiative_Size']]<0.03) ~ 0.1468602,
#                           test_reg[['Initiative_Size']]>=0.03  ~ -0.1816438
#                           ))


# COMMAND ----------

bins <- rbin_manual(data=data,response=NPD_PB,predictor=Parent_VOL_relative_to_comp1_VOL_prelaunch,cut_points= c(1,5))
bins

# COMMAND ----------

data <- data %>% mutate('Parent_VOL_relative_to_comp1_VOL_prelaunch_binned'
                        := case_when(
                          data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<5 ~ -0.53831876,
                          data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=5 & (data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<25) ~ -0.05694034,
                          data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=25 ~ 0.43880153
                          ))

train_reg <- train_reg %>% mutate('Parent_VOL_relative_to_comp1_VOL_prelaunch_binned'
                        := case_when(
                          train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<5 ~ -0.53831876,
                          train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=5 & (train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<25) ~ -0.05694034,
                          train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=25 ~ 0.43880153
                          ))


test_reg <- test_reg %>% mutate('Parent_VOL_relative_to_comp1_VOL_prelaunch_binned'
                        := case_when(
                          test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<5 ~ -0.53831876,
                          test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=5 & (test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<25) ~ -0.05694034,
                          test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=25 ~ 0.43880153
                          ))




# COMMAND ----------

# data <- data %>% mutate('Parent_VOL_relative_to_comp1_VOL_prelaunch_binned'
#                         := case_when(
#                           data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<1 ~ -0.8057981,
#                           data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=1 & (data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<5) ~ -0.3813571,
#                           data[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=5 ~ 0.2508500
#                           ))

# train_reg <- train_reg %>% mutate('Parent_VOL_relative_to_comp1_VOL_prelaunch_binned'
#                         := case_when(
#                           train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<1 ~ -0.8057981,
#                           train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=1 & (train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<5) ~ -0.3813571,
#                           train_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=5 ~ 0.2508500
#                           ))


# test_reg <- test_reg %>% mutate('Parent_VOL_relative_to_comp1_VOL_prelaunch_binned'
#                         := case_when(
#                           test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<1 ~ -0.8057981,
#                           test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=1 & (test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]<5) ~ -0.3813571,
#                           test_reg[['Parent_VOL_relative_to_comp1_VOL_prelaunch']]>=5 ~ 0.2508500
#                           ))




# COMMAND ----------

bins <- rbin_manual(data=data,response=NPD_PB,predictor=pre6Mpre3M_GROWTH_PARENT_TDP,cut_points= c(-0.05,-0.025,0,0.025,0.05))
bins

# COMMAND ----------

# data <- data %>% mutate('pre6Mpre3M_GROWTH_PARENT_TDP_binned'
#                         := case_when(
#                           data[['pre6Mpre3M_GROWTH_PARENT_TDP']]<=-0.025 ~ -0.1880196,
#                           data[['pre6Mpre3M_GROWTH_PARENT_TDP']]>-0.025 & (data[['pre6Mpre3M_GROWTH_PARENT_TDP']]<0) ~  0.4462871,
#                           data[['pre6Mpre3M_GROWTH_PARENT_TDP']]>=0 & (data[['pre6Mpre3M_GROWTH_PARENT_TDP']]<0.05) ~  0.3837667,
#                           data[['pre6Mpre3M_GROWTH_PARENT_TDP']]>=0.05 & (data[['pre6Mpre3M_GROWTH_PARENT_TDP']]<0.1) ~  -0.2846004,
#                           data[['pre6Mpre3M_GROWTH_PARENT_TDP']]>=0.1 ~  -0.4556149
#                           ))




# train_reg <- train_reg %>% mutate('pre6Mpre3M_GROWTH_PARENT_TDP_binned'
#                         := case_when(
#                           train_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]<=-0.025 ~ -0.1880196,
#                           train_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]>-0.025 & (train_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]<0) ~  0.4462871,
#                           train_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]>=0 & (train_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]<0.05) ~  0.3837667,
#                           train_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]>=0.05 & (train_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]<0.1) ~  -0.2846004,
#                           train_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]>=0.1 ~  -0.4556149
#                           ))

# test_reg <- test_reg %>% mutate('pre6Mpre3M_GROWTH_PARENT_TDP_binned'
#                         := case_when(
#                           test_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]<=-0.025 ~ -0.1880196,
#                           test_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]>-0.025 & (test_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]<0) ~  0.4462871,
#                           test_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]>=0 & (test_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]<0.05) ~  0.3837667,
#                           test_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]>=0.05 & (test_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]<0.1) ~  -0.2846004,
#                           test_reg[['pre6Mpre3M_GROWTH_PARENT_TDP']]>=0.1 ~  -0.4556149
#                           ))

# COMMAND ----------

# data <- data %>% mutate('Price_Index_binned'
#                         := case_when(
#                           data[['Price_Index']]<0.8 ~ 1.5789787,
#                           data[['Price_Index']]>=0.8 & (data[['Price_Index']]<1.2) ~ -0.1647715,
#                           data[['Price_Index']]>=1.2 ~ 0.1438942
#                           ))

# train_reg <- train_reg %>% mutate('Price_Index_binned'
#                         := case_when(
#                           train_reg[['Price_Index']]<0.8 ~ 1.5789787,
#                           train_reg[['Price_Index']]>=0.8 & (train_reg[['Price_Index']]<1.2) ~ -0.1647715,
#                           train_reg[['Price_Index']]>=1.2 ~ 0.1438942
#                           ))

# test_reg <- test_reg %>% mutate('Price_Index_binned'
#                         := case_when(
#                           test_reg[['Price_Index']]<0.8 ~ 1.5789787,
#                           test_reg[['Price_Index']]>=0.8 & (test_reg[['Price_Index']]<1.2) ~ -0.1647715,
#                           test_reg[['Price_Index']]>=1.2 ~ 0.1438942
#                           ))

# COMMAND ----------

# bins <- rbin_manual(data=data,response=NPD_PB,predictor=Brand_value_share,cut_points= c(0.005,0.03,0.07,0.15
#                                                                                        ))
# bins

# COMMAND ----------

# data <- data %>% mutate('Brand_value_share_binned'
#                         := case_when(
#                           data[['Brand_value_share']]<0.005 ~ -0.61842363,
#                           data[['Brand_value_share']]>=0.005 & (data[['Brand_value_share']]<0.01) ~ -0.04618938 ,
# 						  data[['Brand_value_share']]>=0.01 & (data[['Brand_value_share']]<0.02) ~ 0.35097692,
# 						  data[['Brand_value_share']]>=0.02 & (data[['Brand_value_share']]<0.05) ~ -0.08701138,
# 						  data[['Brand_value_share']]>=0.05 ~ 0.21267225
#                           ))

# train_reg <- train_reg %>% mutate('Brand_value_share_binned'
#                         := case_when(
#                           train_reg[['Brand_value_share']]<0.005 ~ -0.61842363,
#                           train_reg[['Brand_value_share']]>=0.005 & (train_reg[['Brand_value_share']]<0.01) ~ -0.04618938 ,
# 						  train_reg[['Brand_value_share']]>=0.01 & (train_reg[['Brand_value_share']]<0.02) ~ 0.35097692,
# 						  train_reg[['Brand_value_share']]>=0.02 & (train_reg[['Brand_value_share']]<0.05) ~ -0.08701138,
# 						  train_reg[['Brand_value_share']]>=0.05 ~ 0.21267225
#                           ))

# test_reg <- test_reg %>% mutate('Brand_value_share_binned'
#                         := case_when(
#                           test_reg[['Brand_value_share']]<0.005 ~ -0.61842363,
#                           test_reg[['Brand_value_share']]>=0.005 & (test_reg[['Brand_value_share']]<0.01) ~ -0.04618938 ,
# 						  test_reg[['Brand_value_share']]>=0.01 & (test_reg[['Brand_value_share']]<0.02) ~ 0.35097692,
# 						  test_reg[['Brand_value_share']]>=0.02 & (test_reg[['Brand_value_share']]<0.05) ~ -0.08701138,
# 						  test_reg[['Brand_value_share']]>=0.05 ~ 0.21267225
#                           ))




# COMMAND ----------

# data <- data %>% mutate('Brand_value_share_binned'
#                         := case_when(
#                           data[['Brand_value_share']]<0.005 ~ -0.61774590,
#                           data[['Brand_value_share']]>=0.005 & (data[['Brand_value_share']]<0.03) ~ 0.01332885 ,
# 						  data[['Brand_value_share']]>=0.03 & (data[['Brand_value_share']]<0.07) ~ 0.09028990,
# 						  data[['Brand_value_share']]>=0.07 & (data[['Brand_value_share']]<0.15) ~ 0.16933310,
# 						  data[['Brand_value_share']]>=0.15 ~ 0.35995346
#                           ))

# train_reg <- train_reg %>% mutate('Brand_value_share_binned'
#                         := case_when(
#                           train_reg[['Brand_value_share']]<0.005 ~ -0.61774590,
#                           train_reg[['Brand_value_share']]>=0.005 & (train_reg[['Brand_value_share']]<0.03) ~ 0.01332885 ,
# 						  train_reg[['Brand_value_share']]>=0.03 & (train_reg[['Brand_value_share']]<0.07) ~ 0.09028990,
# 						  train_reg[['Brand_value_share']]>=0.07 & (train_reg[['Brand_value_share']]<0.15) ~ 0.16933310,
# 						  train_reg[['Brand_value_share']]>=0.15 ~ 0.35995346
#                           ))

# test_reg <- test_reg %>% mutate('Brand_value_share_binned'
#                         := case_when(
#                           test_reg[['Brand_value_share']]<0.005 ~ -0.61774590,
#                           test_reg[['Brand_value_share']]>=0.005 & (test_reg[['Brand_value_share']]<0.03) ~ 0.01332885 ,
# 						  test_reg[['Brand_value_share']]>=0.03 & (test_reg[['Brand_value_share']]<0.07) ~ 0.09028990,
# 						  test_reg[['Brand_value_share']]>=0.07 & (test_reg[['Brand_value_share']]<0.15) ~ 0.16933310,
# 						  test_reg[['Brand_value_share']]>=0.15 ~ 0.35995346
#                           ))




# COMMAND ----------

# data$MEAN_PACK_SIZE
bins <- rbin_manual(data=data,response=NPD_PB,predictor=Price_Index,cut_points= c(0.8,1.2))
bins

# COMMAND ----------

# data$MEAN_PACK_SIZE
bins <- rbin_manual(data=data,response=NPD_PB,predictor=Fair_share_index,cut_points= c(1,1.5))
bins

# COMMAND ----------

# data$MEAN_PACK_SIZE
bins <- rbin_manual(data=data,response=NPD_PB,predictor=MEAN_PACK_SIZE,cut_points= c(100,150,200,250,300,500))
bins

# COMMAND ----------

data <- data %>% mutate('MEAN_PACK_SIZE_binned'
                        := case_when(
                          data[['MEAN_PACK_SIZE']]<100 ~ -0.41048539,
                          data[['MEAN_PACK_SIZE']]>=100 & (data[['MEAN_PACK_SIZE']]<150) ~ -0.08366341,
                          data[['MEAN_PACK_SIZE']]>=150 & (data[['MEAN_PACK_SIZE']]<200) ~ 0.09556695,
                          data[['MEAN_PACK_SIZE']]>=200 & (data[['MEAN_PACK_SIZE']]<250) ~ 0.19565041,
                          data[['MEAN_PACK_SIZE']]>=250 & (data[['MEAN_PACK_SIZE']]<300) ~ -0.36396538,
                          data[['MEAN_PACK_SIZE']]>=300 & (data[['MEAN_PACK_SIZE']]<500) ~ 0.20855382,
                          data[['MEAN_PACK_SIZE']]>=500 ~ 0.81468962
                          ))

train_reg <- train_reg %>% mutate('MEAN_PACK_SIZE_binned'
                        := case_when(
                          train_reg[['MEAN_PACK_SIZE']]<100 ~ -0.41048539,
                          train_reg[['MEAN_PACK_SIZE']]>=100 & (train_reg[['MEAN_PACK_SIZE']]<150) ~ -0.08366341,
                          train_reg[['MEAN_PACK_SIZE']]>=150 & (train_reg[['MEAN_PACK_SIZE']]<200) ~ 0.09556695,
                          train_reg[['MEAN_PACK_SIZE']]>=200 & (train_reg[['MEAN_PACK_SIZE']]<250) ~ 0.19565041,
                          train_reg[['MEAN_PACK_SIZE']]>=250 & (train_reg[['MEAN_PACK_SIZE']]<300) ~ -0.36396538,
                          train_reg[['MEAN_PACK_SIZE']]>=300 & (train_reg[['MEAN_PACK_SIZE']]<500) ~ 0.20855382,
                          train_reg[['MEAN_PACK_SIZE']]>=500 ~ 0.81468962
                          ))


test_reg <- test_reg %>% mutate('MEAN_PACK_SIZE_binned'
                        := case_when(
                          test_reg[['MEAN_PACK_SIZE']]<100 ~ -0.41048539,
                          test_reg[['MEAN_PACK_SIZE']]>=100 & (test_reg[['MEAN_PACK_SIZE']]<150) ~ -0.08366341,
                          test_reg[['MEAN_PACK_SIZE']]>=150 & (test_reg[['MEAN_PACK_SIZE']]<200) ~ 0.09556695,
                          test_reg[['MEAN_PACK_SIZE']]>=200 & (test_reg[['MEAN_PACK_SIZE']]<250) ~ 0.19565041,
                          test_reg[['MEAN_PACK_SIZE']]>=250 & (test_reg[['MEAN_PACK_SIZE']]<300) ~ -0.36396538,
                          test_reg[['MEAN_PACK_SIZE']]>=300 & (test_reg[['MEAN_PACK_SIZE']]<500) ~ 0.20855382,
                          test_reg[['MEAN_PACK_SIZE']]>=500 ~ 0.81468962
                          ))

# COMMAND ----------

# data <- data %>% mutate('MEAN_PACK_SIZE_binned'
#                         := case_when(
#                           data[['MEAN_PACK_SIZE']]<75 ~ -0.9881197,
#                           data[['MEAN_PACK_SIZE']]>=75 & (data[['MEAN_PACK_SIZE']]<220) ~ 0.1021244,
#                           data[['MEAN_PACK_SIZE']]>=220 ~ 0.2344034
                          
#                           ))

# train_reg <- train_reg %>% mutate('MEAN_PACK_SIZE_binned'
#                         := case_when(
#                           train_reg[['MEAN_PACK_SIZE']]<75 ~ -0.9881197,
#                           train_reg[['MEAN_PACK_SIZE']]>=75 & (train_reg[['MEAN_PACK_SIZE']]<220) ~ 0.1021244,
#                           train_reg[['MEAN_PACK_SIZE']]>=220 ~ 0.2344034
#                           ))

# test_reg <- test_reg %>% mutate('MEAN_PACK_SIZE_binned'
#                         := case_when(
#                           test_reg[['MEAN_PACK_SIZE']]<75 ~ -0.9881197,
#                           test_reg[['MEAN_PACK_SIZE']]>=75 & (test_reg[['MEAN_PACK_SIZE']]<220) ~ 0.1021244,
#                           test_reg[['MEAN_PACK_SIZE']]>=220 ~ 0.2344034,
#                           ))


# COMMAND ----------

# selected_features<-c(
# 'NPD_TDP_relative_to_PB_TDP_h1h2_binned',
# 'No_of_core_products_binned',
# 'PB_TDP_relative_to_COMP_TDP_1_binned',
# 'No_of_Innovations_binned',
# 'Velocity_Core_Product_binned',
# 'MEAN_PACK_SIZE_binned',
# 'Fair_share_index_binned',
# 'Change_in_PB_TDP_vs_comp2_h1h2_binned'
# 'EM_vs_DM_EM_binned'


# )
selected_features<-paste0(iv_based_features,"_binned")
selected_features

# COMMAND ----------

selected_features<-selected_features[!selected_features %in% c('H1H2_GROWTH_PARENT_TDP_binned','H1H2_GROWTH_NEW_LAUNCH_TDP_binned')]
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

# bin_summary<-data%>%group_by(Brand_value_share_binned)%>%summarise(
#                                                     no_of_NPDs=n(),
#                                                     goods=sum(NPD_PB),
#                                                     bads=no_of_NPDs-goods,
#                                                     perc_goods=goods/no_of_NPDs,
#                                                     perc_bads=bads/no_of_NPDs)
# display(as.data.frame(bin_summary))

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

display(get_var_bins(data,'Price_Index'))

# COMMAND ----------

display(get_var_bins(data,'MEAN_PACK_SIZE'))

# COMMAND ----------

display(as.data.frame(data%>%group_by(MEAN_PACK_SIZE_binned)
                             %>%summarise(no_of_NPDs=n(),
                                          goods=sum(NPD_PB),
                                          bads=no_of_NPDs-goods,
                                          perc_goods=goods/no_of_NPDs,
                                          perc_bads=bads/no_of_NPDs)))

# COMMAND ----------

display(as.data.frame(IV_5_bins$Tables$MEAN_PACK_SIZE))

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
# Model accuracy
# accuracy<-mean(predicted.classes == train_reg$NPD_PB)
# print(accuracy)

pred_tab <- table(predicted.classes, train_reg$NPD_PB)


# # ROC-AUC Curve
# ROCPred <- prediction(predict_reg, train_reg$NPD_PB) 
# ROCPer <- performance(ROCPred, measure = "tpr", 
#                              x.measure = "fpr")
   
# auc <- performance(ROCPred, measure = "auc")
# auc <- auc@y.values[[1]]
# print(auc)
print(auc(train_reg$NPD_PB, predict_reg))
confusionMatrix(pred_tab)

# COMMAND ----------

 ks.test(predict_reg, train_reg$NPD_PB)

# COMMAND ----------

# Test metrics

predict_reg <- predict(logistic_model, 
                       test_reg, type = "response")
predict_reg  
   
# Changing probabilities
predicted.classes <- ifelse(predict_reg > 0.5, 1, 0)
# Model accuracy
# accuracy<-mean(predicted.classes == train_reg$NPD_PB)
# print(accuracy)

pred_tab <- table(predicted.classes, test_reg$NPD_PB)


# # ROC-AUC Curve
# ROCPred <- prediction(predict_reg, train_reg$NPD_PB) 
# ROCPer <- performance(ROCPred, measure = "tpr", 
#                              x.measure = "fpr")
   
# auc <- performance(ROCPred, measure = "auc")
# auc <- auc@y.values[[1]]
# print(auc)
print(auc(test_reg$NPD_PB, predict_reg))
confusionMatrix(pred_tab)

# COMMAND ----------

 ks.test(predict_reg, test_reg$NPD_PB)

# COMMAND ----------

# MAGIC %md ######Cross validation

# COMMAND ----------

col_list<-append(selected_features,'NPD_PB')

# COMMAND ----------

# MAGIC %md ######Overall

# COMMAND ----------

logistic_model_overall <- glm(model_eq, 
                      data = data, 
                      family = "binomial")

# COMMAND ----------

predict_reg <- predict(logistic_model_overall, 
                       data, type = "response")
predict_reg  
   
# Changing probabilities
predicted.classes <- ifelse(predict_reg > 0.5, 1, 0)
# Model accuracy
# accuracy<-mean(predicted.classes == train_reg$NPD_PB)
# print(accuracy)

pred_tab <- table(predicted.classes, data$NPD_PB)


# # ROC-AUC Curve
# ROCPred <- prediction(predict_reg, train_reg$NPD_PB) 
# ROCPer <- performance(ROCPred, measure = "tpr", 
#                              x.measure = "fpr")
   
# auc <- performance(ROCPred, measure = "auc")
# auc <- auc@y.values[[1]]
# print(auc)
print(auc(data$NPD_PB, predict_reg))
confusionMatrix(pred_tab)

# COMMAND ----------

data$predict_reg<- predict_reg

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

display(data)

# COMMAND ----------

display(data[c('NPD_PB','NPD_PB_chk','NPD_PB_avg','predict_reg')])

# COMMAND ----------

