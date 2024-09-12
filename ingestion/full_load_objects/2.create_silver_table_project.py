# Databricks notebook source
# MAGIC %md 
# MAGIC # Create silver table - project 

# COMMAND ----------

# MAGIC %run "../../setup/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -Read Project from Bronze using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType

# COMMAND ----------

df = spark.read.table("<your-catalog>.bronze.project")

# COMMAND ----------

display(df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean data

# COMMAND ----------

# Remove duplicate and nulls in the primary column: Id
semi_clean_df = remove_duplicates_and_nulls(df, "Id")

# COMMAND ----------

from pyspark.sql.functions import col

# Cast createdDate into DateType
clean_df = semi_clean_df.withColumn("CreatedDate", col("CreatedDate").cast(DateType()))

# COMMAND ----------

# Select and rename columns
project_df = clean_df.select(col("Id").alias("id"),
                            col("Name").alias("name"),
                            col("Account__c").alias("account_id"),
                            col("CurrencyIsoCode").alias("currency_code"),
                            col("CreatedDate").alias("created_date"),
                            col("Project_Status__c").alias("project_status"),
                            col("Completion_Date__c").alias("completion_date"),
                            col("Country__c").alias("country"),
                            col("Business_Region__c").alias("business_region"),
                            col("Project_Type__c").alias("project_type"),
                            col("Parent_Opportunity__c").alias("parent_opportunity"),
                            col("Territory__c").alias("territory"),
                            col("Project_Alignment__c").alias("project_alignment"),
                            col("Project_Account_Region__c").alias("project_account_region"),
                            col("Project_Elekta_BU__c").alias("project_elekta_bu"),
                            col("Project_Elekta_Territory__c").alias("project_elekta_territory"),
                            col("Project_Legal_Country__c").alias("project_legal_country"),
                            col("New_Account_Region__c").alias("new_account_region"),
                            col("Project_Manager_Name__c").alias("project_manager_name")
                            )

# COMMAND ----------

# Add ingestion column
final_df = add_ingestion_date(project_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG <your-catalog>

# COMMAND ----------

final_df.write.mode("overwrite").saveAsTable("silver.project")

# COMMAND ----------

dbutils.notebook.exit("Success")
