# Databricks notebook source
# MAGIC %md 
# MAGIC # Create silver table - account

# COMMAND ----------

# MAGIC %run "../../setup/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -Read the csv file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType

# COMMAND ----------

df = spark.read.table("<your-catalog>.bronze.account")

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
account_df = clean_df.select(col("Id").alias("id"),
                            col("Name").alias("name"),
                            col("Type").alias("type"),
                            col("BillingCountry").alias("country"),
                            col("CurrencyIsoCode").alias("currency_code"),
                            col("CreatedDate").alias("created_date"),
                            col("Active__c").alias("active"),
                            col("Account_Region__c").alias("region"),
                            col("Customer_Type__c").alias("customer_type"),
                            col("Country").alias("continent")
                            )

# COMMAND ----------

# Add ingestion column
final_df = add_ingestion_date(account_df)

# COMMAND ----------

final_df.write.mode("overwrite").saveAsTable("<your-catalog>.silver.account")

# COMMAND ----------

dbutils.notebook.exit("Success")
