# Databricks notebook source
def remove_duplicates_and_nulls(df, column_name):
    # Drop rows where the specified column is null
    df_cleaned = df.dropna(subset=[column_name])
    
    # Drop duplicate rows based on the specified column
    df_cleaned = df_cleaned.dropDuplicates([column_name])
    
    return df_cleaned

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

from pyspark.sql.functions import lit
def add_file_date(input_df, v_file_date):
    output_df = input_df.withColumn("file_date",lit(v_file_date))
    return output_df

# COMMAND ----------

from delta.tables import DeltaTable

def merge_delta_data(input_df, db_name, table_name, catalog_name, merge_condition):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    if (spark._jsparkSession.catalog().tableExists(f"{catalog_name}.{db_name}.{table_name}")):
        deltaTable = DeltaTable.forName(spark, f"{catalog_name}.{db_name}.{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition)\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    else:
        input_df.write.mode("overwrite").format("delta").saveAsTable(f"{catalog_name}.{db_name}.{table_name}")
