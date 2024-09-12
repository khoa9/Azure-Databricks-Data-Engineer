# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS <your-catalog>.bronze.account;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS <your-catalog>.bronze.account
# MAGIC   USING CSV
# MAGIC   OPTIONS (
# MAGIC     path 'abfss://bronze@<your-data-lake>.dfs.core.windows.net/account.csv',
# MAGIC     header 'true',
# MAGIC     inferSchema 'true'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS <your-catalog>.bronze.project;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS <your-catalog>.bronze.project
# MAGIC   USING CSV
# MAGIC   OPTIONS (
# MAGIC     path 'abfss://bronze@<your-data-lake>.dfs.core.windows.net/project.csv',
# MAGIC     header 'true',
# MAGIC     inferSchema 'true'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS <your-catalog>.bronze.quote_1;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS <your-catalog>.bronze.quote_1
# MAGIC   USING CSV
# MAGIC   OPTIONS (
# MAGIC     path 'abfss://bronze@<your-data-lake>.dfs.core.windows.net/quote.csv',
# MAGIC     header 'true',
# MAGIC     inferSchema 'true'
# MAGIC   );

# COMMAND ----------

dbutils.notebook.exit("Success")
