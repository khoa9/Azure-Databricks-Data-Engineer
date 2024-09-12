# Databricks notebook source
# MAGIC %md
# MAGIC # Run all ingestion notebooks to create bronze and silver table

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2020-02-01")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Full Load Notebooks

# COMMAND ----------

v_result = dbutils.notebook.run("full_load_objects/0.create_bronze_tables", 0, {"p_file_date":v_file_date})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("full_load_objects/1.create_silver_table_account", 0, {"p_file_date":v_file_date})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("full_load_objects/2.create_silver_table_project", 0, {"p_file_date":v_file_date})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("full_load_objects/3.create_silver_table_quote", 0, {"p_file_date":v_file_date})

# COMMAND ----------

print(v_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Incremental Load Notebooks

# COMMAND ----------

v_result = dbutils.notebook.run("incremental_objects/1.create_bronze_silver_tables_opportunity", 0, {"p_file_date":v_file_date})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("incremental_objects/2.create_bronze_silver_tables_opportunitylineitem", 0, {"p_file_date":v_file_date})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("incremental_objects/3.create_bronze_silver_tables_product", 0, {"p_file_date":v_file_date})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("incremental_objects/4.create_bronze_silver_tables_servicecontract", 0, {"p_file_date":v_file_date})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("incremental_objects/5.create_bronze_silver_tables_quoteline", 0, {"p_file_date":v_file_date})

# COMMAND ----------

print(v_result)
