-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create EXTERNAL LOCATIONS for the following containers and catalog:
-- MAGIC 1. Bronze container - <'bronze_location'>
-- MAGIC 2. Silver container - <'silver_location'>
-- MAGIC 3. Gold container - <'gold_location'>
-- MAGIC 4. Catalog container - <'your-catalog'>
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS <bronze_location>
  URL "abfss://bronze@<your-data-lake>.dfs.core.windows.net/"
  WITH (STORAGE CREDENTIAL `<your-storage-credential-name>`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS <silver_location>
  URL "abfss://silver@<your-data-lake>.dfs.core.windows.net/"
  WITH (STORAGE CREDENTIAL `<your-storage-credential-name>`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS <gold_location>
  URL "abfss://gold@<your-data-lake>.dfs.core.windows.net/"
  WITH (STORAGE CREDENTIAL `<your-storage-credential-name>`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS <your_catalog>
  URL "abfss://<your-catalog>@<your-data-lake>.dfs.core.windows.net/"
  WITH (STORAGE CREDENTIAL `<your-storage-credential-name>`);
