-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create new Catalog and Schemas under the catalog
-- MAGIC 1. Catalog: <your_catalog>
-- MAGIC 2. Schema: bronze
-- MAGIC 3. Schema: silver
-- MAGIC 4. Schema: gold
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE CATALOG IF NOT EXiSTS <your-catalog>
MANAGED LOCATION 'abfss://<your-catalog>@<your-data-lake>.dfs.core.windows.net/';

-- COMMAND ----------

USE CATALOG <your-catalog>

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION 'abfss://bronze@<your-data-lake>.dfs.core.windows.net/';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION 'abfss://silver@<your-data-lake>.dfs.core.windows.net/';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION 'abfss://gold@<your-data-lake>.dfs.core.windows.net/';

-- COMMAND ----------


