# Databricks notebook source
# MAGIC %md
# MAGIC * This file is included in all other Notebooks to get common definitions/configurations for Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration Metadata (from UC Setup)

# COMMAND ----------

# This config view is created in the main 'setup.py' notebook.
# This ensures all notebooks use the same UC paths and names.
try:
    config_df = spark.sql("SELECT * FROM smart_claims_config")
    config = config_df.toPandas().to_dict(orient='records')[0]
except:
    raise Exception("Could not find 'smart_claims_config'. Please run the main 'setup.py' notebook first.")

def getParam(s):
  return config[s]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use Catalog and Schema

# COMMAND ----------

# Ensure the session is using the correct catalog and schema for all subsequent operations.
catalog = getParam("catalog_name")
schema = getParam("schema_name")

print(f"Using catalog: '{catalog}' and schema: '{schema}'")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
