# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Activities (Unity Catalog Version)
# MAGIC * This is the first notebook that should be run to setup the schema, libraries, data etc.
# MAGIC * It creates a volume named 'resource' under the 'workspace.default' schema for the project.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Libraries

# COMMAND ----------

# %pip install git+https://github.com/mlflow/mlflow-export-import/#egg=mlflow-export-import databricks-cli tabulate geopy
%pip install tabulate geopy

# COMMAND ----------

import pandas as pd
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Configuration
# MAGIC * Using a static configuration as requested.
# MAGIC * Catalog: `workspace`
# MAGIC * Schema: `default`
# MAGIC * Volume: `resource`

# COMMAND ----------

# Static configuration for Unity Catalog assets
catalog_name = "workspace"
schema_name = "default"
volume_name = "resource"
username = spark.sql("SELECT current_user()").collect()[0][0]


# Define the base path for the volume. All data and models will live here.
volume_base_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Volume Path: {volume_base_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration (Unity Catalog)

# COMMAND ----------

config = {
  'catalog_name': catalog_name,
  'schema_name': schema_name,
  'volume_path': volume_base_path,
  'Telematics_path': f'{volume_base_path}/data_sources/Telematics',
  'Policy_path': f'{volume_base_path}/data_sources/Policy',
  'Claims_path': f'{volume_base_path}/data_sources/Claims',
  'Accidents_path': f'{volume_base_path}/data_sources/Accidents',
  'Accident_metadata_path': f'{volume_base_path}/data_sources/Accident_metadata',
  'prediction_path': f'{volume_base_path}/predictions_delta',
  
  # --- THIS IS THE FIX ---
  # The input directory is 'severity_model', not 'severity_model/Model'
  'model_input_dir' : f'{volume_base_path}/model',
  
  'image_dir' : f'{volume_base_path}/images',
  'damage_severity_model_experiment' : f'/Users/{username}/car_damage_severity',
  'damage_severity_model_name'   :  f'{catalog_name}.{schema_name}.damage_severity_model', # UC Model Name
  'sql_warehouse_id' : ""
}

def getParam(s):
  return config[s]

# Make the configuration accessible in SQL and other notebooks.
spark.createDataFrame(pd.DataFrame(config, index=[0])).createOrReplaceTempView('smart_claims_config')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tear down & Setup (Schema & Volume)

# COMMAND ----------

def tear_down():
  print(f"Dropping and cleaning volume '{volume_name}'...")
  # We don't drop the schema, just clean the contents of the volume for a fresh start
  try:
    dbutils.fs.rm(volume_base_path, recurse=True)
  except Exception as e:
    print(f"Volume was already empty or did not exist: {e}")

def setup():
  print(f"Creating volume '{volume_name}' if it does not exist...")
  spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
  
  print("Creating necessary subdirectories inside the volume...")
  dbutils.fs.mkdirs(f"{volume_base_path}/data_sources")
  dbutils.fs.mkdirs(f"{volume_base_path}/model")
  dbutils.fs.mkdirs(f"{volume_base_path}/images")


# Run the setup process
#tear_down()
setup()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download and Ingest Data into Volume

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Download model & images to the driver's local disk

# COMMAND ----------

# MAGIC %sh -e
# MAGIC # Download to the driver's temporary local disk, not DBFS
# MAGIC cd /tmp
# MAGIC wget -O resource.zip https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource_bundle.zip
# MAGIC unzip -o resource.zip -d resource_bundle/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Copy files from driver's disk into the Unity Catalog Volume

# COMMAND ----------

# # The source path must start with 'file:/' to indicate it's on the local driver filesystem.
# # The destination is the UC Volume path.
# print("Copying files from driver to UC Volume...")
# dbutils.fs.cp("file:/tmp/resource_bundle/Model", getParam("model_input_dir"), recurse=True)
# dbutils.fs.cp("file:/tmp/resource_bundle/images", getParam("image_dir"), recurse=True)
# dbutils.fs.cp("file:/tmp/resource_bundle/images", getParam("Accidents_path"), recurse=True)
# dbutils.fs.cp("file:/tmp/resource_bundle/image_metadata", getParam("Accident_metadata_path"), recurse=True)
# dbutils.fs.cp("file:/tmp/resource_bundle/Telematics", getParam("Telematics_path"), recurse=True)
# dbutils.fs.cp("file:/tmp/resource_bundle/Policy", getParam("Policy_path"), recurse=True)
# dbutils.fs.cp("file:/tmp/resource_bundle/Claims", getParam("Claims_path"), recurse=True)
# print("File copy complete.")
