# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Activities (Unity Catalog Version)
# MAGIC * This is the first notebook that should be run to setup the schema, libraries, data etc.
# MAGIC * It creates a dedicated schema and a volume in Unity Catalog for the project.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Libraries

# COMMAND ----------

# MAGIC %pip install geopy git+https://github.com/amesar/mlflow-export-import/#egg=mlflow-export-import

# COMMAND ----------

import re
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema & Volume Path Names (Unity Catalog)

# COMMAND ----------

# Get username for creating a unique schema, which is a best practice.
username = spark.sql("SELECT current_user()").collect()[0][0]
user_firstname = username.split('@')[0].replace('.', '_')

# All project assets will be stored in this dedicated schema and volume.
catalog_name = "workspace" # Or your preferred catalog, e.g., "main", "dev"
schema_name = f'smart_claims_{user_firstname}'
volume_name = "smart_claims_resources"

# Define the base path for the volume. All data and models will live here.
volume_base_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Volume Path: {volume_base_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration (Unity Catalog)

# COMMAND ----------

# Configuration now uses the UC Volume path for all file locations.
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
  'model_input_dir' : f'{volume_base_path}/severity_model/Model',
  'image_dir' : f'{volume_base_path}/images',
  'damage_severity_model_experiment' : f'/Users/{username}/car_damage_severity',
  'damage_severity_model_name'   :  f'{catalog_name}.{schema_name}.damage_severity_model', # UC Model Name
  'sql_warehouse_id' : ""
}

def getParam(s):
  return config[s]

# Make the configuration accessible in SQL.
spark.createDataFrame(pd.DataFrame(config, index=[0])).createOrReplaceTempView('smart_claims_config')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tear down & Setup (Schema & Volume)

# COMMAND ----------

def tear_down():
  print(f"Dropping schema '{schema_name}' and all its contents (tables, volumes)...")
  # Dropping the schema also drops the volume and tables inside it.
  spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")

def setup():
  print(f"Creating schema '{schema_name}'...")
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
  spark.sql(f"USE SCHEMA {catalog_name}.{schema_name}")

  print(f"Creating volume '{volume_name}'...")
  spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

  # Create necessary subdirectories within the volume using shell commands
  # Note the use of '/dbfs' prefix when using shell commands to access volumes
  dbfs_volume_path = f"/dbfs{volume_base_path}"
  spark.sql(f"USE CATALOG {catalog_name}") # Ensure the catalog is selected for the session
  dbutils.fs.mkdirs(f"{volume_base_path}/data_sources")
  dbutils.fs.mkdirs(f"{volume_base_path}/severity_model")


# Run the setup process
tear_down()
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

# The source path must start with 'file:/' to indicate it's on the local driver filesystem.
# The destination is the UC Volume path.
print("Copying files from driver to UC Volume...")
dbutils.fs.cp("file:/tmp/resource_bundle/Model", getParam("model_input_dir"), recurse=True)
dbutils.fs.cp("file:/tmp/resource_bundle/images", getParam("image_dir"), recurse=True)
dbutils.fs.cp("file:/tmp/resource_bundle/images", getParam("Accidents_path"), recurse=True)
dbutils.fs.cp("file:/tmp/resource_bundle/image_metadata", getParam("Accident_metadata_path"), recurse=True)
dbutils.fs.cp("file:/tmp/resource_bundle/Telematics", getParam("Telematics_path"), recurse=True)
dbutils.fs.cp("file:/tmp/resource_bundle/Policy", getParam("Policy_path"), recurse=True)
dbutils.fs.cp("file:/tmp/resource_bundle/Claims", getParam("Claims_path"), recurse=True)
print("File copy complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Clean up the driver's temporary files

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/resource.zip /tmp/resource_bundle

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Import model from Volume to MLflow Model Registry (in UC)

# COMMAND ----------

# MAGIC %run ./import_model
