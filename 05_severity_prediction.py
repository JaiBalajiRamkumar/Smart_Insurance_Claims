# Databricks notebook source
# MAGIC %md 
# MAGIC This notebook is available at https://github.com/databricks-industry-solutions/smart-claims

# COMMAND ----------

# MAGIC %md
# MAGIC # Load claim images and score using a direct model load from a UC table
# MAGIC * This notebook loads the model by reading its encoded data from a Unity Catalog table,
# MAGIC * which is the final and correct solution for the Databricks Free Tier.

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

import os
import cloudpickle
import base64
import pandas as pd
from pyspark.sql.functions import col, split, size

# COMMAND ----------

# Load metadata and images
metadata_df = spark.read.format("csv").option("header", "true").load(getParam("Accident_metadata_path"))
acc_df = spark.read.format('binaryFile').load(getParam("Accidents_path"))

# Prepare accident DataFrame
split_col=split(acc_df['path'],'/')
accident_df_id = acc_df.withColumn('image_name1',split_col.getItem(size(split_col) - 1))

# Create Bronze Accident Table
accident_bronze_df = metadata_df.join(accident_df_id,accident_df_id.image_name1==metadata_df.image_name,"leftouter").drop("image_name1")
accident_bronze_df.write.mode("overwrite").format("delta").saveAsTable("bronze_accident")

# Convert to Pandas
accident_df = accident_df_id.toPandas()

# COMMAND ----------

# --- THIS IS THE FIX ---
# Load the model directly from the Unity Catalog table where it was stored.

model_storage_table = "smart_claims_model_storage"
print(f"Loading encoded model from table: '{model_storage_table}'")

# 1. Read the encoded model string from the table
model_base64 = spark.table(model_storage_table).select("model_base64").first()[0]

# 2. Decode the base64 string back into bytes
model_bytes = base64.b64decode(model_base64)

# 3. Use cloudpickle to load the model object from the bytes
loaded_model = cloudpickle.loads(model_bytes)

print("Model loaded successfully from the table.")

# COMMAND ----------

# Predict severity using the loaded model
accident_df['severity'] = loaded_model.predict(accident_df['content'])

# COMMAND ----------

# Convert back to Spark DataFrame
accident_df_spark = spark.createDataFrame(accident_df)

# COMMAND ----------

# Final Join
accident_metadata_df = metadata_df.join(accident_df_spark,accident_df_spark.image_name1==metadata_df.image_name,"leftouter").drop("image_name1","content","path")

# COMMAND ----------

# Display Final Result
display(accident_metadata_df)
