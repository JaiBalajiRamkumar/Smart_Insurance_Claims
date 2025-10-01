# Databricks notebook source
# MAGIC %md
# MAGIC # Save Model to a Unity Catalog Table
# MAGIC * This script reads the model's pickle file, encodes it as a base64 string,
# MAGIC * and saves it into a UC table. This is the only reliable way to persist and reuse a model
# MAGIC * on the Databricks Free Tier, as it avoids all file system access errors.

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

import base64
import os
import pandas as pd

# --- 1. Get Configuration ---
source_model_dir = getParam("model_input_dir")
model_storage_table = "smart_claims_model_storage" # Name of the table to store the model

# --- 2. Construct the path to the model's pickle file ---
# We must use the /dbfs FUSE mount for direct Python file access
model_file_path = f"/dbfs{source_model_dir}/artifacts/pipeline/python_model.pkl"
print(f"Reading model file from: '{model_file_path}'")

# --- 3. Read the model file and encode it as a base64 string ---
try:
    with open(model_file_path, "rb") as f:
        model_bytes = f.read()
    
    # Encode the binary model data into a string that can be stored in a table
    model_base64 = base64.b64encode(model_bytes).decode('utf-8')
    print("Successfully read and encoded the model file.")

except FileNotFoundError:
    raise Exception(f"ERROR: Model file not found at '{model_file_path}'. Please ensure the setup script ran correctly and the path is correct.")


# --- 4. Save the encoded model to a Spark table ---
# Create a DataFrame containing the single encoded model string
model_df = spark.createDataFrame([(1, model_base64)], ["id", "model_base64"])

# Save the DataFrame as a managed table in Unity Catalog
model_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(model_storage_table)

print(f"Successfully saved the encoded model to the table: '{model_storage_table}'")
