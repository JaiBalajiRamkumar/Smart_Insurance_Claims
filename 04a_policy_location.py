# Databricks notebook source
# MAGIC %md 
# MAGIC This notebook is available at https://github.com/databricks-industry-solutions/smart-claims

# COMMAND ----------

# MAGIC %md
# MAGIC # Location
# MAGIC * Uses geopy lib to add lat/long for dashboard display
# MAGIC * This notebook is divided into parts to work around network restrictions in the Databricks Free Tier.
# MAGIC * **Part 1:** Exports the unique addresses to a CSV file.
# MAGIC * **Part 2:** Provides a local script to geocode the exported file.
# MAGIC * **Part 3:** Reads the geocoded file and creates the final table.
# MAGIC * **Input Table:** silver_claim_policy
# MAGIC * **Output Table:** silver_claim_policy_location

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Prepare and Export Addresses for Geocoding
# MAGIC 
# MAGIC Run all the cells in this section. This will create a CSV file in your volume for you to download.

# COMMAND ----------

# Read the input table
policy_claim_df = spark.table("silver_claim_policy")

# Create the 'address' column and handle the ZIP code conversion correctly
# First cast to double to handle decimals, then to int, then to string.
policy_claim_with_address = policy_claim_df.withColumn(
    "address", 
    F.
