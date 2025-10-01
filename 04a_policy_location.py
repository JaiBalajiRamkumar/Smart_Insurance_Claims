# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/smart-claims

# COMMAND ----------

# MAGIC %md
# MAGIC # Location
# MAGIC * Uses geopy lib to add lat/long for dashboard display
# MAGIC * <b>Input Table: </b> silver_claim_policy
# MAGIC * <b> Output Table: </b> silver_claim_policy_location

# COMMAND ----------

# %run ./setup/initialize

# COMMAND ----------

import pyspark.sql.functions as F


policy_claim_df = spark.table("silver_claim_policy")
display(policy_claim_df)

# COMMAND ----------

policy_claim_with_address = policy_claim_df.withColumn("ZIP_CODE", F.col("ZIP_CODE").cast("double").cast("int")).withColumn("address", F.concat(F.col("BOROUGH"), F.lit(", "), F.col("ZIP_CODE").cast("double").cast("int").cast("string")))
display(policy_claim_with_address)

# COMMAND ----------



zip_codes_path = "/Volumes/workspace/default/resource/zip_lat_long.csv"


# Read the zip code reference data and prepare it for joining
unique_address_df = spark.read.format("csv").option("header", "true").load(zip_codes_path) \
    .select(
        F.col("ZIP").alias("ZIP_CODE"),
        F.col("LAT").alias("latitude"),
        F.col("LNG").alias("longitude")
    )



# COMMAND ----------

# unique_address_df = spark.createDataFrame(unique_address)

policy_claim_lat_long = policy_claim_with_address.join(unique_address_df, on="ZIP_CODE")
display(policy_claim_lat_long)

# COMMAND ----------

policy_claim_lat_long.write.format("delta").mode("append").saveAsTable("silver_claim_policy_location")

# COMMAND ----------


