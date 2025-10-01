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
    F.concat(
        F.col("BOROUGH"), 
        F.lit(", "), 
        F.col("ZIP_CODE").cast("double").cast("int").cast("string")
    )
)

print("Address column created successfully.")
display(policy_claim_with_address.limit(5))

# COMMAND ----------

# Get the unique addresses into a Pandas DataFrame
print("Extracting unique addresses...")
policy_claim_with_address_pd = policy_claim_with_address.where(F.col("address").isNotNull()).toPandas()
unique_address_pd = pd.DataFrame(policy_claim_with_address_pd.address.unique(), columns=["address"])

# Convert back to a Spark DataFrame to write it to the volume
unique_address_df = spark.createDataFrame(unique_address_pd)

# Define the output path in your volume
output_path = "/Volumes/workspace/default/resource/addresses_to_geocode_csv"

# Write the addresses to a single CSV file
unique_address_df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(output_path)

print(f"\nSuccessfully saved {unique_address_df.count()} unique addresses to the following location:")
print(output_path)
print("\nNEXT STEP: Go to the Catalog Explorer, navigate to this folder, download the CSV file, and proceed to Part 2.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🛑 STOP: Perform Local Geocoding (Part 2)
# MAGIC 
# MAGIC You must now geocode the data on your local computer because Databricks Free Tier blocks internet access.
# MAGIC 
# MAGIC 1.  **Download the CSV file** from the location printed above (`/Volumes/workspace/default/resource/addresses_to_geocode_csv/`).
# MAGIC 2.  **Save the Python script below** as `geocode_script.py` in the *same folder* as your downloaded CSV file.
# MAGIC 3.  **Install libraries** on your local machine by opening a terminal and running: `pip install pandas geopy tqdm`
# MAGIC 4.  **Run the script** from your terminal: `python geocode_script.py`
# MAGIC 
# MAGIC This will create a new file named `geocoded_addresses.csv`.
# MAGIC 
# MAGIC 5.  **Upload `geocoded_addresses.csv`** back into your Databricks volume at `/Volumes/workspace/default/resource/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Local Python Script (Save as `geocode_script.py`)
# MAGIC ```python
# MAGIC # geocode_script.py
# MAGIC import pandas as pd
# MAGIC from geopy.geocoders import Nominatim
# MAGIC from tqdm import tqdm
# MAGIC import os
# MAGIC 
# MAGIC # Configure the geolocator
# MAGIC geolocator = Nominatim(user_agent="databricks_geocoder_local")
# MAGIC 
# MAGIC def get_lat_long(address):
# MAGIC     """Safely geocodes an address, returning (None, None) on failure."""
# MAGIC     try:
# MAGIC         location = geolocator.geocode(address, timeout=10)
# MAGIC         if location:
# MAGIC             return location.latitude, location.longitude
# MAGIC     except Exception as e:
# MAGIC         print(f"Error geocoding '{address}': {e}")
# MAGIC     return None, None
# MAGIC 
# MAGIC # Find the correct CSV file to read
# MAGIC csv_filename = None
# MAGIC for f in os.listdir('.'):
# MAGIC     if f.endswith('.csv') and f.startswith('part-'):
# MAGIC         csv_filename = f
# MAGIC         break
# MAGIC 
# MAGIC if not csv_filename:
# MAGIC     print("ERROR: Could not find the downloaded CSV file starting with 'part-'. Make sure it's in the same directory.")
# MAGIC     exit()
# MAGIC 
# MAGIC print(f"Reading data from '{csv_filename}'...")
# MAGIC df = pd.read_csv(csv_filename)
# MAGIC 
# MAGIC # Use tqdm to show a progress bar
# MAGIC tqdm.pandas(desc="Geocoding addresses")
# MAGIC 
# MAGIC # Apply the geocoding function to each address
# MAGIC df[['latitude', 'longitude']] = df['address'].progress_apply(
# MAGIC     lambda x: pd.Series(get_lat_long(x))
# MAGIC )
# MAGIC 
# MAGIC # Save the results to a new CSV file
# MAGIC df.to_csv("geocoded_addresses.csv", index=False)
# MAGIC 
# MAGIC print("\nProcessing complete. Results saved to 'geocoded_addresses.csv'.")
# MAGIC print("Please upload this file back to your Databricks volume.")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Import Geocoded Data and Create Final Table
# MAGIC 
# MAGIC After you have successfully uploaded `geocoded_addresses.csv` to your volume, run the cell below to create the final `silver_claim_policy_location` table.

# COMMAND ----------

# Path to the uploaded file in your volume
geocoded_path = "/Volumes/workspace/default/resource/geocoded_addresses.csv"

try:
    # Read the geocoded data into a Spark DataFrame
    geocoded_df = spark.read.format("csv").option("header", "true").load(geocoded_path)
    
    # Join the geocoded data back to your original DataFrame
    silver_claim_policy_location = policy_claim_with_address.join(
        geocoded_df,
        on="address",
        how="left"
    )

    # Save the final result as a managed table
    silver_claim_policy_location.write.mode("overwrite").saveAsTable("silver_claim_policy_location")

    print("Successfully created the final table 'silver_claim_policy_location' with geocoded data.")
    display(spark.table("silver_claim_policy_location"))

except Exception as e:
    print("An error occurred. Did you upload 'geocoded_addresses.csv' to your volume yet?")
    print(f"Error details: {e}")
