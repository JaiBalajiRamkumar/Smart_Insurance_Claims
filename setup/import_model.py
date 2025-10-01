# Databricks notebook source
# MAGIC %md
# MAGIC # Import Model into Unity Catalog
# MAGIC * The pretrained model was copied to a UC Volume during setup.
# MAGIC * This notebook imports the model from the Volume into the Model Registry in Unity Catalog.

# COMMAND ----------

# MAGIC %run ./initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLFlow Utility Functions

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()
host_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName")

def display_experiment_uri(experiment_name):
    """Displays a clickable link to an MLflow Experiment."""
    if host_name:
        try:
            experiment_id = client.get_experiment_by_name(experiment_name).experiment_id
            uri = f"https://{host_name}/#mlflow/experiments/{experiment_id}"
            displayHTML(f'<b>Experiment URI:</b> <a href="{uri}">{uri}</a>')
        except Exception as e:
            print(f"Could not find experiment '{experiment_name}'. Error: {e}")

def display_registered_model_uri(model_name):
    """Displays a clickable link to a Registered Model in UC."""
    if host_name:
        # For UC models, the link format is to the Catalog Explorer
        uri = f"https://{host_name}/explore/data/models/{model_name}"
        displayHTML(f'<b>Registered Model URI:</b> <a href="{uri}">{uri}</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Metadata (from UC Config)

# COMMAND ----------

# The model name is a 3-level UC name: <catalog>.<schema>.<model>
model_name = getParam("damage_severity_model_name")
print(f"Unity Catalog Model Name: {model_name}")

# The experiment where the imported run will be created
experiment_name = getParam("damage_severity_model_experiment")
print(f"Experiment Name: {experiment_name}")

# The source directory for the model files, now on a UC Volume
input_dir = getParam("model_input_dir")
# mlflow-export-import needs a 'dbfs:/' prefix for volume paths
input_dir_dbfs = "dbfs:" + input_dir
print(f"Model Input Directory (on Volume): {input_dir_dbfs}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Model into Unity Catalog Registry

# COMMAND ----------

from mlflow_export_import.model.import_model import ModelImporter
import mlflow

# Set the registry URI to 'databricks-uc' to work with Unity Catalog
mlflow.set_registry_uri('databricks-uc')

importer = ModelImporter(mlflow.tracking.MlflowClient())

# Import the model from the Volume path into the UC model registry
importer.import_model(
    model_name=model_name,
    input_dir=input_dir_dbfs,
    experiment_name=experiment_name,
    delete_model=True, # Deletes and overwrites the UC model if it already exists
    verbose=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Model in MLflow

# COMMAND ----------

display_registered_model_uri(model_name)

# COMMAND ----------

display_experiment_uri(experiment_name)
