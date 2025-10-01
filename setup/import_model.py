# Databricks notebook source
# MAGIC %md
# MAGIC # Register Model into Unity Catalog with a Signature
# MAGIC * This final version creates the required model signature before logging the model.
# MAGIC * This is a mandatory step for all models in Unity Catalog and resolves the final error
# MAGIC * by using a custom model wrapper that is compatible with the Databricks Free Tier.

# COMMAND ----------

# MAGIC %run ./initialize

# COMMAND ----------

import mlflow
import os
import cloudpickle
import pandas as pd
from mlflow.models.signature import infer_signature

# --- 1. Define the Custom Model Wrapper ---
# This class acts as a container for your real model.
# It tells MLflow how to load the model from a UC Volume in a secure way.

class UCVolumeModelWrapper(mlflow.pyfunc.PythonModel):
    
    def load_context(self, context):
        """
        This method is called once when the model is loaded for inference.
        It safely loads the actual model from the path stored in the artifacts.
        """
        # The model path is passed as an artifact. We read it from the local path where MLflow places it.
        with open(context.artifacts["model_path_file"], "r") as f:
            volume_model_path = f.read()
        
        # We must use the /dbfs FUSE mount to access the volume's file content
        dbfs_model_path = "/dbfs" + os.path.join(volume_model_path, "artifacts/pipeline/")
        
        # Load the actual serialized model from the pickle file inside the volume
        with open(os.path.join(dbfs_model_path, "python_model.pkl"), "rb") as f:
            self.model = cloudpickle.load(f)

    def predict(self, context, model_input):
        """
        This method is called for inference. It passes the input to the real model.
        """
        return self.model.predict(model_input)

# COMMAND ----------

# --- 2. Get Configuration ---

model_name = getParam("damage_severity_model_name")
experiment_name = getParam("damage_severity_model_experiment")
source_model_dir = getParam("model_input_dir")

print(f"Target Unity Catalog Model Name: {model_name}")
print(f"Target Experiment Name: {experiment_name}")
print(f"Source Model Directory (on Volume): {source_model_dir}")


# --- 3. Configure MLflow Client ---

mlflow.set_registry_uri('databricks-uc')
mlflow.set_experiment(experiment_name)


# --- 4. Create an Artifact to Store the Model's Volume Path ---
# We write the model's location to a file to be logged as an artifact.

temp_dir = "/tmp/model_path_artifact"
os.makedirs(temp_dir, exist_ok=True)
path_file = os.path.join(temp_dir, "model_path.txt")

with open(path_file, "w") as f:
    f.write(source_model_dir)


# --- 5. Define the Model Signature ---
# Unity Catalog requires all models to have a signature.
# We create sample data to infer the input and output schema.

# The model expects a pandas DataFrame with a column named 'content' containing image bytes.
input_data = pd.DataFrame([{"content": b""}])

# The model's output is a prediction, which we can represent as a float.
output_data = pd.DataFrame([{"prediction": 0.0}])

# Infer the signature from these samples.
signature = infer_signature(input_data, output_data)


# --- 6. Log and Register the Custom Wrapper Model with the Signature ---

print("Starting MLflow run to log and register the model wrapper...")
with mlflow.start_run(run_name="Register Pretrained Model with Signature") as run:
    
    # Log the custom wrapper model, providing the signature, a sample input, and the artifact.
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model_wrapper",
        python_model=UCVolumeModelWrapper(),
        artifacts={"model_path_file": path_file},
        signature=signature,
        input_example=input_data
    )
    
    # Register the newly logged wrapper model into Unity Catalog.
    print(f"Registering model '{model_name}' from URI: '{model_info.model_uri}'")
    registered_model = mlflow.register_model(
        model_uri=model_info.model_uri,
        name=model_name
    )

print("Model registration complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Model Link

# COMMAND ----------
host_name = spark.conf.get("spark.databricks.workspaceUrl")
if host_name:
    uri = f"https://{host_name}/explore/data/models/{model_name}"
    displayHTML(f'<b>Registered Model URI:</b> <a href="{uri}">{uri}</a>')
