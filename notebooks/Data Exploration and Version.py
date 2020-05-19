# Databricks notebook source
import os
import glob
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.data import DataType
from pyspark.sql.functions import isnan, when, count, col

# COMMAND ----------

# Hide cell for demo as we cannot (?) use db secrets here somehow
sp_auth = ServicePrincipalAuthentication(
  tenant_id="<tenantid>",
  service_principal_id="<service_principal_id>",
  service_principal_password="<service_principal_password>"
)

# COMMAND ----------

dbfs_mnt_processed = "/mnt/processed/"

# COMMAND ----------

# DBTITLE 1,Data Exploration
# MAGIC %md Let take a random sample of our dataset

# COMMAND ----------

# DBTITLE 0,Data Exploration
# Read parquet files into Spark DataFrame
df = spark.read.parquet(dbfs_mnt_processed + '/redditcomments/')

# Get a sampled subset of data frame rows
sample_df = df.sample(False, 0.1)

display(sample_df)

# COMMAND ----------

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

# MAGIC %md Normalize Data

# COMMAND ----------

# DBTITLE 0,Normalize columns
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

# MinMaxScaler Transformation
assembler = VectorAssembler(inputCols=["ups","downs","authorlinkkarma","authorkarma"], outputCol="vector").setParams(handleInvalid="skip")
scaler = MinMaxScaler(min=0.0, max=1.0,inputCol="vector", outputCol="vector_scaled")

pipeline = Pipeline(stages=[assembler, scaler])

scalerModel = pipeline.fit(df)
scaledData = scalerModel.transform(df)

#vector_scaled is out normalized data: 
vactorData = scaledData.select("vector","vector_scaled")


# COMMAND ----------

display(scaledData.select("vector_scaled"))

# COMMAND ----------

# MAGIC %md Visualize the top-level categories in our dataset

# COMMAND ----------

# DBTITLE 0,Data Sampling for Experimentation
df = spark.read.parquet(dbfs_mnt_processed + 'redditcomments/')

display(df.groupby("meta").count())

# COMMAND ----------

# MAGIC %md Select a subset with just one top-level category and check it's subcategory's distribution

# COMMAND ----------

df_gaming = df.where(df.meta == "gaming")

display(df_gaming.groupby("subreddit").count())

# COMMAND ----------

# DBTITLE 1,Data Sampling for Experimentation
def match_pattern_on_storage(pattern):
  # Glob for files matching the given pattern
  dbfs_path = glob.glob(r"/dbfs/" + dbfs_mnt_processed + pattern)

  # Get the relative paths of the files to the root of the storage mount
  return [os.path.relpath(path, "/dbfs/" + dbfs_mnt_processed) for path in dbfs_path]

# COMMAND ----------

# MAGIC %md #### Define Dataset Versions

# COMMAND ----------

from azureml.core import Workspace
from azureml.core import Dataset, Datastore
from azureml.data.datapath import DataPath
from azureml.data.dataset_factory import TabularDatasetFactory

# Connect to the Azure Machine Learning Workspace
azureml_workspace = Workspace.from_config(auth=sp_auth)

# Like the DBFS Mount, the Azure ML Datastore references the same `processed` container on Azure Storage
processed_ds = Datastore.get(azureml_workspace, 'datastoreprocessed')

# COMMAND ----------

# MAGIC %md **Dataset A**: a subset of comments in the gaming category. 
# MAGIC 
# MAGIC We will use it to run a quick feasiblity analysis experiment. As well to have a cost-effective way to experiment with changes while we iterate on model versions.

# COMMAND ----------

comments_subset_gaming_dataset = TabularDatasetFactory.from_parquet_files([
      DataPath(processed_ds, path) for path in match_pattern_on_storage(
        "redditcomments/subreddit=gaming/*.parquet"
      )
])


# COMMAND ----------

# MAGIC %md **Dataset B**: the full set of comments for scale model training

# COMMAND ----------

comments_full_dataset = TabularDatasetFactory.from_parquet_files([
      DataPath(processed_ds, path) for path in match_pattern_on_storage(
        "redditcomments/*/*.parquet"
      )
])

# COMMAND ----------

# DBTITLE 1,Register the data set versions in Azure ML for reference during training
comments_full_dataset.register(
    azureml_workspace,
    name="redditcomments",
    create_new_version=True,
    description="The full dataset of comments"
)

comments_subset_gaming_dataset.register(
    azureml_workspace,
    name="redditcomments_gaming",
    create_new_version=True,
    description="The dataset with a sample of comments on the topic `gaming`"
)
