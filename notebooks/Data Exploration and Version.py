# Databricks notebook source - this is an interactive script
import os
import glob
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.data import DataType
from pyspark.sql.functions import isnan, when, count, col


# Hide cell for demo as we cannot (?) use db secrets here somehow
sp_auth = ServicePrincipalAuthentication(
  tenant_id="<tenantid>",
  service_principal_id="<service_principal_id>",
  service_principal_password="<service_principal_password>"
)

dbfs_mnt_processed = "/mnt/processed/"


# Data Exploration
# Let take a random sample of our dataset


# Data Exploration
# Read parquet files into Spark DataFrame
df = spark.read.parquet(dbfs_mnt_processed + '/redditcomments/')

# Get a sampled subset of data frame rows
sample_df = df.sample(False, 0.1)

# when using azure databricks, use this call to visualize the data
#display(sample_df)

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()



# Normalize Data


# Normalize columns
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

# when using azure databricks, use this call to visualize the data
#display(scaledData.select("vector_scaled"))


# ,Data Sampling for Experimentation
df = spark.read.parquet(dbfs_mnt_processed + 'redditcomments/')

display(df.groupby("meta").count())



# Select a subset with just one top-level category and check it's subcategory's distribution
df_gaming = df.where(df.meta == "gaming")

# when using azure databricks, use this call to visualize the data
#display(df_gaming.groupby("subreddit").count())



# Data Sampling for Experimentation
def match_pattern_on_storage(pattern):
  # Glob for files matching the given pattern
  dbfs_path = glob.glob(r"/dbfs/" + dbfs_mnt_processed + pattern)

  # Get the relative paths of the files to the root of the storage mount
  return [os.path.relpath(path, "/dbfs/" + dbfs_mnt_processed) for path in dbfs_path]


# Define Dataset Versions


from azureml.core import Workspace
from azureml.core import Dataset, Datastore
from azureml.data.datapath import DataPath
from azureml.data.dataset_factory import TabularDatasetFactory

# Connect to the Azure Machine Learning Workspace
azureml_workspace = Workspace.from_config(auth=sp_auth)

# Like the DBFS Mount, the Azure ML Datastore references the same `processed` container on Azure Storage
processed_ds = Datastore.get(azureml_workspace, 'datastoreprocessed')



# Dataset A: a subset of comments in the gaming category. 

# We will use it to run a quick feasiblity analysis experiment. As well to have a cost-effective way to experiment with changes while we iterate on model versions.


comments_subset_gaming_dataset = TabularDatasetFactory.from_parquet_files([
      DataPath(processed_ds, path) for path in match_pattern_on_storage(
        "redditcomments/subreddit=gaming/*.parquet"
      )
])



# Dataset: the full set of comments for scale model training


comments_full_dataset = TabularDatasetFactory.from_parquet_files([
      DataPath(processed_ds, path) for path in match_pattern_on_storage(
        "redditcomments/*/*.parquet"
      )
])



# Register the data set versions in Azure ML for reference during training
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
