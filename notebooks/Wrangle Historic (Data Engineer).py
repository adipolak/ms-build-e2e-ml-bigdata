# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Load Raw Comments Data
Fschema = StructType([
  StructField("file_index", StringType(), True),
  StructField("text", StringType(), True),
  StructField("id", StringType(), True),
  StructField("subreddit", StringType(), True),
  StructField("meta", StringType(), True),
  StructField("time", FloatType(), True),
  StructField("author", StringType(), True),
  StructField("ups", FloatType(), True),
  StructField("downs", FloatType(), True),
  StructField("authorlinkkarma", FloatType(), True),
  StructField("authorkarma", FloatType(), True),
  StructField("authorisgold", FloatType(), True)
])
  
df = spark.read.csv("/mnt/test/*.csv", schema=Fschema, header=True)

# cast boolean
df = df.withColumn(
  "authorisgold",
  when(df.authorisgold == '0.0', 0).when(df.authorisgold == '1.0', 1).otherwise(0)
)
df = df.withColumn("authorisgold", df["authorisgold"].cast("boolean"))

# drop file index
df = df.drop("file_index")

display(df)



# when using azure databricks, use this call to visualize the data
# display(df.limit(100))


# Write to storage in parquet format
df \
  .write \
  .partitionBy("subreddit") \
  .option("compression","none") \
  .mode("append") \
  .parquet(dbfs_mnt_processed + "redditcomments/")
