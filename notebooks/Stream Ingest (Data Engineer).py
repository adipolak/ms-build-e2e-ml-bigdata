# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Event Hub Configuration
ehConf = {
  'eventhubs.connectionString': dbutils.secrets.get(scope="mle2ebigdatakv", key="redditstreamingkey") 
}

# COMMAND ----------

# DBTITLE 1,Use Event Hubs to stream data to Spark for pre-processing
inputStream = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

display(inputStream)

# COMMAND ----------

# DBTITLE 1,Parse event body and set schema
expectedSchema = StructType([
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
  StructField("authorisgold", BooleanType(), True)
])

# Split the body into an array
comments_stream = inputStream.select(
  inputStream.enqueuedTime.alias('timestamp'),
  split(inputStream.body.cast('string'), ',').alias('splitted_body')
)

# Map the body array to columns
for index, field in enumerate(expectedSchema):
  comments_stream = comments_stream.withColumn(
    field.name, comments_stream.splitted_body.getItem(index)
  )

# Drop irrelevant columns
comments_stream = comments_stream.drop('timestamp', 'splitted_body')

# Set data types
comments_stream = comments_stream.withColumn(
  "authorisgold",
  when(comments_stream.authorisgold == '0.0', 0).when(comments_stream.authorisgold == '1.0', 1).otherwise(0)
)
comments_stream = comments_stream \
  .withColumn("time", comments_stream["time"].cast("float")) \
  .withColumn("ups", comments_stream["ups"].cast("float")) \
  .withColumn("downs", comments_stream["downs"].cast("float")) \
  .withColumn("authorlinkkarma", comments_stream["authorlinkkarma"].cast("float")) \
  .withColumn("authorkarma", comments_stream["authorkarma"].cast("float")) \
  .withColumn("authorisgold", comments_stream["authorisgold"].cast("boolean")) \

display(comments_stream)

# COMMAND ----------

# DBTITLE 1,Write processed streaming data to storage
# Stream processed data to parquet for the Data Science to explore and build ML models
comments_stream.writeStream \
  .trigger(processingTime = "30 seconds") \
  .format("parquet") \
  .outputMode("append") \
  .partitionBy("subreddit") \
  .option("compression", "none") \
  .option("checkpointLocation", "/mnt/stream/_checkpoints/redditcomments") \
  .start("/mnt/processed/redditcomments/")
