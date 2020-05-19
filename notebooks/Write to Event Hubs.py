# Databricks notebook source
# MAGIC %md #Write data from storage to eventhubs to simulate streaming approach

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

raw_folder = '/mnt/test/*.csv' #'/mnt/raw/reddit'

# Event Hubs Connection Configuration
ehConf = {
  'eventhubs.connectionString': dbutils.secrets.get(scope="mle2ebigdatakv", key="redditstreamingkey")
}


# COMMAND ----------

# MAGIC %md Read from storage using Spark, define 'body' and write to eventhubs:

# COMMAND ----------

# DBTITLE 1,Define schema:
redditSchema = StructType([
  StructField("file_index", StringType(), True),
  StructField("text", StringType(), True),
  StructField("id", StringType(), True),
  StructField("subreddit", StringType(), True),
  StructField("meta", StringType(), True),
  StructField("time", StringType(), True),
  StructField("author", StringType(), True),
  StructField("ups", StringType(), True),
  StructField("downs", StringType(), True),
  StructField("authorlinkkarma", StringType(), True),
  StructField("authorkarma", StringType(), True),
  StructField("authorisgold", StringType(), True)
])

# COMMAND ----------

reddit = spark.read.csv(raw_folder,redditSchema)
reddit.limit(5).toPandas()
reddit.show()

# COMMAND ----------

# DBTITLE 1,Filter header - phase 1
tmp = reddit.where(reddit.id!='1')
tmp = tmp.drop('file_index')
reddit = tmp

# COMMAND ----------

colList = [col(column) for column in reddit.columns ]

# COMMAND ----------

# DBTITLE 1,Merge String columns to create 'body' column 
#TODO merge string columns to formulate a body column that we can later parse according to schema
from pyspark.sql.functions import concat_ws, col, lit # alias('body')
bodydf = reddit.select(concat_ws(',', *colList).alias('body'))

# COMMAND ----------

# DBTITLE 1,Drop CSV headers residue
filtereddf = bodydf.where(bodydf.body!='0,1,2,3,4,5,6,7,8,9,10')

# COMMAND ----------

# DBTITLE 1,Drop entries with Adult content
# from pyspark.sql.functions import not as n

filter_adult_content = filtereddf.filter("LOWER(body) NOT LIKE '%sex%' ")
filter_adult_content.show()

# COMMAND ----------

# DBTITLE 1,Replace words using PySpark regex functionality
filter_adult_content = filter_adult_content.withColumn("body",regexp_replace("body","fuck","life"))
filter_adult_content = filter_adult_content.withColumn("body",regexp_replace("body","idiot","life"))
filter_adult_content.show()

filtereddf = filter_adult_content

# COMMAND ----------

# DBTITLE 1,Write Data to eventhubs
# add a forever loop / for each row write to eventhubs:
df = filtereddf.select("body").dropna()
df.count()


# COMMAND ----------

# DBTITLE 1,Sample chunk of data 
def sampleData(df, index):
    count = df.count();
    howManyTake = 1
    if(count > index):
      howManyTake = index
    else:
      howManyTake = count
    
    return df.sample(False, 1.0*howManyTake/count,42).limit(howManyTake)

# COMMAND ----------

# DBTITLE 1,Send batch to Eventhubs
import time

for lp in range(100, 2000):
  print("Batch number: {}".format(lp))
  sample = sampleData(df, lp)
  sample.write.format("eventhubs").options(**ehConf).save()
  time.sleep(3)
