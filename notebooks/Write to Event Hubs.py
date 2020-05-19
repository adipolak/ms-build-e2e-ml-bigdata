# Databricks notebook source
# Write data from storage to eventhubs to simulate streaming approach



from pyspark.sql.types import *
from pyspark.sql.functions import *



raw_folder = '/mnt/test/*.csv' #'/mnt/raw/reddit'

# Event Hubs Connection Configuration
ehConf = {
  'eventhubs.connectionString': dbutils.secrets.get(scope="mle2ebigdatakv", key="redditstreamingkey")
}



# Read from storage using Spark, define 'body' and write to eventhubs:



# Define schema:
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



reddit = spark.read.csv(raw_folder,redditSchema)
reddit.limit(5).toPandas()
reddit.show()


#Filter header - phase 1
tmp = reddit.where(reddit.id!='1')
tmp = tmp.drop('file_index')
reddit = tmp


colList = [col(column) for column in reddit.columns ]


# Merge String columns to create 'body' column 
#merge string columns to formulate a body column that we can later parse according to schema
from pyspark.sql.functions import concat_ws, col, lit # alias('body')
bodydf = reddit.select(concat_ws(',', *colList).alias('body'))


# Drop CSV headers residue
filtereddf = bodydf.where(bodydf.body!='0,1,2,3,4,5,6,7,8,9,10')


# Write Data to eventhubs
# add a forever loop / for each row write to eventhubs:
df = filtereddf.select("body").dropna()
df.count()




# Sample chunk of data 
def sampleData(df, index):
    count = df.count();
    howManyTake = 1
    if(count > index):
      howManyTake = index
    else:
      howManyTake = count
    
    return df.sample(False, 1.0*howManyTake/count,42).limit(howManyTake)


# Send batch to Eventhubs
import time

for lp in range(100, 2000):
  print("Batch number: {}".format(lp))
  sample = sampleData(df, lp)
  sample.write.format("eventhubs").options(**ehConf).save()
  time.sleep(3)
