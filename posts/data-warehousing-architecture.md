
Gentle Introduction to  Data warehouse for Big Data:






## Common Data Lake architecture for Data Warehousing:
A commonly seen architecture in modern data warehousing is a division of the data lake in three distinct layers:

**Ingestion Layer:(Bronze)**
Store raw data. This is our data lake - a vast pool of raw data. The purpose of this data is not yet defined.


**Refined Layer:(Silver)**
Store transformed data e.g. ready for ML. 

**Feature/Aggregated Layer:(Gold)**
Store data that is ready for consumption e.g. ready for BI. 


------------------



### Type of files:
#### Parquet:

Parquet is an open source file format for Hadoop. It stores nested data structures in a flat columnar format. Compared to a traditional approach where data is stored in row-oriented approach. Parquet help us partition the data according to our index. 

For example, this is how it looks like after writing from Spark job to Parquet:
![](/../images/parition-by-with-parquet.png)



Each parquet file holds metadata on the data: file metadata, column (chunk) metadata and page header metadata. You can read about it [here](https://parquet.apache.org/documentation/latest/).
![](/../images/ParquetFileLayout.gif)
<sub><sup> Diagram from [parquet.apache.org](parquet.apache.org) documentation. <sup><sub>


### Pros:
✅ Allows for low-level reader filter -  useful when running analytics on specific columns and not on the whole entry.
✅ Allows for fast schema enrichment - adding new columns is fast.
✅ 

### Cons:
❗️Not humanly readable - binary file
❗️Schema evolution is VERY expensive - requires to read all the parquet files and reconcile/merge schemas during reading.
❗️Hard to understand, requires operation systems knowledge.




Joining parquet file to create new table with more columns.

Fast for data enrichment





#### Avro:


