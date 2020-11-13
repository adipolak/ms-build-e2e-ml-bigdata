
# MS-Build 2020: Building an End-to-End ML Pipeline for Big Data​

This repo holds information and resources for you to create the Microsoft Build 2020 - Building End-to-End Machine Learning pipelines for Big Data Session demo.


## Prerequisites:
1. [Azure account](https://azure.microsoft.com/free?WT.mc_id=data-0000-adpolak)
2. [Eventhubs](https://docs.microsoft.com/azure/event-hubs/event-hubs-create?WT.mc_id=data-0000-adpolak)
3. [Azure Databricks](https://docs.microsoft.com/azure/azure-databricks/quickstart-create-databricks-workspace-portal?WT.mc_id=data-0000-adpolak)
4. [Azure Machine Learning](https://docs.microsoft.com/azure/machine-learning/tutorial-1st-experiment-sdk-setup?WT.mc_id=data-0000-adpolak)
5. [Azure KeyVault](https://docs.microsoft.com/azure/key-vault/secrets/quick-create-portal?WT.mc_id=data-0000-adpolak)
6. Kubernetes Environment / Azure Container Instance



## Data Flow
1. Ingest stream data into Azure Blob storage with Event hubs and Azure Databricks.
2. Preprocess the data to fit our schema - Apache Spark.
3. Save the data in parquet format - in raw storage directory.
4. Merge Batch(historical) and Stream(new) data with Apache Spark - save in preprocessed storage directory.
5. Create multiple Azure ML(AML) Datasets from Azure Databricks environment - save in refined storage directory.
6. Use Azure Machine Learning cluster compute to run multiple experiments on AML Datasets from VSCode.
7. Log ML models and ML algorithms parameters using MLflow.
8. Serve chosen ML model through Dockerized REST API service on Kubernetes. 
![](https://github.com/adipola/ms-build-e2e-ml-bigdata/blob/master/images/diagram.jpg)

## Tutorials:
* [Ingest](https://dev.to/adipolak/simple-data-ingestion-tutorial-with-yahoo-finance-api-and-python-2m6e) Data with Azure Blob and Eventhubs.
* [Collect, Analyze and Process](https://docs.microsoft.com/azure/azure-databricks/databricks-sentiment-analysis-cognitive-services?WT.mc_id=data-0000-adpolak) Stream data with Azure Databricks and Eventhubs.
* [Track](https://docs.microsoft.com/azure/machine-learning/how-to-use-mlflow?WT.mc_id=data-0000-adpolak) and log ML metrics with MLflow and AML.
* [Log & Deploy](https://docs.microsoft.com/azure/machine-learning/how-to-deploy-and-where?WT.mc_id=data-0000-adpolak) your ML Models to Kubernetes environment.



### Q&A
If you have questions/concerns or would like to chat, contact us:

* [Adi Polak](https://twitter.com/AdiPolak)

* [Dennis Eikelenboom](https://www.linkedin.com/in/denniseikelenboom/)
