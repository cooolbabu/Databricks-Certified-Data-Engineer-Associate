# Databricks notebook source
# MAGIC %md
# MAGIC # Declarative programming will expedite the Return on Investment in your modern data warehouse implementation.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Autoloader Image](https://raw.githubusercontent.com/cooolbabu/Databricks-Certified-Data-Engineer-Associate/main/Autoloader-Demo/DataBricks_Autoloader.png)

# COMMAND ----------

# MAGIC %run ./Helper_functions

# COMMAND ----------

# DBTITLE 1,Get ids and credentials from Azure Key vault.
full_name = "Sreenivas Angara"
linkedIn = "https://www.linkedin.com/in/sreenivasangara/"
blog = "https://cooolbabu.github.io/SreenivasAngara/"

tenant_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="tenant-id")
subscription_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="subscription-id")
application_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="application-id")
secret_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="db1-secret")

storage_account_name = "db0storage"
db0storage_connection_string = dbutils.secrets.get(scope="databricks-kv2023-2", key="db0storage-sas-key")

dataset_bookstore = 'dbfs:/mnt/bookstore'

spark.conf.set(f"dataset.bookstore", dataset_bookstore)

# COMMAND ----------

spark.read.parquet("/mnt/bookstore/orders-raw/01.parquet", header=True).display()

# COMMAND ----------

# DBTITLE 1,Azure configuration to setup event notification
order_stream_config = {
    "cloudFiles.format": "parquet",
    "cloudFiles.schemaLocation": f"{dataset_bookstore}/orders-in-checkpoint_",
    "cloudFiles.tenantId": tenant_id,
    "cloudFiles.subscriptionId": subscription_id,
    "cloudFiles.clientId": application_id,
    "cloudFiles.clientSecret": secret_id,
    "cloudFiles.resourceGroup": "DataBricksLearnRG",
    "cloudFiles.connectionString": db0storage_connection_string,
    "cloudFiles.useNotifications": "true"
}

# COMMAND ----------

# DBTITLE 1,Autoloader using File notifications mode
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder.appName("Order ingestion stream").getOrCreate()

df_orders = ( spark.readStream.format("cloudFiles")
      .options(**order_stream_config)
      .load(f"{dataset_bookstore}/orders-raw") )


df_orders = df_orders.withColumn("ingestion_date", current_timestamp()).withColumn("filename", input_file_name())

(df_orders.writeStream
        .option("checkpointLocation", f"{dataset_bookstore}/orders-in-checkpoint_")
        .table("bronze.orders_in") )

# COMMAND ----------

load_orders_streaming_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ingestion_date, filename, count(*) 
# MAGIC from bronze.orders_in
# MAGIC group by ingestion_date, filename
# MAGIC order by filename

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bronze.orders_in

# COMMAND ----------

# DBTITLE 1,Autoloader using Directory Listing mode
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder.appName("Read-Write Stream").getOrCreate()

df_orders = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/bookstore/orders2-in-checkpoint_")
    .load(f"{dataset_bookstore}/orders-raw")
)

df_orders = df_orders.withColumn("ingestion_date", current_timestamp()).withColumn("filename", input_file_name())

(df_orders.writeStream
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/mnt/bookstore/orders2-in-checkpoint_")
    .table("bronze.orders_in")
)



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ingestion_date, filename, count(*) 
# MAGIC from bronze.orders_in
# MAGIC group by ingestion_date, filename

# COMMAND ----------

# DBTITLE 1,What I showed you
# MAGIC %md
# MAGIC ![Autoloader Image](https://raw.githubusercontent.com/cooolbabu/Databricks-Certified-Data-Engineer-Associate/main/Autoloader-Demo/DataBricks_Autoloader.png)

# COMMAND ----------

# DBTITLE 1,Apache Spark Democratrization - Many vendors are integrating Structured Streaming into their products.
# MAGIC %md
# MAGIC ### - This technology is not just for Fortune 500 companies
# MAGIC ### - Medium and even small businesses can build Modern Data Warehouse to drive Dashboards
# MAGIC ### - If you have 'Architect' in your job title, pay attention to Declarative Pogramming
# MAGIC
# MAGIC ### Keep an eye out for other Product companies start talking about Declarative Programming.
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Agile Development: Iterative and Incremental 
# MAGIC %md
# MAGIC ### Tactical - How do we get started and get the ball rolling
# MAGIC
# MAGIC ![](https://www.visual-paradigm.com/servlet/editor-content/scrum/agile-development-iterative-and-incremental/sites/7/2019/12/iteration-and-backlog.png)
# MAGIC <br>
# MAGIC _source_ : [Visual Paradigm](https://www.visual-paradigm.com/scrum/agile-development-iterative-and-incremental/)
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ### - Start small leveraging Cloud infrastructure with pay-as-go model
# MAGIC ### - Realize Return on Investment(ROI) with each iteration
# MAGIC ### - Expand into other Business areas and Data repositories
# MAGIC ### - Low risk / High reward
# MAGIC

# COMMAND ----------

# DBTITLE 1,What's next? Convergence of two powerful forces
# MAGIC %md
# MAGIC
# MAGIC ### Strategic - Future proofing our deliverables
# MAGIC
# MAGIC # Force One - Declarative Programming
# MAGIC #### - Small code blocks equals rapid solution delivery
# MAGIC #### - Return on Investment is driven by sum of 
# MAGIC #### &emsp; 1. Reduced Explainable code
# MAGIC #### &emsp; 2. Reduced infrastructure code
# MAGIC #### &emsp; 3. Reduction in Enterprise Architecture record keeping 
# MAGIC
# MAGIC # Force two - Generative AI
# MAGIC ####&emsp; &#9889; ChatGPT can write pyspark streaming code &#9889; 
# MAGIC
# MAGIC <br><hr>
# MAGIC ## &emsp; &emsp; &emsp; &emsp; Convergence of these two powerful forces will change our delivery methodology

# COMMAND ----------

# MAGIC %md
# MAGIC #
# MAGIC #
# MAGIC # That's all folks
# MAGIC #
# MAGIC #
# MAGIC ### Thank you - Sreenivas
