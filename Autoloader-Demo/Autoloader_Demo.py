# Databricks notebook source
# MAGIC %run ./Helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ![Autoloader Image](https://raw.githubusercontent.com/cooolbabu/Databricks-Certified-Data-Engineer-Associate/main/Autoloader-Demo/DataBricks_Autoloader.png)

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
db0storage_sas_key = dbutils.secrets.get(scope="databricks-kv2023-2", key="db0storage-sas-key")

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
    "cloudFiles.connectionString": db0storage_sas_key,
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
