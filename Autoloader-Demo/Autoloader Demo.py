# Databricks notebook source
full_name = "Sreenivas Angara"

application_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="application-id")
tenant_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="tenant-id")
secret_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="db1-secret")
subscription_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="subscription-id")
db0storage_sas_key = dbutils.secrets.get(scope="databricks-kv2023-2", key="db0storage-sas-key")


account_name = "db0storage"

data_source_uri = "wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/"
dataset_bookstore = 'dbfs:/mnt/bookstore'
spark.conf.set(f"dataset.bookstore", dataset_bookstore)

# COMMAND ----------

spark.read.parquet("/mnt/bookstore/orders-raw/01.parquet", header=True).display()

# COMMAND ----------

df_orders = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/bookstore/orders/checkpoint_")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders/checkpoint_")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table orders_updates

# COMMAND ----------

import pyspark.sql.functions as func
from pyspark.sql.functions import concat, col, lit
form delta.tables import DeltaTable

streaming_data = "/mnt/bookstore/orders_streaming"
raw_data = "/mnt/bookstore/orders_streaming"





# COMMAND ----------

order_stream_config = {
    "cloudFiles.format": "parquet",
    "cloudFiles.schemaLocation": "dbfs:/mnt/bookstore/orders/checkpoint_",
    "cloudFiles.subscriptionId": subscription_id,
    "cloudFiles.connectionString": db0storage_sas_key,
    "cloudFiles.tenantId": tenant_id,
    "cloudFiles.clientId": application_id,
    "cloudFiles.clientSecret": secret_id,
    "cloudFiles.resourceGroup": "DataBricksLearnRG",
    "cloudFiles.useNotifications": "true"
}

# COMMAND ----------

df = ( spark.readStream.format("cloudFiles")
      .options(**order_stream_config)
      .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/bookstore/orders/checkpoint_")
        .table("orders_updates")
      )

# COMMAND ----------

df.display()

# COMMAND ----------

df.isStreaming
