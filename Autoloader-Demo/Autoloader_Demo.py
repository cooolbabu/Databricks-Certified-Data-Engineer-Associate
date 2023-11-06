# Databricks notebook source
# MAGIC %run ./Helper_functions

# COMMAND ----------

full_name = "Sreenivas Angara"
linkedIn = "https://www.linkedin.com/in/sreenivasangara/"
blog = "https://cooolbabu.github.io/SreenivasAngara/"

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

load_orders_streaming_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.orders_in

# COMMAND ----------

# DBTITLE 1,Count the number of rows in the table bronze.orders_in
# MAGIC %sql
# MAGIC select count(*) from bronze.orders_in

# COMMAND ----------

# DBTITLE 1,Azure configuration to setup event notification
order_stream_config = {
    "cloudFiles.format": "parquet",
    "cloudFiles.schemaLocation": "dbfs:/mnt/bookstore/orders/checkpoint_",
    "cloudFiles.subscriptionId": subscription_id,
    "cloudFiles.connectionString": "BlobEndpoint=https://db0storage.blob.core.windows.net/;QueueEndpoint=https://db0storage.queue.core.windows.net/;FileEndpoint=https://db0storage.file.core.windows.net/;TableEndpoint=https://db0storage.table.core.windows.net/;SharedAccessSignature=sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-12-31T20:28:11Z&st=2023-11-06T12:28:11Z&spr=https&sig=%2F2MqLGfYUKt8kjmjddiuDkW5keNkgYhRWJc8qxR78Yk%3D",
    "cloudFiles.tenantId": tenant_id,
    "cloudFiles.clientId": application_id,
    "cloudFiles.clientSecret": secret_id,
    "cloudFiles.resourceGroup": "DataBricksLearnRG",
    "cloudFiles.useNotifications": "true"
}

# COMMAND ----------

# DBTITLE 1,Autoloader using event notifications
df = ( spark.readStream.format("cloudFiles")
      .options(**order_stream_config)
      .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/bookstore/orders/al_fn_checkpoint_")
        .table("bronze.orders_in")
      )

# COMMAND ----------

load_orders_streamming_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.orders_in

# COMMAND ----------



# COMMAND ----------

df.isStreaming

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from bronze.orders_in

# COMMAND ----------

# DBTITLE 1,Autoloader using default Directory Listing
df_orders = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/bookstore/orders_raw/al_ds_checkpoint_")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/bookstore/orders_raw/al_ds_checkpoint_")
        .table("bronze.orders_in")
)
