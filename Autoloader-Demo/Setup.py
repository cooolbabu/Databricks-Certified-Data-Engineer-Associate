# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Azure ADLS

# COMMAND ----------

# DBTITLE 1,Declare Globals
application_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="application-id")
tenant_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="tenant-id")
secret_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="db1-secret")

account_name = "db0storage"

data_source_uri = "wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/"
dataset_bookstore = 'dbfs:/mnt/bookstore'
spark.conf.set(f"dataset.bookstore", dataset_bookstore)

# COMMAND ----------

# DBTITLE 1,Create Mount Points function
def create_mounts(mount_point, container_name):
  configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": application_id,
            "fs.azure.account.oauth2.client.secret": secret_id,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

  # Optionally, you can add <directory-name> to the source URI of your mount point.
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{mount_point}",
    extra_configs = configs)

# COMMAND ----------

# Add mount points

# COMMAND ----------

for mount_point in ["bookstore", "bronze", "silver", "gold"]:
    create_mounts(mount_point, mount_point)

# COMMAND ----------

# DBTITLE 1,Add mount points - Bookstore, bronze, silver and gold
mount_point = "/mnt/bookstore"
dbutils.fs.ls(mount_point)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Unmount mount points
# Do it later
for mount_point in ["bookstore", "bronze", "silver", "gold"]    
    dbutils.fs.unmount(f"/mnt/{mount_point}")

# COMMAND ----------

spark.read.csv("/mnt/bronze/countries.csv", header=True).display()

# COMMAND ----------

container_name = "bronze"
account_name = "db0storage"
mount_point = "/mnt/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC # Download Dataset

# COMMAND ----------

# DBTITLE 1,Download Derar Dataset
def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

def download_dataset(source, target):
    files = dbutils.fs.ls(source)

    for f in files:
        source_path = f"{source}/{f.name}"
        target_path = f"{target}/{f.name}"
        if not path_exists(target_path):
            print(f"Copying {f.name} ...")
            dbutils.fs.cp(source_path, target_path, True)

# COMMAND ----------

# DBTITLE 1,Download Dataset

download_dataset(data_source_uri, dataset_bookstore)

# COMMAND ----------

# MAGIC %md
# MAGIC # Copy Orders data from Streaming to raw folder

# COMMAND ----------

# Structured Streaming
streaming_dir = f"{dataset_bookstore}/orders-streaming"
raw_dir = f"{dataset_bookstore}/orders-raw"

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1
    
def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

    
def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1


# COMMAND ----------

load_new_data()
