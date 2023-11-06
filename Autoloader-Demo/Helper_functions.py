# Databricks notebook source
# MAGIC %md
# MAGIC # Running Helper functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup Azure ADLS

# COMMAND ----------

# DBTITLE 1,Declare Globals
full_name = "Sreenivas Angara"

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

# MAGIC %md
# MAGIC #### Download Dataset

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
# Uncomment if you want to execute from here 
# download_dataset(data_source_uri, dataset_bookstore)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Copy Orders data from Streaming to raw folder

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

    
def load_orders_streaming_data(all=False):
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

# Uncomment to test here
load_orders_streaming_data()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configure Mount points

# COMMAND ----------

def mount_point_exist(mount_point, mounts):
    
    exists = any(mount.mountPoint == mount_point for mount in mounts)
    if exists:
        print(f"Mount point '{mount_point}' is configured.")
        return True
    else:
        print(f"Mount point '{mount_point}' does not exist.")
        return False


# COMMAND ----------

# DBTITLE 1,Mount mount points
def create_mount_points():
    mounts = dbutils.fs.mounts()

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret_id,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    storage_account_name = "db0storage"

    for m_point in ["bookstore", "bronze", "silver", "gold"]:
        m_point = f"/mnt/{m_point}"
        if not mount_point_exist(m_point, mounts):

            # Create mounts
            dbutils.fs.mount(
                source = f"abfss://{m_point}@{storage_account_name}.dfs.core.windows.net/",
                mount_point = m_point,
                extra_configs = configs)
            print(f"Mount point '{m_point}' is configured")
        

# COMMAND ----------

create_mount_points()

# COMMAND ----------

# DBTITLE 1,Unmount mount points

def unmount_mount_points():
    for m_point in ["bookstore", "bronze", "silver", "gold"]:
        m_point = f"/mnt/{mount_point}"
        if mount_point_exist(m_point):
            dbutils.fs.unmount(m_point)

# COMMAND ----------

#unmount_mount_points()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze
