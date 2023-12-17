# Databricks notebook source
# MAGIC %md 
# MAGIC #Secret Scopes

# COMMAND ----------

# DBTITLE 1,Secret Scopes
display(dbutils.fs.ls('/'))

# COMMAND ----------

# use this to create Secrets  https://adb-6089266189947178.18.azuredatabricks.net/?o=6089266189947178#secrets/createScope
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'databricks-kv2023-2')

# COMMAND ----------

application_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="application-id")
tenant_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="tenant-id")
secret_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="db1-secret")

display(tenant_id)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

application_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="application-id")
tenant_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="tenant-id")
secret_id = dbutils.secrets.get(scope="databricks-kv2023-2", key="db1-secret")

# COMMAND ----------



configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret_id,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
storage_account_name = "databricksdl101"

# COMMAND ----------

m_point = "bronze"

dbutils.fs.mount(
    source = f"abfss://{m_point}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{m_point}",
    extra_configs = configs)

# COMMAND ----------

# MAGIC %md 
# MAGIC # MountPoints using SAS tokens
# MAGIC Ramesh class

# COMMAND ----------

storage_account_name = "databricksdl101"
demo_sas_token = "Generate a SAS key at storage account level and plug it here"

# SAS key was from the storage account level. Container level as prescribed in the course material is not working

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", demo_sas_token)



# COMMAND ----------

display(dbutils.fs.ls(f"abfss://demo@{storage_account_name}.dfs.core.windows.net"))
