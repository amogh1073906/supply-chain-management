# Databricks notebook source
# mounting adls gen2 directory to store the generated results
try:
    dbutils.fs.mount(
    source = "wasbs://supply-chain-management@simulationdevicedata.blob.core.windows.net",
    mount_point = "/mnt/supply-chain-management",
    extra_configs = {"fs.azure.account.key.simulationdevicedata.blob.core.windows.net": "Bd7hAg/1GZ5B824iPKpdWtDxkVlELJ71I90kkYZcckYR3zRilKf8Jk9dCGmpx/SZhEJbbCS1bWop1ThPDtwqFg=="})
except:
    pass

# COMMAND ----------

dbutils.notebook.exit("Mounted Successfully")

# COMMAND ----------


