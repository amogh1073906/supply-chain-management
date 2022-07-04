# Databricks notebook source
from pyspark.sql.functions import col,when,count,length,countDistinct,desc,array,size,length,date_format,to_date
from pyspark.sql.functions import sum,round,datediff,year,month,avg,max,min,when,to_timestamp,substring
from pyspark.sql.types import *

# COMMAND ----------

df_device = spark.read.format("csv").option("inferschema", True).option("header", True).load("/mnt/supply-chain-management/use-case1/device_data_cleaned.csv")

df_products = spark.read.format("csv").option("inferschema", True).option("header", True).load("/mnt/supply-chain-management/use-case1/products_data_final_4.csv")

df_sales = spark.read.format("csv").option("inferschema", True).option("header", True).load("/mnt/supply-chain-management/use-case1/sales_data.csv")

df_stores = spark.read.format("csv").option("inferschema", True).option("header", True).load("/mnt/supply-chain-management/use-case1/store_case_1_final.csv")

# COMMAND ----------

display(df_device)

display(df_products)

display(df_sales)

display(df_stores)

# COMMAND ----------

# MAGIC %md
# MAGIC Product performance based upon their size

# COMMAND ----------

df1 = df_products.join(df_sales,on="Product_id")

df_kpi1 = df1.groupBy(col("Product_type"),col("Product_size")).agg({"SKUs_sold": "sum"}).sort(desc("Product_type"))

display(df_kpi1)

# COMMAND ----------

# MAGIC %md
# MAGIC Sales Per Month and Per Day

# COMMAND ----------

df_sales_1 = df_sales.withColumn('Month',month(df_sales.Order_date)) 

df_sales_1 = df_sales_1.withColumn("Order_date",to_timestamp(col("Order_date"))).withColumn("Day", date_format(col("Order_date"), "d"))

display(df_sales_1)
 

# COMMAND ----------

df_kpi2 = df_sales_1.groupBy(col("Month")).agg({"SKUs_sold": "sum"}).sort(desc("sum(SKUs_sold)"))

display(df_kpi2)


# COMMAND ----------

df_kpi3 = df_sales_1.groupBy(col("Month"),col("Day")).agg({"SKUs_sold": "sum"}).sort(desc("sum(SKUs_sold)"))

display(df_kpi3)

# COMMAND ----------

# MAGIC %md
# MAGIC Total Service Requests Per Month

# COMMAND ----------

df_stores_1 = df_stores.withColumn('Month_Service_Request',month(df_stores.Request_service_date)) 

df_kpi4 = df_stores_1.groupBy(col("Month_Service_Request")).agg({"Month_Service_Request": "count"}).sort(desc("count(Month_Service_Request)"))

display(df_kpi4)

# COMMAND ----------

# MAGIC %md
# MAGIC MTBF (Mean Time Between Failure)

# COMMAND ----------

df_stores_mtbf = df_stores.select(col("Failure_date"),col("Operational_date"),datediff(col("Failure_date"),col("Operational_date")).alias("datediff"))

display(df_stores_mtbf)


# COMMAND ----------

from pyspark.sql.types import FloatType
no_of_failures = df_stores_mtbf.select(count("*")).collect()[0][0]

total_operational_days = df_stores_mtbf.agg({"datediff": "sum"}).collect()[0][0]

KPI5_MTBF = total_operational_days/no_of_failures

from pyspark.sql.types import FloatType
df_kpi5 = spark.createDataFrame([KPI5_MTBF],FloatType())


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC MTTR (Mean Time To Repair)

# COMMAND ----------

df_stores_mttr = df_stores.select(col("Failure_date"),col("Service_successful_date"),datediff(col("Service_successful_date"),col("Failure_date")).alias("datediff"))

display(df_stores_mttr)

# COMMAND ----------

no_of_failures = df_stores_mtbf.select(count("*")).collect()[0][0]

total_service_repair_days = df_stores_mttr.agg({"datediff": "sum"}).collect()[0][0]

KPI6_MTTR = total_service_repair_days/no_of_failures

df_kpi6 = spark.createDataFrame([KPI6_MTTR],FloatType())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Sales vs Capacity per liter

# COMMAND ----------

df_products_sales = df_products.join(df_sales,on="Product_id")

df_products_sales_1 = df_products_sales.select("Product_id","Device_id","Product_name","Product_price","SKUs_sold",(col("Product_price")*col("SKUs_sold")).alias("Total_sales_revenue"))

display(df_products_sales_1)

# COMMAND ----------

df_products_sales_2 = df_products_sales_1.join(df_device,on="Device_id")

df_kpi7 = df_products_sales_2.groupBy(col("Device_capacity")).agg({"Total_sales_revenue": "sum"}).sort(desc("sum(Total_sales_revenue)"))

df_kpi7 = df_kpi7.select(col("Device_capacity"),col("sum(Total_sales_revenue)"),(col("sum(Total_sales_revenue)")/substring('Device_capacity', 0,3).cast(IntegerType())).alias("SalesVsCapacity_per_liter"))

display(df_kpi7)
df_kpi1, df_kpi2, df_kpi3, df_kpi4, KPI5_MTBF, KPI6_MTTR, df_kpi7

# COMMAND ----------

def delete_mounted_dir(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            delete_mounted_dir(f.path)
        dbutils.fs.rm(f.path, True)
        

def writeToDelta(sourceDataframe, deltaPath):
    (sourceDataframe.coalesce(1).write.format('csv').mode('overwrite').save(deltaPath))
    
    
#ALTER TABLE table_name SET TBLPROPERTIES ('delta.minReaderVersion' = '2','delta.minWriterVersion' = '5','delta.columnMapping.mode' = 'name')

# COMMAND ----------

def remove_the_unused_file(path):
    files = dbutils.fs.ls(path)
    for value in files:
        if value.path.endswith('csv') == False:
            dbutils.fs.rm(value.path)

def copy_the_csv_file(path,movedPath,index):
    files = dbutils.fs.ls(path)
    for value in files:
        csvFile = dbutils.fs.ls(value.path)
        dbutils.fs.mv(csvFile[0].path, movedPath+ str(index))

# COMMAND ----------

outputPath = 'dbfs:/supplychainmanagement/output'
movedPath = 'dbfs:/supplychainmanagement/output/usecase1/kpi'
dbutils.fs.rm(outputPath,True)
dbutils.fs.rm(movedPath,True)
kpiList = [df_kpi1, df_kpi2, df_kpi3, df_kpi4, df_kpi5, df_kpi6, df_kpi7]
index=1
for kpi in kpiList:
    #print(kpiList[index])
    kpiOutputPath=outputPath+"/kpi"+str(index)
    writeToDelta(kpi,kpiOutputPath)
    remove_the_unused_file(kpiOutputPath)
    copy_the_csv_file(kpiOutputPath,movedPath,index)
    dbutils.fs.rm(kpiOutputPath)
    index+=1;

# COMMAND ----------

delete_mounted_dir("/mnt/supply-chain-management/output/usecase1")

# COMMAND ----------

dbutils.fs.cp("dbfs:/supplychainmanagement/output/usecase1","/mnt/supply-chain-management/output/usecase1",True)

# COMMAND ----------

dbutils.notebook.exit("UseCase1 Notebook successfully run!!")
