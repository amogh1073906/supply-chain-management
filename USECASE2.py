# Databricks notebook source
import pyspark.sql.functions as F
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, FloatType
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
from pyspark.sql.functions import *
...
col('my_column')
import re
from pyspark.sql.types import *

# COMMAND ----------

orders = spark.read.format('csv').option('inferschema', True).option('header',True).load('/mnt/supply-chain-management/use-case2/orders_final_2.csv')

# COMMAND ----------

products =spark.read.format('csv').option('inferschema',True).option('header',True).load('/mnt/supply-chain-management/use-case2/products_data_final_4.csv')

# COMMAND ----------

stores = spark.read.format('csv').option('inferschema', True).option('header',True).load('/mnt/supply-chain-management/use-case2/stores_data_case_2.csv')

# COMMAND ----------

telemetry = spark.read.format('csv').option('inferschema', True).option('header',True).load('/mnt/supply-chain-management/use-case2/telemetry_data_cleaned_3.csv')

# COMMAND ----------

display(telemetry)
display(orders)
display(products)
display(stores)

# COMMAND ----------

# DBTITLE 1,•	Tracking Order Cycle Time 
df2=orders.withColumn("Order_Delivered_Date",to_timestamp(col("Order_Delivered_Date"),"dd-MM-yyyy"))
df2=orders.withColumn("Order_Purchased_TimeStamp",to_date(col("Order_Purchased_TimeStamp"),"dd-MM-yyyy"))

# COMMAND ----------

display(df2)

# COMMAND ----------

df_orders2 =df2.select(col("Order_Delivered_Date"),col("Order_Purchased_TimeStamp"),datediff(col("Order_Delivered_Date"),col("Order_Purchased_TimeStamp")).alias("datediff"))


# COMMAND ----------

display(df_orders2)

# COMMAND ----------

df_sum_of_datediff = df_orders2.agg({"datediff": "sum"}).collect()[0][0]

# COMMAND ----------

df_count_orderid = orders.agg({"Order_Id": "count"}).collect()[0][0]

# COMMAND ----------

KPI_4 = df_sum_of_datediff/df_count_orderid
from pyspark.sql.types import FloatType
df_kpi4 = spark.createDataFrame([KPI_4],FloatType()) 

# COMMAND ----------

display(df_kpi4)

# COMMAND ----------

# DBTITLE 1,•	On-Time Delivery
df_telemetry_order = df2.join(telemetry,on="Product_id")

# COMMAND ----------

display(df_telemetry_order)

# COMMAND ----------

df_order_tele=df_telemetry_order.select(col("Order_Delivered_Date"),col("Transit_Date"),datediff(col("Order_Delivered_Date"),col("Transit_Date")).alias("datediff")).where(col("datediff") > 0)

# COMMAND ----------

display(df_order_tele)

# COMMAND ----------

df_time_delivery=df_telemetry_order.select(col("Order_Id"),col("Order_Delivered_Date"),col("Estimated_Delivery_Date"),datediff(col("Order_Delivered_Date"),col("Estimated_Delivery_Date")).alias("datediff")).where(col("datediff")==0)

# COMMAND ----------

KPI_2=df_time_delivery.distinct().select(col("Order_Id"),col("Order_Delivered_Date"),col("Estimated_Delivery_Date"))

# COMMAND ----------

display(KPI_2)

# COMMAND ----------

# DBTITLE 1,•	Inventory Visibility/Accuracy 
KPI_1=telemetry.select(col("Product_Id"),col("Quantity"),col("Warehouse_Id"),col("Warehouse_Name"),col("Warehouse_Location")).groupBy(col("Product_Id"),col("Warehouse_Location"),col("Warehouse_Name")).agg({"Quantity": "sum"})


# COMMAND ----------

display(KPI_1)

# COMMAND ----------

# DBTITLE 1,•	Stock to Shelf Time 
df_product_order = products.join(orders,on="Product_id")

# COMMAND ----------

display(df_product_order)

# COMMAND ----------

KPI_3=df_product_order.select(col("Order_Delivered_Date"),col("Manufacturing_date"),col("Product_Id"),datediff(col("Order_Delivered_Date"),col("Manufacturing_date")).alias("datediff")).sort(desc("datediff"))

# COMMAND ----------

display(KPI_3)

# COMMAND ----------

def delete_mounted_dir(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            delete_mounted_dir(f.path)
        dbutils.fs.rm(f.path, True)
        

def writeToDelta(sourceDataframe, deltaPath):
    (sourceDataframe.write.format('csv').mode('overwrite').save(deltaPath))

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

outputPath = 'dbfs:/supplychainmanagement/output1'
movedPath = 'dbfs:/supplychainmanagement/output1/usecase2/kpi'
dbutils.fs.rm(outputPath,True)
dbutils.fs.rm(movedPath,True)
kpiList = [KPI_1, KPI_2, KPI_3, df_kpi4]
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

delete_mounted_dir("/mnt/supply-chain-management/output1/usecase2")

# COMMAND ----------

dbutils.fs.cp("dbfs:/supplychainmanagement/output1/usecase2","/mnt/supply-chain-management/output1/usecase2",True)

# COMMAND ----------

dbutils.notebook.exit("UseCase2 Notebook successfully run!!")
