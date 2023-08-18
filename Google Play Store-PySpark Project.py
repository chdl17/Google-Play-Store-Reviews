# Databricks notebook source
# DBTITLE 1,Import Libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructField, StructType, IntegerType

# COMMAND ----------

# DBTITLE 1,Create Dataframe
df = spark.read.load('/FileStore/tables/googleplaystore.csv',format='csv', header='true', sep=',',inferschema ='true', escape='"')

# COMMAND ----------

df.count()
df.show(1)

# COMMAND ----------

# DBTITLE 1,Print Schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Cleaning and Transformations
df_1= df.drop("Size","Content Rating", "Last Updated", "Android Ver")

# COMMAND ----------

df_1.show(2)

# COMMAND ----------

df_1.printSchema()

# COMMAND ----------

df_1 = df_1.withColumn("Reviews", col("Reviews").cast(IntegerType()))\
    .withColumn("Installs", regexp_replace(col("Installs"),"[^0-9]",""))\
        .withColumn("Installs", col("Installs").cast(IntegerType()))\
            .withColumn("Price", regexp_replace(col("Price"),"[$]",""))\
                .withColumn("Price", col("Price").cast(IntegerType()))
    

# COMMAND ----------

df_1.show(2)

# COMMAND ----------

# DBTITLE 1,Creating new TempView to use SQL commands
df_1.createOrReplaceTempView("GooglePlayStore")

# COMMAND ----------

# MAGIC %sql select * from googleplaystore;

# COMMAND ----------

# DBTITLE 1,Top_10 Reviews Given to the Apps
# MAGIC %sql select App, sum(Reviews) from googleplaystore 
# MAGIC group by 1
# MAGIC order by 2 DESC 
# MAGIC limit 10;

# COMMAND ----------

# DBTITLE 1,top_10 Installed Apps
# MAGIC %sql select App, Type, Sum(Installs) from googleplaystore
# MAGIC group by 1, 2
# MAGIC order by 3 desc
# MAGIC
