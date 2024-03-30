# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "f7fd90f4-93fb-4762-a99f-ef81e9119e2e",
"fs.azure.account.oauth2.client.secret": "3Lr8Q~96cBHa5pnYbZ~VhoK48B5ZOdziBQdSMb20",
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d5295adf-f75c-40c1-b918-9cee28a0e39c/oauth2/token"}


dbutils.fs.mount(
source = "abfss://blob-f-storage@fuelcrstroage.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/fuelcr_drive", #our drive name is fuelcr_drive here
extra_configs = configs)

# COMMAND ----------

#if you need to unmount the drive earlier
#dbutils.fs.unmount("/mnt/fuelcr_drive") #to unmount a drive

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/fuelcr_drive"

# COMMAND ----------

spark

# COMMAND ----------

f12_24be = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f12_24be.csv")
f12_24be.show()

# COMMAND ----------

f12_24ph = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f12_24ph.csv")
f12_24ph.show()

# COMMAND ----------

f95_04c = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f95_04c.csv")
f95_04c.show()

# COMMAND ----------

f05_14c = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f05_14c.csv")
f05_14c.show()

# COMMAND ----------

f15_19c = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f15_19c.csv")

f20c = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f20c.csv")

f21c = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f21c.csv")

f22c = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f22c.csv")

f23c = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f23c.csv")

f24c = spark.read.format("csv").option("header","true").load("/mnt/fuelcr_drive/raw-data/f24c.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Data Cleaning Process

# COMMAND ----------

# MAGIC %md
# MAGIC Merge/Combine all the dataset for the 5-cycle cars together to a single dataframe.

# COMMAND ----------

#combine the 5-cycle car data to a single file
f95_24c = f95_04c.union(f05_14c).union(f15_19c).union(f20c).union(f21c).union(f22c).union(f23c).union(f24c)
f95_24c.show()

# COMMAND ----------

f95_24c.printSchema()

# COMMAND ----------

f12_24be.printSchema()

# COMMAND ----------

f12_24ph.printSchema()

# COMMAND ----------

f95_04c.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC We observe that the Data is not correct. We have to correctly specify the data type to work with our data.

# COMMAND ----------

"Engine size (L)"
"Cylinders"
"City (L/100 km)"
"Highway (L/100 km)"
"Combined (L/100 km)"
"Combined (mpg)"
"CO2 emissions (g/km)"
"CO2 rating"
"Smog rating"

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DateType

f95_24c = f95_24c.withColumn("Engine size (L)", col("Engine size (L)").cast(IntegerType())) \
    .withColumn("Cylinders", col("Cylinders").cast(IntegerType())) \
    .withColumn("City (L/100 km)", col("City (L/100 km)").cast(IntegerType())) \
    .withColumn("Highway (L/100 km)", col("Highway (L/100 km)").cast(IntegerType())) \
    .withColumn("Combined (L/100 km)", col("Combined (L/100 km)").cast(IntegerType())) \
    .withColumn("Combined (mpg)", col("Combined (mpg)").cast(IntegerType())) \
    .withColumn("CO2 emissions (g/km)", col("CO2 emissions (g/km)").cast(IntegerType())) \
    .withColumn("CO2 rating", col("CO2 rating").cast(IntegerType())) \
    .withColumn("Smog rating", col("Smog rating").cast(IntegerType())) \
    .withColumn("Model year", col("Model year").cast(DateType()))
    

# COMMAND ----------

f95_24c.printSchema()

# COMMAND ----------

f12_24ph.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DateType

f12_24ph = f12_24ph.withColumn("Engine size (L)", col("Engine size (L)").cast(IntegerType())) \
    .withColumn("Cylinders", col("Cylinders").cast(IntegerType())) \
    .withColumn("Recharge time (h)", col("Recharge time (h)").cast(IntegerType())) \
    .withColumn("Range 1 (km)", col("Range 1 (km)").cast(IntegerType())) \
    .withColumn("Range 2 (km)", col("Range 2 (km)").cast(IntegerType()))\
    .withColumn("City (L/100 km)", col("City (L/100 km)").cast(IntegerType())) \
    .withColumn("Highway (L/100 km)", col("Highway (L/100 km)").cast(IntegerType())) \
    .withColumn("Combined (L/100 km)", col("Combined (L/100 km)").cast(IntegerType())) \
    .withColumn("Motor (kW)", col("Motor (kW)").cast(IntegerType())) \
    .withColumn("CO2 emissions (g/km)", col("CO2 emissions (g/km)").cast(IntegerType())) \
    .withColumn("CO2 rating", col("CO2 rating").cast(IntegerType())) \
    .withColumn("Smog rating", col("Smog rating").cast(IntegerType())) \
    .withColumn("Model year", col("Model year").cast(DateType()))
    

# COMMAND ----------

f12_24ph.printSchema()

# COMMAND ----------

f12_24be.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DateType

f12_24be = f12_24be.withColumn("Recharge time (h)", col("Recharge time (h)").cast(IntegerType())) \
    .withColumn("Range (km)", col("Range (km)").cast(IntegerType())) \
    .withColumn("City (Le/100 km)", col("City (Le/100 km)").cast(IntegerType())) \
    .withColumn("City (kWh/100 km)", col("City (kWh/100 km)").cast(IntegerType())) \
    .withColumn("Highway (Le/100 km)", col("Highway (Le/100 km)").cast(IntegerType()))\
    .withColumn("Highway (kWh/100 km)", col("Highway (kWh/100 km)").cast(IntegerType())) \
    .withColumn("Combined (Le/100 km)", col("Combined (Le/100 km)").cast(IntegerType())) \
    .withColumn("Combined (kWh/100 km)", col("Combined (kWh/100 km)").cast(IntegerType()))\
    .withColumn("Motor (kW)", col("Motor (kW)").cast(IntegerType())) \
    .withColumn("CO2 emissions (g/km)", col("CO2 emissions (g/km)").cast(IntegerType())) \
    .withColumn("CO2 rating", col("CO2 rating ").cast(IntegerType())) \
    .withColumn("Smog rating", col("Smog rating").cast(IntegerType())) \
    .withColumn("Model year", col("Model year").cast(DateType()))
    

# COMMAND ----------

f95_24c.show()

# COMMAND ----------

f12_24be.show()

# COMMAND ----------

f12_24ph.show()

# COMMAND ----------

# MAGIC %md
# MAGIC After doing our transformation, and creating some new measures, we can export them to the “transformed data” directory in our data lake storage:

# COMMAND ----------

f95_24c.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/fuelcr_drive/transformed-data/f95_24c")

f12_24be.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/fuelcr_drive/transformed-data/f12_24be")

f12_24ph.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/fuelcr_drive/transformed-data/f12_24ph")

