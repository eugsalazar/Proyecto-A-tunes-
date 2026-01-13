# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storage_name", "adlssmartdata0878")
dbutils.widgets.text("container", "chinook")
dbutils.widgets.text("catalogo", "c_a_tunes_dev")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/InvoiceLine.csv"

# COMMAND ----------

df_InvoiceLine = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta)

# COMMAND ----------

InvoiceLine = StructType(fields=[StructField("InvoiceLineId", IntegerType(), False),
                                     StructField("InvoiceId", IntegerType(), True),
                                     StructField("TrackId", IntegerType(), True),
                                     StructField("UnitPrice", DoubleType(), True),
                                     StructField("Quantity", IntegerType(), True)
])




# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_InvoiceLine_final = spark.read\
.option('header', True)\
.schema(InvoiceLine)\
.csv(ruta)


# COMMAND ----------

# DBTITLE 1,select only specific cols
InvoiceLine_selected_df = df_InvoiceLine_final.select(col("InvoiceLineId"), 
                                                col("InvoiceId"), 
                                                col("TrackId"), 
                                                col("UnitPrice"), 
                                                col("Quantity"))


# COMMAND ----------

InvoiceLine_renamed_df = InvoiceLine_selected_df.withColumnRenamed("InvoiceLineId", "Invoice_Line_Id") \
.withColumnRenamed("InvoiceId", "Invoice_Id") \
.withColumnRenamed("TrackId", "Track_Id") \
.withColumnRenamed("UnitPrice", "Unit_Price") 

# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
 InvoiceLine_final_df = InvoiceLine_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

InvoiceLine_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.InvoiceLine")
