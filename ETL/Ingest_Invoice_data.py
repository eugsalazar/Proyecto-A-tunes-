# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

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

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/Invoice.csv"

# COMMAND ----------

Invoice = StructType(fields=[StructField("InvoiceId", IntegerType(), False),
                                  StructField("CustomerId", IntegerType(), True),
                                  StructField("InvoiceDate", DateType(), True),
                                  StructField("BillingAddress", StringType(), True),
                                  StructField("BillingCity", StringType(), True),
                                  StructField("BillingState", StringType(), True),
                                  StructField("BillingCountry", StringType(), True),
                                  StructField("BillingPostalCode", StringType(), True),
                                  StructField("Total", DoubleType(), True)  
])


# COMMAND ----------

Invoice_df = spark.read \
            .option("header", True) \
            .schema(Invoice) \
            .csv(ruta)

# COMMAND ----------

Invoice_with_timestamp_df = Invoice_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

Invoice_selected_df = Invoice_with_timestamp_df.select(col('InvoiceId').alias('Invoice_Id'), 
                                                        col('CustomerId').alias('Customer_Id'), 
                                                        col('InvoiceDate').alias('Invoice_Date'), 
                                                        col('BillingAddress').alias('Billing_Address'), 
                                                        col('BillingCity').alias('Billing_City'), 
                                                        col('BillingState').alias('Billing_State'), 
                                                        col('BillingCountry').alias('Billing_Country'), 
                                                        col('BillingPostalCode').alias('Billing_PostalCode'), 
                                                        col('Total'), 
                                                        col('ingestion_date'))



# COMMAND ----------

Invoice_selected_df.write.mode('overwrite').partitionBy('Invoice_Id').saveAsTable(f'{catalogo}.{esquema}.Invoice')
