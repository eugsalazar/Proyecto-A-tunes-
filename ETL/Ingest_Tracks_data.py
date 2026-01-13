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

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/Track.csv"

# COMMAND ----------

Track = StructType(fields=[StructField("TrackId", IntegerType(), False),
                             StructField("Name", StringType(), False), 
                             StructField("AlbumId", IntegerType(), True),
                            StructField("MediaTypeId", IntegerType(), True),
                            StructField("GenreId", IntegerType(), True),
                            StructField("Composer", StringType(), True),
                            StructField("Milliseconds", IntegerType(), True),
                            StructField("Bytes", IntegerType(), True),
                            StructField("UnitPrice", DoubleType(), True)
])


# COMMAND ----------

Track_df = spark.read \
            .option("header", True) \
            .schema(Track) \
            .csv(ruta)

# COMMAND ----------

Track_with_timestamp_df = Track_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

Track_selected_df = Track_with_timestamp_df.select(col('TrackId').alias('Track_Id'), 
                                                        col('Name').alias('Track_Name'), 
                                                        col('AlbumId').alias('Album_Id'), 
                                                        col('MediaTypeId').alias('Media_Type_Id'), 
                                                        col('GenreId').alias('Genre_Id'), 
                                                        col('Composer'), 
                                                        col('Milliseconds'), 
                                                        col('Bytes'), 
                                                        col('UnitPrice').alias('Unit_Price'), 
                                                        col('ingestion_date').alias('Ingestion_Date'))


# COMMAND ----------

##Track_selected_df.display()

# COMMAND ----------

Track_selected_df.write.mode('overwrite').saveAsTable(f'{catalogo}.{esquema}.Track')

