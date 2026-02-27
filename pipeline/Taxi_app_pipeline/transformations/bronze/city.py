from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField, DateType

source_path = "s3://cabapp-at1/data-store/city"

schema = StructType ([
    StructField("City_Id", StringType(), False),
    StructField("City_Name", StringType(), True)
])
@dp.table(
    name = "transportation_app.bronze.city",
    comment="Ingest Raw City Data",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def raw_table():
    df = spark.read.schema(schema).format("csv").option("header", "true").option("mode","PERMISSIVE").option("columnNameOfCorruptRecord","_corrupted_data").load(source_path)

    df = df.withColumn("source_name", col("_metadata.file_path")).withColumn("ingest_datetime", current_timestamp())
    return df
