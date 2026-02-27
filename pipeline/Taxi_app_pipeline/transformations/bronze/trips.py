from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField, DateType


source_data = "s3://cabapp-at1/data-store/trips"

schema = StructType ([
    StructField("Trip_Id", StringType(), False),
    StructField("Date", DateType(), True),
    StructField("City_Id", IntegerType(), True),
    StructField("Passenger_Type", StringType(), True),
    StructField("Distance_Travelled", IntegerType(), True),
    StructField("Fare_Amount", IntegerType(), True),
    StructField("Passenger_Rating", IntegerType(), True),
    StructField("Driver_Rating", IntegerType(), True)
])

@dp.table(
    name="transportation_app.bronze.trips",
    comment="Ingest Raw Trips Data"
)
@dp.expect("valid_Passenger_Rating", "Passenger_Rating >= 1 AND Passenger_Rating <= 10")
@dp.expect("valid_Driver_Rating", "Driver_Rating >= 1 AND Driver_Rating <= 10")
def trips():
    df = spark.readStream.schema(schema).format("csv").option("header", "true").load(source_data)
    return df