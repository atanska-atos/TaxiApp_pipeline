from pyspark import pipelines as dp
from pyspark.sql.functions import *

table_name = "transportation_app.bronze.trips"

@dp.table(
    name = "transportation_app.silver.Indore_trip",
    comment="Created from bronze table"
)
@dp.expect_all_or_drop({
    "valid_date": "Date is not null",
    "valid_distance": "Distance_Travelled > 0",
    "valid_fare": "Fare_Amount > 0",
    "valid_rating": "Passenger_Rating > 0",
    "valid_driver_rating": "Driver_Rating > 0"})
def indore_all_data():
    df = spark.read.table(table_name).filter(expr("City_Id == 'Indore'")).select("Trip_Id","Date","Passenger_Type","Distance_Travelled","Fare_Amount","Passenger_Rating","Driver_Rating")
    return df