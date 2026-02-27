from pyspark import pipelines as dp
from pyspark.sql.functions import *

table_name = "transportation_app.bronze.trips"
city_table = "transportation_app.bronze.city"

@dp.table(
    name = "transportation_app.silver.Chandigarh_trip",
    comment="Created from bronze table"
)
@dp.expect_all_or_drop({
    "valid_date": "Date is not null",
    "valid_distance": "Distance_Travelled > 0",
    "valid_fare": "Fare_Amount > 0",
    "valid_rating": "Passenger_Rating > 0",
    "valid_driver_rating": "Driver_Rating > 0"})
def chandigarh_all_data():
    city_df = spark.read.table(city_table)
    trips_df = spark.read.table(table_name)
    df = trips_df.join(city_df, trips_df.City_Id == city_df.City_Id).where(col("City_Name") == "Chandigarh").select("Trip_Id","Date","Passenger_Type","Distance_Travelled","Fare_Amount","Passenger_Rating","Driver_Rating")
    return df