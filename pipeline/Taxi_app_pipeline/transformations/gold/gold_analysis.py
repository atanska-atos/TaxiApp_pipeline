from pyspark import pipelines as dp
from pyspark.sql.functions import *
import pyspark.pandas as pd

@dp.materialized_view(
    name = "transportation_app.gold.All_agg_values",
    comment="Basic aggregations"
)
def avg_values_by_city():
    df = spark.read.table("transportation_app.silver.trips")
    count_all = df.groupby("City_Name").agg(count("*").alias("All_travels"), round(avg("Distance_Travelled"), 2).alias("Avg_Distance"), round(avg("Fare_Amount"), 2).alias("Avg_Fare_Amount"), mode("Passenger_Rating").alias("Mode_Passenger_Rating"), mode("Driver_Rating").alias("Mode_Driver_Rating"), round(avg((df["Fare_Amount"] / df["Distance_Travelled"])), 2).alias("Avg_Fare_Per_Distance")).sort("Avg_Fare_Per_Distance")

    return count_all

@dp.materialized_view(
    name = "transportation_app.gold.Repeated_Percent",
    comment="Percent of repeated passengers"
)
def repeated_passengers():
    percentage_repeated = spark.sql("SELECT City_Name, ROUND((COUNT(CASE WHEN Passenger_Type = 'repeated' THEN 1 END) / COUNT(*)) * 100, 2)  AS Repeated_Percentage FROM transportation_app.silver.trips GROUP BY City_Name ORDER BY Repeated_Percentage DESC")

    return percentage_repeated