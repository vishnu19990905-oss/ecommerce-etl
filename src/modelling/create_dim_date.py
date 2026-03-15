from pyspark.sql.functions import *

def create_dim_date(df):

    dim_date = df.select(
        col("order_purchase_date").alias("date")
    ).dropDuplicates()

    dim_date = dim_date.withColumn("year", year(col("date"))) \
                       .withColumn("month", month(col("date"))) \
                       .withColumn("quarter", quarter(col("date"))) \
                       .withColumn("day_of_week", dayofweek(col("date")))

    return dim_date
