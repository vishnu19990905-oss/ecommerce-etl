from pyspark.sql.types import *

def read_Orders(spark, Orders_path):

    Orders_Schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_timestamp", TimestampType(), True),
        StructField("order_estimated_delivery_date", DateType(), True),
        StructField("Orders_corrupt_record", StringType(), True)
    ])

    df_Orders = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "Orders_corrupt_record") \
        .option("header", "true") \
        .schema(Orders_Schema) \
        .csv(Orders_path)

    return df_Orders

