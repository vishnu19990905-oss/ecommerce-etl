from pyspark.sql.types import *

def read_OrderItems(spark, OrderItems_path):

    Order_Items_Schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("shipping_charges", DoubleType(), True),
        StructField("Order_Items_corrupt_record", StringType(), True)
    ])

    df_Order_Items = (
        spark.read
            .option("multiline", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "Order_Items_corrupt_record") \
            .schema(Order_Items_Schema) \
            .json(OrderItems_path)
    )

    return df_Order_Items

