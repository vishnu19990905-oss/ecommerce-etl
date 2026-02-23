from pyspark.sql.types import *

def read_Products(spark, products_path):

    product_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_category_name", StringType(), True),
        StructField("product_weight_g", DoubleType(), True),
        StructField("product_length_cm", DoubleType(), True),
        StructField("product_height_cm", DoubleType(), True),
        StructField("product_width_cm", DoubleType(), True),
        StructField("products_corrupt_record", StringType(), True)
    ])

    df_Products = (
        spark.read
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "products_corrupt_record")
            .option("header", "true")
            .schema(product_schema)
            .csv(products_path)
    )

    return df_Products
