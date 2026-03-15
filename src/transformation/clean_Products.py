from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def clean_Products(df_Products: DataFrame) -> DataFrame:

    # Remove Corrupt Records
    df_Products = df_Products.filter(df_Products.products_corrupt_record.isNull())

    # Drop rows where primary key is null
    df_Products = df_Products.dropna(subset=["product_id"])

    # Remove duplicates based on business key
    df_Products = df_Products.dropDuplicates(["product_id"])

    # Handle Null Values
    df_Products = df_Products.fillna({
        "product_weight_g": 0,
        "product_length_cm": 0,
        "product_height_cm": 0,
        "product_width_cm": 0
    })

    # Drop corrupt column
    df_Products = df_Products.drop("products_corrupt_record")

    return df_Products


