from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def clean_OrderItems(df_OrderItems: DataFrame) -> DataFrame:

    # Remove Corrupt Records
    df_OrderItems = df_OrderItems.filter(df_OrderItems.Order_Items_corrupt_record.isNull())

    # Drop rows where primary key is null
    df_OrderItems = df_OrderItems.dropna(subset=["order_id"])

    # Remove duplicates based on business key
    df_OrderItems = df_OrderItems.dropDuplicates(["order_id"])

    # Handle Null Values
    df_OrderItems = df_OrderItems.fillna({
        "shipping_charges": 0
    })

    # Drop corrupt column
    df_OrderItems = df_OrderItems.drop("Order_Items_corrupt_record")

    return df_OrderItems

