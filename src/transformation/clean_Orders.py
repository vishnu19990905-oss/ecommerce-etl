from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def clean_Orders(df_Orders: DataFrame) -> DataFrame:

    # Remove Corrupt Records
    df_Orders = df_Orders.filter(df_Orders.Orders_corrupt_record.isNull())

    # Drop rows where primary key is null
    df_Orders = df_Orders.dropna(subset=["order_id"])

    # Remove duplicates based on business key
    df_Orders = df_Orders.dropDuplicates(["order_id"])

    # Handle Null Values
    df_Orders = df_Orders.fillna({
        "order_status": "unknown"
    })

    # Drop corrupt column
    df_Orders = df_Orders.drop("Orders_corrupt_record")

    return df_Orders

