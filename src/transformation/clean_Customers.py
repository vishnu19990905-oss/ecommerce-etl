from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def clean_Customers(df_Customers: DataFrame) -> DataFrame:

    # Remove Corrupt Records
    df_Customers = df_Customers.filter(df_Customers.Customers_corrupt_record.isNull())

    # Drop rows where primary key is null
    df_Customers = df_Customers.dropna(subset=["customer_id"])

    # Remove duplicates based on business key
    df_Customers = df_Customers.dropDuplicates(["customer_id"])

    # Handle Null Values
    df_Customers = df_Customers.fillna({
        "customer_city": "unknown",
        "customer_state": "NA"
    })

    # Drop corrupt column
    df_Customers = df_Customers.drop("Customers_corrupt_record")

    return df_Customers
