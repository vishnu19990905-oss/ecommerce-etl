from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def clean_Payments(df_Payments: DataFrame) -> DataFrame:

    # Remove Corrupt Records
    df_Payments = df_Payments.filter(df_Payments.payments_corrupt_record.isNull())

    # Drop rows where primary key is null
    df_Payments = df_Payments.dropna(subset=["order_id"])

    # Remove duplicates based on business key
    df_Payments = df_Payments.dropDuplicates(["order_id","payment_type"])

    # Handle Null Values
    df_Payments = df_Payments.fillna({
        "payment_value": 0
    })

    # Drop corrupt column
    df_Payments = df_Payments.drop("payments_corrupt_record")

    return df_Payments

