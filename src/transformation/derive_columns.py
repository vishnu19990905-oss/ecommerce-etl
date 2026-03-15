from pyspark.sql import DataFrame
from pyspark.sql.functions import *


# -----------------------------
# Orders Derived Columns
# -----------------------------
def derive_orders_columns(df_Orders: DataFrame) -> DataFrame:

    df_Orders = df_Orders.withColumn(
        "order_purchase_date",
        to_date(col("order_purchase_timestamp"))
    ).withColumn(
        "order_purchase_year",
        year(col("order_purchase_timestamp"))
    ).withColumn(
        "order_purchase_month",
        month(col("order_purchase_timestamp"))
    ).withColumn(
        "delivery_time_days",
        datediff(
            col("order_delivered_timestamp"),
            col("order_purchase_timestamp")
        )
    )

    return df_Orders


# -----------------------------
# Order Items Derived Columns
# -----------------------------
def derive_order_items_columns(df_Order_Items: DataFrame) -> DataFrame:

    df_Order_Items = df_Order_Items.withColumn(
        "item_total_amount",
        col("price") + col("shipping_charges")
    )

    return df_Order_Items


# -----------------------------
# Payments Derived Columns
# -----------------------------
def derive_payments_columns(df_Payments: DataFrame) -> DataFrame:

    df_Payments = df_Payments.withColumn(
        "is_installment",
        when(col("payment_installments") > 1, "YES")
        .otherwise("NO")
    )

    return df_Payments
