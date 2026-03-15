from pyspark.sql.functions import *

def create_fact_sales(df):

    fact_sales = df.select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("order_purchase_date"),
        col("price"),
        col("shipping_charges"),
        col("item_total_amount"),
        col("payment_value"),
        col("delivery_time_days"),
        col("payment_type")
    )

    return fact_sales
