from pyspark.sql.functions import *

def create_dim_customers(df):

    dim_customers = df.select(
        col("customer_id"),
        col("customer_city"),
        col("customer_state")
    ).dropDuplicates(["customer_id"])

    return dim_customers
