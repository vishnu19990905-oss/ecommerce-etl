from pyspark.sql.functions import *

def create_dim_payments(df):

    dim_payments = df.select(
        col("payment_type"),
        col("is_installment")
    ).dropDuplicates()

    return dim_payments
