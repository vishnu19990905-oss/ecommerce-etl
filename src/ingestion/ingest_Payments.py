from pyspark.sql.types import *

def read_Payments(spark, Payments_path):

    payments_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
        StructField("payments_corrupt_record", StringType(), True)
    ])

    df_Payments = (
        spark.read
            .option("mode", "PERMISSIVE") \
            .option("multiline", "true") \
            .option("columnNameOfCorruptRecord", "payments_corrupt_record") \
            .schema(payments_schema) \
            .json(Payments_path)
    )

    return df_Payments
