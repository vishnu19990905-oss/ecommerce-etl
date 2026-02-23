from pyspark.sql.types import *

def read_Customers(spark, path):

    Customers_Schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_zip_code_prefix", IntegerType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("Customers_corrupt_record", StringType(), True)
    ])

    df_Customers = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "Customers_corrupt_record") \
        .option("header", "true") \
        .schema(Customers_Schema) \
        .csv(path)

    return df_Customers
