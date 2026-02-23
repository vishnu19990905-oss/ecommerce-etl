def extract_Customers(spark, Customers_path):

    df_check = spark.read.option("header", "true").csv(Customers_path)

    if df_check.rdd.isEmpty():
        raise ValueError("Customers file is empty")

    return Customers_path
