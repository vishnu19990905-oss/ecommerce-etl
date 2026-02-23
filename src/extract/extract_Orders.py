def extract_Orders(spark, Orders_path):

    df_check = spark.read.option("header", "true").csv(Orders_path)

    if df_check.rdd.isEmpty():
        raise ValueError("Orders file is empty")

    return Orders_path
