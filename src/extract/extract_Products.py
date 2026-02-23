def extract_Products(spark, Products_path):

    df_check = spark.read.option("header", "true").csv(Products_path)

    if df_check.rdd.isEmpty():
        raise ValueError("Products file is empty")

    return Products_path
