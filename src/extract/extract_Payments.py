def extract_Payments(spark, Payments_path):

    df_check = spark.read.option("header", "true").csv(Payments_path)

    if df_check.rdd.isEmpty():
        raise ValueError("Payments file is empty")

    return Payments_path
