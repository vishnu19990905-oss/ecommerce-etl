def extract_OrderItems(spark, OrderItems_path):

    df_check = spark.read.option("header", "true").csv(OrderItems_path)

    if df_check.rdd.isEmpty():
        raise ValueError("OrderItems file is empty")

    return OrderItems_path
