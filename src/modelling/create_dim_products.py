from pyspark.sql.functions import col

def create_dim_products(df):

    dim_products = df.select(
        col("product_id"),
        col("product_category_name")
    ).dropDuplicates(["product_id"])

    return dim_products
