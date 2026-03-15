from pyspark.sql import SparkSession

# -----------------------------
# Ingestion Layer
# -----------------------------
from ingestion import (
    read_Customers,
    read_Orders,
    read_OrderItems,
    read_Products,
    read_Payments
)

# -----------------------------
# Transformation Layer
# -----------------------------
from transformation import (
    clean_Customers,
    clean_Orders,
    clean_OrderItems,
    clean_Products,
    clean_Payments,
    derive_orders_columns,
    derive_order_items_columns,
    derive_payments_columns,
    join_datasets
)

# -----------------------------
# Modelling Layer
# -----------------------------
from modelling import (
    create_dim_customers,
    create_dim_products,
    create_dim_payments,
    create_dim_date,
    create_fact_sales
)

# -----------------------------
# Validation Layer
# -----------------------------
from validation import (
    check_duplicates,
    check_nulls
)


def create_spark_session():
    spark = SparkSession.builder \
        .appName("Ecommerce ETL Pipeline") \
        .getOrCreate()
    return spark


def main():

    spark = create_spark_session()

    print("Starting ETL Pipeline...")

    # -----------------------------
    # Raw Dataset Paths
    # -----------------------------
    customers_path = "../data/raw/df_Customers.csv"
    orders_path = "../data/raw/df_Orders.csv"
    orderitems_path = "../data/raw/df_OrderItems.json"
    products_path = "../data/raw/df_Products.csv"
    payments_path = "../data/raw/df_Payments.json"

    # -----------------------------
    # Ingestion Layer
    # -----------------------------
    print("Reading raw datasets...")

    customers_df = read_Customers(spark, customers_path)
    orders_df = read_Orders(spark, orders_path)
    order_items_df = read_OrderItems(spark, orderitems_path)
    products_df = read_Products(spark, products_path)
    payments_df = read_Payments(spark, payments_path)

    # -----------------------------
    # Cleaning Layer
    # -----------------------------
    print("Cleaning datasets...")

    customers_clean = clean_Customers(customers_df)
    orders_clean = clean_Orders(orders_df)
    order_items_clean = clean_OrderItems(order_items_df)
    products_clean = clean_Products(products_df)
    payments_clean = clean_Payments(payments_df)

    # -----------------------------
    # Derived Columns Layer
    # -----------------------------
    print("Creating derived columns...")

    orders_enriched = derive_orders_columns(orders_clean)
    order_items_enriched = derive_order_items_columns(order_items_clean)
    payments_enriched = derive_payments_columns(payments_clean)

    # -----------------------------
    # Save Processed Layer
    # -----------------------------
    print("Writing processed layer...")

    customers_clean.write.mode("overwrite").parquet("../data/processed/customers")
    orders_enriched.write.mode("overwrite").parquet("../data/processed/orders")
    order_items_enriched.write.mode("overwrite").parquet("../data/processed/order_items")
    products_clean.write.mode("overwrite").parquet("../data/processed/products")
    payments_enriched.write.mode("overwrite").parquet("../data/processed/payments")

    # -----------------------------
    # Join Datasets
    # -----------------------------
    print("Joining datasets...")

    final_df = join_datasets(
        customers_clean,
        orders_enriched,
        order_items_enriched,
        payments_enriched,
        products_clean
    )

    print("Final dataset count:", final_df.count())
    final_df.show(5)

    # -----------------------------
    # Validation Layer
    # -----------------------------
    print("Running validations...")

    check_duplicates(final_df, ["order_id", "product_id"])

    check_nulls(final_df, [
        "order_id",
        "customer_id",
        "product_id",
        "order_purchase_date",
        "price",
        "payment_value"
    ])

    # -----------------------------
    # Modelling Layer
    # -----------------------------
    print("Creating dimension tables...")

    dim_customers = create_dim_customers(final_df)
    dim_products = create_dim_products(final_df)
    dim_payments = create_dim_payments(final_df)
    dim_date = create_dim_date(final_df)

    print("Creating fact table...")

    fact_sales = create_fact_sales(final_df)

    print("dim_customers:", dim_customers.count())
    print("dim_products:", dim_products.count())
    print("dim_payments:", dim_payments.count())
    print("dim_date:", dim_date.count())
    print("fact_sales:", fact_sales.count())

    # -----------------------------
    # Save Warehouse Layer
    # -----------------------------
    print("Writing warehouse tables...")

    dim_customers.write.mode("overwrite").parquet("../data/warehouse/dim_customers")
    dim_products.write.mode("overwrite").parquet("../data/warehouse/dim_products")
    dim_payments.write.mode("overwrite").parquet("../data/warehouse/dim_payments")
    dim_date.write.mode("overwrite").parquet("../data/warehouse/dim_date")

    fact_sales.write \
        .mode("overwrite") \
        .partitionBy("order_purchase_date") \
        .parquet("../data/warehouse/fact_sales")

    print("ETL Pipeline Completed Successfully!")

    spark.stop()


if __name__ == "__main__":
    main()
