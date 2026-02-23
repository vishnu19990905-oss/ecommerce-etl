from pyspark.sql import SparkSession
from extract import extract_Customers, extract_Orders, extract_OrderItems, extract_Products, extract_Payments
from ingestion import read_Customers, read_Orders, read_OrderItems, read_Products, read_Payments


def create_spark_session():
    spark = SparkSession.builder \
        .appName("Ecommerce ETL Pipeline") \
        .getOrCreate()
    return spark


def main():
    # Create Spark session
    spark = create_spark_session()

    # Define dataset paths
    Customers_path = "/home/vishnu/DE_Project/ecommerce-etl/data/practice_data/df_Customers.csv"
    Orders_path = "/home/vishnu/DE_Project/ecommerce-etl/data/practice_data/df_Orders.csv"
    OrderItems_path = "/home/vishnu/DE_Project/ecommerce-etl/data/practice_data/df_OrderItems.json"
    Products_path = "/home/vishnu/DE_Project/ecommerce-etl/data/practice_data/df_Products.csv"
    Payments_path = "/home/vishnu/DE_Project/ecommerce-etl/data/practice_data/df_Payments.json"

    # extract datasets
    customers_raw_df = extract_Customers(spark, Customers_path)
    Orders_raw_df = extract_Orders(spark, Orders_path)
    OrderItems_raw_df = extract_OrderItems(spark, OrderItems_path)
    Products_raw_df = extract_Products(spark, Products_path)
    Payments_raw_df = extract_Payments(spark, Payments_path)

    # Read datasets
    customers_df = read_Customers(spark, Customers_path)
    Orders_df = read_Orders(spark, Orders_path)
    OrderItems_df = read_OrderItems(spark, OrderItems_path)
    Products_df = read_Products(spark, Products_path)
    Payments_df = read_Payments(spark, Payments_path)

    # Just for testing ingest
    customers_df.show(5)
    Orders_df.show(5)
    OrderItems_df.show(5)
    Products_df.show(5)
    Payments_df.show(5)

    spark.stop()


if __name__ == "__main__":
    main()
