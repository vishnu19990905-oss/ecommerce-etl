from pyspark.sql.functions import col

def join_datasets(df_Customers,
                  df_Orders,
                  df_Order_Items,
                  df_Payments,
                  df_Products):

    customers = df_Customers.alias("c")
    orders = df_Orders.alias("o")
    items = df_Order_Items.alias("oi")
    payments = df_Payments.alias("p")
    products = df_Products.alias("pr")

    # Orders + Customers
    orders_customers = orders.join(
        customers,
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )

    # Join Order Items
    orders_items = orders_customers.join(
        items,
        col("o.order_id") == col("oi.order_id"),
        "left"
    )

    # Join Products
    orders_products = orders_items.join(
        products,
        col("oi.product_id") == col("pr.product_id"),
        "left"
    )

    # Join Payments
    final_df = orders_products.join(
        payments,
        col("o.order_id") == col("p.order_id"),
        "left"
    )

    final_df = final_df.select(
        col("o.order_id"),
        col("c.customer_id"),
        col("c.customer_city"),
        col("c.customer_state"),
        col("pr.product_id"),
        col("pr.product_category_name"),
        col("o.delivery_time_days"),
        col("oi.price"),
        col("oi.shipping_charges"),
        col("oi.item_total_amount"),
        col("p.payment_type"),
        col("p.payment_value"),
        col("p.is_installment"),
        col("o.order_status"),
        col("o.order_purchase_timestamp"),
        col("o.order_purchase_date")
    )

    return final_df

