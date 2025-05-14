from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, expr
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("ETL to Star Schema")
    .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
    .getOrCreate()
)

postgres_url = "jdbc:postgresql://postgres:5432/spark_db"
postgres_props = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "org.postgresql.Driver"
}

stg = (
    spark.read
    .format("jdbc")
    .option("url", postgres_url)
    .option("dbtable", "mock_data")
    .option("user", postgres_props["user"])
    .option("password", postgres_props["password"])
    .option("driver", postgres_props["driver"])
    .load()
)


def fill_dim_tables(df, partition_col, order_col, selects, renames, target_table):
    win = Window.partitionBy(partition_col).orderBy(order_col)
    df_dim = (
        df
        .select(partition_col, order_col, *selects)
        .withColumn("rn", row_number().over(win))
        .filter(col("rn") == 1)
        .drop("rn", order_col)
    )
    for old_name, new_name in renames.items():
        df_dim = df_dim.withColumnRenamed(old_name, new_name)
    df_dim.write \
        .mode("append") \
        .jdbc(postgres_url, target_table, properties=postgres_props)


fill_dim_tables(
    stg,
    partition_col="sale_customer_id",
    order_col="sale_date",
    selects=[
        "customer_first_name", "customer_last_name",
        "customer_age", "customer_email",
        "customer_country", "customer_postal_code"
    ],
    renames={
        "sale_customer_id": "customer_id",
        "customer_first_name": "first_name",
        "customer_last_name": "last_name",
        "customer_age": "age",
        "customer_email": "email",
        "customer_country": "country",
        "customer_postal_code": "postal_code"
    },
    target_table="dim_customer"
)

fill_dim_tables(
    stg,
    partition_col="sale_seller_id",
    order_col="sale_date",
    selects=[
        "seller_first_name", "seller_last_name",
        "seller_email", "seller_country",
        "seller_postal_code"
    ],
    renames={
        "sale_seller_id": "seller_id",
        "seller_first_name": "first_name",
        "seller_last_name": "last_name",
        "seller_email": "email",
        "seller_country": "country",
        "seller_postal_code": "postal_code"
    },
    target_table="dim_seller"
)

fill_dim_tables(
    stg,
    partition_col="sale_product_id",
    order_col="sale_date",
    selects=[
        "product_name", "product_category", "product_weight",
        "product_color", "product_size", "product_brand",
        "product_material", "product_description",
        "product_rating", "product_reviews",
        "product_release_date", "product_expiry_date",
        "product_price"
    ],
    renames={
        "sale_product_id": "product_id",
        "product_name": "name",
        "product_category": "category",
        "product_weight": "weight",
        "product_color": "color",
        "product_size": "size",
        "product_brand": "brand",
        "product_material": "material",
        "product_description": "description",
        "product_rating": "rating",
        "product_reviews": "reviews",
        "product_release_date": "release_date",
        "product_expiry_date": "expiry_date",
        "product_price": "unit_price"
    },
    target_table="dim_product"
)

fill_dim_tables(
    stg,
    partition_col="store_name",
    order_col="sale_date",
    selects=[
        "store_location", "store_city", "store_state",
        "store_country", "store_phone", "store_email"
    ],
    renames={
        "store_name": "name",
        "store_location": "location",
        "store_city": "city",
        "store_state": "state",
        "store_country": "country",
        "store_phone": "phone",
        "store_email": "email"
    },
    target_table="dim_store"
)

fill_dim_tables(
    stg,
    partition_col="supplier_name",
    order_col="sale_date",
    selects=[
        "supplier_contact", "supplier_email", "supplier_phone",
        "supplier_address", "supplier_city", "supplier_country"
    ],
    renames={
        "supplier_name": "name",
        "supplier_contact": "contact",
        "supplier_email": "email",
        "supplier_phone": "phone",
        "supplier_address": "address",
        "supplier_city": "city",
        "supplier_country": "country"
    },
    target_table="dim_supplier"
)

dim_dates = (
    stg
    .select("sale_date")
    .distinct()
    .withColumn("year", expr("year(sale_date)"))
    .withColumn("quarter", expr("quarter(sale_date)"))
    .withColumn("month", expr("month(sale_date)"))
    .withColumn("day", expr("day(sale_date)"))
    .withColumn("weekday", expr("dayofweek(sale_date)"))
)
dim_dates.write \
    .mode("append") \
    .jdbc(postgres_url, "dim_date", properties=postgres_props)

dim_customer = spark.read.jdbc(postgres_url, "dim_customer", properties=postgres_props)
dim_seller = spark.read.jdbc(postgres_url, "dim_seller", properties=postgres_props)
dim_product = spark.read.jdbc(postgres_url, "dim_product", properties=postgres_props)
dim_store = spark.read.jdbc(postgres_url, "dim_store", properties=postgres_props)
dim_supplier = spark.read.jdbc(postgres_url, "dim_supplier", properties=postgres_props)
dim_date = spark.read.jdbc(postgres_url, "dim_date", properties=postgres_props)

fact_sales = (
    stg
    .join(dim_date, stg.sale_date == dim_date.sale_date)
    .join(dim_customer, stg.sale_customer_id == dim_customer.customer_id)
    .join(dim_seller, stg.sale_seller_id == dim_seller.seller_id)
    .join(dim_product, stg.sale_product_id == dim_product.product_id)
    .join(dim_store, stg.store_name == dim_store.name)
    .join(dim_supplier, stg.supplier_name == dim_supplier.name)
    .select(
        dim_date.date_sk.alias("date_sk"),
        dim_customer.customer_sk.alias("customer_sk"),
        dim_seller.seller_sk.alias("seller_sk"),
        dim_product.product_sk.alias("product_sk"),
        dim_store.store_sk.alias("store_sk"),
        dim_supplier.supplier_sk.alias("supplier_sk"),
        col("sale_quantity"),
        col("sale_total_price"),
        col("unit_price")
    )
)
fact_sales.write \
    .mode("append") \
    .jdbc(postgres_url, "fact_sales", properties=postgres_props)

spark.stop()
