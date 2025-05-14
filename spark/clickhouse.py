from pyspark.sql import SparkSession, functions
from pyspark.sql.window import Window


spark = (
    SparkSession
    .builder
    .appName("ReportsToClickHouse")
    .config(
        "spark.jars",
        "/opt/spark/jars/postgresql-42.6.0.jar,"
        "/opt/spark/jars/clickhouse-jdbc-0.4.6.jar"
    )
    .getOrCreate()
)

postgres_url = "jdbc:postgresql://postgres:5432/spark_db"
postgres_props = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "org.postgresql.Driver"
}

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
clickhouse_driver = "com.clickhouse.jdbc.ClickHouseDriver"

fact_sales = spark.read.jdbc(url=postgres_url, table="fact_sales", properties=postgres_props)
d_product = spark.read.jdbc(url=postgres_url, table="dim_product", properties=postgres_props)
d_customer = spark.read.jdbc(url=postgres_url, table="dim_customer", properties=postgres_props)
d_date = spark.read.jdbc(url=postgres_url, table="dim_date", properties=postgres_props)
d_store = spark.read.jdbc(url=postgres_url, table="dim_store", properties=postgres_props)
d_supplier = spark.read.jdbc(url=postgres_url, table="dim_supplier", properties=postgres_props)

# Топ-10 самых продаваемых продуктов
top10_products = (
    fact_sales.groupBy("product_sk")
    .agg(
        functions.sum("sale_quantity").alias("total_sold_units"),
        functions.sum("sale_total_price").alias("total_revenue")
    )
    .join(d_product, "product_sk")
    .select("product_id", "name", "category", "total_sold_units", "total_revenue")
    .orderBy(functions.desc("total_sold_units"))
    .limit(10)
)
top10_products.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "top10_products") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Общая выручка по категориям продуктов
revenue_by_category = (
    fact_sales.join(d_product, "product_sk")
    .groupBy("category")
    .agg(functions.sum("sale_total_price").alias("category_revenue"))
    .orderBy(functions.desc("category_revenue"))
)
revenue_by_category.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "revenue_by_category") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Средний рейтинг и количество отзывов для каждого продукта
product_ratings = d_product.select(
    "product_id", "name", "category", "rating", "reviews"
)
product_ratings.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "product_ratings") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Топ-10 клиентов с наибольшей общей суммой покупок
top10_customers = (
    fact_sales.groupBy("customer_sk")
    .agg(functions.sum("sale_total_price").alias("total_purchase_sum"))
    .join(d_customer, "customer_sk")
    .select("customer_id", "first_name", "last_name", "country", "total_purchase_sum")
    .orderBy(functions.desc("total_purchase_sum"))
    .limit(10)
)
top10_customers.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "top10_customers") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Распределение клиентов по странам
customers_by_country = (
    d_customer.groupBy("country")
    .agg(functions.countDistinct("customer_id").alias("customer_count"))
    .orderBy(functions.desc("customer_count"))
)
customers_by_country.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "customers_by_country") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Средний чек для каждого клиента
average_order_by_customer = (
    fact_sales.groupBy("customer_sk")
    .agg((functions.sum("sale_total_price") / functions.count("sale_quantity")).alias("average_order_value"))
    .join(d_customer, "customer_sk")
    .select("customer_id", "average_order_value")
)
average_order_by_customer.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "average_order_by_customer") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Месячные и годовые тренды продаж
monthly_trends = (
    fact_sales.join(d_date, "date_sk")
    .groupBy("year", "month")
    .agg(
        functions.sum("sale_total_price").alias("total_revenue"),
        functions.sum("sale_quantity").alias("total_quantity")
    )
    .orderBy("year", "month")
)
monthly_trends.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "monthly_trends") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

yearly_trends = (
    fact_sales.join(d_date, "date_sk")
    .groupBy("year")
    .agg(
        functions.sum("sale_total_price").alias("total_revenue"),
        functions.sum("sale_quantity").alias("total_quantity")
    )
    .orderBy("year")
)
yearly_trends.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "yearly_trends") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Сравнение выручки за разные периоды (месячные изменения)
mom = (
    monthly_trends
    .withColumn(
        "prev_month_revenue",
        functions.lag("total_revenue").over(Window.orderBy("year", "month"))
    )
    .na.fill({"prev_month_revenue": 0.0})
    .withColumn(
        "mom_change",
        (functions.col("total_revenue") - functions.col("prev_month_revenue")) /
        functions.when(functions.col("prev_month_revenue") == 0, 1)
          .otherwise(functions.col("prev_month_revenue"))
    )
    .withColumn(
        "mom_change_pct",
        functions.round(functions.col("mom_change") * 100, 2)
    )
    .na.fill({"mom_change": 0.0, "mom_change_pct": 0.0})
    .select(
        "year",
        "month",
        "total_revenue",
        "prev_month_revenue",
        "mom_change",
        "mom_change_pct"
    )
)

mom.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "mom_trends") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Средний размер заказа по месяцам
average_order_value = (
    monthly_trends
    .withColumn("average_order_value", functions.col("total_revenue") / functions.col("total_quantity"))
    .select("year", "month", "average_order_value")
)
average_order_value.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "average_order_value_by_month") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Топ-5 магазинов с наибольшей выручкой
top5_stores = (
    fact_sales.groupBy("store_sk")
    .agg(functions.sum("sale_total_price").alias("total_revenue"))
    .join(d_store, "store_sk")
    .select("name", "city", "country", "total_revenue")
    .orderBy(functions.desc("total_revenue"))
    .limit(5)
)
top5_stores.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "top5_stores") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Распределение продаж по городам и странам
sales_by_city_country = (
    fact_sales.join(d_store, "store_sk")
    .groupBy("city", "country")
    .agg(
        functions.sum("sale_total_price").alias("total_revenue"),
        functions.sum("sale_quantity").alias("total_quantity")
    )
    .orderBy(functions.desc("total_revenue"))
)
sales_by_city_country.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "sales_by_city_country") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Средний чек для каждого магазина
average_order_by_store = (
    fact_sales.groupBy("store_sk")
    .agg((functions.sum("sale_total_price") / functions.count("sale_quantity")).alias("average_order_value"))
    .join(d_store, "store_sk")
    .select("name", "average_order_value")
)
average_order_by_store.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "average_order_by_store") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Топ-5 поставщиков с наибольшей выручкой
top5_suppliers = (
    fact_sales.groupBy("supplier_sk")
    .agg(functions.sum("sale_total_price").alias("total_revenue"))
    .join(d_supplier, "supplier_sk")
    .select("name", "city", "country", "total_revenue")
    .orderBy(functions.desc("total_revenue"))
    .limit(5)
)
top5_suppliers.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "top5_suppliers") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Средняя цена товаров от каждого поставщика
average_price_by_supplier = (
    fact_sales.groupBy("supplier_sk")
    .agg(functions.avg("unit_price").alias("average_product_price"))
    .join(d_supplier, "supplier_sk")
    .select("name", "average_product_price")
)
average_price_by_supplier.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "average_price_by_supplier") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Распределение продаж по странам поставщиков
sales_by_supplier_country = (
    fact_sales.join(d_supplier, "supplier_sk")
    .groupBy("country")
    .agg(
        functions.sum("sale_total_price").alias("total_revenue"),
        functions.sum("sale_quantity").alias("total_quantity")
    )
    .orderBy(functions.desc("total_revenue"))
)
sales_by_supplier_country.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "sales_by_supplier_country") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Продукты с наивысшим и наименьшим рейтингом
highest_rated = d_product.orderBy(functions.desc("rating")).limit(10) \
    .select("product_id", "name", "rating")
highest_rated.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "highest_rated_products") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

lowest_rated = d_product.orderBy(functions.asc("rating")).limit(10) \
    .select("product_id", "name", "rating")
lowest_rated.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "lowest_rated_products") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Корреляция между рейтингом и объемом продаж
rating_sales = (
    fact_sales.join(d_product, "product_sk")
    .groupBy("product_id", "name")
    .agg(
        functions.avg("rating").alias("average_rating"),
        functions.sum("sale_quantity").alias("total_quantity")
    )
)
corr_value = rating_sales.stat.corr("average_rating", "total_quantity")
corr_df = spark.createDataFrame([(corr_value,)], ["rating_sales_correlation"])
corr_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "rating_sales_correlation") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# Продукты с наибольшим количеством отзывов
top_reviewed_products = d_product.orderBy(functions.desc("reviews")).limit(10) \
    .select("product_id", "name", "reviews")
top_reviewed_products.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", clickhouse_url) \
    .option("dbtable", "top_reviewed_products") \
    .option("driver", clickhouse_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

spark.stop()
