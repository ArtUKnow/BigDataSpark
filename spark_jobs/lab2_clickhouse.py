from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL_ClickHouse") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

jdbc_pg = "jdbc:postgresql://postgres:5432/analytics"
prop_pg = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

jdbc_ch = "jdbc:clickhouse://clickhouse:8123/analytics"
prop_ch = {
    "user": "default",
    "password": "password",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "compress": "false",
    "decompress": "false",
    "jdbcCompliant": "false"
}

fact_sales = spark.read.jdbc(url=jdbc_pg, table="fact_sales", properties=prop_pg)
dim_product = spark.read.jdbc(url=jdbc_pg, table="dim_product", properties=prop_pg)
dim_customer = spark.read.jdbc(url=jdbc_pg, table="dim_customer", properties=prop_pg)
dim_store = spark.read.jdbc(url=jdbc_pg, table="dim_store", properties=prop_pg)
dim_supplier = spark.read.jdbc(url=jdbc_pg, table="dim_supplier", properties=prop_pg)
dim_location = spark.read.jdbc(url=jdbc_pg, table="dim_location", properties=prop_pg)

fact_sales.createOrReplaceTempView("fact_sales")
dim_product.createOrReplaceTempView("dim_product")
dim_customer.createOrReplaceTempView("dim_customer")
dim_store.createOrReplaceTempView("dim_store")
dim_supplier.createOrReplaceTempView("dim_supplier")
dim_location.createOrReplaceTempView("dim_location")

report_product = spark.sql("""
    SELECT 
        p.product_name,
        p.category,
        p.rating,
        p.reviews,
        SUM(f.quantity) as total_quantity,
        SUM(f.total_price) as total_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_name, p.category, p.rating, p.reviews
""")
report_product.write.jdbc(url=jdbc_ch, table="report_product", mode="append", properties=prop_ch)

report_customer = spark.sql("""
    SELECT 
        c.first_name,
        c.last_name,
        l.country,
        SUM(f.total_price) as total_spent,
        AVG(f.total_price) as avg_check
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    LEFT JOIN dim_location l ON c.location_id = l.location_id
    GROUP BY c.first_name, c.last_name, l.country
""")
report_customer.write.jdbc(url=jdbc_ch, table="report_customer", mode="append", properties=prop_ch)

report_time = spark.sql("""
    SELECT 
        YEAR(f.sale_date) as sale_year,
        MONTH(f.sale_date) as sale_month,
        SUM(f.total_price) as total_revenue,
        AVG(f.total_price) as avg_order_size
    FROM fact_sales f
    GROUP BY YEAR(f.sale_date), MONTH(f.sale_date)
""")
report_time.write.jdbc(url=jdbc_ch, table="report_time", mode="append", properties=prop_ch)

report_store = spark.sql("""
    SELECT 
        s.store_name,
        l.city,
        l.country,
        SUM(f.total_price) as total_revenue,
        AVG(f.total_price) as avg_check
    FROM fact_sales f
    JOIN dim_store s ON f.store_id = s.store_id
    LEFT JOIN dim_location l ON s.location_id = l.location_id
    GROUP BY s.store_name, l.city, l.country
""")
report_store.write.jdbc(url=jdbc_ch, table="report_store", mode="append", properties=prop_ch)

report_supplier = spark.sql("""
    SELECT 
        sup.supplier_name,
        l.country as supplier_country,
        SUM(f.total_price) as total_revenue,
        AVG(p.price) as avg_product_price
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_supplier sup ON p.supplier_id = sup.supplier_id
    LEFT JOIN dim_location l ON sup.location_id = l.location_id
    GROUP BY sup.supplier_name, l.country
""")
report_supplier.write.jdbc(url=jdbc_ch, table="report_supplier", mode="append", properties=prop_ch)

report_quality = spark.sql("""
    SELECT 
        p.product_name,
        p.rating,
        p.reviews,
        SUM(f.quantity) as total_sold
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_name, p.rating, p.reviews
""")
report_quality.write.jdbc(url=jdbc_ch, table="report_quality", mode="append", properties=prop_ch)

spark.stop()
