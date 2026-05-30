from pyspark.sql.functions import expr
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, when

spark = SparkSession.builder \
    .appName("ETL_Postgres") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/analytics"
properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

raw_df = spark.read.jdbc(url=jdbc_url, table="raw_data", properties=properties)
raw_df.createOrReplaceTempView("raw_data")

dim_location = spark.sql("""
    SELECT DISTINCT country, city, state, postal_code, address FROM (
        SELECT customer_country as country, NULL as city, NULL as state, customer_postal_code as postal_code, NULL as address FROM raw_data WHERE customer_country IS NOT NULL OR customer_postal_code IS NOT NULL
        UNION
        SELECT seller_country, NULL, NULL, seller_postal_code, NULL FROM raw_data WHERE seller_country IS NOT NULL OR seller_postal_code IS NOT NULL
        UNION
        SELECT store_country, store_city, store_state, NULL, store_location FROM raw_data WHERE store_country IS NOT NULL OR store_city IS NOT NULL OR store_state IS NOT NULL OR store_location IS NOT NULL
        UNION
        SELECT supplier_country, supplier_city, NULL, NULL, supplier_address FROM raw_data WHERE supplier_country IS NOT NULL OR supplier_city IS NOT NULL OR supplier_address IS NOT NULL
    )
""").withColumn("location_id", row_number().over(Window.orderBy(expr("1"))))

dim_location.createOrReplaceTempView("dim_location_temp")

dim_customer = spark.sql("""
    SELECT DISTINCT 
        CAST(r.sale_customer_id AS INT) as source_customer_id,
        r.customer_first_name as first_name,
        r.customer_last_name as last_name,
        CAST(r.customer_age AS INT) as age,
        r.customer_email as email,
        l.location_id
    FROM raw_data r
    LEFT JOIN dim_location_temp l ON 
        (l.country = r.customer_country OR (l.country IS NULL AND r.customer_country IS NULL)) AND
        (l.postal_code = r.customer_postal_code OR (l.postal_code IS NULL AND r.customer_postal_code IS NULL)) AND
        l.city IS NULL AND l.state IS NULL AND l.address IS NULL
    WHERE r.sale_customer_id IS NOT NULL AND r.sale_customer_id != ''
""").withColumn("customer_id", row_number().over(Window.orderBy(expr("1"))))
dim_customer.createOrReplaceTempView("dim_customer_temp")

dim_pet = spark.sql("""
    SELECT DISTINCT 
        r.customer_pet_type as pet_type,
        r.pet_category,
        r.customer_pet_breed as pet_breed,
        r.customer_pet_name as pet_name,
        c.customer_id
    FROM raw_data r
    JOIN dim_customer_temp c ON c.source_customer_id = CAST(r.sale_customer_id AS INT)
    WHERE r.customer_pet_type IS NOT NULL AND r.customer_pet_type != ''
""").withColumn("pet_id", row_number().over(Window.orderBy(expr("1"))))
dim_pet.createOrReplaceTempView("dim_pet_temp")

dim_seller = spark.sql("""
    SELECT DISTINCT 
        CAST(r.sale_seller_id AS INT) as source_seller_id,
        r.seller_first_name as first_name,
        r.seller_last_name as last_name,
        r.seller_email as email,
        l.location_id
    FROM raw_data r
    LEFT JOIN dim_location_temp l ON 
        (l.country = r.seller_country OR (l.country IS NULL AND r.seller_country IS NULL)) AND
        (l.postal_code = r.seller_postal_code OR (l.postal_code IS NULL AND r.seller_postal_code IS NULL)) AND
        l.city IS NULL AND l.state IS NULL AND l.address IS NULL
    WHERE r.sale_seller_id IS NOT NULL AND r.sale_seller_id != ''
""").withColumn("seller_id", row_number().over(Window.orderBy(expr("1"))))
dim_seller.createOrReplaceTempView("dim_seller_temp")

dim_supplier = spark.sql("""
    SELECT DISTINCT 
        r.supplier_name,
        r.supplier_contact as contact_name,
        r.supplier_email as email,
        r.supplier_phone as phone,
        l.location_id
    FROM raw_data r
    LEFT JOIN dim_location_temp l ON 
        (l.country = r.supplier_country OR (l.country IS NULL AND r.supplier_country IS NULL)) AND
        (l.city = r.supplier_city OR (l.city IS NULL AND r.supplier_city IS NULL)) AND
        (l.address = r.supplier_address OR (l.address IS NULL AND r.supplier_address IS NULL)) AND
        l.state IS NULL AND l.postal_code IS NULL
    WHERE r.supplier_name IS NOT NULL AND r.supplier_name != ''
""").withColumn("supplier_id", row_number().over(Window.orderBy(expr("1"))))
dim_supplier.createOrReplaceTempView("dim_supplier_temp")

dim_store = spark.sql("""
    SELECT DISTINCT 
        r.store_name,
        r.store_phone as phone,
        r.store_email as email,
        l.location_id
    FROM raw_data r
    LEFT JOIN dim_location_temp l ON 
        (l.country = r.store_country OR (l.country IS NULL AND r.store_country IS NULL)) AND
        (l.city = r.store_city OR (l.city IS NULL AND r.store_city IS NULL)) AND
        (l.state = r.store_state OR (l.state IS NULL AND r.store_state IS NULL)) AND
        (l.address = r.store_location OR (l.address IS NULL AND r.store_location IS NULL)) AND
        l.postal_code IS NULL
    WHERE r.store_name IS NOT NULL AND r.store_name != ''
""").withColumn("store_id", row_number().over(Window.orderBy(expr("1"))))
dim_store.createOrReplaceTempView("dim_store_temp")

dim_product = spark.sql("""
    SELECT DISTINCT 
        CAST(r.sale_product_id AS INT) as source_product_id,
        r.product_name,
        r.product_category as category,
        CAST(r.product_price AS NUMERIC) as price,
        CAST(r.product_weight AS NUMERIC) as weight,
        r.product_color as color,
        r.product_size as size,
        r.product_brand as brand,
        r.product_material as material,
        r.product_description as description,
        CAST(r.product_rating AS NUMERIC) as rating,
        CAST(r.product_reviews AS INT) as reviews,
        TO_DATE(r.product_release_date, 'MM/dd/yyyy') as release_date,
        TO_DATE(r.product_expiry_date, 'MM/dd/yyyy') as expiry_date,
        s.supplier_id
    FROM raw_data r
    LEFT JOIN dim_supplier_temp s ON s.supplier_name = r.supplier_name
    WHERE r.sale_product_id IS NOT NULL AND r.sale_product_id != ''
""").withColumn("product_id", row_number().over(Window.orderBy(expr("1"))))
dim_product.createOrReplaceTempView("dim_product_temp")

fact_sales = spark.sql("""
    SELECT 
        CAST(r.id AS INT) as source_id,
        TO_DATE(r.sale_date, 'MM/dd/yyyy') as sale_date,
        c.customer_id,
        sl.seller_id,
        p.product_id,
        st.store_id,
        CAST(r.sale_quantity AS INT) as quantity,
        CAST(r.sale_total_price AS NUMERIC) as total_price
    FROM raw_data r
    LEFT JOIN dim_customer_temp c ON c.source_customer_id = CAST(r.sale_customer_id AS INT)
    LEFT JOIN dim_seller_temp sl ON sl.source_seller_id = CAST(r.sale_seller_id AS INT)
    LEFT JOIN dim_product_temp p ON p.source_product_id = CAST(r.sale_product_id AS INT)
    LEFT JOIN dim_store_temp st ON st.store_name = r.store_name
""")

dim_location.coalesce(1).write.jdbc(url=jdbc_url, table="dim_location", mode="append", properties=properties)
dim_customer.coalesce(1).write.jdbc(url=jdbc_url, table="dim_customer", mode="append", properties=properties)
dim_pet.coalesce(1).write.jdbc(url=jdbc_url, table="dim_pet", mode="append", properties=properties)
dim_seller.coalesce(1).write.jdbc(url=jdbc_url, table="dim_seller", mode="append", properties=properties)
dim_supplier.coalesce(1).write.jdbc(url=jdbc_url, table="dim_supplier", mode="append", properties=properties)
dim_store.coalesce(1).write.jdbc(url=jdbc_url, table="dim_store", mode="append", properties=properties)
dim_product.coalesce(1).write.jdbc(url=jdbc_url, table="dim_product", mode="append", properties=properties)
fact_sales.coalesce(1).write.jdbc(url=jdbc_url, table="fact_sales", mode="append", properties=properties)

spark.stop()
