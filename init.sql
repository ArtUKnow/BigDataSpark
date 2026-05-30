CREATE TABLE staging_raw_data (
    id INT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age TEXT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price TEXT,
    product_quantity TEXT,
    sale_date TEXT,
    sale_customer_id TEXT,
    sale_seller_id TEXT,
    sale_product_id TEXT,
    sale_quantity TEXT,
    sale_total_price TEXT,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight TEXT,
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating TEXT,
    product_reviews TEXT,
    product_release_date TEXT,
    product_expiry_date TEXT,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE raw_data (
    file_num INT,
    LIKE staging_raw_data
);

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA.csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 0, * FROM staging_raw_data;

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA (1).csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 1, * FROM staging_raw_data;

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA (2).csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 2, * FROM staging_raw_data;

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA (3).csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 3, * FROM staging_raw_data;

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA (4).csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 4, * FROM staging_raw_data;

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA (5).csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 5, * FROM staging_raw_data;

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA (6).csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 6, * FROM staging_raw_data;

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA (7).csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 7, * FROM staging_raw_data;

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA (8).csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 8, * FROM staging_raw_data;

TRUNCATE staging_raw_data;
COPY staging_raw_data FROM '/data/MOCK_DATA (9).csv' DELIMITER ',' CSV HEADER;
INSERT INTO raw_data SELECT 9, * FROM staging_raw_data;

DROP TABLE staging_raw_data;

CREATE TABLE dim_location (
    location_id BIGSERIAL PRIMARY KEY,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    address TEXT,
    UNIQUE (country, city, state, postal_code, address)
);

CREATE TABLE dim_customer (
    customer_id BIGSERIAL PRIMARY KEY,
    source_customer_id BIGINT,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT,
    location_id BIGINT REFERENCES dim_location(location_id)
);

CREATE TABLE dim_pet (
    pet_id BIGSERIAL PRIMARY KEY,
    pet_type TEXT,
    pet_category TEXT,
    pet_breed TEXT,
    pet_name TEXT,
    customer_id BIGINT REFERENCES dim_customer(customer_id)
);

CREATE TABLE dim_seller (
    seller_id BIGSERIAL PRIMARY KEY,
    source_seller_id BIGINT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    location_id BIGINT REFERENCES dim_location(location_id)
);

CREATE TABLE dim_supplier (
    supplier_id BIGSERIAL PRIMARY KEY,
    supplier_name TEXT,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    location_id BIGINT REFERENCES dim_location(location_id),
    UNIQUE (supplier_name)
);

CREATE TABLE dim_store (
    store_id BIGSERIAL PRIMARY KEY,
    store_name TEXT,
    phone TEXT,
    email TEXT,
    location_id BIGINT REFERENCES dim_location(location_id),
    UNIQUE (store_name)
);

CREATE TABLE dim_product (
    product_id BIGSERIAL PRIMARY KEY,
    source_product_id BIGINT,
    product_name TEXT,
    category TEXT,
    price NUMERIC,
    weight NUMERIC,
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC,
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    supplier_id BIGINT REFERENCES dim_supplier(supplier_id)
);

CREATE TABLE fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    source_id INT,
    sale_date DATE,
    customer_id BIGINT REFERENCES dim_customer(customer_id),
    seller_id BIGINT REFERENCES dim_seller(seller_id),
    product_id BIGINT REFERENCES dim_product(product_id),
    store_id BIGINT REFERENCES dim_store(store_id),
    quantity INT,
    total_price NUMERIC
);
