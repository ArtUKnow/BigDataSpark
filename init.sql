CREATE TABLE raw_data (
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

COPY raw_data FROM '/data/MOCK_DATA.csv' DELIMITER ',' CSV HEADER;
COPY raw_data FROM '/data/MOCK_DATA (1).csv' DELIMITER ',' CSV HEADER;
COPY raw_data FROM '/data/MOCK_DATA (2).csv' DELIMITER ',' CSV HEADER;
COPY raw_data FROM '/data/MOCK_DATA (3).csv' DELIMITER ',' CSV HEADER;
COPY raw_data FROM '/data/MOCK_DATA (4).csv' DELIMITER ',' CSV HEADER;
COPY raw_data FROM '/data/MOCK_DATA (5).csv' DELIMITER ',' CSV HEADER;
COPY raw_data FROM '/data/MOCK_DATA (6).csv' DELIMITER ',' CSV HEADER;
COPY raw_data FROM '/data/MOCK_DATA (7).csv' DELIMITER ',' CSV HEADER;
COPY raw_data FROM '/data/MOCK_DATA (8).csv' DELIMITER ',' CSV HEADER;
COPY raw_data FROM '/data/MOCK_DATA (9).csv' DELIMITER ',' CSV HEADER;

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    address TEXT,
    UNIQUE (country, city, state, postal_code, address)
);

CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    source_customer_id INT,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT,
    location_id INT REFERENCES dim_location(location_id)
);

CREATE TABLE dim_pet (
    pet_id SERIAL PRIMARY KEY,
    pet_type TEXT,
    pet_category TEXT,
    pet_breed TEXT,
    pet_name TEXT,
    customer_id INT REFERENCES dim_customer(customer_id)
);

CREATE TABLE dim_seller (
    seller_id SERIAL PRIMARY KEY,
    source_seller_id INT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    location_id INT REFERENCES dim_location(location_id)
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name TEXT,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    location_id INT REFERENCES dim_location(location_id),
    UNIQUE (supplier_name)
);

CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name TEXT,
    phone TEXT,
    email TEXT,
    location_id INT REFERENCES dim_location(location_id),
    UNIQUE (store_name)
);

CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    source_product_id INT,
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
    supplier_id INT REFERENCES dim_supplier(supplier_id)
);

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    source_id INT,
    sale_date DATE,
    customer_id INT REFERENCES dim_customer(customer_id),
    seller_id INT REFERENCES dim_seller(seller_id),
    product_id INT REFERENCES dim_product(product_id),
    store_id INT REFERENCES dim_store(store_id),
    quantity INT,
    total_price NUMERIC
);
