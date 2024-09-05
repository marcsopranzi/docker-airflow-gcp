CREATE SCHEMA IF NOT EXISTS dwh;

CREATE SCHEMA IF NOT EXISTS stage;

-- Create Data Warehouse tables with Type 2 SCD
drop table if exists dwh.dim_customer;


CREATE TABLE dwh.dim_customer (
	customer_key serial4 PRIMARY KEY,
	id varchar(50) NOT NULL,
	source_system varchar(20) NOT NULL,
	customer_id int4 NULL,
	customer_name varchar(100) NULL,
	customer_address varchar(255) NULL,
    created_at timestamp NULL,
    updated_at timestamp NULL,
	dwh_load_date timestamp NULL,
	is_active bool NULL DEFAULT true,
	effective_start_date timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	effective_end_date timestamp NULL
);


drop table if exists dwh.dim_product;

CREATE TABLE dwh.dim_product (
    product_key serial4 PRIMARY KEY,
    id varchar(50) NOT NULL,
    product_id INT,
    source_system varchar(20) NOT NULL,
    product_name VARCHAR(100),
    product_category VARCHAR(100),
    product_price DECIMAL(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    dwh_load_date timestamp,
    is_active BOOLEAN DEFAULT TRUE,
    effective_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_end_date TIMESTAMP
);


drop table if exists dwh.dim_store;

CREATE TABLE dwh.dim_store (
    store_key serial4 PRIMARY KEY,
    id varchar(50) NOT NULL,
    source_system varchar(20) NOT NULL,
    store_id INT,
    store_name VARCHAR(100),
    store_location VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    dwh_load_date timestamp,
    is_active BOOLEAN DEFAULT TRUE,
    effective_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_end_date TIMESTAMP
);



DROP TABLE IF EXISTS dwh.fact_sales;

CREATE TABLE dwh.fact_sales (
    sales_key serial8 PRIMARY KEY,
    sales_id INT,
    id varchar(50) NOT NULL,
    source_system varchar(20) NOT NULL,
    customer_key INT,
    product_key INT,
    store_key INT,
    sale_time TIMESTAMP,
    sale_date_key DATE,
    quantity INT,
    total_amount DECIMAL(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    dwh_load_date TIMESTAMP,
    unique(id));