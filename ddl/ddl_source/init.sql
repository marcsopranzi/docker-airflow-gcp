drop table if exists customers;
CREATE TABLE customers (
    customer_id serial4 PRIMARY KEY,
    name VARCHAR(100),
    address VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);


drop table if exists products;
CREATE TABLE products (
    product_id serial4 PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

drop table if exists stores;
CREATE TABLE stores (
    store_id serial2 PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

drop table if exists sales;

CREATE TABLE sales (
    sale_id serial4 PRIMARY KEY,
    customer_id INT,
    product_id INT,
    store_id INT,
    sale_date TIMESTAMP,
    quantity INT,
    total_amount DECIMAL(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);