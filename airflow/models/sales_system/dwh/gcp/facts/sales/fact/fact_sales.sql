CREATE TABLE IF NOT EXISTS `{project_id}.dwh.fact_sales` (
    sales_key STRING NOT NULL,
    sales_id INT64 NOT NULL,
    id string NOT NULL,
    source_system STRING NOT NULL,
    customer_key string,
    product_key string,
    store_key string,
    sale_time TIMESTAMP,
    sale_date_key DATE,
    quantity INT64,
    total_amount NUMERIC(10, 2),
    updated_at TIMESTAMP,
    created_at TIMESTAMP,
    created_date DATE,
    dwh_load_date TIMESTAMP
)
PARTITION BY sale_date_key
CLUSTER BY  sales_key, customer_key, product_key, store_key;


MERGE `{project_id}.dwh.fact_sales` target
USING `{project_id}.staging.fact_sales` source ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET
        target.sales_id = source.sales_id,
        target.source_system = source.source_system,
        target.customer_key = source.customer_key,
        target.product_key = source.product_key,
        target.store_key = source.store_key,
        target.sale_time = source.sale_time,
        target.sale_date_key = source.sale_date_key,
        target.quantity = source.quantity,
        target.total_amount = source.total_amount,
        target.updated_at = source.updated_at,
        target.dwh_load_date = source.dwh_load_date
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        sales_key,
        sales_id,
        id,
        source_system,
        customer_key,
        product_key,
        store_key,
        sale_time,
        sale_date_key,
        quantity,
        total_amount,
        updated_at,
        dwh_load_date
    )
    VALUES (
        CONCAT(CAST(UNIX_SECONDS(CURRENT_TIMESTAMP()) AS STRING), '_', CAST(source.id AS STRING)),
        source.sales_id,
        source.id,
        source.source_system,
        source.customer_key,
        source.product_key,
        source.store_key,
        source.sale_time,
        source.sale_date_key,
        source.quantity,
        source.total_amount,
        source.updated_at,
        source.dwh_load_date
    );