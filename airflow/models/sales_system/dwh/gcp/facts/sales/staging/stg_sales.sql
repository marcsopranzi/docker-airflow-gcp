DROP TABLE IF EXISTS `{project_id}.staging.fact_sales`;

CREATE TABLE `{project_id}.staging.fact_sales` AS
SELECT
    'global' || '_' || CAST(sale.sale_id AS STRING) AS id,
    'global' AS source_system,
    sale.sale_id AS sales_id,
    dim_customer.customer_key AS customer_key,
    dim_product.product_key AS product_key,
    dim_store.store_key AS store_key,
    sale.sale_date AS sale_time,
    cast(sale.sale_date as date) AS sale_date_key,
    sale.quantity,
    cast(sale.total_amount as NUMERIC) AS total_amount,
    sale.created_at,
    sale.updated_at,
    PARSE_TIMESTAMP('%Y%m%d%H%M%S', '{time_stamp}') AS dwh_load_date
FROM `{project_id}.staging.sales` sale
JOIN `{project_id}.dwh.dim_customer` dim_customer ON sale.customer_id = dim_customer.customer_id AND
                                                    'global' = dim_customer.source_system AND
                                                    dim_customer.is_active = true
JOIN `{project_id}.dwh.dim_product` dim_product ON sale.product_id = dim_product.product_id AND
                                                    'global' = dim_product.source_system AND
                                                    dim_product.is_active = true
JOIN `{project_id}.dwh.dim_store` dim_store ON sale.store_id = dim_store.store_id AND
                                                'global' = dim_store.source_system AND
                                                    dim_store.is_active = true;
