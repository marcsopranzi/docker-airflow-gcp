INSERT INTO dwh.fact_sales (
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
SELECT
    staging.sales_id,
    staging.id,
    staging.source_system,
    staging.customer_key,
    staging.product_key,
    staging.store_key,
    staging.sale_time,
    staging.sale_date_key,
    staging.quantity,
    staging.total_amount,
    staging.updated_at,
    staging.dwh_load_date
FROM stage.fact_sales staging
ON CONFLICT (id) DO UPDATE
SET
   customer_key = EXCLUDED.customer_key,
   product_key = EXCLUDED.product_key,
   store_key = EXCLUDED.store_key,
   sale_time = EXCLUDED.sale_time,
   quantity = EXCLUDED.quantity,
   total_amount = EXCLUDED.total_amount,
   updated_at = EXCLUDED.updated_at,
   dwh_load_date = EXCLUDED.dwh_load_date
   ;