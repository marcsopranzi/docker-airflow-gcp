DROP TABLE IF EXISTS stage.fact_sales;
CREATE TABLE stage.fact_sales AS
SELECT
    'emea' || '_' || CAST( sale.sale_id AS varchar) AS id,
    'emea' AS source_system,
    sale.sale_id AS sales_id,
    dim_cust.customer_key AS customer_key,
    dim_prod.product_key AS product_key,
    dim_store.store_key AS store_key,
    sale.sale_date as sale_time,
    sale.sale_date::date as sale_date_key,
    sale.quantity,
    sale.total_amount,
    sale.sale_date as updated_at,
    CURRENT_TIMESTAMP AS dwh_load_date
FROM public.sales sale
LEFT JOIN dwh.dim_customer dim_cust ON sale.customer_id = dim_cust.customer_id and dim_cust.is_active = true
LEFT JOIN dwh.dim_product dim_prod ON sale.product_id = dim_prod.product_id and dim_prod.is_active = true
LEFT JOIN dwh.dim_store dim_store ON sale.store_id = dim_store.store_id and dim_store.is_active = true;