drop table if exists stage.dim_product;

create table stage.dim_product as
SELECT  'emea' || '_' || CAST( product.product_id AS varchar) AS id,
        'emea' AS source_system,
        product.product_id,
        product.name AS product_name,
		product.category AS product_category,
		product.price as product_price,
        product.created_at,
        product.updated_at,
        CURRENT_TIMESTAMP AS dwh_load_date
FROM public.products product;