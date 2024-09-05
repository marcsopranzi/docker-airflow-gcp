DROP TABLE IF EXISTS `{project_id}.staging.dim_product`;

CREATE TABLE `{project_id}.staging.dim_product` AS
SELECT CONCAT('global', '_', CAST(product_id AS STRING)) AS id,
       'global' AS source_system,
       product.product_id,
       product.name AS product_name,
       product.category AS product_category,
       SAFE_CAST(product.price AS NUMERIC) AS product_price,
       product.created_at,
       CAST(product.created_at AS DATE) AS created_date,
       product.updated_at,
       PARSE_TIMESTAMP('%Y%m%d%H%M%S', '{time_stamp}') AS dwh_load_date
FROM `{project_id}.staging.products` product;