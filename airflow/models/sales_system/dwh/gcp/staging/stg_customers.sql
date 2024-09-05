DROP TABLE IF EXISTS `{project_id}.staging.dim_customer`;

CREATE OR REPLACE TABLE `{project_id}.staging.dim_customer` AS
SELECT
    CONCAT('global', '_', CAST(customer_id AS STRING)) AS id,
    'global' AS source_system,
    -- {time_stamp} AS source_system,
    customer.customer_id,
    customer.name AS customer_name,
    customer.address AS customer_address,
    customer.created_at,
    cast(customer.created_at as date) as created_date,
    customer.updated_at,
    PARSE_TIMESTAMP('%Y%m%d%H%M%S', '{time_stamp}') AS dwh_load_date
FROM `{project_id}.staging.customers` customer;