CREATE TABLE IF NOT EXISTS `{project_id}.dwh.dim_product` (
    product_key STRING NOT NULL,
    id STRING NOT NULL,
    source_system STRING NOT NULL,
    product_id INT64,
    product_name STRING,
    product_category STRING,
    product_price NUMERIC(10, 2),
    created_at TIMESTAMP,
    created_date DATE,
    updated_at TIMESTAMP,
    dwh_load_date TIMESTAMP,
    is_active BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP
)
PARTITION BY created_date
CLUSTER BY  product_key, is_active;

BEGIN;

    merge `{project_id}.dwh.dim_product` prod
    using `{project_id}.staging.dim_product` staging on prod.product_id = staging.product_id AND
                                                            prod.source_system = staging.source_system

    when matched and prod.is_active = TRUE AND (
        MD5(prod.product_name) != MD5(staging.product_name) OR
        MD5(prod.product_category) != MD5(staging.product_category) OR
        MD5(CAST(prod.product_price AS STRING)) != MD5(CAST(staging.product_price AS STRING))
    ) then
    update set
        prod.is_active = FALSE,
        prod.effective_end_date = CURRENT_TIMESTAMP();

    insert into `{project_id}.dwh.dim_product` (
        product_key,
        id,
        source_system,
        product_id,
        product_name,
        product_category,
        product_price,
        created_at,
        created_date,
        updated_at,
        dwh_load_date,
        is_active,
        effective_start_date,
        effective_end_date
    )
    select
        GENERATE_UUID(),
        staging.id,
        staging.source_system,
        staging.product_id,
        staging.product_name,
        staging.product_category,
        staging.product_price,
        staging.created_at,
        staging.created_date,
        staging.updated_at,
        staging.dwh_load_date,
        TRUE,
        CURRENT_TIMESTAMP(),
        NULL
    FROM `{project_id}.staging.dim_product` staging
    LEFT JOIN `{project_id}.dwh.dim_product` prod ON prod.product_id = staging.product_id AND
                                                        staging.source_system = prod.source_system AND
                                                        prod.is_active = TRUE
    WHERE prod.product_id IS NULL;

COMMIT;

