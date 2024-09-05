CREATE TABLE IF NOT EXISTS `{project_id}.dwh.dim_customer` (
    customer_key STRING NOT NULL,
    id STRING NOT NULL,
    source_system STRING NOT NULL,
    customer_id INT64,
    customer_name STRING,
    customer_address STRING,
    created_at TIMESTAMP,
    created_date DATE,
    updated_at TIMESTAMP,
    dwh_load_date TIMESTAMP,
    is_active BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP
)
PARTITION BY created_date
CLUSTER BY  customer_key, is_active;


BEGIN;

    MERGE `{project_id}.dwh.dim_customer` prod
    USING `{project_id}.staging.dim_customer` staging
    ON prod.customer_id = staging.customer_id AND
        prod.source_system = staging.source_system

    WHEN MATCHED AND prod.is_active = TRUE AND (
        MD5(prod.customer_name) != MD5(staging.customer_name) OR
        MD5(prod.customer_address) != MD5(staging.customer_address) OR
        MD5(CAST(prod.updated_at AS STRING)) != MD5(CAST(staging.updated_at AS STRING))
    ) THEN
    UPDATE SET
        prod.is_active = FALSE,
        prod.effective_end_date = CURRENT_TIMESTAMP();

    INSERT INTO `{project_id}.dwh.dim_customer` (
        customer_key,
        id,
        source_system,
        customer_id,
        customer_name,
        customer_address,
        created_at,
        created_date,
        updated_at,
        dwh_load_date,
        is_active,
        effective_start_date,
        effective_end_date
    )
    SELECT
        GENERATE_UUID(),
        staging.id,
        staging.source_system,
        staging.customer_id,
        staging.customer_name,
        staging.customer_address,
        staging.created_at,
        staging.created_date,
        staging.updated_at,
        staging.dwh_load_date,
        TRUE,
        CURRENT_TIMESTAMP(),
        NULL
    FROM `{project_id}.staging.dim_customer` staging
    LEFT JOIN `{project_id}.dwh.dim_customer` prod ON staging.customer_id = prod.customer_id AND
                staging.source_system = prod.source_system AND
                prod.is_active = TRUE
    WHERE prod.customer_id IS NULL;

COMMIT;