-- Create dim_store table
CREATE TABLE IF NOT EXISTS `{project_id}.dwh.dim_store` (
    store_key STRING NOT NULL,
    id STRING NOT NULL,
    source_system STRING NOT NULL,
    store_id INT64,
    store_name STRING,
    store_location STRING,
    created_at TIMESTAMP,
    created_date DATE,
    updated_at TIMESTAMP,
    dwh_load_date TIMESTAMP,
    is_active BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP
)
PARTITION BY created_date
CLUSTER BY  store_key, is_active;



BEGIN;

    MERGE `{project_id}.dwh.dim_store` prod
    USING `{project_id}.staging.dim_store` staging ON prod.store_id = staging.store_id AND
                                                        prod.source_system = staging.source_system

    WHEN MATCHED AND prod.is_active = TRUE AND (
        MD5(prod.store_name) != MD5(staging.store_name) OR
        MD5(prod.store_location) != MD5(staging.store_location) OR
        MD5(CAST(prod.updated_at AS STRING)) != MD5(CAST(staging.updated_at AS STRING))
    ) THEN
    UPDATE SET
        prod.is_active = FALSE,
        prod.effective_end_date = CURRENT_TIMESTAMP();

    INSERT INTO `{project_id}.dwh.dim_store` (
        store_key,
        id,
        source_system,
        store_id,
        store_name,
        store_location,
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
        staging.store_id,
        staging.store_name,
        staging.store_location,
        staging.created_at,
        staging.created_date,
        staging.updated_at,
        CURRENT_TIMESTAMP(),
        TRUE,
        CURRENT_TIMESTAMP(),
        null
    FROM `{project_id}.staging.dim_store` staging
    left join `{project_id}.dwh.dim_store` prod ON prod.store_id = staging.store_id AND
                                                    staging.source_system = prod.source_system AND
                                                    prod.is_active = TRUE
    where prod.store_id is null;
COMMIT;