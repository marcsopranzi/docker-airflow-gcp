BEGIN;

WITH updated AS (
    UPDATE dwh.dim_store
    SET is_active = FALSE,
        effective_end_date = CURRENT_TIMESTAMP
    WHERE id IN (
                SELECT staging.id
                FROM stage.dim_store staging
                JOIN dwh.dim_store dim_store ON dim_store.id = staging.id
                WHERE
                    (
                    staging.store_name <> COALESCE(dim_store.store_name, '')
                    OR staging.store_location <> COALESCE(dim_store.store_location, '')
                    )
                AND dim_store.is_active = TRUE
                )
)
INSERT INTO dwh.dim_store (
    id,
    source_system,
    store_id,
    store_name,
    store_location,
    updated_at,
    dwh_load_date,
    is_active,
    effective_start_date,
    effective_end_date
)
SELECT
    staging.id,
    staging.source_system,
    staging.store_id,
    COALESCE(staging.store_name, dim_store.store_name),
    COALESCE(staging.store_location, dim_store.store_location),
    staging.updated_at,
    staging.dwh_load_date,
    TRUE,
    CURRENT_TIMESTAMP,
    NULL
FROM stage.dim_store staging
LEFT JOIN dwh.dim_store dim_store ON dim_store.id = staging.id AND
                                    dim_store.is_active = TRUE
WHERE dim_store.id IS NULL  -- Insert if no active record exists
   OR (staging.store_name <> COALESCE(dim_store.store_name, '')
       OR staging.store_location <> COALESCE(dim_store.store_location, ''));
;
COMMIT;
