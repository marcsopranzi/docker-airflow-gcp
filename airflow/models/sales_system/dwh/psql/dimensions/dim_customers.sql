
BEGIN;
WITH updated AS (
    UPDATE dwh.dim_customer
    SET is_active = FALSE,
        effective_end_date = CURRENT_TIMESTAMP
    WHERE id IN (SELECT staging.id
                FROM stage.dim_customer staging
                join dwh.dim_customer dim_customer on dim_customer.id = staging.id
                WHERE
                    (
                    staging.customer_name <> COALESCE(dim_customer.customer_name, '')
                    OR staging.customer_address <> COALESCE(dim_customer.customer_address, '')
                    )
                AND dim_customer.is_active = TRUE
                )
)

INSERT INTO dwh.dim_customer (
    id,
    source_system,
    customer_id,
    customer_name,
    customer_address,
    updated_at,
    dwh_load_date,
    is_active,
    effective_start_date,
    effective_end_date
)
SELECT
    staging.id,
    staging.source_system,
    staging.customer_id,
    COALESCE(staging.customer_name, dim_customer.customer_name),
    COALESCE(staging.customer_address, dim_customer.customer_address),
    staging.updated_at,
    staging.dwh_load_date,
    TRUE,
    CURRENT_TIMESTAMP,
    NULL
FROM stage.dim_customer staging
left join dwh.dim_customer dim_customer on dim_customer.id = staging.id and
                                            dim_customer.is_active = TRUE
WHERE dim_customer.id IS NULL
    OR (staging.customer_name <> COALESCE(dim_customer.customer_name, '')
        OR staging.customer_address <> COALESCE(dim_customer.customer_address, ''));

COMMIT;

