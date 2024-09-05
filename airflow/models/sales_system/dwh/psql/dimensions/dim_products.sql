BEGIN;
WITH updated AS (
    UPDATE dwh.dim_product
    SET is_active = FALSE,
        effective_end_date = CURRENT_TIMESTAMP
    WHERE id IN (
                SELECT staging.id
                FROM stage.dim_product staging
                JOIN dwh.dim_product dim_product ON dim_product.id = staging.id
                WHERE
                    (
                    staging.product_name <> COALESCE(dim_product.product_name, '')
                    OR staging.product_category <> COALESCE(dim_product.product_category, '')
                    OR staging.product_price <> COALESCE(dim_product.product_price, 0.0)
                    )
                AND dim_product.is_active = TRUE
                )
)


INSERT INTO dwh.dim_product (
    id,
    source_system,
    product_id,
    product_name,
    product_category,
    product_price,
    updated_at,
    dwh_load_date,
    is_active,
    effective_start_date,
    effective_end_date
)
SELECT
    staging.id,
    staging.source_system,
    staging.product_id,
    COALESCE(staging.product_name, dim_product.product_name),
    COALESCE(staging.product_category, dim_product.product_category),
    staging.product_price,
    staging.updated_at,
    staging.dwh_load_date,
    TRUE,
    CURRENT_TIMESTAMP,
    NULL
FROM stage.dim_product staging
LEFT JOIN dwh.dim_product dim_product ON dim_product.id = staging.id AND
                                    dim_product.is_active = TRUE  -- Compare only against active records
WHERE dim_product.id IS NULL  -- Insert if no active record exists
   OR (staging.product_name <> COALESCE(dim_product.product_name, '')
       OR staging.product_category <> COALESCE(dim_product.product_category, ''))
       OR staging.product_price <> COALESCE(dim_product.product_price, 0.0);



COMMIT;