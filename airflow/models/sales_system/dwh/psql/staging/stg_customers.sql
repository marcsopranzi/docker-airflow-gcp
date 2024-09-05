DROP TABLE IF EXISTS stage.dim_customer;

-- Step 4: Create the stage.dim_customer table
CREATE TABLE stage.dim_customer AS
SELECT
    'emea' || '_' || CAST(customer.customer_id AS varchar) AS id,
    'emea' AS source_system,
    customer.customer_id,
    customer.name as customer_name,
    customer.address as customer_address,
    customer.created_at,
    customer.updated_at,
    CURRENT_TIMESTAMP AS dwh_load_date
FROM public.customers customer;

