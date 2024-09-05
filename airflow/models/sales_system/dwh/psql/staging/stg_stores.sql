drop table if exists stage.dim_store;

create table stage.dim_store as
SELECT  'emea' || '_' || CAST( store.store_id AS varchar) AS id,
        'emea' AS source_system,
        store.store_id,
        store.name AS store_name,
		store.LOCATION AS store_location,
        store.created_at,
        store.updated_at,
        CURRENT_TIMESTAMP AS dwh_load_date
FROM public.stores store