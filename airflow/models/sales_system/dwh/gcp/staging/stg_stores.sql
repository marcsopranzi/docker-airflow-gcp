drop table if exists `{project_id}.staging.dim_store`;

create table `{project_id}.staging.dim_store` as
SELECT  CONCAT('global', '_', CAST(store_id AS STRING)) AS id,
        'global' AS source_system,
        store.store_id,
        store.name AS store_name,
		store.LOCATION AS store_location,
        store.created_at,
        cast(store.created_at as date) as created_date,
        store.updated_at,
        PARSE_TIMESTAMP('%Y%m%d%H%M%S', '{time_stamp}') AS dwh_load_date
FROM `{project_id}.staging.stores` store