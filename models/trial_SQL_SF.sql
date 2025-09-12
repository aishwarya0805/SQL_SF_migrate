{{ config(materialized='incremental',
   unique_key = 'ID',
   incremental_strategy='merge') }}

SELECT distinct
    id,
    UPPER(name) AS customer_name,
    UPPER(email) AS email,
    TO_DATE(created_at) AS signup_date,
    CURRENT_TIMESTAMP as updated_at
FROM {{ source('public', 'customers') }}
--FROM aish_sql_sf.public.customers;

{% if is_incremental() %}
    -- only load rows where signedup is newer than the max already in the target table
    where signup_date > (SELECT COALESCE(MAX(signup_date), '1900-01-01') FROM {{ this }})
{% endif %}
