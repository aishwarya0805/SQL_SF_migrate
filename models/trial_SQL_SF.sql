{{ config(materialized='incremental') }}

SELECT
    id,
    UPPER(name) AS customer_name,
    UPPER(email) AS email,
    TO_VARCHAR(created_at, 'DD-MM-YYYY') AS signup_date
FROM {{ source('public', 'customers') }}
--FROM aish_sql_sf.public.customers;
