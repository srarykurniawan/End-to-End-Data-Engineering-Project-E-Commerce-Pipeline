{{
  config(
    schema = 'dw',
    materialized = 'table'
  )
}}

WITH browsers AS (

    SELECT DISTINCT
        browser_name,
        os_type,

        CASE
            WHEN browser_name ILIKE '%chrome%' THEN 'Chrome'
            WHEN browser_name ILIKE '%firefox%' THEN 'Firefox'
            WHEN browser_name ILIKE '%safari%' THEN 'Safari'
            WHEN browser_name ILIKE '%opera%' THEN 'Opera'
            WHEN browser_name ILIKE '%edge%' THEN 'Edge'
            ELSE 'Other'
        END AS browser_family

    FROM {{ ref('stg_purchases') }}

)

SELECT
    ROW_NUMBER() OVER (ORDER BY browser_name, os_type) AS browser_key,
    browser_name,
    os_type,
    browser_family,
    NOW() AS updated_at
FROM browsers