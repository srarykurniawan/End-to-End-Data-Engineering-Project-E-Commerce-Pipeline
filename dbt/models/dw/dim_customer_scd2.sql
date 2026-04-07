{{
  config(
    schema='dw',
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='append'
  )
}}

WITH source_data AS (

    SELECT
        email                               AS customer_id,
        email,
        job_title,
        NULL::VARCHAR                       AS company,
        customer_segment,
        spending_tier,
        total_transactions,
        total_spend,
        avg_spend,
        min_purchase,
        max_purchase,
        preferred_browser,
        preferred_os,
        preferred_cc_provider,
        preferred_language,
        preferred_time_of_day,
        has_expired_cc,
        unique_cards_used,
        fraud_risk_score,
        fraud_risk_category,
        engagement_level,
        first_seen_at,
        last_seen_at
    FROM {{ ref('mart_customer_360') }}

),

-- generate surrogate key
source_with_key AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY customer_id)     AS customer_key,
        *
    FROM source_data

)

{% if is_incremental() %}

-- hanya ambil customer baru
, new_customers AS (

    SELECT s.*
    FROM source_with_key s
    LEFT JOIN {{ this }} d
        ON s.customer_id = d.customer_id
        AND d.is_current = TRUE
    WHERE d.customer_id IS NULL

)

SELECT
    customer_key,
    customer_id,
    email,
    job_title,
    company,
    customer_segment,
    spending_tier,
    total_transactions,
    total_spend,
    avg_spend,
    min_purchase,
    max_purchase,
    preferred_browser,
    preferred_os,
    preferred_cc_provider,
    preferred_language,
    preferred_time_of_day,
    has_expired_cc,
    unique_cards_used,
    fraud_risk_score,
    fraud_risk_category,
    engagement_level,
    NOW()                   AS effective_date,
    NULL::TIMESTAMP         AS expiry_date,
    TRUE                    AS is_current,
    1                       AS version,
    'new_customer'          AS change_reason,
    first_seen_at,
    last_seen_at,
    NOW()                   AS dw_created_at,
    NOW()                   AS dw_updated_at

FROM new_customers

{% else %}

-- initial load
SELECT
    customer_key,
    customer_id,
    email,
    job_title,
    company,
    customer_segment,
    spending_tier,
    total_transactions,
    total_spend,
    avg_spend,
    min_purchase,
    max_purchase,
    preferred_browser,
    preferred_os,
    preferred_cc_provider,
    preferred_language,
    preferred_time_of_day,
    has_expired_cc,
    unique_cards_used,
    fraud_risk_score,
    fraud_risk_category,
    engagement_level,
    NOW()                   AS effective_date,
    NULL::TIMESTAMP         AS expiry_date,
    TRUE                    AS is_current,
    1                       AS version,
    'initial_load'          AS change_reason,
    first_seen_at,
    last_seen_at,
    NOW()                   AS dw_created_at,
    NOW()                   AS dw_updated_at

FROM source_with_key

{% endif %}