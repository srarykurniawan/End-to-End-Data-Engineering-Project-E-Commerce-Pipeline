{{
  config(
    schema              = 'dw',
    materialized        = 'incremental',
    unique_key          = 'transaction_id',
    incremental_strategy = 'delete+insert'
  )
}}

WITH transactions AS (

    SELECT
        purchase_id                             AS transaction_id,
        email,
        CAST(credit_card_number AS VARCHAR)     AS credit_card_number,
        purchase_price,

        -- Generate AM / PM from timestamp
        CASE
            WHEN EXTRACT(HOUR FROM ingested_at) < 12 THEN 'AM'
            ELSE 'PM'
        END                                     AS am_or_pm,

        language_code                           AS language,
        browser_name,
        os_type,
        ip_address,
        ingested_at,
        batch_id,

        -- Fraud flags
        CASE
            WHEN purchase_price > 75 THEN TRUE
            ELSE FALSE
        END                                     AS flag_high_value,

        CASE
            WHEN cc_exp_year < 2026 THEN TRUE
            WHEN cc_exp_year = 2026 AND cc_exp_month < 3 THEN TRUE
            ELSE FALSE
        END                                     AS flag_expired_cc,

        CASE
            WHEN LENGTH(CAST(cc_security_code AS VARCHAR)) != 3 THEN TRUE
            ELSE FALSE
        END                                     AS flag_abnormal_cvv

    FROM {{ ref('stg_purchases') }}

    {% if is_incremental() %}
    WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

with_fraud AS (

    SELECT
        t.*,
        mc.fraud_risk_score,

        CASE
            WHEN mc.fraud_risk_score > 75 THEN TRUE
            ELSE FALSE
        END                                     AS flag_high_risk_customer

    FROM transactions t
    LEFT JOIN {{ ref('mart_customer_360') }} mc
        ON t.email = mc.email
),

with_keys AS (

    SELECT
        wf.transaction_id,
        wf.purchase_price,
        wf.am_or_pm,
        wf.language,
        wf.ingested_at,
        wf.batch_id,

        wf.flag_high_value,
        wf.flag_expired_cc,
        wf.flag_abnormal_cvv,
        wf.flag_high_risk_customer,

        -- date_key (YYYYMMDD)
        TO_CHAR(COALESCE(wf.ingested_at, NOW()), 'YYYYMMDD')::INTEGER AS date_key,

        dc.customer_key,
        dp.payment_key,
        db.browser_key

    FROM with_fraud wf

    LEFT JOIN {{ ref('dim_customer_scd2') }} dc
        ON wf.email = dc.customer_id
        AND dc.is_current = TRUE

    LEFT JOIN {{ ref('dim_payment') }} dp
        ON wf.credit_card_number = dp.credit_card_number

    LEFT JOIN {{ ref('dim_browser') }} db
        ON wf.browser_name = db.browser_name
        AND wf.os_type = db.os_type
)

SELECT
    transaction_id,
    customer_key,
    payment_key,
    date_key,
    browser_key,
    NULL::INTEGER                                   AS location_key,

    purchase_price,
    am_or_pm,
    language,

    flag_high_value,
    flag_expired_cc,
    flag_abnormal_cvv,
    flag_high_risk_customer,

    CASE
        WHEN (flag_high_value OR flag_expired_cc OR flag_abnormal_cvv OR flag_high_risk_customer)
        THEN TRUE ELSE FALSE
    END                                             AS is_fraud_flagged,

    (flag_high_value::INTEGER +
     flag_expired_cc::INTEGER +
     flag_abnormal_cvv::INTEGER +
     flag_high_risk_customer::INTEGER)              AS total_fraud_flags,

    'batch'                                         AS source,
    batch_id,
    ingested_at,
    NOW()                                           AS dw_loaded_at

FROM with_keys