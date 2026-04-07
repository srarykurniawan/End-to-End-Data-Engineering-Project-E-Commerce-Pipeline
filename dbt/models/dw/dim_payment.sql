{{
  config(
    schema        = 'dw',
    materialized  = 'table'
  )
}}

WITH payment_data AS (

    SELECT DISTINCT
        CAST(credit_card_number AS VARCHAR)        AS credit_card_number,
        cc_provider,
        cc_exp_date,
        cc_exp_month,
        cc_exp_year,

        CASE
            WHEN cc_exp_year < 2026 THEN TRUE
            WHEN cc_exp_year = 2026 AND cc_exp_month < 3 THEN TRUE
            ELSE FALSE
        END                                         AS is_expired,

        LENGTH(CAST(cc_security_code AS VARCHAR))   AS cvv_length,

        CASE
            WHEN LENGTH(CAST(cc_security_code AS VARCHAR)) != 3 THEN TRUE
            ELSE FALSE
        END                                         AS is_cvv_abnormal

    FROM {{ ref('stg_purchases') }}
    WHERE credit_card_number IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY credit_card_number) AS payment_key,
    credit_card_number,
    cc_provider,
    cc_exp_date,
    cc_exp_month,
    cc_exp_year,
    is_expired,
    cvv_length,
    is_cvv_abnormal,
    NOW()                                           AS updated_at
FROM payment_data