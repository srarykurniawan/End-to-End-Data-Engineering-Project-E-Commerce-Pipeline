-- =============================================================================
-- models/marts/mart_fraud_signals.sql
-- Layer: Gold / Marts
-- Grain: one row per transaction with fraud flags
-- =============================================================================

with purchases as (
    select * from {{ ref('stg_purchases') }}
),

customer_360 as (
    select
        email,
        fraud_risk_score,
        fraud_risk_category,
        unique_cards_used,
        total_transactions
    from {{ ref('mart_customer_360') }}
),

flagged as (
    select
        -- ── Identity ──────────────────────────────────────────────────────────
        p.purchase_id,
        p.email,
        p.purchase_price,
        p.cc_provider,
        p.cc_provider_simple,
        p.credit_card_number,
        p.cc_exp_date,
        p.cc_exp_month,
        p.cc_exp_year,
        p.cc_security_code,
        p.cvv_length,
        p.ip_address,
        p.time_of_day,
        p.os_type,
        p.browser_name,
        p.language_code,
        p.ingested_at,

        -- ── Customer profile ──────────────────────────────────────────────────
        c.fraud_risk_score,
        c.fraud_risk_category,
        c.unique_cards_used,
        c.total_transactions                            as customer_total_txns,

        -- ── Fraud Flags ───────────────────────────────────────────────────────
        -- Flag 1: High value transaction
        case
            when p.purchase_price >= {{ var('fraud_price_threshold', 75) }}
            then true else false
        end                                             as flag_high_value,

        -- Flag 2: CC is expired
        p.is_cc_expired                                 as flag_expired_cc,

        -- Flag 3: Abnormal CVV
        p.is_cvv_abnormal                               as flag_abnormal_cvv,

        -- Flag 4: High risk customer (score > 60)
        case
            when c.fraud_risk_score > 60 then true else false
        end                                             as flag_high_risk_customer

    from purchases p
    left join customer_360 c on p.email = c.email
)

select
    *,
    -- Total flags (0–4)
    (flag_high_value::int +
     flag_expired_cc::int +
     flag_abnormal_cvv::int +
     flag_high_risk_customer::int)                      as total_flags,

    current_timestamp                                   as refreshed_at

from flagged
