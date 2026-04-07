-- =============================================================================
-- models/intermediate/int_customer_stats.sql
-- Layer: Silver / Intermediate
-- Materialization: TABLE
-- Source: stg_purchases
-- =============================================================================
-- Aggregasi statistik per customer (1 row per unique email).
-- Digunakan oleh mart_customer_360 dan mart_fraud_signals.
-- =============================================================================

with purchases as (
    select * from {{ ref('stg_purchases') }}
),

aggregated as (
    select
        email,

        -- ── Transaction stats ─────────────────────────────────────────────────
        count(*)                                        as total_transactions,
        sum(purchase_price)                             as total_spend,
        round(avg(purchase_price)::numeric, 2)          as avg_spend,
        min(purchase_price)                             as min_purchase,
        max(purchase_price)                             as max_purchase,

        -- ── Preferences (MODE = most frequent value) ──────────────────────────
        mode() within group (order by browser_name)     as preferred_browser,
        mode() within group (order by os_type)          as preferred_os,
        mode() within group (order by cc_provider_simple) as preferred_cc_provider,
        mode() within group (order by language_code)    as preferred_language,
        mode() within group (order by time_of_day)      as preferred_time_of_day,

        -- ── CC info ───────────────────────────────────────────────────────────
        bool_or(is_cc_expired)                          as has_expired_cc,
        count(distinct credit_card_number)              as unique_cards_used,

        -- ── Profile ───────────────────────────────────────────────────────────
        max(job_title)                                  as job_title,
        max(company_name)                               as company_name,

        -- ── Timestamps ────────────────────────────────────────────────────────
        min(ingested_at)                                as first_seen_at,
        max(ingested_at)                                as last_seen_at

    from purchases
    group by email
)

select * from aggregated
