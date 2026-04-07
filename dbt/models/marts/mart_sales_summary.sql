-- =============================================================================
-- models/marts/mart_sales_summary.sql
-- Layer: Gold / Marts
-- Grain: one row per CC Provider
-- =============================================================================

with purchases as (
    select * from {{ ref('stg_purchases') }}
),

totals as (
    select
        sum(purchase_price)     as grand_total_revenue,
        count(*)                as grand_total_transactions
    from purchases
),

per_provider as (
    select
        cc_provider                                             as cc_provider,
        cc_provider_simple                                      as cc_provider_group,
        count(*)                                                as total_transactions,
        count(distinct email)                                   as unique_customers,
        round(sum(purchase_price)::numeric, 2)                  as total_revenue,
        round(avg(purchase_price)::numeric, 2)                  as avg_order_value,
        round(min(purchase_price)::numeric, 2)                  as min_order_value,
        round(max(purchase_price)::numeric, 2)                  as max_order_value,

        -- Price tier breakdown
        count(*) filter (where price_tier = 'Premium')          as premium_count,
        count(*) filter (where price_tier = 'High')             as high_count,
        count(*) filter (where price_tier = 'Medium')           as medium_count,
        count(*) filter (where price_tier = 'Low')              as low_count,

        -- Fraud signals
        count(*) filter (where is_cc_expired = true)            as expired_cc_transactions,
        count(*) filter (where is_cvv_abnormal = true)          as abnormal_cvv_transactions

    from purchases
    group by cc_provider, cc_provider_simple
)

select
    p.cc_provider,
    p.cc_provider_group,
    p.total_transactions,
    p.unique_customers,
    p.total_revenue,
    p.avg_order_value,
    p.min_order_value,
    p.max_order_value,

    -- Market share
    round(p.total_revenue / nullif(t.grand_total_revenue, 0) * 100, 2)         as revenue_share_pct,
    round(p.total_transactions::numeric / nullif(t.grand_total_transactions, 0) * 100, 2) as transaction_share_pct,

    -- Price tier counts
    p.premium_count,
    p.high_count,
    p.medium_count,
    p.low_count,

    -- Fraud rates
    p.expired_cc_transactions,
    round(p.expired_cc_transactions::numeric / nullif(p.total_transactions, 0) * 100, 2) as expired_cc_pct,
    p.abnormal_cvv_transactions,

    current_timestamp                                                           as refreshed_at

from per_provider p
cross join totals t
order by p.total_revenue desc
