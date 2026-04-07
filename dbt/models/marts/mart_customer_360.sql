-- =============================================================================
-- models/marts/mart_customer_360.sql
-- Layer: Gold / Marts
-- Grain: one row per customer (email)
-- =============================================================================

with customer_stats as (
    select * from {{ ref('int_customer_stats') }}
),

segmented as (
    select
        email,
        job_title,
        company_name,
        preferred_browser,
        preferred_os,
        preferred_cc_provider,
        preferred_language,
        preferred_time_of_day,
        has_expired_cc,
        unique_cards_used,
        total_transactions,
        round(total_spend::numeric, 2)                  as total_spend,
        avg_spend,
        min_purchase,
        max_purchase,
        first_seen_at,
        last_seen_at,

        -- ── Customer Segment (by transaction frequency) ───────────────────────
        case
            when total_transactions >= 10 then 'VIP'
            when total_transactions >= 4  then 'Regular'
            when total_transactions >= 2  then 'Occasional'
            else 'One-time'
        end                                             as customer_segment,

        -- ── Spending Tier (by total spend) ────────────────────────────────────
        case
            when total_spend >= 300 then 'Premium'
            when total_spend >= 150 then 'High'
            when total_spend >= 75  then 'Medium'
            else 'Low'
        end                                             as spending_tier,

        -- ── Fraud Risk Score (0–100, rule-based) ──────────────────────────────
        least(100, (
            -- 30 pts: has at least one expired CC
            case when has_expired_cc then 30 else 0 end +
            -- 20 pts: uses many different cards (potential carding)
            case when unique_cards_used >= 3 then 20
                 when unique_cards_used = 2  then 10
                 else 0 end +
            -- 30 pts: high-value transaction (relative to $0-100 dataset)
            case when max_purchase >= {{ var('fraud_price_threshold', 75) }} then 30 else 0 end +
            -- 20 pts: very frequent (unusual in this dataset)
            case when total_transactions >= 5 then 20
                 when total_transactions >= 3 then 10
                 else 0 end
        ))                                              as fraud_risk_score,

        -- ── Engagement Level ──────────────────────────────────────────────────
        case
            when total_transactions >= 5  then 'High'
            when total_transactions >= 2  then 'Medium'
            else 'Low'
        end                                             as engagement_level

    from customer_stats
)

select
    *,
    -- Fraud risk category derived from score
    case
        when fraud_risk_score >= 80 then 'Critical'
        when fraud_risk_score >= 60 then 'High'
        when fraud_risk_score >= 40 then 'Medium'
        when fraud_risk_score >= 20 then 'Low'
        else 'Clean'
    end                                                 as fraud_risk_category,

    current_timestamp                                   as refreshed_at

from segmented
