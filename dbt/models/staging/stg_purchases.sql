-- =============================================================================
-- models/staging/stg_purchases.sql
-- Layer: Silver / Staging
-- Materialization: VIEW
-- Source: raw.ecommerce_purchases
-- =============================================================================
-- Transformasi:
--   1. Rename kolom ke snake_case
--   2. Cast tipe data yang benar
--   3. Parse cc_exp_date → cc_exp_month, cc_exp_year
--   4. Derive os_type, browser_name dari user-agent string
--   5. Derive price_tier, cc_provider_simple, is_cc_expired
--   6. Generate surrogate purchase_id
-- =============================================================================

with source as (
    select * from {{ source('raw', 'ecommerce_purchases') }}
),

cleaned as (
    select
        -- ── Surrogate key ──────────────────────────────────────────────────────
        md5(
            coalesce(trim(email), '') ||
            coalesce(trim(credit_card::text), '') ||
            coalesce(purchase_price::text, '') ||
            coalesce(ingested_at::text, '')
        )                                               as purchase_id,

        -- ── Metadata ──────────────────────────────────────────────────────────
        id                                              as raw_id,
        batch_id,
        source,
        ingested_at,

        -- ── Customer ──────────────────────────────────────────────────────────
        lower(trim(email))                              as email,
        split_part(lower(trim(email)), '@', 2)          as email_domain,
        trim(job)                                       as job_title,
        trim(address)                                   as address,
        trim(lot)                                       as lot_number,
        trim(company)                                   as company_name,
        trim(ip_address)                                as ip_address,

        -- ── Transaction ───────────────────────────────────────────────────────
        purchase_price::numeric(10,2)                   as purchase_price,
        upper(trim(am_or_pm))                           as time_of_day,

        -- ── Credit Card ───────────────────────────────────────────────────────
        trim(credit_card::text)                         as credit_card_number,
        trim(cc_provider)                               as cc_provider,
        trim(cc_exp_date)                               as cc_exp_date,
        trim(cc_security_code::text)                    as cc_security_code,
        length(trim(cc_security_code::text))            as cvv_length,

        -- Parse CC expiry → bulan & tahun numerik
        split_part(trim(cc_exp_date), '/', 1)::integer  as cc_exp_month,
        (split_part(trim(cc_exp_date), '/', 2)::integer + 2000)
                                                        as cc_exp_year,

        -- ── Browser / Device ──────────────────────────────────────────────────
        trim(browser_info)                              as browser_info,
        lower(trim(language))                           as language_code,

        -- OS dari user-agent string
        case
            when lower(browser_info) like '%windows%'   then 'Windows'
            when lower(browser_info) like '%macintosh%' then 'macOS'
            when lower(browser_info) like '%linux%'     then 'Linux'
            when lower(browser_info) like '%iphone%'    then 'iOS'
            when lower(browser_info) like '%android%'   then 'Android'
            else 'Other'
        end                                             as os_type,

        -- Browser dari user-agent prefix (Mozilla = Firefox/Chrome/Safari, Opera = Opera)
        case
            when lower(browser_info) like 'opera%'      then 'Opera'
            when lower(browser_info) like 'mozilla%'    then 'Mozilla'
            else 'Other'
        end                                             as browser_name,

        -- ── Derived Flags ─────────────────────────────────────────────────────
        -- CC expired jika exp_year < {{ var('current_year', 2026) }}
        -- atau exp_year = current_year AND exp_month < current_month
        case
            when (split_part(trim(cc_exp_date), '/', 2)::integer + 2000) < {{ var('current_year', 2026) }}
                then true
            when (split_part(trim(cc_exp_date), '/', 2)::integer + 2000) = {{ var('current_year', 2026) }}
                and split_part(trim(cc_exp_date), '/', 1)::integer < 3
                then true
            else false
        end                                             as is_cc_expired,

        -- CVV abnormal = bukan 3 digit
        case
            when length(trim(cc_security_code::text)) != 3 then true
            else false
        end                                             as is_cvv_abnormal,

        -- Price tier (sesuai rentang harga dataset $0-$100)
        case
            when purchase_price::numeric < 25   then 'Low'
            when purchase_price::numeric < 50   then 'Medium'
            when purchase_price::numeric < 75   then 'High'
            else 'Premium'
        end                                             as price_tier,

        -- CC provider simplified untuk grouping
        case
            when lower(cc_provider) like '%visa%'        then 'Visa'
            when lower(cc_provider) like '%mastercard%'  then 'Mastercard'
            when lower(cc_provider) like '%american exp%'
              or lower(cc_provider) like '%amex%'        then 'Amex'
            when lower(cc_provider) like '%discover%'    then 'Discover'
            when lower(cc_provider) like '%jcb%'         then 'JCB'
            when lower(cc_provider) like '%diners%'      then 'Diners Club'
            when lower(cc_provider) like '%maestro%'     then 'Maestro'
            when lower(cc_provider) like '%voyager%'     then 'Voyager'
            else cc_provider
        end                                             as cc_provider_simple

    from source
    where
        email is not null
        and trim(email) != ''
        and purchase_price is not null
        and purchase_price::numeric >= {{ var('min_price_valid', 0) }}
)

select * from cleaned
