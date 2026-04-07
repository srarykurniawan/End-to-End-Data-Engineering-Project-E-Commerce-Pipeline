-- =============================================================================
-- macros/fraud_flags.sql
-- Reusable fraud detection macros used across dbt models.
-- =============================================================================

-- Check if a CC is expired given month and year columns
{% macro is_cc_expired(month_col, year_col, current_year=2026, current_month=3) %}
    case
        when {{ year_col }} < {{ current_year }} then true
        when {{ year_col }} = {{ current_year }}
             and {{ month_col }} < {{ current_month }} then true
        else false
    end
{% endmacro %}

-- Compute fraud risk score from customer stats columns
{% macro fraud_risk_score(has_expired_cc, unique_cards_used, max_purchase, total_transactions, threshold=75) %}
    least(100, (
        case when {{ has_expired_cc }} then 30 else 0 end +
        case when {{ unique_cards_used }} >= 3 then 20
             when {{ unique_cards_used }} = 2  then 10
             else 0 end +
        case when {{ max_purchase }} >= {{ threshold }} then 30 else 0 end +
        case when {{ total_transactions }} >= 5 then 20
             when {{ total_transactions }} >= 3 then 10
             else 0 end
    ))
{% endmacro %}

-- Categorize a risk score into a label
{% macro fraud_risk_category(score_col) %}
    case
        when {{ score_col }} >= 80 then 'Critical'
        when {{ score_col }} >= 60 then 'High'
        when {{ score_col }} >= 40 then 'Medium'
        when {{ score_col }} >= 20 then 'Low'
        else 'Clean'
    end
{% endmacro %}
