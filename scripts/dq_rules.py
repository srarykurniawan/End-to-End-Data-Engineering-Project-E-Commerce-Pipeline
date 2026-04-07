DQ_RULES = [

    {
        "rule_id": "raw_not_null_user_id",
        "table": "raw.ecommerce_purchases",
        "check_sql": """
            SELECT COUNT(*)
            FROM raw.ecommerce_purchases
            WHERE user_id IS NULL
        """,
        "threshold": 0,
        "severity": "critical"
    },

    {
        "rule_id": "raw_not_null_transaction_id",
        "table": "raw.ecommerce_purchases",
        "check_sql": """
            SELECT COUNT(*)
            FROM raw.ecommerce_purchases
            WHERE transaction_id IS NULL
        """,
        "threshold": 0,
        "severity": "critical"
    },

    {
        "rule_id": "amount_positive",
        "table": "raw.ecommerce_purchases",
        "check_sql": """
            SELECT COUNT(*)
            FROM raw.ecommerce_purchases
            WHERE purchase_amount <= 0
        """,
        "threshold": 0,
        "severity": "warning"
    }

]