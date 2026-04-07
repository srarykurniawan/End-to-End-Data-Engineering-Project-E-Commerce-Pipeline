"""
tests/ingestion/test_batch_ingest.py
Unit tests for batch_ingest.py data cleaning functions.
Run: pytest tests/ -v
"""

import sys
import os
import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../ingestion'))
from batch_ingest import clean_dataframe, validate_dataframe


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_df():
    """Minimal valid DataFrame matching CSV structure."""
    return pd.DataFrame({
        'Address':          ['123 Main St', '456 Oak Ave'],
        'Lot':              ['12 rn', '34 in'],
        'AM or PM':         ['AM', 'pm'],          # 'pm' should be uppercased
        'Browser Info':     ['Mozilla/5.0 (Windows NT 10.0)', 'Opera/9.56 (Linux)'],
        'Company':          ['Acme Corp', 'Widget LLC'],
        'Credit Card':      [4111111111111111, 5500005555555559],
        'CC Exp Date':      ['03/26', '12/25'],
        'CC Security Code': [123, 456],
        'CC Provider':      ['VISA 16 digit', 'Mastercard'],
        'Email':            ['alice@example.com', 'bob@example.com'],
        'Job':              ['Engineer', 'Designer'],
        'IP Address':       ['192.168.1.1', '10.0.0.1'],
        'Language':         ['en', 'fr'],
        'Purchase Price':   [55.99, 89.50],
    })


@pytest.fixture
def dirty_df():
    """DataFrame with known data quality issues."""
    return pd.DataFrame({
        'Address':          ['  123 Main St  ', None],
        'Lot':              ['12 rn', '34 in'],
        'AM or PM':         ['am', 'XY'],          # 'XY' invalid
        'Browser Info':     ['Mozilla/5.0', 'Opera/9.56'],
        'Company':          ['Acme Corp', 'Widget LLC'],
        'Credit Card':      [4111111111111111, 5500005555555559],
        'CC Exp Date':      ['03/26', '12/25'],
        'CC Security Code': [123, 456],
        'CC Provider':      ['VISA 16 digit', 'Mastercard'],
        'Email':            ['alice@example.com', None],   # None email
        'Job':              ['Engineer', 'Designer'],
        'IP Address':       ['192.168.1.1', '10.0.0.1'],
        'Language':         ['en', 'fr'],
        'Purchase Price':   [55.99, -10.00],               # Negative price
    })


# ── Tests: clean_dataframe ────────────────────────────────────────────────────

class TestCleanDataframe:

    def test_column_renaming(self, sample_df):
        cleaned = clean_dataframe(sample_df)
        expected_cols = ['address', 'lot', 'am_or_pm', 'browser_info', 'company',
                         'credit_card', 'cc_exp_date', 'cc_security_code', 'cc_provider',
                         'email', 'job', 'ip_address', 'language', 'purchase_price']
        for col in expected_cols:
            assert col in cleaned.columns, f"Missing column: {col}"

    def test_am_or_pm_uppercased(self, sample_df):
        cleaned = clean_dataframe(sample_df)
        assert all(v in ('AM', 'PM', None) for v in cleaned['am_or_pm'].dropna())

    def test_am_or_pm_invalid_set_null(self, dirty_df):
        cleaned = clean_dataframe(dirty_df)
        # 'XY' should become None
        assert cleaned['am_or_pm'].isna().sum() >= 1

    def test_purchase_price_numeric(self, sample_df):
        cleaned = clean_dataframe(sample_df)
        assert pd.api.types.is_float_dtype(cleaned['purchase_price'])

    def test_credit_card_to_string(self, sample_df):
        cleaned = clean_dataframe(sample_df)
        assert cleaned['credit_card'].dtype == object

    def test_whitespace_stripped(self, dirty_df):
        cleaned = clean_dataframe(dirty_df)
        assert cleaned['address'].iloc[0] == '123 Main St'

    def test_shape_preserved(self, sample_df):
        cleaned = clean_dataframe(sample_df)
        assert cleaned.shape[0] == len(sample_df)


# ── Tests: validate_dataframe ─────────────────────────────────────────────────

class TestValidateDataframe:

    def test_clean_data_no_issues(self, sample_df):
        cleaned = clean_dataframe(sample_df)
        issues  = validate_dataframe(cleaned)
        assert len(issues) == 0

    def test_null_email_detected(self, dirty_df):
        cleaned = clean_dataframe(dirty_df)
        issues  = validate_dataframe(cleaned)
        assert 'null_email' in issues

    def test_return_type_is_dict(self, sample_df):
        cleaned = clean_dataframe(sample_df)
        issues  = validate_dataframe(cleaned)
        assert isinstance(issues, dict)


# ── Tests: edge cases ─────────────────────────────────────────────────────────

class TestEdgeCases:

    def test_empty_dataframe(self):
        empty = pd.DataFrame(columns=[
            'Address','Lot','AM or PM','Browser Info','Company',
            'Credit Card','CC Exp Date','CC Security Code','CC Provider',
            'Email','Job','IP Address','Language','Purchase Price'
        ])
        cleaned = clean_dataframe(empty)
        assert len(cleaned) == 0

    def test_all_null_purchase_price(self, sample_df):
        sample_df['Purchase Price'] = None
        cleaned = clean_dataframe(sample_df)
        assert cleaned['purchase_price'].isna().all()

    def test_non_numeric_purchase_price(self, sample_df):
        sample_df.loc[0, 'Purchase Price'] = 'INVALID'
        cleaned = clean_dataframe(sample_df)
        assert pd.isna(cleaned['purchase_price'].iloc[0])
