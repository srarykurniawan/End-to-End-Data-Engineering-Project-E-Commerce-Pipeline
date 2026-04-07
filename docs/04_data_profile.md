# Data Profile Report — Ecommerce_Purchases.csv

_Generated: 2026-03-15 13:44:09_



## 1. Dataset Overview

| Metric | Value |

|--------|-------|

| File | Ecommerce_Purchases.csv |

| Total Rows | 10,000 |

| Total Columns | 22 |

| Total Missing Values | 0 |

| Memory Usage | 8666.7 KB |

| Unique Customers (Email) | 9,954 |

| Unique Companies | 8,653 |

| Unique Jobs | 623 |

| Date Profiled | 2026-03-15 |



## 2. Column-by-Column Profile

| Column | Type | Null | Unique | Sample Values |

|--------|------|------|--------|---------------|

| Address | str | 0 | 10,000 | ['16629 Pace Camp Apt. 448\nAlexisborough, NE 77130-7478', ' |

| Lot | str | 0 | 9,822 | ['46 in', '28 rn', '94 vE'] |

| AM or PM | str | 0 | 2 | ['PM', 'AM'] |

| Browser Info | str | 0 | 8,287 | ['Opera/9.56.(X11; Linux x86_64; sl-SI) Presto/2.9.183 Versi |

| Company | str | 0 | 8,653 | ['Martinez-Herman', 'Fletcher, Richards and Whitaker', 'Simp |

| Credit Card | int64 | 0 | 10,000 | [6011929061123406, 3337758169645356, 675957666125] |

| CC Exp Date | str | 0 | 121 | ['02/20', '11/18', '08/19'] |

| CC Security Code | int64 | 0 | 1,758 | [900, 561, 699] |

| CC Provider | str | 0 | 10 | ['JCB 16 digit', 'Mastercard', 'Discover'] |

| Email | str | 0 | 9,954 | ['pdunlap@yahoo.com', 'anthony41@reed.com', 'amymiller@moral |

| Job | str | 0 | 623 | ['Scientist, product/process development', 'Drilling enginee |

| IP Address | str | 0 | 10,000 | ['149.146.147.205', '15.160.41.51', '132.207.160.22'] |

| Language | str | 0 | 9 | ['el', 'fr', 'de'] |

| Purchase Price | float64 | 0 | 6,349 | [98.14, 70.73, 0.95] |

| browser_name | str | 0 | 2 | ['Opera', 'Mozilla'] |

| cc_month | int64 | 0 | 12 | [2, 11, 8] |

| cc_year | int64 | 0 | 11 | [20, 18, 19] |

| cc_year_full | int64 | 0 | 11 | [2020, 2018, 2019] |

| is_expired | bool | 0 | 2 | [True, False] |

| cvv_len | int64 | 0 | 4 | [3, 4, 2] |

| is_cvv_abnormal | bool | 0 | 2 | [False, True] |

| price_tier | category | 0 | 4 | ['Premium($75-100)', 'High($50-75)', 'Low($0-25)'] |



## 3. Purchase Price — Statistical Summary

| Statistic | Value |

|-----------|-------|

| count | $10000.00 |

| mean | $50.35 |

| std | $29.02 |

| min | $0.00 |

| 25% | $25.15 |

| 50% | $50.50 |

| 75% | $75.77 |

| max | $99.99 |



### 3.1 Price Tier Distribution

| Price Tier | Count | Percentage |

|------------|-------|------------|

| Low($0-25) | 2,477 | 24.8% |

| Medium($25-50) | 2,471 | 24.7% |

| High($50-75) | 2,488 | 24.9% |

| Premium($75-100) | 2,564 | 25.6% |



## 4. CC Provider Analysis

| CC Provider | Transactions | Revenue | Avg Price | Revenue Share |

|-------------|-------------|---------|-----------|---------------|

| VISA 16 digit | 1,715 | $85,528.86 | $49.87 | 17.0% |

| JCB 16 digit | 1,716 | $84,597.33 | $49.30 | 16.8% |

| JCB 15 digit | 868 | $44,376.60 | $51.13 | 8.8% |

| Voyager | 829 | $43,085.77 | $51.97 | 8.6% |

| American Express | 849 | $42,865.52 | $50.49 | 8.5% |

| Maestro | 846 | $42,620.78 | $50.38 | 8.5% |

| Discover | 817 | $42,208.13 | $51.66 | 8.4% |

| Mastercard | 816 | $40,835.10 | $50.04 | 8.1% |

| VISA 13 digit | 777 | $39,976.54 | $51.45 | 7.9% |

| Diners Club / Carte Blanche | 767 | $37,378.39 | $48.73 | 7.4% |



## 5. Credit Card Expiry Analysis

| Status | Count | Percentage |

|--------|-------|------------|

| Expired CC | 9,526 | 95.3% |

| Valid CC | 474 | 4.7% |



> ⚠️ **CRITICAL FINDING:** 9,526 dari 10,000 kartu kredit (95.3%) 

> sudah expired per March 2026. Ini adalah fraud signal utama.



## 6. CVV (CC Security Code) Analysis

| CVV Length | Count | Status | Percentage |

|------------|-------|--------|------------|

| 1 digits | 71 | ABNORMAL | 0.7% |

| 2 digits | 830 | ABNORMAL | 8.3% |

| 3 digits | 8,311 | Normal | 83.1% |

| 4 digits | 788 | Normal (Amex) | 7.9% |



**Total CVV Abnormal:** 1,689 (16.9%)



## 7. Language Distribution

| Code | Language | Count | Percentage |

|------|----------|-------|------------|

| de | German | 1,155 | 11.6% |

| ru | Russian | 1,155 | 11.6% |

| el | Greek | 1,137 | 11.4% |

| pt | Portuguese | 1,118 | 11.2% |

| en | English | 1,098 | 11.0% |

| fr | French | 1,097 | 11.0% |

| es | Spanish | 1,095 | 10.9% |

| it | Italian | 1,086 | 10.9% |

| zh | Chinese | 1,059 | 10.6% |



## 8. Browser & Transaction Time

### 8.1 Browser Distribution

| Browser | Count | Percentage |

|---------|-------|------------|

| Mozilla | 7,924 | 79.2% |

| Opera | 2,076 | 20.8% |



### 8.2 Transaction Time (AM/PM)

| Time | Count | Percentage |

|------|-------|------------|

| PM | 5,068 | 50.7% |

| AM | 4,932 | 49.3% |



## 9. Customer Segmentation Preview

| Segment | Criteria | Customer Count | Percentage |

|---------|----------|----------------|------------|

| VIP | >= 10 tx | 0 | 0.0% |

| Regular | 4-9 tx | 0 | 0.0% |

| Occasional | 2-3 tx | 46 | 0.5% |

| One-time | 1 tx | 9,908 | 99.5% |



## 10. Data Quality Summary

| Issue | Severity | Count | Percentage | Handling |

|-------|----------|-------|------------|----------|

| Missing values (any col) | INFO | 0 | 0.0% | — Dataset complete |

| CC Expired | HIGH | 9,526 | 95.3% | flag_expired_cc in mart_fraud |

| CVV Abnormal (not 3-digit) | MEDIUM | 1,689 | 16.9% | flag_abnormal_cvv |

| Purchase Price = $0.00 | LOW | 2 | 0.02% | dbt not_null equivalent |

| Duplicate email (multi-tx) | INFO | 46 | 0.5% | Normal, handled by int_customer_stats |



**Overall Data Quality Score:** 98.2% (penalti hanya untuk CVV abnormal; expired CC adalah valid business data)


