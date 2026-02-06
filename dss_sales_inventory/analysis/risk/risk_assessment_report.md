# Risk Assessment Report
## Executive Summary
- Total products analysed: **100**
- High risk products: **1**
- Average risk score: **0.0667**

## Top 10 High-Risk Products
| product_id | risk_score | VaR(95%) | CI |
|------------|-----------:|---------:|----|
| 62 | 1.0000 | -0.29 | [-0.33, 0.31] |
| 67 | 0.2362 | -1.40 | [-1.54, 0.58] |
| 81 | 0.2313 | -0.38 | [-0.40, 0.14] |
| 40 | 0.1677 | -0.86 | [-0.89, 0.08] |
| 85 | 0.1619 | -0.42 | [-0.43, 0.03] |
| 2 | 0.1312 | -2.72 | [-2.81, -0.36] |
| 33 | 0.1164 | -3.25 | [-3.37, -0.94] |
| 22 | 0.1129 | -0.40 | [-0.41, -0.15] |
| 28 | 0.1078 | -3.07 | [-3.07, -1.32] |
| 68 | 0.0998 | -1.72 | [-1.72, -0.96] |

## Decision Interpretation
Products classified as **High risk** with strongly negative VaR should be reviewed for:
- safety stock adjustments
- reorder policy revisions
- pricing buffers or promotion controls

## Methodological Warning
This Monte Carlo simulation is based on simplified and independent probability distributions. It does not capture tail risks, systemic shocks, supplier failures, or extreme market disruptions.
