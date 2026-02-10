# Risk Assessment Report
## Executive Summary
- Total products analysed: **100**
- High risk products: **1**
- Average risk score: **0.0658**

## Top 10 High-Risk Products
| product_id | risk_score | VaR(95%) | CI |
|------------|-----------:|---------:|----|
| 62 | 1.0000 | -0.29 | [-0.32, 0.32] |
| 67 | 0.2504 | -1.38 | [-1.48, 0.63] |
| 81 | 0.2341 | -0.38 | [-0.41, 0.14] |
| 40 | 0.1642 | -0.85 | [-0.88, 0.08] |
| 85 | 0.1611 | -0.42 | [-0.43, 0.03] |
| 2 | 0.1317 | -2.75 | [-2.88, -0.37] |
| 33 | 0.1201 | -3.28 | [-3.38, -0.93] |
| 22 | 0.1138 | -0.40 | [-0.41, -0.15] |
| 28 | 0.1103 | -3.07 | [-3.07, -1.32] |
| 68 | 0.1001 | -1.72 | [-1.72, -0.96] |

## Decision Interpretation
Products classified as **High risk** with strongly negative VaR should be reviewed for:
- safety stock adjustments
- reorder policy revisions
- pricing buffers or promotion controls

## Methodological Warning
This Monte Carlo simulation is based on simplified and independent probability distributions. It does not capture tail risks, systemic shocks, supplier failures, or extreme market disruptions.
