# Risk Assessment Report
## Executive Summary
- Total products analysed: **100**
- High risk products: **1**
- Average risk score: **0.0620**

## Top 10 High-Risk Products
| product_id | risk_score | VaR(95%) | CI |
|------------|-----------:|---------:|----|
| 62 | 1.0000 | -0.29 | [-0.33, 0.30] |
| 67 | 0.2159 | -1.37 | [-1.48, 0.65] |
| 81 | 0.2098 | -0.38 | [-0.40, 0.14] |
| 40 | 0.1551 | -0.85 | [-0.87, 0.09] |
| 85 | 0.1531 | -0.42 | [-0.43, 0.03] |
| 2 | 0.1215 | -2.76 | [-2.88, -0.37] |
| 33 | 0.1100 | -3.24 | [-3.34, -0.90] |
| 22 | 0.1046 | -0.40 | [-0.41, -0.15] |
| 28 | 0.1027 | -3.07 | [-3.07, -1.32] |
| 68 | 0.0929 | -1.72 | [-1.72, -0.96] |

## Decision Interpretation
Products classified as **High risk** with strongly negative VaR should be reviewed for:
- safety stock adjustments
- reorder policy revisions
- pricing buffers or promotion controls

## Methodological Warning
This Monte Carlo simulation is based on simplified and independent probability distributions. It does not capture tail risks, systemic shocks, supplier failures, or extreme market disruptions.
