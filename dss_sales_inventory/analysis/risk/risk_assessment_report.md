# Risk Assessment Report
## Executive Summary
- Total products analysed: **100**
- High risk products: **1**
- Average risk score: **0.0600**

## Top 10 High-Risk Products
| product_id | risk_score | VaR(95%) | CI |
|------------|-----------:|---------:|----|
| 62 | 1.0000 | -0.30 | [-0.33, 0.32] |
| 67 | 0.2082 | -1.38 | [-1.50, 0.62] |
| 81 | 0.2074 | -0.37 | [-0.39, 0.15] |
| 40 | 0.1496 | -0.85 | [-0.87, 0.08] |
| 85 | 0.1391 | -0.41 | [-0.43, 0.03] |
| 2 | 0.1172 | -2.80 | [-2.93, -0.36] |
| 33 | 0.1051 | -3.26 | [-3.36, -0.89] |
| 22 | 0.0992 | -0.40 | [-0.41, -0.15] |
| 28 | 0.0955 | -3.07 | [-3.07, -1.32] |
| 68 | 0.0877 | -1.72 | [-1.72, -0.96] |

## Decision Interpretation
Products classified as **High risk** with strongly negative VaR should be reviewed for:
- safety stock adjustments
- reorder policy revisions
- pricing buffers or promotion controls

## Methodological Warning
This Monte Carlo simulation is based on simplified and independent probability distributions. It does not capture tail risks, systemic shocks, supplier failures, or extreme market disruptions.
