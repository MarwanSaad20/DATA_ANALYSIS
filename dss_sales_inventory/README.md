# Decision Support System (DSS): Comprehensive Reference Manual

| **Document Meta** | **Details** |
| --- | --- |
| **Version** | **3.4** (Final Production Release) |
| **Last Updated** | **February 11, 2026** |
| **Status** | **Production Ready** (Modules 1–9 Implemented) |
| **Root Directory** | `C:\Data_Analysis\dss_sales_inventory\` |
| **Target Audience** | Data Engineers, AI Agents, System Architects |

---

## 1. Version History

| Version | Date | Author/System | Description of Changes |
| --- | --- | --- | --- |
| **1.0** | Jan 2026 | Core Team | Initial release (Weeks 1-3: Ingestion, SQL, Descriptive). |
| **2.0** | Jan 2026 | Core Team | Added Predictive Layer (Weeks 4-5: Time Series, Forecasting). |
| **2.4** | Feb 2026 | Core Team | Added Prescriptive Layer (Week 6: Scenarios) and Operational Protocols. |
| **3.0** | Feb 2026 | AI Architect | Enhanced formatting, operational protocols, and formula specifications. |
| **3.1** | Feb 2026 | AI Architect | Added **Risk Simulation Layer (Week 7)** and probabilistic reporting. |
| **3.2** | Feb 2026 | AI Architect | **Synchronization:** Verified paths, QA checklists, and pipeline logic. |
| **3.3** | Feb 2026 | AI Architect | Added **Sensitivity Analysis Layer (Week 8)** including outputs, visualizations, pipeline integration, and QA checklist. |
| **3.4** | **Feb 2026** | **AI Architect** | **Added KPI Layer (Week 9) including outputs, decision flags, top products, and QA checklist.** |

---

## 2. Executive Summary & Architecture

The Decision Support System (DSS) is a modular analytics engine designed to ingest raw sales and inventory data, process it through a multi-stage pipeline, and deliver actionable insights. The system transitions from **Descriptive Analysis** (historical view) to **Prescriptive Analysis** (strategic simulation), **Probabilistic Risk Assessment**, **Sensitivity Analysis**, and finally a consolidated **KPI Layer** for executive decision-making.

### 2.1 Core Architectural Principles

1. **Dict-Passing Architecture:** State is managed by passing a Python dictionary (`data = {'sales': df, 'inventory': df}`) between functions, ensuring statelessness and high testability.
2. **Traceability:** A unique `correlation_id` (UUID) is generated at the pipeline's initialization and propagated through every log entry and transformation step (See **Section 6.1** for Logging Standards).
3. **Immutability:** Raw data in `data/raw` is read-only. All transformations result in new artifacts stored in `data/processed` or `analysis/`.
4. **Modularity:** Each analytical stage (SQL, Time Series, Forecast, Scenarios, Risk, Sensitivity, KPI) functions as an independent unit.

### 2.2 Project Directory Tree

The project follows a strict hierarchical structure.

```text
C:\DATA_ANALYSIS\DSS_SALES_INVENTORY
|   pipeline.py                     # [Entry Point] Main orchestrator
|   README.md
|
+---analysis
|   |   analysis.py
|   |   analytics.db                # SQLite Database
|   |   run_sql_layer.py
|   |
|   +---forecast
|   |       forecast_evaluation.md
|   |       forecast_results.csv    # [Week 5 Output]
|   |       short_term_forecast.py
|   |
|   +---kpis                        # [Week 9 New Module]
|   |       kpi_definitions.py
|   |       kpi_documentation.md
|   |       product_kpis.csv
|   |
|   +---risk
|   |       product_risk_scores.csv # [Week 7 Output]
|   |       risk_assessment_report.md
|   |       risk_simulation.py
|   |
|   +---scenarios
|   |       scenarios_comparison.xlsx
|   |       scenario_analysis.py
|   |       scenario_insights.md
|   |
|   +---sensitivity
|   |   |   sensitivity_analysis.py
|   |   |   sensitivity_findings.md
|   |   |
|   |   \---outputs
|   |           sensitivity_heatmap.png
|   |           sensitivity_spider.png
|   |           sensitivity_tornado.png
|   |
|   +---sql
|   |       advanced_analysis.sql
|   |       views.sql
|   |
|   +---time_series
|   |       time_series_analysis.py
|   |       trend_insights.md
|
+---cleaning
|       cleaning.py
|
+---data
|   +---processed                   # Cleaned Intermediate Files
|   |       inventory_cleaned.csv
|   |       inventory_features.csv
|   |       sales_cleaned.csv
|   |       sales_features.csv
|   |
|   \---raw                         # Immutable Inputs
|           inventory.csv
|           sales.csv
|
+---features
|       features.py
|
+---ingestion
|       ingestion.py
|
\---reporting
    \---outputs
        |   analysis_summary.csv
        |   inventory_status_view.csv
        |
        \---plots                   # Visualization Images
                trend_daily_revenue.png
                combined_all_products_trend.png

```

---

## 3. Data Flow & Pipeline Sequence

### 3.1 Pipeline Execution Logic

The data journey follows a directed dependency graph.

```mermaid
graph TD
    %% Nodes
    RAW[("Raw Data<br/>(data/raw/*.csv)")]
    CLEAN[("Cleaning & Features<br/>(data/processed/*.csv)")]
    
    SQL["SQL Layer<br/>(Inventory Status)"]
    TS["Time Series<br/>(Trend Slopes)"]
    FC["Forecast Layer<br/>(analysis/forecast/forecast_results.csv)"]
    
    SCEN["Scenario Analysis<br/>(analysis/scenarios/)"]
    RISK["Risk Simulation<br/>(analysis/risk/)"]
    SENS["Sensitivity Analysis<br/>(analysis/sensitivity/)"]
    KPI["KPI Layer<br/>(analysis/kpis/)"]
    
    REPORT["Reporting & Outputs"]

    %% Flow
    RAW --> |Ingestion| CLEAN
    CLEAN --> SQL
    CLEAN --> TS
    CLEAN --> FC
    
    %% Dependencies
    FC --> SCEN
    CLEAN --> SCEN
    
    FC --> RISK
    CLEAN --> RISK
    
    %% Advanced Analysis
    RISK --> SENS
    SCEN --> SENS
    
    %% KPI Integration
    CLEAN --> KPI
    FC --> KPI
    RISK --> KPI
    
    %% Reporting
    SQL --> REPORT
    TS --> REPORT
    SENS --> REPORT
    KPI --> REPORT

```

1. **Ingestion & Cleaning:** Loads raw CSVs, sanitizes types, and fills nulls.
2. **Feature Engineering:** Merges sales and inventory to create the foundational `inventory_features.csv`.
3. **Analytical Branching:** Data flows in parallel to SQL, Time Series, and Forecast.
4. **Strategic Layer:**
* **Scenarios:** Simulates market conditions.
* **Risk:** Monte Carlo simulations.
* **Sensitivity:** Perturbs inputs to test robustness.
* **KPI Layer (Week 9):** Aggregates Inventory, Forecast, and Risk data to generate final executive decision flags (Safe vs. Intervention).



### 3.2 Module Summary Table

| Module | Primary Script | Core Functions | Inputs | Outputs |
| --- | --- | --- | --- | --- |
| **Ingestion** | `ingestion.py` | `load_csv_safely` | Raw CSVs | Pandas DataFrames |
| **Cleaning** | `cleaning.py` | `clean_data` | Raw DataFrames | `sales_cleaned.csv` |
| **Features** | `features.py` | `merge_data` | Cleaned DataFrames | `inventory_features.csv` |
| **SQL Layer** | `run_sql_layer.py` | `export_view` | Feature Data | `inventory_status_view.csv` |
| **Time Series** | `time_series_analysis.py` | `detect_trend` | Daily Sales | `trend_insights.md` |
| **Forecast** | `short_term_forecast.py` | `predict_demand` | Time Series Data | `analysis/forecast/forecast_results.csv` |
| **Scenarios** | `scenario_analysis.py` | `simulate_scenario` | Forecasts, Inventory | `scenarios_comparison.xlsx` |
| **Risk (Week 7)** | `risk_simulation.py` | `run_monte_carlo` | `forecast_results.csv`<br>

<br>`inventory_features.csv` | `product_risk_scores.csv`<br>

<br>`risk_assessment_report.md` |
| **Sensitivity (Week 8)** | `sensitivity_analysis.py` | `run_sensitivity_analysis` | `data dict` (sales, inv, risk) | `sensitivity_findings.md`, `outputs/sensitivity_*.png` |
| **KPI Layer (Week 9)** | `analysis/kpis/kpi_definitions.py` | `run_kpi_layer`, `compute_decision_flags` | `forecast_results.csv`<br>

<br>`product_risk_scores.csv` | `analysis/kpis/product_kpis.csv`<br>

<br>`analysis/kpis/kpi_documentation.md` |

---

## 4. Technical Specifications & Formulas

### 4.1 Data Dictionary

| Field Name | Type | Origin | Description |
| --- | --- | --- | --- |
| `product_id` | `int` | Raw | Unique identifier for products. |
| `inventory_risk_score` | `float` | KPI Layer | Composite score (0-100) combining AI risk and operational coverage. |
| `demand_pressure_index` | `float` | KPI Layer | Normalized ratio of Forecast / Stock (0-100). |
| `profitability_margin` | `float` | KPI Layer | Normalized expected profit (0-100). |
| `decision_flag` | `string` | KPI Layer | Actionable output: "Safe" or "Requires Intervention". |
| `sensitivity_ranking` | `Series` | Sensitivity Layer | Normalized sensitivity score per variable. |

### 4.2 Master Formulas Table (KPI Update)

| Metric | Formula Logic | Description |
| --- | --- | --- |
| **Operational Risk** | `1 / (1 + stock_ratio)` | Risk increases as stock coverage decreases. |
| **Inv Risk Score** | `0.6 * AI_Risk + 0.4 * Op_Risk` | Weighted average of Probabilistic Risk (Week 7) and Operational Risk. |
| **Demand Pressure** | `Forecast / Stock` (Normalized) | Measures how much of current stock is claimed by predicted demand. |
| **Decision Logic** | `(Risk≥70 OR Pressure≥75) AND Profit≥40` | Flags high-risk items that are also high-value/profitable. |

---

## 5. Detailed Module Implementation & QA Checklists

### 5.1 Ingestion & Cleaning (Week 1)

* **Goal:** Ensure data hygiene.
* **QA Checklist:**
* [ ] File Existence: `data/raw/sales.csv` and `data/raw/inventory.csv` are present.
* [ ] Schema Check: `product_id` is Integer.



### 5.2 SQL Decision Layer (Weeks 2-3)

* **Goal:** Operational reporting.
* **QA Checklist:**
* [ ] View Generation: `inventory_status_view.csv` created.



### 5.3 Time Series Analysis (Week 4)

* **Goal:** Trend detection.
* **QA Checklist:**
* [ ] Plot Generation: `reporting/plots/` contains `.png` files.



### 5.4 Short-Term Forecast (Week 5)

* **Goal:** 28-day demand prediction.
* **QA Checklist:**
* [ ] File Path: Output exists at `analysis/forecast/forecast_results.csv`.
* [ ] Sanity Check: No negative forecasts.



### 5.5 Scenario Analysis (Week 6)

* **Goal:** "What-if" simulations.
* **QA Checklist:**
* [ ] Formatting: `scenarios_comparison.xlsx` is readable.



### 5.6 Probabilistic Risk Simulation (Week 7)

* **Goal:** Monte Carlo simulation for risk scoring.
* **QA Checklist:**
* [ ] Range Check: `risk_score` is normalized between 0.0 and 1.0.



### 5.7 Sensitivity Analysis (Week 8)

* **Goal:** Evaluate model robustness via OAT perturbation.
* **QA Checklist:**
* [ ] Sensitivity scores are normalized (0–1).
* [ ] Tornado, Radar, and Heatmap images are generated.



### 5.8 KPI Layer (Week 9)

* **Goal:** Aggregate insights into executive decision flags.
* **Script:** `analysis/kpis/kpi_definitions.py`
* **Execution Overview:**
* **Total Products Processed:** 100
* **Status:** Success


* **Data Sources & Deduplication:**
| Dataset | Rows (Raw) | Unique Products | Status |
| :--- | :--- | :--- | :--- |
| Inventory View | 100 | 100 | Deduped |
| Inventory Features | 100 | 100 | Deduped |
| Forecast | 2,800 | 100 | Aggregated |
| Risk Results | 100 | 100 | Deduped |
* **Column Resolutions:**
* **Stock:** `stock_on_hand` (Primary) -> `quantity` (Fallback)
* **Demand:** `forecast_quantity`
* **Profit:** `expected_profit_mean` (from Risk) or Computed (`unit_price` - `unit_cost`)


* **KPI Statistics:**
| KPI | Min | Mean | Median | Max |
| :--- | :--- | :--- | :--- | :--- |
| `inventory_risk_score` | 12.50 | 45.20 | 42.10 | 98.50 |
| `demand_pressure_index` | 5.00 | 38.40 | 35.00 | 92.00 |
| `profitability_margin` | 10.00 | 55.60 | 58.00 | 99.00 |
* **Decision Flag Distribution:**
* **Safe:** 98 products
* **Requires Intervention:** 2 products


* **Top 10 Products by Risk & Pressure:**
| product_id | inventory_risk_score | demand_pressure_index | decision_flag |
| :--- | :--- | :--- | :--- |
| 1045 | 98.50 | 92.00 | **Requires Intervention** |
| 1089 | 88.20 | 85.10 | **Requires Intervention** |
| 1012 | 68.00 | 65.00 | Safe |
| 1003 | 64.50 | 60.20 | Safe |
| 1022 | 62.10 | 58.40 | Safe |
| 1056 | 59.00 | 55.00 | Safe |
| 1034 | 55.20 | 50.10 | Safe |
| 1078 | 50.00 | 48.00 | Safe |
| 1099 | 48.50 | 45.00 | Safe |
| 1011 | 45.00 | 42.00 | Safe |
* **QA Checklist:**
* [ ] **Execution:** Script executes successfully.
* [ ] **Validation:** KPI values are within 0-100 range.
* [ ] **Reporting:** `kpi_documentation.md` generated with statistics.
* [ ] **Logic:** Decision flags correctly identify high risk/pressure items.



---

## 6. Operational Protocols

### 6.1 Logging Standards

* **Pattern:** `%(asctime)s - %(levelname)s - [Correlation ID] - %(message)s`
* **Mandate:** Every function entry/exit and exception must be logged with the ID.

### 6.2 Error Taxonomy

| Exception | Severity | Trigger | Resolution Action |
| --- | --- | --- | --- |
| `FileNotFoundError` | **Critical** | Missing input CSVs. | Check `data/raw` or previous pipeline steps. |
| `ValidationError` | **Critical** | Schema mismatch/Bad types. | Reject source data; clean manually. |
| `InsufficientDataError` | **Warning** | History < 14 days. | Log warning, skip product, continue. |
| `MissingForecastDataError` | **Critical** | Missing `forecast_results.csv`. | Run `short_term_forecast.py` first. |

---

*End of Comprehensive Reference Manual v3.4*