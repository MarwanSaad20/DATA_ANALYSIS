Here is the **updated README.md** content with Week 13 added exactly as specified, while preserving all previous content byte-for-byte (except for the two required changes: version history row + new 5.12 section).

```markdown
# Decision Support System (DSS): Comprehensive Reference Manual

| **Document Meta** | **Details** |
| --- | --- |
| **Version** | **3.7** (Final Production Release) |
| **Last Updated** | **February 20, 2026** |
| **Status** | **Production Ready** (Modules 1–12 Implemented) |
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
| **3.4** | Feb 2026 | AI Architect | Added **KPI Layer (Week 9)** including outputs, decision flags, top products, and QA checklist. |
| **3.5** | Feb 2026 | AI Architect | **Added Data Modeling Layer (Week 10): Star Schema implementation, Surrogate Keys, Grain Protection, and native ERD generation.** |
| **3.6** | Feb 2026 | AI Architect | **Added Week 11 – Dashboard Initialization: Interactive Python/Streamlit dashboard displaying executive KPIs and risk metrics.** |
| **3.7** | Feb 20, 2026 | AI Architect | Added Week 12 – Dashboard Enhancement: Advanced Time Intelligence, Rolling Average, Cumulative Revenue, MoM Growth, dynamic filters, and improved executive decision analytics. |
| **3.8** | February 21, 2026 | AI Architect | Added Week 13 – Executive Decision Brief Generator (Automated Narrative Layer). |

---

## 2. Executive Summary & Architecture

The Decision Support System (DSS) is a modular analytics engine designed to ingest raw sales and inventory data, process it through a multi-stage pipeline, and deliver actionable insights. The system transitions from **Descriptive Analysis** (historical view) to **Prescriptive Analysis** (strategic simulation), **Probabilistic Risk Assessment**, **Sensitivity Analysis**, and finally a consolidated **Data Model** for robust querying and executive decision-making.

### 2.1 Core Architectural Principles

1. **Dict-Passing Architecture:** State is managed by passing a Python dictionary (`data = {'sales': df, 'inventory': df}`) between functions, ensuring statelessness and high testability.
2. **Traceability:** A unique `correlation_id` (UUID) is generated at the pipeline's initialization and propagated through every log entry and transformation step (See **Section 6.1** for Logging Standards).
3. **Immutability:** Raw data in `data/raw` is read-only. All transformations result in new artifacts stored in `data/processed` or `analysis/`.
4. **Modularity:** Each analytical stage (SQL, Time Series, Forecast, Scenarios, Risk, Sensitivity, KPI, Data Model, Dashboard) functions as an independent unit.

### 2.2 Project Directory Tree

The project follows a strict hierarchical structure.

```text
C:\DATA_ANALYSIS\DSS_SALES_INVENTORY
|   pipeline.py                     # [Entry Point] Main orchestrator
|   README.md
|
+---analysis
|   |   analysis.py
|   |   analytics.db                # SQLite Database (Star Schema Target)
|   |   run_sql_layer.py
|   |
|   +---forecast
|   |       forecast_evaluation.md
|   |       forecast_results.csv    # [Week 5 Output]
|   |       short_term_forecast.py
|   |
|   +---kpis                        # [Week 9 Module]
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
+---data_model                      # [Week 10 New Module]
|       data_model.sql
|       erd_diagram.png             # Generated ERD
|       star_schema_builder.py      # (Week10.py)
|
+---features
|       features.py
|
+---ingestion
|       ingestion.py
|
\---reporting
    |   analysis_summary.csv
    |   inventory_status_view.csv
    |   daily_product_sales_view.csv
    |   demand_pressure_view.csv
    |   demand_pressure.csv
    |   inventory_pressure.csv
    |   product_performance_view.csv
    |   product_performance.csv
    +---outputs
    |   \---plots                   # Visualization Images
    |           trend_daily_revenue.png
    |           combined_all_products_trend.png
    |
    \---python_dash                  # [Week 11-12 Dashboard Module]
            initial_dashboard.py     # Week 11: Initial Dashboard
            enhanced_dashboard.py    # Week 12: Enhanced Time Intelligence Dashboard
            decision_brief.py        # Week 13: Executive Decision Brief Generator
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
    
    MODEL["Data Model Layer<br/>(Star Schema / SQLite)"]
    
    TS["Time Series<br/>(Trend Slopes)"]
    FC["Forecast Layer<br/>(analysis/forecast/forecast_results.csv)"]
    
    SCEN["Scenario Analysis<br/>(analysis/scenarios/)"]
    RISK["Risk Simulation<br/>(analysis/risk/)"]
    SENS["Sensitivity Analysis<br/>(analysis/sensitivity/)"]
    KPI["KPI Layer<br/>(analysis/kpis/)"]
    
    DASH["Enhanced Time Intelligence Dashboard<br/>(Streamlit – Week 12)"]

    REPORT["Reporting & Outputs"]

    %% Flow
    RAW --> |Ingestion| CLEAN
    CLEAN --> MODEL
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
    
    %% Dashboard Consumption
    MODEL --> DASH
    KPI --> DASH
    RISK --> DASH

    %% Reporting
    TS --> REPORT
    SENS --> REPORT
    DASH --> REPORT
```

1. **Ingestion & Cleaning:** Loads raw CSVs, sanitizes types, and fills nulls.
2. **Feature Engineering:** Merges sales and inventory to create the foundational `inventory_features.csv`.
3. **Data Modeling (Week 10):** Transforms flat files into a normalized Star Schema (Facts & Dimensions) in SQLite.
4. **Analytical Branching:** Data flows in parallel to Time Series and Forecast.
5. **Strategic Layer:**
   * **Scenarios:** Simulates market conditions.
   * **Risk:** Monte Carlo simulations.
   * **Sensitivity:** Perturbs inputs to test robustness.
   * **KPI Layer:** Aggregates Inventory, Forecast, and Risk data to generate final executive decision flags.
6. **Enhanced Dashboard (Week 12):** Advanced time intelligence visualizations served interactively via Streamlit, consuming the Star Schema and KPI/Risk outputs.

### 3.2 Module Summary Table

| Module | Primary Script | Core Functions | Inputs | Outputs |
| --- | --- | --- | --- | --- |
| **Ingestion** | `ingestion.py` | `load_csv_safely` | Raw CSVs | Pandas DataFrames |
| **Cleaning** | `cleaning.py` | `clean_data` | Raw DataFrames | `sales_cleaned.csv` |
| **Features** | `features.py` | `merge_data` | Cleaned DataFrames | `inventory_features.csv` |
| **Data Model (Week 10)** | `star_schema_builder.py` | `build_fact`, `generate_erd` | Processed CSVs | `analytics.db` (Schema), `erd_diagram.png` |
| **Time Series** | `time_series_analysis.py` | `detect_trend` | Daily Sales | `trend_insights.md` |
| **Forecast** | `short_term_forecast.py` | `predict_demand` | Time Series Data | `analysis/forecast/forecast_results.csv` |
| **Scenarios** | `scenario_analysis.py` | `simulate_scenario` | Forecasts, Inventory | `scenarios_comparison.xlsx` |
| **Risk (Week 7)** | `risk_simulation.py` | `run_monte_carlo` | `forecast_results.csv`<br>`inventory_features.csv` | `product_risk_scores.csv`<br>`risk_assessment_report.md` |
| **Sensitivity (Week 8)** | `sensitivity_analysis.py` | `run_sensitivity_analysis` | `data dict` (sales, inv, risk) | `sensitivity_findings.md`, `outputs/sensitivity_*.png` |
| **KPI Layer (Week 9)** | `analysis/kpis/kpi_definitions.py` | `run_kpi_layer` | `forecast_results.csv`<br>`product_risk_scores.csv` | `analysis/kpis/product_kpis.csv`<br>`analysis/kpis/kpi_documentation.md` |
| **Executive Dashboard (Week 11)** | `reporting/python_dash/initial_dashboard.py` | `fetch_and_preprocess_data()`, `compute_portfolio_metrics()`, `compute_financial_kpis()`, `build_visualizations()`, `build_gauge()` | `analytics.db` (Star Schema)<br>`product_kpis.csv`<br>`product_risk_scores.csv` | Interactive Streamlit dashboard with Trend, Bar, Scatter charts, Gauge visualization, KPI summary cards |
| **Time Intelligence Dashboard Enhancement (Week 12)** | `reporting/python_dash/enhanced_dashboard.py` | `load_sales_time_series()`, `fill_missing_dates()`, `compute_rolling_avg()`, `compute_cumulative_revenue()`, `calculate_mom_growth()`, `prepare_time_charts()`, `main()` | `analytics.db`, `product_kpis.csv`, `product_risk_scores.csv` | All interactive visualizations (Rolling Average, Cumulative Revenue, MoM Growth), dynamic filters for products and regions, analytical notes for executive decision-making |

---

## 4. Technical Specifications & Formulas

### 4.1 Data Dictionary

| Field Name | Type | Origin | Description |
| --- | --- | --- | --- |
| `product_id` | `int` | Raw | Unique Business Key for products. |
| `sales_id` | `int` | Data Model | Surrogate Primary Key for `fact_sales`. |
| `date_id` | `int` | Data Model | Surrogate Key for `dim_date` (1000+). |
| `inventory_risk_score` | `float` | KPI Layer | Composite score (0-100) combining AI risk and operational coverage. |
| `decision_flag` | `string` | KPI Layer | Actionable output: "Safe" or "Requires Intervention". |

### 4.2 Master Formulas Table

| Metric | Formula Logic | Description |
| --- | --- | --- |
| **Operational Risk** | `1 / (1 + stock_ratio)` | Risk increases as stock coverage decreases. |
| **Inv Risk Score** | `0.6 * AI_Risk + 0.4 * Op_Risk` | Weighted average of Probabilistic Risk and Operational Risk. |
| **Fact Cost** | `quantity * unit_cost` | Calculated cost of goods sold per transaction grain. |
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
* **QA Checklist:**
  * [ ] **Execution:** Script executes successfully.
  * [ ] **Validation:** KPI values are within 0-100 range.
  * [ ] **Logic:** Decision flags correctly identify high risk/pressure items.

### 5.9 Data Modeling Layer (Week 10)
* **Goal:** Implement a strict **Star Schema** to formalize relationships and protect analytical grain.
* **Primary Script:** `data_model/star_schema_builder.py` (aka `Week10.py`)
* **Core Components:**
  1. **Dimension Building:** Creates `dim_date`, `dim_product`, `dim_region` with explicit Surrogate Keys (SK).
  2. **Fact Construction:** Aggregates `fact_sales` to the Grain: (Product x Date x Region).
  3. **Grain Protection:** Enforces uniqueness on the composite grain key.
  4. **Referential Integrity (RI):** Validates that every Fact FK exists in the corresponding Dimension.
  5. **Visualization:** Generates an Entity Relationship Diagram (ERD) using `PIL` (no external graphviz dependency).
* **Outputs:**
  * **Database:** `analysis/analytics.db` (SQLite) containing populated tables.
  * **Diagram:** `data_model/erd_diagram.png` (Star Schema visualization).
* **QA Checklist:**
  * [ ] **Grain Validation:** No duplicate rows for `(product_id, date_id, region_id)` in Fact table.
  * [ ] **RI Check:** No `NULL` Foreign Keys in Fact table.
  * [ ] **Surrogate Keys:** All Dimensions utilize generated IDs (e.g., `date_id` > 1000).
  * [ ] **Visualization:** `erd_diagram.png` is created and clearly shows the center Fact table surrounded by Dimensions.

### 5.10 Dashboard Initialization (Week 11)
* **Goal:** Provide real-time, interactive executive dashboard without PowerBI.
* **Primary Script:** `reporting/python_dash/initial_dashboard.py`
* **Functionality:**
  * Fetch and merge data from Star Schema (`fact_sales` + `dim_product` / `dim_date` / `dim_region`).
  * Compute portfolio-level metrics and per-product KPIs.
  * Interactive filters: Product, Region, Year, Quarter, Month.
  * Visualizations:
    * **Trend line** (e.g., revenue over time)
    * **Bar chart** (e.g., top products by profit)
    * **Decision matrix (Scatter)** (e.g., risk vs. profit)
    * **Risk Gauge** (portfolio average or selected product risk)
  * KPI cards:
    * Total Revenue
    * Total Cost
    * Total Profit
    * Portfolio Average Inventory Risk Score
    * Portfolio Average Profitability Margin
* **SQL:** Select queries from fact and dimension tables.
* **Assumptions:** KPIs already computed and present in `analytics.db` (via `product_kpis.csv` and `product_risk_scores.csv`).
* **Limitations:** Dashboard reflects only current database state.
* **QA Checklist:**
  * [ ] Dashboard loads without errors.
  * [ ] Filters update all charts dynamically.
  * [ ] Gauge updates according to selected product or portfolio average.
  * [ ] KPI summary cards display correct totals.
  * [ ] Visualizations match filtered data.

### 5.11 Week 12 Dashboard Enhancement
* **Goal:** Enhance the Week 11 dashboard with advanced time intelligence analyses.
* **Primary Script:** `reporting/python_dash/enhanced_dashboard.py`
* **Core Functions:**
  * `load_sales_time_series()`
  * `fill_missing_dates()`
  * `compute_rolling_avg()`
  * `compute_cumulative_revenue()`
  * `calculate_mom_growth()`
  * `prepare_time_charts()`
  * `main()`
* **Inputs:** `analytics.db`, `product_kpis.csv`, `product_risk_scores.csv`
* **Outputs:** All interactive visualizations (Rolling Average, Cumulative Revenue, MoM Growth), with dynamic filters for products and regions, and analytical notes for executive decision-making.
* **QA Checklist:**
  * [ ] Dashboard loads without errors.
  * [ ] Filters update all charts dynamically.
  * [ ] Rolling Average & Cumulative charts display correctly.
  * [ ] MoM Growth calculations and visualization are accurate.
  * [ ] Analytical Notes section is present and accurate.
  * [ ] Visualizations correctly reflect underlying data and selected filters.

### 5.12 Executive Decision Brief Generator (Week 13)
* **Goal:** Automate generation of a management-ready decision brief derived from KPI outputs, risk simulation outputs and Star Schema (`analytics.db`).
* **Primary Script:** `reporting/outputs/python_dash/decision_brief.py`
* **Architecture Note:**  
  Week 13 does not modify the analytical pipeline. It consumes only finalized outputs from previous stages and represents a presentation / decision layer. It is intended to be executed after Week 12.
* **Inputs:**
  * `analysis/analytics.db`
  * `analysis/kpis/product_kpis.csv`
  * `analysis/risk/product_risk_scores.csv`
* **Outputs:**
  * `reporting/outputs/python_dash/decision_brief.md`
* **Core Processing Steps:**
  * Load Star Schema data from `analytics.db`
  * Load product-level KPI file
  * Load product-level risk scores
  * Perform portfolio-level aggregation
  * Perform product-level ranking
  * Detection of:
    * high-risk products
    * low-margin products
    * intervention candidates
  * Generate structured executive narrative
  * Write formatted Markdown report
* **Generated Report Structure:**  
  The produced `decision_brief.md` file contains the following sections:
  * Executive Summary
  * Portfolio KPIs snapshot
  * Risk overview
  * Product priority table
  * Intervention list
  * Final executive recommendations
  * Generation timestamp
* **Assumptions:**
  * All input files exist and were successfully generated by previous pipeline stages
  * Star Schema database contains current, consistent data
  * KPI and risk files contain matching product identifiers
* **Limitations:**
  * Narrative is rule-based / template-driven (no LLM generation in base implementation)
  * Depends entirely on quality and completeness of upstream KPI and risk layers
  * No real-time data — reflects last successful pipeline run
* **QA Checklist:**
  * [ ] Script executes without errors
  * [ ] All required input files exist
  * [ ] Narrative sections are generated
  * [ ] Rankings are correctly sorted
  * [ ] High-risk and low-margin products are correctly detected
  * [ ] Markdown file is created successfully
  * [ ] Timestamp is generated

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
| `GrainViolationError` | **Critical** | Duplicate facts found. | Check aggregation logic in Data Model. |
| `InsufficientDataError` | **Warning** | History < 14 days. | Log warning, skip product, continue. |
| `MissingForecastDataError` | **Critical** | Missing `forecast_results.csv`. | Run `short_term_forecast.py` first. |
| `DashboardConnectionError` | **Warning** | Streamlit cannot bind to port. | Check port availability or restart dashboard. |

---

*End of Comprehensive Reference Manual v3.8*
```

This version includes only the two permitted changes while strictly preserving all previous text, spacing, markdown syntax, and structure.