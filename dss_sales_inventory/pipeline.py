import uuid
import logging
import sys
from pathlib import Path

# ========================
# Path Setup (Fix for Import Issues)
# ========================
# Dynamically add the project root to sys.path to ensure modules can be imported
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ========================
# Imports
# ========================
from ingestion.ingestion import run_ingestion
from cleaning.cleaning import run_cleaning
from features.features import run_features
from analysis.analysis import run_analysis

# SQL analytics layer
from analysis.run_sql_layer import main as run_sql_layer

# Time Series Analysis
from analysis.time_series.time_series_analysis import main as run_time_series_analysis

# Short-Term Forecast Layer (Week 5)
from analysis.forecast.short_term_forecast import run_short_term_forecast

# Scenario Analysis Layer (Week 6)
from analysis.scenarios.scenario_analysis import run_scenario_analysis

# Risk Simulation Layer (Week 7)
from analysis.risk.risk_simulation import run_risk_simulation

# Sensitivity Analysis Layer (Week 8)
from analysis.sensitivity.sensitivity_analysis import run_sensitivity_analysis

# KPI Layer (Week 9)
from analysis.kpis.kpi_definitions import run_kpi_layer


# ========================
# Logger configuration
# ========================
dss_logger = logging.getLogger('dss_logger')
dss_logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(stage)s] [%(run_id)s] [%(function)s] %(message)s"
)
console_handler.setFormatter(formatter)

if not dss_logger.handlers:
    dss_logger.addHandler(console_handler)


# ========================
# Main pipeline
# ========================
if __name__ == "__main__":
    correlation_id = str(uuid.uuid4())

    dss_logger.info(
        "Pipeline started",
        extra={
            "run_id": correlation_id,
            "stage": "PIPELINE",
            "function": "main",
            "rows_in": None,
            "rows_out": None,
            "status": "STARTED"
        }
    )

    data = None

    try:
        # ========================
        # Stage 1: Ingestion
        # ========================
        dss_logger.info(
            "Ingestion stage started",
            extra={
                "run_id": correlation_id,
                "stage": "INGESTION",
                "function": "run_ingestion",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        # Ingestion returns the initial raw dataframe
        data = run_ingestion(correlation_id)
        dss_logger.info(
            "Ingestion stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "INGESTION",
                "function": "run_ingestion",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 2: Cleaning
        # ========================
        dss_logger.info(
            "Cleaning stage started",
            extra={
                "run_id": correlation_id,
                "stage": "CLEANING",
                "function": "run_cleaning",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        # Cleaning processes the dataframe
        data = run_cleaning(data, correlation_id)
        dss_logger.info(
            "Cleaning stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "CLEANING",
                "function": "run_cleaning",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 3: Features
        # ========================
        dss_logger.info(
            "Features stage started",
            extra={
                "run_id": correlation_id,
                "stage": "FEATURES",
                "function": "run_features",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        
        # run_features now returns a dictionary: {'sales': df, 'inventory': df}
        data = run_features(data, correlation_id)
        
        # Verify structure for logging purposes (optional validation)
        if isinstance(data, dict) and 'sales' in data and 'inventory' in data:
            sales_shape = data['sales'].shape
            inv_shape = data['inventory'].shape
            dss_logger.info(f"Features split created - Sales: {sales_shape}, Inventory: {inv_shape}",
                            extra={"run_id": correlation_id, "stage": "FEATURES", "function": "run_features", "rows_in": None, "rows_out": None, "status": "INFO"})

        dss_logger.info(
            "Features stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "FEATURES",
                "function": "run_features",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 4: Analysis (Python layer)
        # ========================
        dss_logger.info(
            "Analysis stage started",
            extra={
                "run_id": correlation_id,
                "stage": "ANALYSIS",
                "function": "run_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        # Passing the dictionary containing 'sales' and 'inventory'
        run_analysis(data, correlation_id)
        
        dss_logger.info(
            "Analysis stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "ANALYSIS",
                "function": "run_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 5: SQL Analytics Layer (Week 2+3)
        # ========================
        dss_logger.info(
            "SQL analytics stage started",
            extra={
                "run_id": correlation_id,
                "stage": "SQL_ANALYTICS",
                "function": "run_sql_layer",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        run_sql_layer()
        dss_logger.info(
            "SQL analytics stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "SQL_ANALYTICS",
                "function": "run_sql_layer",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 6: Time Series Analysis (Week 4)
        # ========================
        dss_logger.info(
            "Time Series Analysis stage started",
            extra={
                "run_id": correlation_id,
                "stage": "TIME_SERIES",
                "function": "run_time_series_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )

        # Using dynamic path for root_dir based on script location
        ROOT_DIR = str(Path(__file__).resolve().parent)
        run_time_series_analysis(root_dir=ROOT_DIR)

        dss_logger.info(
            "Time Series Analysis stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "TIME_SERIES",
                "function": "run_time_series_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 7: Short-Term Forecast Layer (Week 5)
        # ========================
        dss_logger.info(
            "Short-Term Forecast stage started",
            extra={
                "run_id": correlation_id,
                "stage": "SHORT_TERM_FORECAST",
                "function": "run_short_term_forecast",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        run_short_term_forecast(correlation_id)
        dss_logger.info(
            "Short-Term Forecast stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "SHORT_TERM_FORECAST",
                "function": "run_short_term_forecast",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 8: Scenario Analysis Layer (Week 6)
        # ========================
        dss_logger.info(
            "Scenario Analysis stage started",
            extra={
                "run_id": correlation_id,
                "stage": "SCENARIO_ANALYSIS",
                "function": "run_scenario_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        scenarios_df = run_scenario_analysis(correlation_id)
        data["scenarios"] = scenarios_df
        dss_logger.info(
            "Scenario Analysis stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "SCENARIO_ANALYSIS",
                "function": "run_scenario_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 9: Risk Simulation Layer (MOVED BEFORE SENSITIVITY)
        # ========================
        dss_logger.info(
            "Risk Simulation stage started",
            extra={
                "run_id": correlation_id,
                "stage": "RISK_SIMULATION",
                "function": "run_risk_simulation",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        run_risk_simulation(correlation_id)
        dss_logger.info(
            "Risk Simulation stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "RISK_SIMULATION",
                "function": "run_risk_simulation",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 10: KPI Layer (Week 9)
        # ========================
        dss_logger.info(
            "KPI Layer stage started",
            extra={
                "run_id": correlation_id,
                "stage": "KPIS",
                "function": "run_kpi_layer",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        data = run_kpi_layer(data, correlation_id)
        dss_logger.info(
            "KPI Layer stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "KPIS",
                "function": "run_kpi_layer",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 10: Sensitivity Analysis Layer (Week 8)
        # ========================
        dss_logger.info(
            "Sensitivity Analysis stage started",
            extra={
                "run_id": correlation_id,
                "stage": "SENSITIVITY_ANALYSIS",
                "function": "run_sensitivity_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )

        # Execute Sensitivity Analysis
        # Ensure run_sensitivity_analysis handles data['sales'] and data['inventory'] internally
        data = run_sensitivity_analysis(data, correlation_id)
        
        dss_logger.info(
            "Sensitivity Analysis stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "SENSITIVITY_ANALYSIS",
                "function": "run_sensitivity_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Pipeline success
        # ========================
        dss_logger.info(
            "Pipeline completed successfully",
            extra={
                "run_id": correlation_id,
                "stage": "PIPELINE",
                "function": "main",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

    except Exception as e:
        dss_logger.error(
            f"Pipeline failed with error: {str(e)}",
            extra={
                "run_id": correlation_id,
                "stage": "PIPELINE",
                "function": "main",
                "rows_in": None,
                "rows_out": None,
                "status": "FAILED"
            },
            exc_info=True
        )
        sys.exit(1)