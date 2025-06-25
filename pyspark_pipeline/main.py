# Main script for the PySpark ETL pipeline.
# This module will orchestrate the ingestion, transformation, and KPI generation steps.

import sys
import os
import logging

from pyspark.sql import SparkSession

from pyspark_pipeline.ingestion import load_purchase_data
from pyspark_pipeline.transformations import clean_and_transform_data
# Constants from transformations will be replaced by config
# from pyspark_pipeline.transformations import ALLOWED_CATEGORIES as DEFAULT_ALLOWED_CATEGORIES
# from pyspark_pipeline.transformations import ALLOWED_PAYMENT_METHODS as DEFAULT_ALLOWED_PAYMENT_METHODS

from pyspark_pipeline.kpi_generation import (
    calculate_top_selling_products,
    calculate_total_revenue_by_category,
    calculate_preferred_payment_method_by_category,
    calculate_purchase_frequency_per_user,
    calculate_average_order_value
)
from pyspark_pipeline.logging_utils import setup_logging
from pyspark_pipeline.config_utils import load_config # Import config loading utility

# Initialize logger for this module at the module level
# Logger name will be 'pyspark_pipeline.main' when run with `python -m pyspark_pipeline.main`
logger = logging.getLogger(__name__)


def get_spark_session(app_name: str) -> SparkSession: # Removed default for app_name
    """
    Initializes and returns a SparkSession.

    Args:
        app_name: The name for the Spark application.

    Returns:
        A SparkSession object.
    """
    logger.debug(f"Initializing SparkSession with app_name: {app_name}")
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    return spark

def main():
    """
    Main function to orchestrate the PySpark retail data analytics pipeline.
    """
    # Load configuration first
    config = load_config() # Loads 'config.yaml' from project root by default
    if config is None:
        # setup_logging might not be called if config fails early, so use basic print/stderr
        # Or, call setup_logging with a default level before config loading for very early logs.
        # For now, assume load_config logs its own errors sufficiently.
        print("FATAL: Configuration loading failed. Exiting application.", file=sys.stderr)
        sys.exit(1)

    # Setup logging using level from config
    log_level_str = config.get('logging', {}).get('level', 'INFO').upper()
    actual_log_level = getattr(logging, log_level_str, logging.INFO)
    setup_logging(level=actual_log_level)

    logger.info("Starting Retail Analytics Pipeline application.")
    logger.debug(f"Configuration loaded: {config}") # Log entire config at debug

    spark = None
    try:
        # Get Spark application name from config
        app_name_default = "RetailAnalyticsPipeline_DefaultApp"
        app_name = config.get('application', {}).get('name', app_name_default)
        if app_name == app_name_default:
            logger.warning(f"Application name not found in config, using default: {app_name_default}")

        spark = get_spark_session(app_name=app_name)
        logger.info(f"SparkSession initialized successfully with app_name: {spark.conf.get('spark.app.name')}.")

        # Get input file path from config
        project_root_for_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        default_rel_file_path = "sample_purchases.csv" # Fallback if not in config
        rel_file_path = config.get('input_data', {}).get('file_path', default_rel_file_path)
        if rel_file_path == default_rel_file_path:
            logger.warning(f"Input file path not found in config, using default: {default_rel_file_path}")

        file_path = os.path.join(project_root_for_file, rel_file_path)
        logger.info(f"Starting data pipeline for file: {file_path}")

        # 1. Ingestion
        logger.info("--- Step 1: Ingesting Data ---")
        raw_df = load_purchase_data(spark, file_path)

        if raw_df is None:
            logger.error(f"Data ingestion failed for file: {file_path}. Pipeline cannot proceed.")
            return

        raw_count = raw_df.count()
        logger.info(f"Raw data count: {raw_count}")

        if raw_count == 0:
             logger.warning(f"No data loaded from '{file_path}'. Further processing will reflect this.")
        else:
            logger.info("Raw data sample (first 5 rows will be shown if DEBUG level is enabled):")
            if logger.isEnabledFor(logging.DEBUG):
                raw_df.show(5, truncate=False)
                logger.debug("Raw DataFrame schema:")
                raw_df.printSchema()

        # 2. Transformation
        logger.info("--- Step 2: Cleaning and Transforming Data ---")
        # Get transformation rules from config
        transform_rules = config.get('processing_rules', {}).get('transformations', {})
        allowed_categories = transform_rules.get('allowed_categories', ["men", "women", "kids"]) # Default if not in config
        allowed_payment_methods = transform_rules.get('allowed_payment_methods', ["upi", "credit_card", "wallet"])

        if not transform_rules.get('allowed_categories'):
            logger.warning("Allowed categories not found in config, using defaults.")
        if not transform_rules.get('allowed_payment_methods'):
            logger.warning("Allowed payment methods not found in config, using defaults.")

        # NOTE: clean_and_transform_data signature will need to be updated to accept these.
        # For this subtask, we are preparing main.py. This call will fail until transformations.py is updated.
        # cleaned_df = clean_and_transform_data(raw_df) # Original call
        cleaned_df = clean_and_transform_data(raw_df, allowed_categories, allowed_payment_methods) # Updated call

        if cleaned_df is None:
            logger.error("Data transformation failed. Pipeline cannot proceed.")
            return

        cleaned_count = cleaned_df.count()
        logger.info(f"Cleaned data count: {cleaned_count}")

        if cleaned_count > 0:
            logger.info("Cleaned data sample (first 5 rows will be shown if DEBUG level is enabled):")
            if logger.isEnabledFor(logging.DEBUG):
                cleaned_df.show(5, truncate=False)
                logger.debug("Cleaned DataFrame schema:")
                cleaned_df.printSchema()
        else:
            logger.warning("No data remaining after cleaning and transformation. KPIs will be based on empty data.")

        # 3. KPI Generation & Reporting
        logger.info("--- Step 3: Generating and Printing KPIs ---")

        kpi_rules = config.get('processing_rules', {}).get('kpi_generation', {})
        top_n_val = kpi_rules.get('top_selling_products', {}).get('top_n_default', 5)
        if top_n_val == 5 and not kpi_rules.get('top_selling_products', {}).get('top_n_default'):
             logger.warning("top_n_default for top selling products not found in config, using default: 5")


        logger.info(f"Calculating Top Selling Products (Top {top_n_val})...")
        # NOTE: calculate_top_selling_products signature might not need change if top_n already exists with default
        top_products_df = calculate_top_selling_products(cleaned_df, top_n=top_n_val)
        top_products_df.show(truncate=False)

        logger.info("Calculating Total Revenue by Category...")
        # NOTE: calculate_total_revenue_by_category signature will need to be updated for allowed_categories
        revenue_by_cat_df = calculate_total_revenue_by_category(cleaned_df, spark, allowed_categories)
        revenue_by_cat_df.show(truncate=False)

        logger.info("Calculating Preferred Payment Method by Category...")
        # NOTE: calculate_preferred_payment_method_by_category signature will need to be updated for allowed_categories and allowed_payment_methods
        preferred_pm_df = calculate_preferred_payment_method_by_category(cleaned_df, spark, allowed_categories, allowed_payment_methods)
        preferred_pm_df.show(truncate=False)

        logger.info("Calculating Purchase Frequency per User...")
        purchase_freq_df = calculate_purchase_frequency_per_user(cleaned_df)
        purchase_freq_df.show(truncate=False)

        logger.info("Calculating Average Order Value...")
        aov = calculate_average_order_value(cleaned_df)
        logger.info(f"Average Order Value: ${aov:.2f}")

        logger.info("--- Pipeline Execution Finished ---")

    except FileNotFoundError:
        logger.error(f"Input file not found error for path: {file_path}")
        logger.error(f"Please ensure the file exists at the specified path in config.yaml or as default.")
    except Exception as e:
        logger.error(f"A critical error occurred in the PySpark pipeline execution: {e}", exc_info=True)
    finally:
        if spark:
            logger.info("Stopping SparkSession.")
            spark.stop()

if __name__ == '__main__':
    main()
pass
