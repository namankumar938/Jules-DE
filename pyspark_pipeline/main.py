# Main script for the PySpark ETL pipeline.
# This module will orchestrate the ingestion, transformation, and KPI generation steps.

import sys
import os
import logging # Import logging to get a logger instance in main

from pyspark.sql import SparkSession

from pyspark_pipeline.ingestion import load_purchase_data
from pyspark_pipeline.transformations import clean_and_transform_data
from pyspark_pipeline.kpi_generation import (
    calculate_top_selling_products,
    calculate_total_revenue_by_category,
    calculate_preferred_payment_method_by_category,
    calculate_purchase_frequency_per_user,
    calculate_average_order_value
)
from pyspark_pipeline.logging_utils import setup_logging

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "RetailAnalyticsPipeline") -> SparkSession:
    """
    Initializes and returns a SparkSession.

    Args:
        app_name: The name for the Spark application. Defaults to "RetailAnalyticsPipeline".

    Returns:
        A SparkSession object.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    return spark

def main():
    """
    Main function to orchestrate the PySpark retail data analytics pipeline.
    """
    setup_logging(level=logging.INFO) # Configure logging at INFO level
    logger.info("Starting Retail Analytics Pipeline application.")

    spark = None
    project_root_for_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    # Point back to the correct, existing file
    file_path = os.path.join(project_root_for_file, "sample_purchases.csv")

    try:
        spark = get_spark_session()
        logger.info("SparkSession initialized successfully.")

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
             logger.warning(f"No data loaded from '{file_path}' (file might be empty or header-only). Further processing will reflect this.")
        else:
            logger.info("Raw data sample (first 5 rows will be shown if DEBUG level is enabled):")
            if logger.isEnabledFor(logging.DEBUG):
                raw_df.show(5, truncate=False)
                logger.debug("Raw DataFrame schema:")
                raw_df.printSchema()

        # 2. Transformation
        logger.info("--- Step 2: Cleaning and Transforming Data ---")
        cleaned_df = clean_and_transform_data(raw_df)

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

        logger.info("Calculating Top Selling Products (Top 5)...")
        top_products_df = calculate_top_selling_products(cleaned_df, top_n=5)
        top_products_df.show(truncate=False)

        logger.info("Calculating Total Revenue by Category...")
        revenue_by_cat_df = calculate_total_revenue_by_category(cleaned_df, spark)
        revenue_by_cat_df.show(truncate=False)

        logger.info("Calculating Preferred Payment Method by Category...")
        preferred_pm_df = calculate_preferred_payment_method_by_category(cleaned_df, spark)
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
        logger.error(f"Please ensure 'sample_purchases.csv' exists at {file_path}.")
    except Exception as e:
        logger.error(f"A critical error occurred in the PySpark pipeline execution: {e}", exc_info=True)
    finally:
        if spark:
            logger.info("Stopping SparkSession.")
            spark.stop()

if __name__ == '__main__':
    main()
pass
