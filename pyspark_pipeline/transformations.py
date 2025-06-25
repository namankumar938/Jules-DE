# PySpark Data Transformation Logic will go here.
# This module will handle cleaning, validation, and transformation of PySpark DataFrames.

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)

# Hardcoded constants for allowed values are removed.
# These will now be passed as parameters to clean_and_transform_data.

def clean_and_transform_data(
    df: DataFrame | None,
    allowed_categories: list,
    allowed_payment_methods: list
) -> DataFrame | None:
    """
    Cleans and transforms the raw purchase data DataFrame.

    Args:
        df: The input DataFrame loaded from purchase data. Can be None if ingestion failed.
        allowed_categories: A list of allowed category strings.
        allowed_payment_methods: A list of allowed payment_method strings.

    Returns:
        A DataFrame containing only valid and cleaned records,
        with normalized 'category' and 'payment_method' columns, or None if an error occurs
        or if the input DataFrame is None/invalid.
    """
    if df is None:
        logger.error("Input DataFrame is None. Cannot perform transformations.")
        return None

    if not isinstance(df, DataFrame):
        logger.error(f"Input must be a valid PySpark DataFrame. Got type: {type(df)}")
        return None

    if not allowed_categories or not isinstance(allowed_categories, list):
        logger.error("allowed_categories must be a non-empty list.")
        return None # Or raise ValueError
    if not allowed_payment_methods or not isinstance(allowed_payment_methods, list):
        logger.error("allowed_payment_methods must be a non-empty list.")
        return None # Or raise ValueError

    try:
        logger.debug("Starting data cleaning and transformation process.")

        df_transformed = df.withColumn("category_trimmed", trim(col("category")))
        df_filtered_missing = df_transformed.filter(
            (col("category_trimmed").isNotNull()) & (col("category_trimmed") != "") &
            (col("price").isNotNull()) &
            (col("quantity").isNotNull())
        )

        df_validated_values = df_filtered_missing.filter(
            (col("price") > 0) &
            (col("quantity") > 0)
        )

        df_normalized_category = df_validated_values.withColumn(
            "category_normalized", lower(col("category_trimmed"))
        )
        # Use the passed parameter for filtering
        df_validated_category = df_normalized_category.filter(
            col("category_normalized").isin(allowed_categories)
        )

        df_normalized_payment = df_validated_category.withColumn(
            "payment_method_trimmed", trim(col("payment_method"))
        ).withColumn(
            "payment_method_normalized", lower(col("payment_method_trimmed"))
        )
        # Use the passed parameter for filtering
        df_validated_payment = df_normalized_payment.filter(
            col("payment_method_normalized").isin(allowed_payment_methods)
        )

        final_df = df_validated_payment.select(
            col("event_timestamp"),
            col("user_id"),
            col("order_id"),
            col("product_id"),
            col("product_name"),
            col("category_normalized").alias("category"),
            col("price"),
            col("quantity"),
            col("payment_method_normalized").alias("payment_method")
        )

        logger.info("Data cleaning and transformation completed successfully.")
        return final_df

    except AnalysisException as e:
        logger.error(f"Spark AnalysisException during data transformation: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error during data transformation: {e}", exc_info=True)
        return None


if __name__ == '__main__':
    import sys
    import os

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from pyspark_pipeline.logging_utils import setup_logging
    from pyspark_pipeline.config_utils import load_config # Import config loader
    setup_logging()

    spark_session_main = None
    raw_df_main = None

    # Load configuration to get parameters for testing
    config = load_config()
    if config is None:
        logger.critical("Failed to load configuration for transformations.py test run. Exiting.")
        sys.exit(1)

    # Get transformation rules from config, with defaults if not found
    transform_rules_config = config.get('processing_rules', {}).get('transformations', {})
    test_cats = transform_rules_config.get('allowed_categories', ["men", "women", "kids"])
    test_pm = transform_rules_config.get('allowed_payment_methods', ["upi", "credit_card", "wallet"])
    logger.info(f"Using categories for test: {test_cats}")
    logger.info(f"Using payment methods for test: {test_pm}")

    try:
        from pyspark_pipeline.main import get_spark_session
        spark_session_main = get_spark_session(app_name="TransformationTestConfig") # Updated app name
        logger.info("SparkSession obtained from pyspark_pipeline.main.")
    except ImportError:
        logger.warning("Could not import get_spark_session from pyspark_pipeline.main. Creating local SparkSession.")
        from pyspark.sql import SparkSession
        spark_session_main = SparkSession.builder \
            .appName("TransformationTestLocalConfig") \
            .master("local[*]") \
            .getOrCreate()

    if spark_session_main:
        try:
            from pyspark_pipeline.ingestion import load_purchase_data

            # Determine input file path from config
            input_data_config = config.get('input_data', {})
            rel_csv_file_path = input_data_config.get('file_path', "../sample_purchases.csv") # Default to known sample
            csv_file_path = os.path.join(project_root, rel_csv_file_path) # Construct path relative to project_root

            logger.info(f"--- Testing transformations with data from: {csv_file_path} ---")
            raw_df_main = load_purchase_data(spark_session_main, csv_file_path)

            if raw_df_main is not None:
                try:
                    raw_df_count = raw_df_main.count()
                    logger.info(f"Raw DataFrame row count: {raw_df_count}")
                    if raw_df_count > 0:
                        logger.debug("Raw DataFrame schema:") # Changed to debug
                        raw_df_main.printSchema()
                        logger.debug("Raw DataFrame sample (first 5 rows):") # Changed to debug
                        raw_df_main.show(5, truncate=False)
                    else:
                        logger.info("Raw DataFrame is empty.")

                    logger.info("Applying cleaning and transformations with config-driven rules...")
                    cleaned_df = clean_and_transform_data(raw_df_main, test_cats, test_pm)

                    if cleaned_df is not None:
                        cleaned_df_count = cleaned_df.count()
                        logger.info(f"Cleaned DataFrame row count: {cleaned_df_count}")
                        if cleaned_df_count > 0:
                            logger.info("Cleaned DataFrame schema:")
                            cleaned_df.printSchema()
                            logger.info("Cleaned DataFrame sample (all rows):")
                            cleaned_df.show(truncate=False)
                        else:
                            logger.info("Cleaned DataFrame is empty after transformations.")
                    else:
                        logger.error("Cleaned DataFrame is None, an error occurred in transformations.")
                except Exception as e_inner:
                    logger.error(f"Error processing raw DataFrame: {e_inner}", exc_info=True)
            else:
                logger.error("Raw DataFrame is None (ingestion failed). Skipping transformations test.")

            logger.info("--- Testing clean_and_transform_data with None input (using config rules) ---")
            none_input_df = clean_and_transform_data(None, test_cats, test_pm)
            if none_input_df is None:
                logger.info("Correctly handled None input to clean_and_transform_data, returned None.")
            else:
                logger.error("Failed to handle None input correctly for clean_and_transform_data.")

        except Exception as e:
            logger.error(f"An error occurred during __main__ testing: {e}", exc_info=True)
        finally:
            if spark_session_main:
                spark_session_main.stop()
                logger.info("SparkSession stopped.")
    else:
        logger.critical("Spark session could not be initialized for testing.")

pass
