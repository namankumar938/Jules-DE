# PySpark Data Transformation Logic will go here.
# This module will handle cleaning, validation, and transformation of PySpark DataFrames.

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim # `when` is not used in current transformations
from pyspark.sql.utils import AnalysisException # For Spark-specific analysis errors

logger = logging.getLogger(__name__)

# Define allowed values for categorical columns
ALLOWED_CATEGORIES = ["men", "women", "kids"]
ALLOWED_PAYMENT_METHODS = ["upi", "credit_card", "wallet"]

def clean_and_transform_data(df: DataFrame | None) -> DataFrame | None:
    """
    Cleans and transforms the raw purchase data DataFrame.

    Args:
        df: The input DataFrame loaded from purchase data. Can be None if ingestion failed.

    Returns:
        A DataFrame containing only valid and cleaned records,
        with normalized 'category' and 'payment_method' columns, or None if an error occurs
        or if the input DataFrame is None.
    """
    if df is None:
        logger.error("Input DataFrame is None. Cannot perform transformations.")
        return None

    if not isinstance(df, DataFrame):
        logger.error(f"Input must be a valid PySpark DataFrame. Got type: {type(df)}")
        return None # Or raise ValueError as before, but None is consistent with failure modes

    try:
        logger.debug("Starting data cleaning and transformation process.")

        # Initial count for logging/debugging if df is not empty
        # Spark actions like count() can be expensive, use judiciously or only on active dev.
        # initial_count = df.count()
        # logger.debug(f"Initial record count for transformation: {initial_count}")

        # 1. Handle Missing Values
        df_transformed = df.withColumn("category_trimmed", trim(col("category")))
        df_filtered_missing = df_transformed.filter(
            (col("category_trimmed").isNotNull()) & (col("category_trimmed") != "") &
            (col("price").isNotNull()) &
            (col("quantity").isNotNull())
        )

        # 2. Validate Data Values
        df_validated_values = df_filtered_missing.filter(
            (col("price") > 0) &
            (col("quantity") > 0)
        )

        # 3. Normalize and Validate Category
        df_normalized_category = df_validated_values.withColumn(
            "category_normalized", lower(col("category_trimmed"))
        )
        df_validated_category = df_normalized_category.filter(
            col("category_normalized").isin(ALLOWED_CATEGORIES)
        )

        # 4. Normalize and Validate Payment Method
        df_normalized_payment = df_validated_category.withColumn(
            "payment_method_trimmed", trim(col("payment_method"))
        ).withColumn(
            "payment_method_normalized", lower(col("payment_method_trimmed"))
        )
        df_validated_payment = df_normalized_payment.filter(
            col("payment_method_normalized").isin(ALLOWED_PAYMENT_METHODS)
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

        # final_count = final_df.count() # Action to materialize transformations and get count
        logger.info("Data cleaning and transformation completed successfully.")
        # logger.debug(f"Record count after transformation: {final_count}")
        return final_df

    except AnalysisException as e:
        logger.error(f"Spark AnalysisException during data transformation: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during data transformation: {e}")
        # import traceback # For more detailed stack trace during development
        # logger.error(traceback.format_exc())
        return None


if __name__ == '__main__':
    import sys
    import os

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from pyspark_pipeline.logging_utils import setup_logging
    setup_logging() # Configure logging using the new utility

    spark_session_main = None
    raw_df_main = None # Renamed to avoid confusion with df inside function

    try:
        from pyspark_pipeline.main import get_spark_session
        spark_session_main = get_spark_session(app_name="TransformationTest")
        logger.info("SparkSession obtained from pyspark_pipeline.main.")
    except ImportError:
        logger.warning("Could not import get_spark_session from pyspark_pipeline.main. Creating local SparkSession.")
        from pyspark.sql import SparkSession
        spark_session_main = SparkSession.builder \
            .appName("TransformationTestLocal") \
            .master("local[*]") \
            .getOrCreate()

    if spark_session_main:
        try:
            from pyspark_pipeline.ingestion import load_purchase_data

            csv_file_path = "../sample_purchases.csv"
            logger.info(f"--- Testing transformations with data from: {csv_file_path} ---")
            raw_df_main = load_purchase_data(spark_session_main, csv_file_path)

            if raw_df_main is not None:
                try:
                    raw_df_count = raw_df_main.count()
                    logger.info(f"Raw DataFrame row count: {raw_df_count}")
                    if raw_df_count > 0:
                        logger.info("Raw DataFrame schema:")
                        raw_df_main.printSchema()
                        logger.info("Raw DataFrame sample (first 5 rows):")
                        raw_df_main.show(5, truncate=False)
                    else:
                        logger.info("Raw DataFrame is empty.")

                    logger.info("Applying cleaning and transformations...")
                    cleaned_df = clean_and_transform_data(raw_df_main)

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
                except Exception as e_inner: # Catch Spark exceptions during operations on raw_df_main
                    logger.error(f"Error processing raw DataFrame: {e_inner}", exc_info=True)
            else:
                logger.error("Raw DataFrame is None (ingestion failed). Skipping transformations test.")

            # Test with None input to clean_and_transform_data
            logger.info("--- Testing clean_and_transform_data with None input ---")
            none_input_df = clean_and_transform_data(None)
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
