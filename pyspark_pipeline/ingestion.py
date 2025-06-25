# PySpark Data Ingestion Logic will go here.
# This module will be responsible for reading data from sources (e.g., CSV files)
# into PySpark DataFrames.

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, IntegerType # TimestampType could be used with a known format
)
from pyspark.sql.utils import AnalysisException

# Configure basic logging at the module level or in the main application
# For standalone script testing, it can be configured in if __name__ == '__main__'
logger = logging.getLogger(__name__)

def get_purchase_data_schema() -> StructType:
    """
    Defines and returns the schema for the purchase data CSV.
    """
    schema = StructType([
        StructField("event_timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("payment_method", StringType(), True)
    ])
    return schema

def load_purchase_data(spark: SparkSession, file_path: str) -> DataFrame | None:
    """
    Loads purchase data from a CSV file into a PySpark DataFrame.

    Args:
        spark: The SparkSession object.
        file_path: The path to the CSV file.

    Returns:
        A PySpark DataFrame containing the purchase data, or None if an error occurs.
    """
    purchase_schema = get_purchase_data_schema()

    try:
        logger.debug(f"Attempting to load data from path: {file_path} with schema.")
        df = spark.read.csv(
            file_path,
            header=True,
            schema=purchase_schema,
            # Default mode is PERMISSIVE. Fields that cannot be cast to schema type become null.
        )
        # Check if DataFrame is empty, which might happen if file exists but is empty or only header
        # Spark read.csv with schema on empty file returns empty DF, not error.
        # A count operation would trigger the actual read.
        # For now, we consider successful read if no exception.
        # A more robust check could be df.head() or df.count() inside try-catch

        logger.info(f"Successfully initiated loading data from {file_path}")
        return df
    except AnalysisException as e:
        # This typically covers issues like file not found, or issues Spark can detect pre-job
        logger.error(f"Spark AnalysisException while loading CSV '{file_path}': {e}")
        return None
    except Exception as e:
        # Catch other unexpected errors during DataFrame creation or initial read attempt
        logger.error(f"Unexpected error loading CSV '{file_path}': {e}")
        return None


if __name__ == '__main__':
    import sys
    import os

    # Add the project root to sys.path to allow absolute imports
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from pyspark_pipeline.logging_utils import setup_logging
    setup_logging() # Configure logging using the new utility

    spark_session_main = None
    try:
        from pyspark_pipeline.main import get_spark_session
        spark_session_main = get_spark_session(app_name="IngestionTest")
        logger.info("SparkSession obtained from pyspark_pipeline.main for testing.")
    except ImportError:
        logger.warning("Could not import get_spark_session from pyspark_pipeline.main. Creating a local SparkSession for testing.")
        # Fallback if direct execution and main.py or its function is not found/runnable standalone
        from pyspark.sql import SparkSession # Ensure SparkSession is imported for this fallback
        spark_session_main = SparkSession.builder \
            .appName("IngestionTestLocal") \
            .master("local[*]") \
            .getOrCreate()

    if spark_session_main:
        # Test with a valid file
        csv_file_path = "../sample_purchases.csv"
        logger.info(f"--- Testing with valid file: {csv_file_path} ---")
        loaded_df = load_purchase_data(spark_session_main, csv_file_path)

        if loaded_df is not None:
            try:
                # An action like count() is needed to actually trigger the read and potential AnalysisExceptions for empty/malformed files
                # not caught by the initial read.csv call (which is lazy).
                count = loaded_df.count()
                logger.info(f"Total rows loaded: {count}")
                if count > 0:
                    logger.info("Schema of the loaded DataFrame:")
                    loaded_df.printSchema()
                    logger.info("Showing top 5 rows of the loaded DataFrame:")
                    loaded_df.show(5)
                    logger.info("Checking for nulls in price or quantity (indicative of parsing issues):")
                    loaded_df.filter("price is NULL or quantity is NULL").show()
                else:
                    logger.info("Loaded DataFrame is empty (file might be empty, header-only, or all rows failed parsing to schema).")
            except Exception as e: # Catch Spark exceptions if count() or show() fails
                logger.error(f"Error processing loaded DataFrame from {csv_file_path}: {e}", exc_info=True)
        else:
            logger.error(f"Failed to load DataFrame from {csv_file_path}. `loaded_df` is None.")

        # Test with a non-existent file
        non_existent_file_path = "../non_existent_sample.csv"
        logger.info(f"--- Testing with non-existent file: {non_existent_file_path} ---")
        error_df = load_purchase_data(spark_session_main, non_existent_file_path)

        if error_df is None:
            logger.info(f"Correctly handled non-existent file. load_purchase_data returned None for {non_existent_file_path}.")
        else:
            # This case should ideally not be reached if load_purchase_data returns None for errors.
            logger.error(f"Incorrectly handled non-existent file. Expected None, got DataFrame for {non_existent_file_path}.")
            if isinstance(error_df, DataFrame): # Check if it's a DataFrame before calling .show()
                error_df.show()

        spark_session_main.stop()
        logger.info("SparkSession stopped.")
    else:
        logger.critical("Spark session could not be initialized for testing.")

pass
