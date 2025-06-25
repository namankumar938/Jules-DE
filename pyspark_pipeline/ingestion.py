# PySpark Data Ingestion Logic will go here.
# This module will be responsible for reading data from sources (e.g., CSV files)
# into PySpark DataFrames.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, IntegerType # TimestampType could be used with a known format
)

def get_purchase_data_schema() -> StructType:
    """
    Defines and returns the schema for the purchase data CSV.
    """
    schema = StructType([
        StructField("event_timestamp", StringType(), True), # Using StringType for now
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

def load_purchase_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Loads purchase data from a CSV file into a PySpark DataFrame.

    Args:
        spark: The SparkSession object.
        file_path: The path to the CSV file.

    Returns:
        A PySpark DataFrame containing the purchase data.
        Returns an empty DataFrame with the schema if an error occurs during loading.
    """
    purchase_schema = get_purchase_data_schema()

    try:
        df = spark.read.csv(
            file_path,
            header=True,
            schema=purchase_schema,
            # mode="FAILFAST" # Use FAILFAST to abort if any malformed record is found
            # mode="DROPMALFORMED" # Use DROPMALFORMED to drop rows that don't match schema
            # Default mode is PERMISSIVE, which sets fields to null if they don't match schema.
            # For price/quantity, if conversion to Float/Integer fails, they become null.
        )
        return df
    except Exception as e:
        print(f"Error loading CSV file '{file_path}': {e}")
        # Return an empty DataFrame with the defined schema in case of error
        return spark.createDataFrame([], schema=purchase_schema)


if __name__ == '__main__':
    # This main block is for basic testing of the ingestion function.
    # Assumes PySpark is installed and sample_purchases.csv is in the parent directory.

    # Relative path to the CSV file from the project root
    # When running `python pyspark_pipeline/ingestion.py` the current dir is `/app`
    csv_file_path = "../sample_purchases.csv"

    # Import get_spark_session from main.py, assuming it's in the same package pyspark_pipeline
    # To make this work, ensure pyspark_pipeline is treated as a package
    # and PYTHONPATH includes the project root. For direct script run, relative import might be tricky.
    # A common way is to add project root to sys.path if needed, or structure for package execution.

    # For simplicity in direct execution of this script, let's manage Spark session here.
    # In a full application, spark session would likely be managed by main.py or a similar entry point.

    spark_session_main = None
    try:
        # Try to import from the main module if possible (e.g. if PYTHONPATH is set up)
        # This allows using the centralized get_spark_session
        from pyspark_pipeline.main import get_spark_session # Assuming main.py is in the same package
        spark_session_main = get_spark_session(app_name="IngestionTest")
        print("SparkSession obtained from pyspark_pipeline.main")
    except ImportError:
        print("Could not import get_spark_session from pyspark_pipeline.main. Creating a local SparkSession for testing.")
        spark_session_main = SparkSession.builder \
            .appName("IngestionTestLocal") \
            .master("local[*]") \
            .getOrCreate()

    if spark_session_main:
        print(f"Loading data from: {csv_file_path}")
        loaded_df = load_purchase_data(spark_session_main, csv_file_path)

        if loaded_df.count() == 0 and not spark_session_main.catalog._jcatalog.tableExists("dummy"): # crude check if it's an empty df due to error
             print(f"Loaded DataFrame is empty. This might be due to an error during loading or the file '{csv_file_path}' being empty/not found.")

        print("\nSchema of the loaded DataFrame:")
        loaded_df.printSchema()

        print("\nShowing top 5 rows of the loaded DataFrame:")
        loaded_df.show(5)

        # Example of handling potential nulls due to schema enforcement
        # If 'PRICE_ERROR' was in a price column, Spark would make it null.
        print("\nChecking for nulls in price or quantity (indicative of parsing issues):")
        loaded_df.filter("price is NULL or quantity is NULL").show()

        spark_session_main.stop()
        print("\nSparkSession stopped.")
    else:
        print("Spark session could not be initialized.")

pass
