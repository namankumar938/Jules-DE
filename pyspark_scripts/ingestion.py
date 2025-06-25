from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def define_schema():
    """
    Defines the schema for the purchase data.
    This is more robust than schema inference for production pipelines.
    """
    return StructType([
        StructField("timestamp", StringType(), True), # Read as string initially, then convert
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("payment_method", StringType(), True)
    ])

def load_data(spark: SparkSession, file_path: str, schema: StructType = None, infer_schema: bool = False) -> DataFrame:
    """
    Loads data from a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): The active SparkSession.
        file_path (str): The path to the CSV file.
        schema (StructType, optional): The predefined schema to use.
                                       If None and infer_schema is False, will try to use a default defined schema.
                                       If None and infer_schema is True, Spark will infer schema.
        infer_schema (bool): If True and schema is None, Spark will infer the schema.
                             Defaults to False. It's generally recommended to provide an explicit schema for production.


    Returns:
        DataFrame: The Spark DataFrame containing the loaded data, or None if an error occurs.
    """
    if not spark:
        print("Error: SparkSession is not available.")
        return None
    if not file_path:
        print("Error: File path is not provided.")
        return None

    try:
        print(f"Attempting to load data from: {file_path}")

        # Determine schema to use
        # For this project, we'll use a predefined one as it's better practice than inferring.
        # The `infer_schema` flag is kept for flexibility if ever needed.
        current_schema = schema
        if current_schema is None and not infer_schema:
            print("Using predefined schema for data loading.")
            current_schema = define_schema()
        elif infer_schema and current_schema is None:
            print("Inferring schema from data. This may not be accurate for all types.")

        df_reader = spark.read.format("csv").option("header", "true")

        if current_schema:
            df = df_reader.schema(current_schema).load(file_path)
        else: # This case implies infer_schema=True and schema=None
            df = df_reader.option("inferSchema", "true").load(file_path)

        print(f"Data loaded successfully into DataFrame. Number of rows: {df.count()}")
        print("DataFrame Schema:")
        df.printSchema()
        print("First 5 rows of loaded data:")
        df.show(5, truncate=False)
        return df
    except Exception as e:
        print(f"Error loading data from {file_path}: {e}")
        # Consider how to handle this: raise, return None, or log more extensively
        return None # Or raise e

if __name__ == "__main__":
    # Example Usage (for testing the module directly)
    # When running with `python -m pyspark_scripts.ingestion`,
    # relative imports within the package work correctly.
    from .spark_utils import get_spark_session, stop_spark_session

    spark = None
    try:
        spark = get_spark_session(app_name="IngestionTest")
        if spark:
            # Ensure you have the sample data generated from the Pandas version
            # or create a dummy CSV for testing.
            # For this test, we assume 'data/sample_purchases.csv' exists.
            # Path should be relative to the project root when running tests this way
            sample_data_path = "data/sample_purchases.csv"

            print(f"\n--- Testing with predefined schema (Recommended) ---")
            df_loaded_predefined = load_data(spark, sample_data_path) # Uses define_schema() by default
            if df_loaded_predefined:
                print(f"Successfully loaded with predefined schema. Row count: {df_loaded_predefined.count()}")

            print(f"\n--- Testing with schema inference (For comparison) ---")
            # Note: Inferring schema can be slow and might get types wrong (e.g., timestamp as string)
            df_loaded_inferred = load_data(spark, sample_data_path, infer_schema=True)
            if df_loaded_inferred:
                 print(f"Successfully loaded with inferred schema. Row count: {df_loaded_inferred.count()}")

            print(f"\n--- Testing with an explicitly passed schema (Alternative) ---")
            explicit_custom_schema = StructType([
                StructField("timestamp", StringType(), True),
                StructField("user_id", StringType(), True),
                # ... add other fields if testing a different structure
            ])
            # df_loaded_explicit = load_data(spark, sample_data_path, schema=explicit_custom_schema) # Example
            # if df_loaded_explicit:
            #      print(f"Successfully loaded with explicit custom schema. Row count: {df_loaded_explicit.count()}")


    except Exception as e:
        print(f"An error occurred during ingestion module test: {e}")
    finally:
        if spark:
            stop_spark_session(spark)
    print("Ingestion module test finished.")
