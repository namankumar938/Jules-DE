# PySpark Data Transformation Logic will go here.
# This module will handle cleaning, validation, and transformation of PySpark DataFrames.

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, when, trim

# Define allowed values for categorical columns
ALLOWED_CATEGORIES = ["men", "women", "kids"]
ALLOWED_PAYMENT_METHODS = ["upi", "credit_card", "wallet"]

def clean_and_transform_data(df: DataFrame) -> DataFrame:
    """
    Cleans and transforms the raw purchase data DataFrame.

    Args:
        df: The input DataFrame loaded from purchase data.

    Returns:
        A DataFrame containing only valid and cleaned records,
        with normalized 'category' and 'payment_method' columns.
    """
    # Ensure df is not None and is a DataFrame
    if not df or not isinstance(df, DataFrame):
        raise ValueError("Input must be a valid PySpark DataFrame.")

    # Initial count for logging/debugging
    # initial_count = df.count()
    # print(f"Initial record count: {initial_count}")

    # 1. Handle Missing Values
    # Filter out rows where category is null or an empty string
    # Also trim whitespace from category before checking if it's empty
    df_transformed = df.withColumn("category_trimmed", trim(col("category")))
    df_filtered_missing = df_transformed.filter(
        (col("category_trimmed").isNotNull()) & (col("category_trimmed") != "") &
        (col("price").isNotNull()) &
        (col("quantity").isNotNull())
    )
    # count_after_missing = df_filtered_missing.count()
    # print(f"Count after filtering missing essential values: {count_after_missing}")


    # 2. Validate Data Values
    df_validated_values = df_filtered_missing.filter(
        (col("price") > 0) &
        (col("quantity") > 0)
    )
    # count_after_value_validation = df_validated_values.count()
    # print(f"Count after validating price/quantity > 0: {count_after_value_validation}")

    # 3. Normalize and Validate Category
    df_normalized_category = df_validated_values.withColumn(
        "category_normalized", lower(col("category_trimmed")) # Use the trimmed category
    )
    df_validated_category = df_normalized_category.filter(
        col("category_normalized").isin(ALLOWED_CATEGORIES)
    )
    # count_after_category_validation = df_validated_category.count()
    # print(f"Count after category normalization/validation: {count_after_category_validation}")


    # 4. Normalize and Validate Payment Method
    # Trim whitespace from payment_method before normalization
    df_normalized_payment = df_validated_category.withColumn(
        "payment_method_trimmed", trim(col("payment_method"))
    ).withColumn(
        "payment_method_normalized", lower(col("payment_method_trimmed"))
    )
    df_validated_payment = df_normalized_payment.filter(
        col("payment_method_normalized").isin(ALLOWED_PAYMENT_METHODS)
    )
    # count_after_payment_validation = df_validated_payment.count()
    # print(f"Count after payment method normalization/validation: {count_after_payment_validation}")

    # Select and rename columns to match original schema but with normalized values
    # Drop intermediate columns like 'category_trimmed', 'category_normalized', etc.
    final_df = df_validated_payment.select(
        col("event_timestamp"),
        col("user_id"),
        col("order_id"),
        col("product_id"),
        col("product_name"),
        col("category_normalized").alias("category"), # Use normalized category
        col("price"),
        col("quantity"),
        col("payment_method_normalized").alias("payment_method") # Use normalized payment_method
    )

    # final_count = final_df.count()
    # print(f"Final record count after all transformations: {final_count}")

    return final_df


if __name__ == '__main__':
    # This main block is for basic testing of the transformation function.
    # Assumes PySpark is installed and related modules are accessible.

    import sys
    import os
    # Add the project root to sys.path to allow absolute imports like 'from pyspark_pipeline.main'
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    spark_session_main = None
    raw_df = None

    try:
        # Attempt to import from the main module for SparkSession
        from pyspark_pipeline.main import get_spark_session
        spark_session_main = get_spark_session(app_name="TransformationTest")
        print("SparkSession obtained from pyspark_pipeline.main")
    except ImportError:
        print("Could not import get_spark_session from pyspark_pipeline.main. Creating a local SparkSession for testing.")
        from pyspark.sql import SparkSession # Import here if not available globally
        spark_session_main = SparkSession.builder \
            .appName("TransformationTestLocal") \
            .master("local[*]") \
            .getOrCreate()

    if spark_session_main:
        try:
            from pyspark_pipeline.ingestion import load_purchase_data # Assuming ingestion.py is in the same package

            # Relative path to the CSV file from the project root
            # When running `python pyspark_pipeline/transformations.py` the current dir is `/app`
            csv_file_path = "../sample_purchases.csv"
            print(f"Loading data from: {csv_file_path}")
            raw_df = load_purchase_data(spark_session_main, csv_file_path)

            raw_df_count = raw_df.count()
            print(f"\nRaw DataFrame row count: {raw_df_count}")
            print("Raw DataFrame schema:")
            raw_df.printSchema()
            print("Raw DataFrame sample (first 5 rows that might include nulls from ingestion):")
            raw_df.show(5, truncate=False) # Show some raw data

            print("\nApplying cleaning and transformations...")
            cleaned_df = clean_and_transform_data(raw_df.alias("raw_df_alias")) # Use alias to avoid potential self-join ambiguity if df is used inside

            cleaned_df_count = cleaned_df.count()
            print(f"\nCleaned DataFrame row count: {cleaned_df_count}")
            print("Cleaned DataFrame schema:")
            cleaned_df.printSchema()
            print("Cleaned DataFrame sample (all rows):")
            cleaned_df.show(truncate=False)

        except Exception as e:
            print(f"An error occurred during testing: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if spark_session_main:
                spark_session_main.stop()
                print("\nSparkSession stopped.")
    else:
        print("Spark session could not be initialized for testing.")

pass
