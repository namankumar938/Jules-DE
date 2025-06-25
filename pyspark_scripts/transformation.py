from pyspark.sql import DataFrame
from pyspark.sql.functions import col, try_to_timestamp, round as spark_round

def convert_timestamp(df: DataFrame, input_col_name="timestamp", output_col_name="timestamp_converted",
                      timestamp_format="yyyy-MM-dd HH:mm:ss") -> DataFrame:
    """
    Converts a string column containing timestamps to Spark TimestampType.

    Args:
        df (DataFrame): Input Spark DataFrame.
        input_col_name (str): Name of the column containing string timestamps.
        output_col_name (str): Name for the new column with converted timestamps.
        timestamp_format (str): Format of the input timestamp string.

    Returns:
        DataFrame: DataFrame with the added converted timestamp column.
                  Returns original DataFrame if input column not found.
    """
    if input_col_name not in df.columns:
        print(f"Warning: Column '{input_col_name}' not found in DataFrame. Skipping timestamp conversion.")
        return df

    print(f"Converting column '{input_col_name}' to TimestampType as '{output_col_name}' (attempting auto-parse first, then with format if needed)...")
    # Use try_to_timestamp to return NULL for unparseable dates instead of erroring
    # Try without format first
    df_transformed = df.withColumn(output_col_name, try_to_timestamp(col(input_col_name)))

    # If the above results in too many nulls for valid strings that need a specific format,
    # then we would revert to: try_to_timestamp(col(input_col_name), timestamp_format)
    # For now, let's test if the default parser handles "yyyy-MM-dd HH:mm:ss"

    # Drop the original string timestamp column and rename the new one
    df_transformed = df_transformed.drop(input_col_name).withColumnRenamed(output_col_name, input_col_name)

    print(f"Timestamp conversion complete. New schema for '{input_col_name}':")
    df_transformed.select(input_col_name).printSchema()
    return df_transformed

def calculate_total_purchase_amount(df: DataFrame, price_col="price", quantity_col="quantity", output_col="total_purchase_amount") -> DataFrame:
    """
    Calculates the total purchase amount (price * quantity).

    Args:
        df (DataFrame): Input Spark DataFrame.
        price_col (str): Name of the column containing item price.
        quantity_col (str): Name of the column containing item quantity.
        output_col (str): Name for the new column with the total purchase amount.

    Returns:
        DataFrame: DataFrame with the added total_purchase_amount column.
                  Returns original DataFrame if price or quantity columns are not found.
    """
    if price_col not in df.columns or quantity_col not in df.columns:
        missing_cols = []
        if price_col not in df.columns: missing_cols.append(price_col)
        if quantity_col not in df.columns: missing_cols.append(quantity_col)
        print(f"Warning: Columns {missing_cols} not found. Skipping calculation of '{output_col}'.")
        return df

    print(f"Calculating '{output_col}' as '{price_col}' * '{quantity_col}'...")
    # Ensure the result is rounded to 2 decimal places, similar to pandas version
    df_transformed = df.withColumn(output_col, spark_round(col(price_col) * col(quantity_col), 2))

    print(f"'{output_col}' calculated. Schema for new column:")
    df_transformed.select(output_col).printSchema()
    return df_transformed

def transform_data(df: DataFrame) -> DataFrame:
    """
    Applies all defined transformations to the DataFrame.

    Args:
        df (DataFrame): The raw Spark DataFrame (loaded with defined schema).

    Returns:
        DataFrame: The transformed DataFrame.
    """
    if df is None:
        print("Error: Input DataFrame is None. Cannot perform transformations.")
        return None

    print("\n--- Starting Data Transformation ---")

    # 1. Convert timestamp string to TimestampType
    df_transformed = convert_timestamp(df, input_col_name="timestamp")

    # 2. Calculate total_purchase_amount
    df_transformed = calculate_total_purchase_amount(df_transformed)

    print("\n--- Data Transformation Complete ---")
    print("Schema of transformed DataFrame:")
    df_transformed.printSchema()
    print("First 5 rows of transformed data:")
    df_transformed.show(5, truncate=False)

    return df_transformed

if __name__ == "__main__":
    # Example Usage (for testing the module directly)
    # When running with `python -m pyspark_scripts.transformation`,
    # relative imports within the package work correctly.
    from .spark_utils import get_spark_session, stop_spark_session
    from .ingestion import load_data

    spark = None
    try:
        spark = get_spark_session(app_name="TransformationTest")
        if spark:
            # Load sample data using the ingestion module (which uses predefined schema)
            raw_df = load_data(spark, file_path="data/sample_purchases.csv")

            if raw_df:
                # Apply transformations
                transformed_df = transform_data(raw_df)

                if transformed_df:
                    print("\nTransformed data sample (from test block):")
                    transformed_df.select("timestamp", "price", "quantity", "total_purchase_amount").show(5, truncate=False)
                    print(f"Row count of transformed DataFrame: {transformed_df.count()}")
            else:
                print("Transformation test skipped: Could not load raw data.")
    except Exception as e:
        print(f"An error occurred during transformation module test: {e}")
    finally:
        if spark:
            stop_spark_session(spark)
    print("Transformation module test finished.")
