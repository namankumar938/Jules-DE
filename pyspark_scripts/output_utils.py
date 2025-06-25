import json
from pyspark.sql import DataFrame

def save_dataframe(df: DataFrame, path: str, file_format: str = "parquet", mode: str = "overwrite", **options):
    """
    Saves a Spark DataFrame to a specified path and format.

    Args:
        df (DataFrame): The Spark DataFrame to save.
        path (str): The output path.
        file_format (str): The format to save in (e.g., "parquet", "csv", "json").
        mode (str): Spark save mode (e.g., "overwrite", "append", "ignore", "errorifexists").
        **options: Additional options for the DataFrameWriter. E.g., header=True for CSV.
    """
    if df is None:
        print(f"Warning: DataFrame is None. Cannot save to {path}.")
        return
    if not path:
        print("Warning: Output path is not specified. Cannot save DataFrame.")
        return

    try:
        writer = df.write.mode(mode).format(file_format)
        for option_key, option_value in options.items():
            writer = writer.option(option_key, option_value)

        writer.save(path)
        print(f"DataFrame saved successfully to {path} in {file_format} format (mode: {mode}).")
    except Exception as e:
        print(f"Error saving DataFrame to {path}: {e}")

def save_kpis_to_json(kpis: dict, path: str):
    """
    Saves calculated KPIs to a JSON file.
    Spark DataFrames within the kpis dict will be collected and converted to a list of dicts.

    Args:
        kpis (dict): Dictionary of KPIs. Values can be floats, ints, or Spark DataFrames.
        path (str): The output path for the JSON file.
    """
    if not kpis:
        print("Warning: KPIs dictionary is empty or None. Nothing to save to JSON.")
        return
    if not path:
        print("Warning: Output path for KPIs JSON is not specified.")
        return

    serializable_kpis = {}
    print(f"Preparing KPIs for JSON serialization to {path}...")
    for key, value in kpis.items():
        if isinstance(value, DataFrame):
            print(f"Collecting KPI DataFrame '{key}' for JSON serialization...")
            try:
                # Collect DataFrame to a list of dictionaries
                collected_data = [row.asDict() for row in value.collect()]
                serializable_kpis[key] = collected_data
                print(f"KPI DataFrame '{key}' collected. Records: {len(collected_data)}")
            except Exception as e:
                print(f"Error collecting DataFrame KPI '{key}': {e}. Skipping this KPI.")
                serializable_kpis[key] = f"Error collecting data: {str(e)}"
        else:
            # Directly add non-DataFrame KPIs (like total_revenue float)
            serializable_kpis[key] = value
            print(f"KPI '{key}' (type: {type(value)}) added directly.")

    try:
        with open(path, 'w') as f:
            json.dump(serializable_kpis, f, indent=4)
        print(f"KPIs successfully saved to JSON file: {path}")
    except Exception as e:
        print(f"Error writing KPIs to JSON file {path}: {e}")

if __name__ == "__main__":
    # Example usage (requires a Spark session and sample data)
    from .spark_utils import get_spark_session, stop_spark_session # Relative import for module execution

    spark = None
    try:
        spark = get_spark_session(app_name="OutputUtilsTest")
        if spark:
            # Create a sample DataFrame
            data = [("productA", 100.0), ("productB", 150.0)]
            columns = ["product_name", "revenue"]
            sample_df = spark.createDataFrame(data, columns)

            # Test saving DataFrame
            # Note: In a real test, you might want to use a temporary directory
            output_df_path_parquet = "data/test_output_df.parquet"
            output_df_path_csv = "data/test_output_df.csv"

            print(f"\n--- Testing save_dataframe (Parquet) ---")
            save_dataframe(sample_df, output_df_path_parquet, file_format="parquet", mode="overwrite")
            # You would typically verify the saved data by reading it back:
            # loaded_parquet_df = spark.read.parquet(output_df_path_parquet)
            # loaded_parquet_df.show()

            print(f"\n--- Testing save_dataframe (CSV) ---")
            save_dataframe(sample_df, output_df_path_csv, file_format="csv", mode="overwrite", header="true")
            # loaded_csv_df = spark.read.csv(output_df_path_csv, header=True, inferSchema=True)
            # loaded_csv_df.show()

            # Create sample KPIs dictionary
            sample_kpis = {
                "total_revenue": 250.00,
                "category_summary": spark.createDataFrame([("cat1", 100.0), ("cat2", 150.0)], ["category", "total_revenue"])
            }

            output_kpis_path = "data/test_kpis.json"
            print(f"\n--- Testing save_kpis_to_json ---")
            save_kpis_to_json(sample_kpis, output_kpis_path)
            # You would typically verify by reading and parsing the JSON file.
            # with open(output_kpis_path, 'r') as f_read:
            #     loaded_kpis_json = json.load(f_read)
            #     print("Loaded KPIs from JSON:", json.dumps(loaded_kpis_json, indent=2))

            print("\nOutput utils test completed. Check 'data/' directory for test_output_df.* and test_kpis.json.")

    except Exception as e:
        print(f"An error occurred during output_utils module test: {e}")
    finally:
        if spark:
            stop_spark_session(spark)
    print("Output utils module test finished.")
