# Main script for the PySpark ETL pipeline.
# This module will orchestrate the ingestion, transformation, and KPI generation steps.

import sys # For potential path adjustments if not using PYTHONPATH
import os  # For potential path adjustments

from pyspark.sql import SparkSession

# It's generally better to rely on PYTHONPATH for imports when running as a module.
# However, for direct script execution or some environments, path adjustments might be attempted.
# If running with `PYTHONPATH=/app python -m pyspark_pipeline.main`, these are not strictly needed.
try:
    from pyspark_pipeline.ingestion import load_purchase_data
    from pyspark_pipeline.transformations import clean_and_transform_data
    from pyspark_pipeline.kpi_generation import (
        calculate_top_selling_products,
        calculate_total_revenue_by_category,
        calculate_preferred_payment_method_by_category,
        calculate_purchase_frequency_per_user,
        calculate_average_order_value
    )
except ModuleNotFoundError:
    # This block is a fallback if the script is run in a way that pyspark_pipeline is not in sys.path.
    # For example, `python pyspark_pipeline/main.py` from the root without PYTHONPATH set.
    print("Attempting to adjust sys.path for direct script execution...")
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from pyspark_pipeline.ingestion import load_purchase_data
    from pyspark_pipeline.transformations import clean_and_transform_data
    from pyspark_pipeline.kpi_generation import (
        calculate_top_selling_products,
        calculate_total_revenue_by_category,
        calculate_preferred_payment_method_by_category,
        calculate_purchase_frequency_per_user,
        calculate_average_order_value
    )


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
    spark = None  # Initialize spark to None for the finally block
    try:
        spark = get_spark_session()
        print("SparkSession initialized successfully.")

        # Define input file path using an absolute path to avoid ambiguity
        # os.path.dirname(__file__) gives the directory of the current script (main.py)
        # which is /app/pyspark_pipeline when run as a module.
        # So, os.path.join(os.path.dirname(__file__), '..') gives /app (project root)
        project_root_for_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_root_for_file, "sample_purchases.csv")
        # Forcing file:// scheme for local files can sometimes help Spark resolve paths correctly.
        # However, an absolute path should generally be sufficient.
        # file_path = "file://" + file_path

        print(f"\nStarting data pipeline for absolute file path: {file_path}")

        # 1. Ingestion
        print("\n--- Step 1: Ingesting Data ---")
        raw_df = load_purchase_data(spark, file_path)
        raw_count = raw_df.count()
        print(f"Raw data count: {raw_count}")
        if raw_count == 0 and not spark.catalog._jcatalog.tableExists("dummy"): # crude check for empty df
             print(f"WARNING: No data loaded from '{file_path}'. Further processing might yield empty results or errors.")
        else:
            print("Raw data sample (first 5 rows):")
            raw_df.show(5, truncate=False)
            raw_df.printSchema()

        # 2. Transformation
        print("\n--- Step 2: Cleaning and Transforming Data ---")
        # Check if raw_df actually has data to avoid errors if load_purchase_data returned empty on error
        if raw_count > 0 : # only proceed if raw_df is not empty
            cleaned_df = clean_and_transform_data(raw_df)
            cleaned_count = cleaned_df.count()
            print(f"Cleaned data count: {cleaned_count}")
            if cleaned_count > 0:
                print("Cleaned data sample (first 5 rows):")
                cleaned_df.show(5, truncate=False)
                cleaned_df.printSchema()
            else:
                print("WARNING: No data remaining after cleaning and transformation.")
        else: # raw_df was empty
            print("Skipping transformation as raw data is empty.")
            # Create an empty DataFrame with the expected schema if needed by KPI functions,
            # or ensure KPI functions handle truly empty DFs.
            # For now, our KPI functions should handle empty DFs.
            # To be safe, we can pass raw_df (which is empty) or create one with schema
            from pyspark_pipeline.ingestion import get_purchase_data_schema
            cleaned_df = spark.createDataFrame([], get_purchase_data_schema()) # Pass an empty DF with schema
            cleaned_count = 0


        # 3. KPI Generation & Reporting
        print("\n--- Step 3: Generating and Printing KPIs ---")

        if cleaned_count > 0:
            print("\nCalculating Top Selling Products (Top 5)...")
            top_products_df = calculate_top_selling_products(cleaned_df, top_n=5)
            top_products_df.show(truncate=False)

            print("\nCalculating Total Revenue by Category...")
            revenue_by_cat_df = calculate_total_revenue_by_category(cleaned_df, spark)
            revenue_by_cat_df.show(truncate=False)

            print("\nCalculating Preferred Payment Method by Category...")
            preferred_pm_df = calculate_preferred_payment_method_by_category(cleaned_df, spark)
            preferred_pm_df.show(truncate=False)

            print("\nCalculating Purchase Frequency per User...")
            purchase_freq_df = calculate_purchase_frequency_per_user(cleaned_df)
            purchase_freq_df.show(truncate=False) # Show all for sample, can be limited

            print("\nCalculating Average Order Value...")
            aov = calculate_average_order_value(cleaned_df)
            print(f"Average Order Value: ${aov:.2f}")
        else:
            print("Skipping KPI generation as there is no cleaned data.")
            # Optionally print empty/default states for KPIs
            print("\nTop Selling Products (Top 5):\n (No data)")
            print("\nTotal Revenue by Category:\n (No data or all categories 0.0)")
            # ... and so on for other KPIs, or call formatters with empty/default data.
            # For simplicity, just printing messages here.
            # Our KPI functions return empty DFs or 0.0 for AOV, so they could be called.
            calculate_total_revenue_by_category(cleaned_df, spark).show() # Will show all categories with 0.0
            calculate_preferred_payment_method_by_category(cleaned_df, spark).show() # Will show all combos with 0


        print("\n--- Pipeline Execution Finished ---")

    except FileNotFoundError:
        print(f"ERROR: Input file not found at path: {file_path}")
        print(f"Please ensure 'sample_purchases.csv' exists at {os.path.join(project_root_for_file, 'sample_purchases.csv')}.")
    except Exception as e:
        print(f"An error occurred during the PySpark pipeline execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if spark: # Check if spark was initialized
            print("\nStopping SparkSession.")
            spark.stop()

if __name__ == '__main__':
    main()
pass
