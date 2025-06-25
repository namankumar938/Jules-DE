# Main orchestrator for the PySpark Retail Data Analytics Pipeline

from pyspark_scripts.spark_utils import get_spark_session, stop_spark_session
from pyspark_scripts.ingestion import load_data
from pyspark_scripts.transformation import transform_data
from pyspark_scripts.kpi_generation import calculate_all_kpis
from pyspark_scripts.output_utils import save_dataframe, save_kpis_to_json

def run_pipeline():
    """
    Runs the full PySpark retail data analytics pipeline.
    """
    spark = None
    try:
        # 1. Initialize Spark Session
        spark = get_spark_session(app_name="RetailPySparkPipeline")

        if not spark:
            print("Failed to initialize SparkSession. Exiting pipeline.")
            return

        # 2. Define data paths
        # Assuming the script is run from the project root, or paths are accessible
        raw_data_path = "data/sample_purchases.csv"
        processed_output_path_parquet = "data/pyspark_processed_purchases.parquet"
        processed_output_path_csv = "data/pyspark_processed_purchases.csv" # For easier inspection
        kpis_output_path_json = "data/pyspark_kpis.json"
        # In a real scenario, these paths might come from a config file or arguments

        print(f"\n--- Starting Retail Data Analytics Pipeline (PySpark) ---")

        # 3. Ingestion
        print("\n--- Step 1: Data Ingestion ---")
        raw_df = load_data(spark, file_path=raw_data_path)
        if raw_df is None:
            print("Data ingestion failed. Exiting pipeline.")
            return
        raw_df.cache() # Cache raw_df as it might be used for multiple transformations/analyses

        # 4. Transformation
        print("\n--- Step 2: Data Transformation ---")
        transformed_df = transform_data(raw_df)
        if transformed_df is None:
            print("Data transformation failed. Exiting pipeline.")
            return
        transformed_df.cache() # Cache transformed_df as it's used for all KPI calculations

        # 5. KPI Generation
        print("\n--- Step 3: KPI Generation ---")
        kpis = calculate_all_kpis(transformed_df, top_n_products=5)
        if not kpis:
            print("KPI generation failed. Exiting pipeline.")
            return

        # 6. Process/Display KPIs (Saving will be in a dedicated step/module)
        print("\n--- Pipeline Execution Summary: Calculated KPIs ---")
        print(f"Total Revenue: ${kpis.get('total_revenue', 0.0):,.2f}")

        print("\nRevenue by Category:")
        if kpis.get('revenue_by_category') is not None:
            kpis['revenue_by_category'].show(truncate=False)

        print("\nTop 5 Selling Products:")
        if kpis.get('top_selling_products') is not None:
            kpis['top_selling_products'].show(truncate=False)

        print("\nPreferred Payment Methods:")
        if kpis.get('preferred_payment_methods') is not None:
            kpis['preferred_payment_methods'].show(truncate=False)

        # 7. Save Outputs
        print("\n--- Step 4: Saving Outputs ---")
        # Save transformed DataFrame (example: as Parquet and as CSV for inspection)
        if transformed_df:
            save_dataframe(transformed_df, processed_output_path_parquet, file_format="parquet", mode="overwrite")
            save_dataframe(transformed_df, processed_output_path_csv, file_format="csv", mode="overwrite", header="true") # For CSV, include header

        # Save KPIs
        if kpis:
            save_kpis_to_json(kpis, kpis_output_path_json)

        print("\n--- Retail Data Analytics Pipeline (PySpark) Finished Successfully ---")

    except Exception as e:
        print(f"An error occurred during the PySpark pipeline execution: {e}")
        # In a production environment, consider more robust error logging (e.g., to a file or monitoring system)
    finally:
        # 7. Stop Spark Session
        if spark:
            print("\nStopping SparkSession...")
            stop_spark_session(spark)

if __name__ == "__main__":
    run_pipeline()
