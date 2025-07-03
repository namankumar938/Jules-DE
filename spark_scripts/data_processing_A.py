from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.transform_helpers import (
    filter_by_min_age,
    drop_na_values,
    rename_column,
    add_age_offset_column,
    apply_standard_customer_preprocessing
)

def process_data_A(spark, input_path, output_path):
    """
    Processes data from input_path and writes to output_path.
    Uses shared transformation helpers.
    """
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Using individual helpers
    # df = filter_by_min_age(df, age_column="age", min_age=18)
    # df = drop_na_values(df, subset_columns=["name", "email"])
    # df = rename_column(df, old_column_name="user_id", new_column_name="customer_id")

    # Or using a composite helper
    df = apply_standard_customer_preprocessing(
        df,
        age_col="age",
        min_age_threshold=18,
        id_col="user_id",
        new_id_col="customer_id",
        na_check_cols=["name", "email"]
    )
    df = add_age_offset_column(df, new_column_name="age_plus_ten", age_column_name="age", offset=10)

    # Some other specific logic for A
    df = df.filter(col("department") == "sales")

    df.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("DataProcessingA").getOrCreate()

    # Example usage (requires a sample CSV file)
    # Create a dummy input.csv for this to run:
    # id,name,email,age,department
    # 1,Alice,alice@example.com,25,sales
    # 2,Bob,,30,hr
    # 3,Charlie,charlie@example.com,17,sales
    # 4,David,david@example.com,40,marketing
    # 5,Eve,eve@example.com,22,sales

    # spark_session.createDataFrame([
    #     (1, "Alice", "alice@example.com", 25, "sales"),
    #     (2, "Bob", None, 30, "hr"),
    #     (3, "Charlie", "charlie@example.com", 17, "sales"),
    #     (4, "David", "david@example.com", 40, "marketing"),
    #     (5, "Eve", "eve@example.com", 22, "sales"),
    # ], ["user_id", "name", "email", "age", "department"]) \
    # .write.csv("input_A.csv", header=True, mode="overwrite")

    # process_data_A(spark_session, "input_A.csv", "output_A.parquet")
    # print("Data processing A complete. Output at output_A.parquet")
    spark_session.stop()
