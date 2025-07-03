from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.transform_helpers import (
    filter_by_min_age,
    drop_na_values,
    rename_column,
    add_age_offset_column
    # We could also use apply_standard_customer_preprocessing here if we adjust parameters
    # or create a different composite function for 'client' data if it's common.
)

def process_data_B(spark, input_path, output_path):
    """
    Processes data from input_path and writes to output_path.
    Uses shared transformation helpers.
    """
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Using individual helpers as the composite one might not fit directly due to variations
    df = filter_by_min_age(df, age_column="age", min_age=20)
    df = drop_na_values(df, subset_columns=["name", "contact_info"])
    df = rename_column(df, old_column_name="user_id", new_column_name="client_id")
    df = add_age_offset_column(df, new_column_name="age_plus_five", age_column_name="age", offset=5)

    # Some other specific logic for B
    df = df.filter(col("status") == "active")

    df.write.mode("overwrite").json(output_path)

if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("DataProcessingB").getOrCreate()

    # Example usage (requires a sample CSV file)
    # Create a dummy input.csv for this to run:
    # id,name,contact_info,age,status
    # 1,Peter,peter@example.com,25,active
    # 2,Lois,,30,inactive
    # 3,Stewie,stewie@example.com,19,active
    # 4,Brian,brian@example.com,40,active
    # 5,Meg,meg@example.com,22,inactive

    # spark_session.createDataFrame([
    #     (1, "Peter", "peter@example.com", 25, "active"),
    #     (2, "Lois", None, 30, "inactive"),
    #     (3, "Stewie", "stewie@example.com", 19, "active"),
    #     (4, "Brian", "brian@example.com", 40, "active"),
    #     (5, "Meg", "meg@example.com", 22, "inactive"),
    # ], ["user_id", "name", "contact_info", "age", "status"]) \
    # .write.csv("input_B.csv", header=True, mode="overwrite")

    # process_data_B(spark_session, "input_B.csv", "output_B.json")
    # print("Data processing B complete. Output at output_B.json")
    spark_session.stop()
