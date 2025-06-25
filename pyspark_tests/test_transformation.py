import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pandas.testing import assert_frame_equal as pd_assert_frame_equal # For comparing collected results

# Adjust sys.path
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark_scripts.spark_utils import get_spark_session, stop_spark_session
from pyspark_scripts.transformation import convert_timestamp, calculate_total_purchase_amount, transform_data

class TestTransformation(unittest.TestCase):

    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session(app_name="TransformationUnittest")

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            stop_spark_session(cls.spark)

    def test_convert_timestamp(self):
        print("Running test_convert_timestamp...")
        data = [("2023-01-01 10:30:00",), ("2023-02-15 20:05:10",), ("Invalid Date",)]
        schema = StructType([StructField("event_time_str", StringType(), True)])
        df = self.spark.createDataFrame(data, schema)

        transformed_df = convert_timestamp(df, input_col_name="event_time_str", output_col_name="event_time")

        # Check new column name and type (after rename it should be 'event_time_str' but as TimestampType)
        self.assertIn("event_time_str", transformed_df.columns) # After rename
        self.assertIsInstance(transformed_df.schema["event_time_str"].dataType, TimestampType)

        results = transformed_df.collect()
        self.assertIsNotNone(results[0]["event_time_str"]) # Valid date
        self.assertIsNotNone(results[1]["event_time_str"]) # Valid date
        self.assertIsNone(results[2]["event_time_str"])    # Invalid date should result in null
        print("test_convert_timestamp PASSED")

    def test_calculate_total_purchase_amount(self):
        print("Running test_calculate_total_purchase_amount...")
        data = [(10.0, 2), (5.5, 3), (100.0, 1), (None, 5), (20.0, None)] # Includes None values
        schema = StructType([
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True)
        ])
        df = self.spark.createDataFrame(data, schema)

        transformed_df = calculate_total_purchase_amount(df)
        self.assertIn("total_purchase_amount", transformed_df.columns)

        expected_data = [(10.0, 2, 20.0), (5.5, 3, 16.5), (100.0, 1, 100.0), (None, 5, None), (20.0, None, None)]
        expected_cols_pd = ["price", "quantity", "total_purchase_amount"]

        # Collect results and compare with Pandas DataFrame for easier assertion
        results_pd = transformed_df.toPandas()

        import pandas as pd
        expected_pd = pd.DataFrame(expected_data, columns=expected_cols_pd)

        # Sort by price to ensure order for comparison, handle NaNs appropriately
        results_pd = results_pd.sort_values(by="price").reset_index(drop=True)
        expected_pd = expected_pd.sort_values(by="price").reset_index(drop=True)

        pd_assert_frame_equal(results_pd[expected_cols_pd], expected_pd[expected_cols_pd], check_dtype=False, rtol=1e-5)
        print("test_calculate_total_purchase_amount PASSED")

    def test_transform_data_orchestrator(self):
        # This is a more integrative test for the main transform_data function
        print("Running test_transform_data_orchestrator...")
        raw_data = [
            ("user1", "P1", "ProductA", "catA", 10.0, 2, "upi", "2023-01-01 12:00:00"),
            ("user2", "P2", "ProductB", "catB", 5.0,  3, "card", "2023-01-02 15:30:00")
        ]
        # Schema matches what ingestion module would produce (timestamp as string)
        raw_schema = StructType([
            StructField("user_id", StringType(), True), StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True), StructField("category", StringType(), True),
            StructField("price", DoubleType(), True), StructField("quantity", IntegerType(), True),
            StructField("payment_method", StringType(), True), StructField("timestamp", StringType(), True)
        ])
        raw_df = self.spark.createDataFrame(raw_data, raw_schema)

        transformed_df = transform_data(raw_df)

        self.assertIn("timestamp", transformed_df.columns)
        self.assertIsInstance(transformed_df.schema["timestamp"].dataType, TimestampType)
        self.assertIn("total_purchase_amount", transformed_df.columns)
        self.assertEqual(transformed_df.schema["total_purchase_amount"].dataType.simpleString(), "double")

        # Check a calculated value
        first_row_transformed = transformed_df.orderBy("user_id").first()
        self.assertEqual(first_row_transformed["total_purchase_amount"], 20.0)
        print("test_transform_data_orchestrator PASSED")


if __name__ == '__main__':
    # Need to ensure pandas is available if running this test file directly and it's not already in the venv
    # For the main run_in_bash_session, it should be handled by the pip install.
    try:
        import pandas
    except ImportError:
        print("Pandas not found, some assertions in test_calculate_total_purchase_amount might fail if run directly without full venv.")
    unittest.main(verbosity=2)
