import unittest
import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Adjust sys.path
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark_scripts.spark_utils import get_spark_session, stop_spark_session
from pyspark_scripts.ingestion import load_data, define_schema

class TestIngestion(unittest.TestCase):

    spark = None
    temp_csv_file = None

    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session(app_name="IngestionUnittest")

        # Create a temporary CSV file for testing
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.temp_csv_file_path = os.path.join(cls.temp_dir.name, "test_purchases.csv")

        # Sample CSV data
        csv_content = (
            "timestamp,user_id,product_id,product_name,category,price,quantity,payment_method\n"
            "2023-01-01 10:00:00,user1,P101,Laptop,electronics,1200.00,1,credit_card\n"
            "2023-01-01 11:00:00,user2,P102,Mouse,electronics,25.00,2,upi\n"
            "bad_timestamp,user3,P103,Keyboard,electronics,75.00,1,wallet" # A row that might have issues if not for defined schema
        )
        with open(cls.temp_csv_file_path, 'w') as f:
            f.write(csv_content)

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            stop_spark_session(cls.spark)
        cls.temp_dir.cleanup() # Remove temporary directory and its contents

    def test_load_data_with_defined_schema(self):
        print("Running test_load_data_with_defined_schema...")
        self.assertTrue(os.path.exists(self.temp_csv_file_path), "Temporary CSV file should exist.")

        test_schema = define_schema() # Get the schema used in ingestion module
        df = load_data(self.spark, self.temp_csv_file_path, schema=test_schema)

        self.assertIsNotNone(df, "DataFrame should not be None after loading with defined schema.")
        self.assertEqual(df.count(), 3, "DataFrame should have 3 rows from test CSV.")

        # Check schema of loaded DataFrame (should match defined_schema, with types converted by Spark where applicable)
        # Example: price should be double, quantity integer
        expected_types = {
            "timestamp": "string", # Loaded as string by our defined schema
            "user_id": "string",
            "product_id": "string",
            "product_name": "string",
            "category": "string",
            "price": "double",
            "quantity": "int", # Spark's IntegerType().simpleString() is "int"
            "payment_method": "string"
        }
        for field in df.schema.fields:
            self.assertEqual(field.dataType.simpleString(), expected_types[field.name], f"Type mismatch for {field.name}")

        first_row = df.first()
        self.assertEqual(first_row["user_id"], "user1")
        self.assertEqual(first_row["price"], 1200.00)
        print("test_load_data_with_defined_schema PASSED")

    def test_load_data_with_schema_inference(self):
        print("Running test_load_data_with_schema_inference...")
        df = load_data(self.spark, self.temp_csv_file_path, infer_schema=True) # Explicitly ask to infer

        self.assertIsNotNone(df, "DataFrame should not be None after loading with inferred schema.")
        self.assertEqual(df.count(), 3, "DataFrame should have 3 rows.")

        # With inferSchema, 'timestamp' might be inferred as TimestampType if format is standard
        # or string otherwise. 'price' should be double, 'quantity' int.
        # This is more of a check that inference runs.
        actual_timestamp_type = df.schema["timestamp"].dataType.simpleString()
        self.assertIn(actual_timestamp_type, ["string", "timestamp"], "Timestamp type after inference is unexpected.")
        self.assertEqual(df.schema["price"].dataType.simpleString(), "double")
        self.assertEqual(df.schema["quantity"].dataType.simpleString(), "int") # Spark's IntegerType().simpleString() is "int"
        print("test_load_data_with_schema_inference PASSED")

    def test_load_data_file_not_found(self):
        print("Running test_load_data_file_not_found...")
        df = load_data(self.spark, "non_existent_file.csv")
        self.assertIsNone(df, "DataFrame should be None for a non-existent file.")
        print("test_load_data_file_not_found PASSED")

if __name__ == '__main__':
    unittest.main(verbosity=2)
