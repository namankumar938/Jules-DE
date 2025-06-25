import unittest
from pyspark.sql import SparkSession

# Adjust sys.path to import from pyspark_scripts
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark_scripts.spark_utils import get_spark_session, stop_spark_session

class TestSparkUtils(unittest.TestCase):

    spark = None

    @classmethod
    def setUpClass(cls):
        # Start Spark session once for all tests in this class
        try:
            cls.spark_for_tests = get_spark_session(app_name="SparkUtilsUnittest")
        except Exception as e:
            # If Spark session fails to start, tests will likely fail or be skipped.
            # This can happen if Spark is not properly configured in the environment.
            print(f"Failed to start Spark session for tests: {e}")
            cls.spark_for_tests = None


    @classmethod
    def tearDownClass(cls):
        # Stop Spark session once after all tests in this class
        if cls.spark_for_tests:
            stop_spark_session(cls.spark_for_tests)

    def test_get_spark_session_creates_session(self):
        print("Running test_get_spark_session_creates_session...")
        self.assertIsNotNone(self.spark_for_tests, "Spark session should be created by setUpClass.")
        self.assertIsInstance(self.spark_for_tests, SparkSession, "Object should be a SparkSession instance.")
        print("test_get_spark_session_creates_session PASSED")

    def test_get_spark_session_app_name(self):
        print("Running test_get_spark_session_app_name...")
        if self.spark_for_tests: # Only run if session was created
            self.assertEqual(self.spark_for_tests.conf.get("spark.app.name"), "SparkUtilsUnittest")
        else:
            self.skipTest("Spark session not available.")
        print("test_get_spark_session_app_name PASSED")

    # stop_spark_session is implicitly tested by tearDownClass,
    # but we could add a specific test if needed, though it's harder to assert stoppage directly.

if __name__ == '__main__':
    unittest.main(verbosity=2)
