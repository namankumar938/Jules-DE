import unittest
import builtins
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
import pandas as pd

# Adjust sys.path
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark_scripts.spark_utils import get_spark_session, stop_spark_session
from pyspark_scripts.kpi_generation import (
    calculate_total_revenue,
    calculate_revenue_by_category,
    calculate_top_n_selling_products,
    calculate_preferred_payment_methods,
    calculate_all_kpis
)

class TestKPIGeneration(unittest.TestCase):

    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session(app_name="KPIGenUnittest")
        # Sample transformed DataFrame structure
        cls.transformed_data_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("payment_method", StringType(), True),
            StructField("timestamp", TimestampType(), True), # Assuming timestamp is converted
            StructField("total_purchase_amount", DoubleType(), True) # Key column for KPIs
        ])

        cls.sample_transformed_data = [
            ("u1", "P1", "Laptop", "electronics", 1200.00, 1, "card", None, 1200.00),
            ("u2", "P2", "Mouse", "electronics", 25.00, 2, "upi", None, 50.00),
            ("u3", "P3", "Shirt", "fashion", 50.00, 1, "wallet", None, 50.00),
            ("u4", "P1", "Laptop", "electronics", 1200.00, 1, "card", None, 1200.00), # Repeat purchase
            ("u5", "P4", "Jeans", "fashion", 75.00, 1, "upi", None, 75.00),
            ("u6", "P5", "Book", "books", 20.00, 3, "card", None, 60.00),
        ]
        cls.df = cls.spark.createDataFrame(cls.sample_transformed_data, cls.transformed_data_schema)
        cls.df.cache() # Cache for performance in tests

    @classmethod
    def tearDownClass(cls):
        if cls.df:
            cls.df.unpersist()
        if cls.spark:
            stop_spark_session(cls.spark)

    def test_calculate_total_revenue(self):
        print("Running test_calculate_total_revenue...")
        expected_revenue = 1200.00 + 50.00 + 50.00 + 1200.00 + 75.00 + 60.00 # = 2635.00
        actual_revenue = calculate_total_revenue(self.df)
        self.assertAlmostEqual(actual_revenue, expected_revenue, places=2)
        print("test_calculate_total_revenue PASSED")

    def test_calculate_revenue_by_category(self):
        print("Running test_calculate_revenue_by_category...")
        revenue_by_cat_df = calculate_revenue_by_category(self.df)

        expected_data = [
            ("electronics", 1200.00 + 50.00 + 1200.00), # 2450.00
            ("fashion", 50.00 + 75.00),                 # 125.00
            ("books", 60.00)                            # 60.00
        ]
        expected_pd = pd.DataFrame(expected_data, columns=["category", "total_revenue"]).sort_values(by="category").reset_index(drop=True)

        results_pd = revenue_by_cat_df.toPandas().sort_values(by="category").reset_index(drop=True)
        # Round to 2 decimal places for consistent comparison if PySpark produces more
        results_pd["total_revenue"] = results_pd["total_revenue"].round(2)

        pd_assert_frame_equal(results_pd, expected_pd, check_dtype=False)
        print("test_calculate_revenue_by_category PASSED")

    def test_calculate_top_n_selling_products(self):
        print("Running test_calculate_top_n_selling_products...")
        top_products_df = calculate_top_n_selling_products(self.df, top_n=2)

        expected_data = [
            ("Laptop", 1200.00 + 1200.00), # 2400.00
            ("Jeans", 75.00)
        ] # Sorted by revenue descending
        expected_pd = pd.DataFrame(expected_data, columns=["product_name", "total_revenue"])

        results_pd = top_products_df.toPandas()
        results_pd["total_revenue"] = results_pd["total_revenue"].round(2)

        pd_assert_frame_equal(results_pd, expected_pd, check_dtype=False)
        print("test_calculate_top_n_selling_products PASSED")


    def test_calculate_preferred_payment_methods(self):
        print("Running test_calculate_preferred_payment_methods...")
        payment_methods_df = calculate_preferred_payment_methods(self.df)

        expected_data = [
            ("card", 3),
            ("upi", 2),
            ("wallet", 1)
        ] # Sorted by count descending
        expected_pd = pd.DataFrame(expected_data, columns=["payment_method", "transaction_count"])

        results_pd = payment_methods_df.toPandas()
        pd_assert_frame_equal(results_pd, expected_pd, check_dtype=False)
        print("test_calculate_preferred_payment_methods PASSED")

    def test_calculate_all_kpis(self):
        print("Running test_calculate_all_kpis...")
        kpis = calculate_all_kpis(self.df, top_n_products=2)

        self.assertIn("total_revenue", kpis)
        self.assertAlmostEqual(kpis["total_revenue"], 2635.00, places=2)

        self.assertIn("revenue_by_category", kpis)
        self.assertIsInstance(kpis["revenue_by_category"], type(self.df)) # Check if it's a DataFrame
        self.assertEqual(kpis["revenue_by_category"].count(), 3)

        self.assertIn("top_selling_products", kpis)
        self.assertIsInstance(kpis["top_selling_products"], type(self.df))
        self.assertEqual(kpis["top_selling_products"].count(), 2) # top_n=2

        self.assertIn("preferred_payment_methods", kpis)
        self.assertIsInstance(kpis["preferred_payment_methods"], type(self.df))
        self.assertEqual(kpis["preferred_payment_methods"].count(), 3)
        print("test_calculate_all_kpis PASSED")


if __name__ == '__main__':
    # Need to ensure pandas is available
    try:
        import pandas
    except ImportError:
        print("Pandas not found, assertions will fail if run directly without full venv.")
    unittest.main(verbosity=2)
