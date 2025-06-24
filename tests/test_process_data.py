import unittest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import sys
import os

# Add the 'scripts' directory to sys.path to allow importing process_data
# This is a common way to handle imports for testing scripts in a sub-directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from process_data import transform_data, calculate_kpis

class TestProcessData(unittest.TestCase):

    def test_transform_data(self):
        print("\nRunning test_transform_data...")
        data = {
            'price': [10.0, 20.0, 5.0],
            'quantity': [1, 2, 3],
            'category': ['men', 'women', 'men'] # Needed for later KPI tests
        }
        input_df = pd.DataFrame(data)

        expected_data = {
            'price': [10.0, 20.0, 5.0],
            'quantity': [1, 2, 3],
            'category': ['men', 'women', 'men'],
            'total_purchase_amount': [10.0, 40.0, 15.0]
        }
        expected_df = pd.DataFrame(expected_data)

        transformed_df = transform_data(input_df.copy()) # Use .copy() to avoid modifying input_df if transform_data modifies in place

        self.assertIn('total_purchase_amount', transformed_df.columns)
        assert_frame_equal(transformed_df.sort_index(axis=1), expected_df.sort_index(axis=1), check_dtype=False)
        print("test_transform_data PASSED")

    def test_calculate_kpis_total_revenue(self):
        print("\nRunning test_calculate_kpis_total_revenue...")
        data = {
            'category': ['men', 'women', 'kids', 'men'],
            'product_name': ['Shirt', 'Dress', 'Toy', 'Jeans'],
            'total_purchase_amount': [100.0, 150.0, 50.0, 200.0],
            'payment_method': ['upi', 'card', 'wallet', 'upi']
        }
        input_df = pd.DataFrame(data)

        kpis = calculate_kpis(input_df)

        self.assertIsNotNone(kpis)
        self.assertAlmostEqual(kpis['total_revenue'], 500.0)
        print("test_calculate_kpis_total_revenue PASSED")

    def test_calculate_kpis_revenue_by_category(self):
        print("\nRunning test_calculate_kpis_revenue_by_category...")
        data = {
            'category': ['men', 'women', 'kids', 'men', 'women'],
            'product_name': ['Shirt', 'Dress', 'Toy', 'Jeans', 'Scarf'],
            'total_purchase_amount': [100.0, 150.0, 50.0, 200.0, 75.0],
            'payment_method': ['upi', 'card', 'wallet', 'upi', 'card']
        }
        input_df = pd.DataFrame(data)

        kpis = calculate_kpis(input_df)

        expected_revenue_by_category = {
            'men': 300.0,
            'women': 225.0,
            'kids': 50.0
        }

        self.assertDictEqual(kpis['revenue_by_category'], expected_revenue_by_category)
        print("test_calculate_kpis_revenue_by_category PASSED")

    def test_calculate_kpis_top_selling_products(self):
        print("\nRunning test_calculate_kpis_top_selling_products...")
        data = {
            'category': ['men', 'women', 'kids', 'men', 'women'],
            'product_name': ['Shirt', 'Dress', 'Toy', 'Shirt', 'Dress'], # Repeated product
            'total_purchase_amount': [100.0, 150.0, 50.0, 75.0, 80.0], # Shirt: 175, Dress: 230
            'payment_method': ['upi', 'card', 'wallet', 'upi', 'card']
        }
        input_df = pd.DataFrame(data)

        kpis = calculate_kpis(input_df, top_n_products=2)

        expected_top_products = {
            'Dress': 230.0,
            'Shirt': 175.0
        }
        self.assertDictEqual(kpis['top_selling_products'], expected_top_products)
        print("test_calculate_kpis_top_selling_products PASSED")

    def test_calculate_kpis_payment_methods(self):
        print("\nRunning test_calculate_kpis_payment_methods...")
        data = {
            'category': ['men', 'women', 'kids', 'men', 'women'],
            'product_name': ['A', 'B', 'C', 'D', 'E'],
            'total_purchase_amount': [10, 20, 30, 40, 50],
            'payment_method': ['upi', 'card', 'wallet', 'upi', 'card']
        }
        input_df = pd.DataFrame(data)

        kpis = calculate_kpis(input_df)

        expected_payment_methods = {
            'upi': 2,
            'card': 2,
            'wallet': 1
        }
        self.assertDictEqual(kpis['preferred_payment_methods'], expected_payment_methods)
        print("test_calculate_kpis_payment_methods PASSED")

if __name__ == '__main__':
    # Redirect print statements from the module to dev/null during tests
    # to keep test output clean, then restore.
    # This is specifically for the print statements inside the functions being tested.

    original_stdout = sys.stdout # Save a reference to the original standard output

    # Suppress prints from the imported module functions if they are too verbose
    # For now, the prints in the module are useful for understanding flow, so we might keep them
    # but for automated tests, one might want to suppress them.
    # Example: If process_data.py had many prints:
    # import process_data
    # process_data.VERBOSE = False # Assuming a global VERBOSE flag in process_data

    print("Starting unit tests for process_data.py...")
    unittest.main(verbosity=2)
    print("Unit tests finished.")
