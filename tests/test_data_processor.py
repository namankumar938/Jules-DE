import unittest
import datetime # Required for creating sample datetime objects for tests

# Assuming data_processor and data_structures are in the parent directory
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_processor import (
    clean_data,
    transform_data,
    calculate_top_selling_products,
    calculate_total_revenue_by_category,
    calculate_total_revenue_by_payment_method,
    calculate_average_purchase_value,
    calculate_purchase_frequency_per_user
)
from data_structures import create_purchase_record


class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up some common test data."""
        self.now = datetime.datetime.now()
        self.sample_record_valid_1 = create_purchase_record(
            timestamp=self.now - datetime.timedelta(days=1),
            user_id=1,
            product_id=101,
            product_name="Laptop",
            product_category="electronics",
            product_price=1200.00,
            quantity=1,
            payment_method="credit card"
        )
        self.sample_record_valid_2 = create_purchase_record(
            timestamp=self.now - datetime.timedelta(days=2),
            user_id=2,
            product_id=102,
            product_name="Mouse",
            product_category="electronics",
            product_price=25.00,
            quantity=2,
            payment_method="upi"
        )
        self.sample_record_valid_3 = create_purchase_record(
            timestamp=self.now - datetime.timedelta(days=3),
            user_id=1,
            product_id=103,
            product_name="Keyboard",
            product_category="electronics",
            product_price=75.00,
            quantity=1,
            payment_method="wallet"
        )
        self.sample_record_valid_4_women_clothing = create_purchase_record(
            timestamp=self.now - datetime.timedelta(days=4),
            user_id=3,
            product_id=201,
            product_name="Dress",
            product_category="women",
            product_price=150.00,
            quantity=1,
            payment_method="credit card"
        )
        self.sample_record_valid_5_men_clothing = create_purchase_record(
            timestamp=self.now - datetime.timedelta(days=5),
            user_id=2,
            product_id=202,
            product_name="Shirt",
            product_category="men",
            product_price=50.00,
            quantity=3,
            payment_method="upi"
        )

    def test_clean_data_valid(self):
        raw_data = [self.sample_record_valid_1.copy(), self.sample_record_valid_2.copy()]
        cleaned = clean_data(raw_data)
        self.assertEqual(len(cleaned), 2)
        self.assertEqual(cleaned[0]['product_price'], 1200.00)
        self.assertIsInstance(cleaned[0]['product_price'], float)
        self.assertEqual(cleaned[1]['quantity'], 2)
        self.assertIsInstance(cleaned[1]['quantity'], int)

    def test_clean_data_missing_price(self):
        record_missing_price = self.sample_record_valid_1.copy()
        del record_missing_price['product_price']
        raw_data = [record_missing_price, self.sample_record_valid_2.copy()]
        # Assuming print for warnings, so can't directly test print output here easily
        cleaned = clean_data(raw_data)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]['product_id'], 102) # Only valid_2 should remain

    def test_clean_data_missing_quantity(self):
        record_missing_quantity = self.sample_record_valid_1.copy()
        del record_missing_quantity['quantity']
        raw_data = [record_missing_quantity, self.sample_record_valid_2.copy()]
        cleaned = clean_data(raw_data)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]['product_id'], 102)

    def test_clean_data_none_price(self):
        record_none_price = self.sample_record_valid_1.copy()
        record_none_price['product_price'] = None
        raw_data = [record_none_price, self.sample_record_valid_2.copy()]
        cleaned = clean_data(raw_data)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]['product_id'], 102)

    def test_clean_data_incorrect_type_price(self):
        record_invalid_price = self.sample_record_valid_1.copy()
        record_invalid_price['product_price'] = "not-a-float"
        raw_data = [record_invalid_price, self.sample_record_valid_2.copy()]
        cleaned = clean_data(raw_data)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]['product_id'], 102)

    def test_clean_data_incorrect_type_quantity(self):
        record_invalid_quantity = self.sample_record_valid_1.copy()
        record_invalid_quantity['quantity'] = "not-an-int"
        raw_data = [record_invalid_quantity, self.sample_record_valid_2.copy()]
        cleaned = clean_data(raw_data)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]['product_id'], 102)

    def test_clean_data_empty_input(self):
        cleaned = clean_data([])
        self.assertEqual(len(cleaned), 0)

    def test_transform_data_calculates_total_purchase_value(self):
        # Assuming clean_data has already processed these records
        data = [self.sample_record_valid_1.copy(), self.sample_record_valid_2.copy()]
        # Manually ensure types are correct as if clean_data ran
        data[0]['product_price'] = float(data[0]['product_price'])
        data[0]['quantity'] = int(data[0]['quantity'])
        data[1]['product_price'] = float(data[1]['product_price'])
        data[1]['quantity'] = int(data[1]['quantity'])

        transformed = transform_data(data)
        self.assertEqual(len(transformed), 2)
        self.assertIn('total_purchase_value', transformed[0])
        self.assertEqual(transformed[0]['total_purchase_value'], 1200.00 * 1)
        self.assertIn('total_purchase_value', transformed[1])
        self.assertEqual(transformed[1]['total_purchase_value'], 25.00 * 2)

    def test_transform_data_multiple_records(self):
        data = [
            self.sample_record_valid_1.copy(),
            self.sample_record_valid_2.copy(),
            self.sample_record_valid_3.copy()
        ]
        # Manually ensure types
        for rec in data:
            rec['product_price'] = float(rec['product_price'])
            rec['quantity'] = int(rec['quantity'])

        transformed = transform_data(data)
        self.assertEqual(len(transformed), 3)
        self.assertEqual(transformed[0]['total_purchase_value'], 1200.00)
        self.assertEqual(transformed[1]['total_purchase_value'], 50.00)
        self.assertEqual(transformed[2]['total_purchase_value'], 75.00)

    def test_transform_data_empty_input(self):
        transformed = transform_data([])
        self.assertEqual(len(transformed), 0)

    def test_calculate_top_selling_products(self):
        # Create a dataset for this test
        data = [
            {'product_name': 'A', 'quantity': 10, 'total_purchase_value': 100.0},
            {'product_name': 'B', 'quantity': 20, 'total_purchase_value': 50.0},
            {'product_name': 'A', 'quantity': 5, 'total_purchase_value': 50.0},
            {'product_name': 'C', 'quantity': 15, 'total_purchase_value': 200.0},
            {'product_name': 'B', 'quantity': 10, 'total_purchase_value': 25.0},
        ]
        top_qty, top_rev = calculate_top_selling_products(data, n=2)

        # Expected: A: 15, B: 30, C: 15. Top 2 by Qty: B, A (or C)
        # Expected: A: 150, B: 75, C: 200. Top 2 by Rev: C, A
        self.assertEqual(top_qty[0], ('B', 30))
        # Order for second place might vary if counts are equal, check both possibilities
        self.assertTrue(top_qty[1] == ('A', 15) or top_qty[1] == ('C', 15))

        self.assertEqual(top_rev[0], ('C', 200.0))
        self.assertEqual(top_rev[1], ('A', 150.0))

    def test_calculate_top_selling_products_empty(self):
        top_qty, top_rev = calculate_top_selling_products([], n=2)
        self.assertEqual(top_qty, [])
        self.assertEqual(top_rev, [])

    def test_calculate_total_revenue_by_category(self):
        data = [
            {'product_category': 'electronics', 'total_purchase_value': 100.0},
            {'product_category': 'women', 'total_purchase_value': 50.0},
            {'product_category': 'electronics', 'total_purchase_value': 150.0},
            {'product_category': 'men', 'total_purchase_value': 70.0},
            {'product_category': 'women', 'total_purchase_value': 30.0},
        ]
        revenue_by_cat = calculate_total_revenue_by_category(data)
        self.assertEqual(revenue_by_cat.get('electronics'), 250.0)
        self.assertEqual(revenue_by_cat.get('women'), 80.0)
        self.assertEqual(revenue_by_cat.get('men'), 70.0)
        self.assertIsNone(revenue_by_cat.get('kids')) # kids category not in data

    def test_calculate_total_revenue_by_category_empty(self):
        revenue_by_cat = calculate_total_revenue_by_category([])
        self.assertEqual(revenue_by_cat, {})

    def test_calculate_total_revenue_by_payment_method(self):
        data = [
            {'payment_method': 'credit card', 'total_purchase_value': 100.0},
            {'payment_method': 'upi', 'total_purchase_value': 50.0},
            {'payment_method': 'credit card', 'total_purchase_value': 150.0},
        ]
        revenue_by_method = calculate_total_revenue_by_payment_method(data)
        self.assertEqual(revenue_by_method.get('credit card'), 250.0)
        self.assertEqual(revenue_by_method.get('upi'), 50.0)
        self.assertIsNone(revenue_by_method.get('wallet'))

    def test_calculate_total_revenue_by_payment_method_empty(self):
        revenue_by_method = calculate_total_revenue_by_payment_method([])
        self.assertEqual(revenue_by_method, {})

    def test_calculate_average_purchase_value(self):
        data = [
            {'total_purchase_value': 100.0},
            {'total_purchase_value': 50.0},
            {'total_purchase_value': 150.0}, # Sum = 300, Count = 3
        ]
        avg_value = calculate_average_purchase_value(data)
        self.assertEqual(avg_value, 100.0)

    def test_calculate_average_purchase_value_single(self):
        data = [{'total_purchase_value': 75.0}]
        avg_value = calculate_average_purchase_value(data)
        self.assertEqual(avg_value, 75.0)

    def test_calculate_average_purchase_value_empty(self):
        avg_value = calculate_average_purchase_value([])
        self.assertEqual(avg_value, 0.0)

    def test_calculate_purchase_frequency_per_user(self):
        data = [
            {'user_id': 1}, {'user_id': 1}, {'user_id': 2}, # User 1: 2, User 2: 1. Total 3 purchases, 2 users. Avg = 1.5
            {'user_id': 3}, {'user_id': 1}, {'user_id': 2}, # User 1: 3, User 2: 2, User 3: 1. Total 6 purchases, 3 users. Avg = 2.0
        ]
        # Re-calculating based on the final state of data:
        # User 1: 3 purchases
        # User 2: 2 purchases
        # User 3: 1 purchase
        # Total purchases = 6. Total unique users = 3. Average = 6 / 3 = 2.0
        avg_freq = calculate_purchase_frequency_per_user(data)
        self.assertEqual(avg_freq, 2.0)

    def test_calculate_purchase_frequency_per_user_single_user_multiple_purchases(self):
        data = [{'user_id': 1}, {'user_id': 1}, {'user_id': 1}] # 3 purchases / 1 user = 3.0
        avg_freq = calculate_purchase_frequency_per_user(data)
        self.assertEqual(avg_freq, 3.0)

    def test_calculate_purchase_frequency_per_user_multiple_users_single_purchases(self):
        data = [{'user_id': 1}, {'user_id': 2}, {'user_id': 3}] # 3 purchases / 3 users = 1.0
        avg_freq = calculate_purchase_frequency_per_user(data)
        self.assertEqual(avg_freq, 1.0)

    def test_calculate_purchase_frequency_per_user_empty(self):
        avg_freq = calculate_purchase_frequency_per_user([])
        self.assertEqual(avg_freq, 0.0)


if __name__ == '__main__':
    unittest.main()
