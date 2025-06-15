import unittest
from data_processing import (
    clean_and_transform_data,
    calculate_top_selling_products,
    calculate_total_revenue_by_category,
    calculate_preferred_payment_method_by_category,
    calculate_purchase_frequency_per_user,
    calculate_average_order_value,
    ALLOWED_CATEGORIES,
    ALLOWED_PAYMENT_METHODS
)

class TestDataProcessing(unittest.TestCase):

    # --- Test Data ---
    RAW_DATA_VALID = [
        {'event_timestamp': 'ts1', 'user_id': 'u1', 'order_id': 'o1', 'product_id': 'p1', 'product_name': 'N1', 'category': 'men', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'},
        {'event_timestamp': 'ts2', 'user_id': 'u2', 'order_id': 'o2', 'product_id': 'p2', 'product_name': 'N2', 'category': 'women', 'price': 20.0, 'quantity': 2, 'payment_method': 'credit_card'},
    ]
    CLEANED_DATA_VALID = [ # Expected from RAW_DATA_VALID
        {'event_timestamp': 'ts1', 'user_id': 'u1', 'order_id': 'o1', 'product_id': 'p1', 'product_name': 'N1', 'category': 'men', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'},
        {'event_timestamp': 'ts2', 'user_id': 'u2', 'order_id': 'o2', 'product_id': 'p2', 'product_name': 'N2', 'category': 'women', 'price': 20.0, 'quantity': 2, 'payment_method': 'credit_card'},
    ]

    KPI_TEST_DATA = [ # Already cleaned data for KPI functions
        {'user_id': 'u1', 'order_id': 'o1', 'product_id': 'pA', 'product_name': 'Alpha', 'category': 'men', 'price': 10.0, 'quantity': 2, 'payment_method': 'upi'}, # Revenue: 20
        {'user_id': 'u2', 'order_id': 'o2', 'product_id': 'pB', 'product_name': 'Beta', 'category': 'women', 'price': 15.0, 'quantity': 1, 'payment_method': 'credit_card'}, # Revenue: 15
        {'user_id': 'u1', 'order_id': 'o3', 'product_id': 'pA', 'product_name': 'Alpha Prime', 'category': 'men', 'price': 12.0, 'quantity': 3, 'payment_method': 'wallet'}, # Revenue: 36
        {'user_id': 'u3', 'order_id': 'o4', 'product_id': 'pC', 'product_name': 'Charlie', 'category': 'kids', 'price': 5.0, 'quantity': 5, 'payment_method': 'upi'}, # Revenue: 25
        {'user_id': 'u2', 'order_id': 'o2', 'product_id': 'pD', 'product_name': 'Delta', 'category': 'women', 'price': 20.0, 'quantity': 1, 'payment_method': 'credit_card'}, # Revenue: 20 (part of order o2)
    ]
    # pA: 5 sold (Alpha)
    # pB: 1 sold (Beta)
    # pC: 5 sold (Charlie)
    # pD: 1 sold (Delta)

    # --- Tests for clean_and_transform_data ---
    def test_clean_valid_data(self):
        result = clean_and_transform_data(self.RAW_DATA_VALID)
        self.assertListEqual(result, self.CLEANED_DATA_VALID)

    def test_clean_empty_input(self):
        self.assertListEqual(clean_and_transform_data([]), [])
        self.assertListEqual(clean_and_transform_data(None), []) # Test None input

    def test_clean_missing_category(self):
        data = [{'category': None, 'price': 10.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])
        data = [{'category': "", 'price': 10.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])

    def test_clean_missing_price_quantity(self):
        data = [{'category': 'men', 'price': None, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])
        data = [{'category': 'men', 'price': 10.0, 'quantity': None, 'payment_method': 'upi', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])

    def test_clean_invalid_price(self):
        data = [{'category': 'men', 'price': 0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])
        data = [{'category': 'men', 'price': -10.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])

    def test_clean_invalid_quantity(self):
        data = [{'category': 'men', 'price': 10.0, 'quantity': 0, 'payment_method': 'upi', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])
        data = [{'category': 'men', 'price': 10.0, 'quantity': -1, 'payment_method': 'upi', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])

    def test_clean_invalid_category_value(self):
        data = [{'category': 'unisex', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])

    def test_clean_invalid_payment_method_value(self):
        data = [{'category': 'men', 'price': 10.0, 'quantity': 1, 'payment_method': 'cash', 'product_id': 'p1'}]
        self.assertListEqual(clean_and_transform_data(data), [])

    def test_clean_case_insensitivity_and_normalization(self):
        data = [
            {'category': 'MeN', 'price': 10.0, 'quantity': 1, 'payment_method': 'UpI', 'product_id': 'p1', 'user_id':'u1', 'order_id':'o1', 'product_name':'N1'},
            {'category': 'WOMEN', 'price': 20.0, 'quantity': 2, 'payment_method': 'CreDit_CarD', 'product_id': 'p2', 'user_id':'u2', 'order_id':'o2', 'product_name':'N2'}
        ]
        expected = [
            {'category': 'men', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1', 'user_id':'u1', 'order_id':'o1', 'product_name':'N1'},
            {'category': 'women', 'price': 20.0, 'quantity': 2, 'payment_method': 'credit_card', 'product_id': 'p2', 'user_id':'u2', 'order_id':'o2', 'product_name':'N2'}
        ]
        self.assertListEqual(clean_and_transform_data(data), expected)

    def test_clean_mixed_valid_invalid(self):
        data = [
            self.RAW_DATA_VALID[0], # Valid
            {'category': 'unisex', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p_invalid_cat'},
            self.RAW_DATA_VALID[1], # Valid
            {'category': 'men', 'price': -5.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p_invalid_price'},
            {'category': 'kids', 'price': 5.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p_valid_kids', 'user_id':'u3', 'order_id':'o3', 'product_name':'N3'}
        ]
        expected = [
            self.CLEANED_DATA_VALID[0],
            self.CLEANED_DATA_VALID[1],
            {'category': 'kids', 'price': 5.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p_valid_kids', 'user_id':'u3', 'order_id':'o3', 'product_name':'N3'}
        ]
        # Sort by product_id for comparison if order is not guaranteed and matters for the test
        # However, clean_and_transform_data preserves order of valid items.
        self.assertListEqual(clean_and_transform_data(data), expected)

    # --- Tests for KPI Functions ---

    # calculate_top_selling_products
    def test_top_selling_empty(self):
        self.assertListEqual(calculate_top_selling_products([]), [])

    def test_top_selling_basic(self):
        result = calculate_top_selling_products(self.KPI_TEST_DATA, top_n=2)
        # pA: 5, pC: 5. Order between pA and pC can vary if quantities are same.
        # pB: 1, pD: 1.
        # Expected: pA (5), pC (5)
        self.assertEqual(len(result), 2)
        # Check that the top 2 products are pA and pC, quantities are 5 for both
        # Convert to set of tuples for order-agnostic comparison of elements
        result_set = {(item['product_id'], item['total_quantity_sold']) for item in result}
        expected_set = {('pA', 5), ('pC', 5)}
        self.assertSetEqual(result_set, expected_set)
        # Also check product names are present
        for item in result:
            self.assertIn(item['product_name'], ['Alpha', 'Charlie'])


    def test_top_selling_tie_and_name_consistency(self):
        # pA (Alpha, then Alpha Prime): 2+3=5. Should pick 'Alpha' (first name encountered).
        # pC (Charlie): 5
        result = calculate_top_selling_products(self.KPI_TEST_DATA, top_n=1) # Get only the very top
        # If there's a tie for top_n=1, it could be pA or pC. Let's check for one of them.
        self.assertEqual(result[0]['total_quantity_sold'], 5)
        self.assertIn(result[0]['product_id'], ['pA', 'pC'])

        full_result = calculate_top_selling_products(self.KPI_TEST_DATA, top_n=10)
        pa_item = next(item for item in full_result if item['product_id'] == 'pA')
        self.assertEqual(pa_item['product_name'], 'Alpha') # First name for pA

    def test_top_selling_more_products_than_top_n(self):
        result = calculate_top_selling_products(self.KPI_TEST_DATA, top_n=1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['total_quantity_sold'], 5)


    def test_top_selling_less_products_than_top_n(self):
        result = calculate_top_selling_products(self.KPI_TEST_DATA, top_n=10) # top_n is larger than unique products
        self.assertEqual(len(result), 4) # pA, pB, pC, pD

    # calculate_total_revenue_by_category
    def test_revenue_empty(self):
        expected = {cat: 0.0 for cat in ALLOWED_CATEGORIES}
        self.assertDictEqual(calculate_total_revenue_by_category([]), expected)

    def test_revenue_basic(self):
        result = calculate_total_revenue_by_category(self.KPI_TEST_DATA)
        # men: pA (10*2) + pA (12*3) = 20 + 36 = 56
        # women: pB (15*1) + pD (20*1) = 15 + 20 = 35
        # kids: pC (5*5) = 25
        expected = {'men': 56.0, 'women': 35.0, 'kids': 25.0}
        self.assertDictEqual(result, expected)

    def test_revenue_no_sales_in_one_category(self):
        data = [
            {'user_id': 'u1', 'order_id': 'o1', 'product_id': 'pA', 'product_name': 'Alpha', 'category': 'men', 'price': 10.0, 'quantity': 2, 'payment_method': 'upi'},
            # No 'women' or 'kids' sales
        ]
        result = calculate_total_revenue_by_category(data)
        expected = {'men': 20.0, 'women': 0.0, 'kids': 0.0}
        self.assertDictEqual(result, expected)

    # calculate_preferred_payment_method_by_category
    def test_payment_method_empty(self):
        expected = {
            cat: {pm: 0 for pm in ALLOWED_PAYMENT_METHODS} for cat in ALLOWED_CATEGORIES
        }
        self.assertDictEqual(calculate_preferred_payment_method_by_category([]), expected)

    def test_payment_method_basic(self):
        result = calculate_preferred_payment_method_by_category(self.KPI_TEST_DATA)
        # men: upi (1 from pA), wallet (1 from pA)
        # women: credit_card (1 from pB), credit_card (1 from pD)
        # kids: upi (1 from pC)
        expected = {
            'men': {'upi': 1, 'credit_card': 0, 'wallet': 1},
            'women': {'upi': 0, 'credit_card': 2, 'wallet': 0},
            'kids': {'upi': 1, 'credit_card': 0, 'wallet': 0}
        }
        self.assertDictEqual(result, expected)

    def test_payment_method_no_sales_in_category(self):
        data = [
            {'user_id': 'u1', 'order_id': 'o1', 'product_id': 'pA', 'product_name': 'Alpha', 'category': 'men', 'price': 10.0, 'quantity': 2, 'payment_method': 'upi'},
        ] # Only 'men' sales
        result = calculate_preferred_payment_method_by_category(data)
        expected = {
            'men': {'upi': 1, 'credit_card': 0, 'wallet': 0},
            'women': {'upi': 0, 'credit_card': 0, 'wallet': 0},
            'kids': {'upi': 0, 'credit_card': 0, 'wallet': 0}
        }
        self.assertDictEqual(result, expected)

    # calculate_purchase_frequency_per_user
    def test_frequency_empty(self):
        self.assertDictEqual(calculate_purchase_frequency_per_user([]), {})

    def test_frequency_basic(self):
        result = calculate_purchase_frequency_per_user(self.KPI_TEST_DATA)
        # u1: pA, pA => 2 items
        # u2: pB, pD => 2 items
        # u3: pC => 1 item
        expected = {'u1': 2, 'u2': 2, 'u3': 1}
        self.assertDictEqual(result, expected)

    # calculate_average_order_value
    def test_aov_empty(self):
        self.assertAlmostEqual(calculate_average_order_value([]), 0.0)

    def test_aov_single_order_single_item(self):
        data = [{'user_id': 'u1', 'order_id': 'o1', 'product_id': 'pA', 'category': 'men', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'}]
        self.assertAlmostEqual(calculate_average_order_value(data), 10.0)

    def test_aov_single_order_multiple_items(self):
        data = [
            {'user_id': 'u1', 'order_id': 'o1', 'product_id': 'pA', 'category': 'men', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'}, # val 10
            {'user_id': 'u1', 'order_id': 'o1', 'product_id': 'pB', 'category': 'men', 'price': 20.0, 'quantity': 2, 'payment_method': 'upi'}  # val 40
        ] # Single order o1, total value 50
        self.assertAlmostEqual(calculate_average_order_value(data), 50.0)

    def test_aov_multiple_orders(self):
        # Using self.KPI_TEST_DATA
        # Order o1: pA (10*2) = 20
        # Order o2: pB (15*1) + pD (20*1) = 15 + 20 = 35
        # Order o3: pA (12*3) = 36
        # Order o4: pC (5*5) = 25
        # Total value = 20 + 35 + 36 + 25 = 116
        # Number of orders = 4
        # AOV = 116 / 4 = 29.0
        self.assertAlmostEqual(calculate_average_order_value(self.KPI_TEST_DATA), 29.0)

if __name__ == '__main__':
    unittest.main()
