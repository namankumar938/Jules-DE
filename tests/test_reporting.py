import unittest
from unittest.mock import patch, call # Import call for checking call arguments
import io
from reporting import (
    format_and_print_top_selling_products,
    format_and_print_total_revenue_by_category,
    format_and_print_preferred_payment_methods,
    format_and_print_purchase_frequency,
    format_and_print_average_order_value,
    generate_kpi_report
)
# Import constants from data_processing to help construct expected outputs or inputs
from data_processing import ALLOWED_CATEGORIES, ALLOWED_PAYMENT_METHODS

class TestReporting(unittest.TestCase):

    # --- Tests for format_and_print_... functions ---

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_top_selling_products_basic(self, mock_stdout):
        sample_products = [
            {'product_id': 'pA', 'product_name': 'Product Alpha', 'total_quantity_sold': 100},
            {'product_id': 'pB', 'product_name': 'Product Beta', 'total_quantity_sold': 50}
        ]
        format_and_print_top_selling_products(sample_products)
        output = mock_stdout.getvalue()
        self.assertIn("--- Top Selling Products ---", output)
        self.assertIn("Product Alpha", output)
        self.assertIn("100", output)
        self.assertIn("Product Beta", output)
        self.assertIn("50", output)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_top_selling_products_empty(self, mock_stdout):
        format_and_print_top_selling_products([])
        output = mock_stdout.getvalue()
        self.assertIn("--- Top Selling Products ---", output)
        self.assertIn("No products sold or data available.", output)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_total_revenue_by_category_basic(self, mock_stdout):
        sample_revenue = {'men': 1250.75, 'women': 800.50, 'kids': 0.0}
        format_and_print_total_revenue_by_category(sample_revenue)
        output = mock_stdout.getvalue()
        self.assertIn("--- Total Revenue by Category ---", output)
        self.assertIn("Men", output)
        self.assertIn("$1,250.75", output)
        self.assertIn("Women", output)
        self.assertIn("$800.50", output)
        self.assertIn("Kids", output)
        self.assertIn("$0.00", output) # Ensure all categories are printed

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_total_revenue_by_category_empty(self, mock_stdout):
        format_and_print_total_revenue_by_category({}) # Empty dict
        output = mock_stdout.getvalue()
        self.assertIn("--- Total Revenue by Category ---", output)
        for cat in ALLOWED_CATEGORIES: # Should still print all categories with $0.00
            self.assertIn(cat.capitalize(), output)
            self.assertIn("$0.00", output)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_preferred_payment_methods_basic(self, mock_stdout):
        sample_payment_data = {
            'men': {'upi': 10, 'credit_card': 5, 'wallet': 2},
            'women': {'upi': 3, 'credit_card': 8, 'wallet': 1},
            'kids': {'upi': 0, 'credit_card': 0, 'wallet': 0} # Kids has no sales
        }
        format_and_print_preferred_payment_methods(sample_payment_data)
        output = mock_stdout.getvalue()
        self.assertIn("--- Preferred Payment Methods by Category ---", output)
        self.assertIn("Category: Men", output)
        self.assertIn("Upi             | 10", output) # Check one specific value for men
        self.assertIn("Category: Women", output)
        self.assertIn("Credit card     | 8", output)  # Check one specific value for women
        self.assertIn("Category: Kids", output)
        for pm in ALLOWED_PAYMENT_METHODS: # Ensure all payment methods are listed for kids
            self.assertIn(f"{pm.replace('_', ' ').capitalize():<15} | 0", output)


    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_preferred_payment_methods_empty(self, mock_stdout):
        format_and_print_preferred_payment_methods({})
        output = mock_stdout.getvalue()
        self.assertIn("--- Preferred Payment Methods by Category ---", output)
        for cat in ALLOWED_CATEGORIES:
            self.assertIn(f"Category: {cat.capitalize()}", output)
            for pm in ALLOWED_PAYMENT_METHODS:
                 self.assertIn(f"{pm.replace('_', ' ').capitalize():<15} | 0", output)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_purchase_frequency_basic(self, mock_stdout):
        sample_freq_data = {'user123': 10, 'user456': 5, 'user789': 12}
        format_and_print_purchase_frequency(sample_freq_data, top_n_users=2)
        output = mock_stdout.getvalue()
        self.assertIn("--- Purchase Frequency (Top 2 Users by Item Count) ---", output)
        self.assertIn("user789         | 12", output) # Top user
        self.assertIn("user123         | 10", output) # Second top user
        self.assertNotIn("user456", output) # Should not be present if top_n=2

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_purchase_frequency_less_than_top_n(self, mock_stdout):
        sample_freq_data = {'user123': 10}
        format_and_print_purchase_frequency(sample_freq_data, top_n_users=5)
        output = mock_stdout.getvalue()
        self.assertIn("user123         | 10", output)
        self.assertIn("(All users shown", output)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_purchase_frequency_empty(self, mock_stdout):
        format_and_print_purchase_frequency({})
        output = mock_stdout.getvalue()
        self.assertIn("--- Purchase Frequency", output) # Check header part
        self.assertIn("No user purchase data available.", output)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_average_order_value_basic(self, mock_stdout):
        format_and_print_average_order_value(123.456)
        output = mock_stdout.getvalue()
        self.assertIn("--- Average Order Value ---", output)
        self.assertIn("$123.46", output) # Check formatting and rounding

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_format_average_order_value_zero(self, mock_stdout):
        format_and_print_average_order_value(0.0)
        output = mock_stdout.getvalue()
        self.assertIn("--- Average Order Value ---", output)
        self.assertIn("$0.00", output)

    # --- Tests for generate_kpi_report ---

    @patch('reporting.format_and_print_average_order_value')
    @patch('reporting.format_and_print_purchase_frequency')
    @patch('reporting.format_and_print_preferred_payment_methods')
    @patch('reporting.format_and_print_total_revenue_by_category')
    @patch('reporting.format_and_print_top_selling_products')
    @patch('reporting.calculate_average_order_value', return_value=12.34)
    @patch('reporting.calculate_purchase_frequency_per_user', return_value={'u1': 1})
    @patch('reporting.calculate_preferred_payment_method_by_category', return_value={'men': {'upi': 1}})
    @patch('reporting.calculate_total_revenue_by_category', return_value={'men': 100.0})
    @patch('reporting.calculate_top_selling_products', return_value=[{'id': 'p1'}])
    def test_generate_kpi_report_calls_calcs_and_formatters(
        self, mock_calc_top_prod, mock_calc_rev_cat, mock_calc_pref_pm, mock_calc_freq, mock_calc_aov,
        mock_print_top_prod, mock_print_rev_cat, mock_print_pref_pm, mock_print_freq, mock_print_aov
    ):
        sample_cleaned_data = [{'category': 'men', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'}]
        generate_kpi_report(sample_cleaned_data)

        mock_calc_top_prod.assert_called_once_with(sample_cleaned_data, top_n=5)
        mock_calc_rev_cat.assert_called_once_with(sample_cleaned_data)
        mock_calc_pref_pm.assert_called_once_with(sample_cleaned_data)
        mock_calc_freq.assert_called_once_with(sample_cleaned_data)
        mock_calc_aov.assert_called_once_with(sample_cleaned_data)

        mock_print_top_prod.assert_called_once_with([{'id': 'p1'}])
        mock_print_rev_cat.assert_called_once_with({'men': 100.0})
        mock_print_pref_pm.assert_called_once_with({'men': {'upi': 1}})
        mock_print_freq.assert_called_once_with({'u1': 1}, top_n_users=5)
        mock_print_aov.assert_called_once_with(12.34)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_generate_kpi_report_output_structure(self, mock_stdout):
        # Use a very small, simple dataset to minimize recalculation here,
        # as we are testing structure, not specific KPI values in this test.
        # The specific KPI values are tested by the individual format_and_print tests
        # and the KPI calculation function tests in test_data_processing.
        sample_cleaned_data = [
             {'user_id': 'u1', 'order_id': 'o1', 'product_id': 'pA', 'product_name': 'Alpha', 'category': 'men', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'},
        ]
        generate_kpi_report(sample_cleaned_data)
        output = mock_stdout.getvalue()

        self.assertIn("Sales KPI Report", output)
        self.assertIn("--- Top Selling Products ---", output)
        self.assertIn("--- Total Revenue by Category ---", output)
        self.assertIn("--- Preferred Payment Methods by Category ---", output)
        self.assertIn("--- Purchase Frequency", output)
        self.assertIn("--- Average Order Value ---", output)
        self.assertIn("--- End of Report ---", output)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_generate_kpi_report_empty_data(self, mock_stdout):
        generate_kpi_report([]) # Empty cleaned data
        output = mock_stdout.getvalue()
        self.assertIn("Sales KPI Report", output)
        self.assertIn("No data available to generate the report after cleaning.", output)
        self.assertIn("No products sold or data available.", output) # From top_selling_products
        self.assertIn("Category: Men", output) # Still prints structure for others
        self.assertIn("$0.00", output) # For revenue and AOV
        self.assertIn("No user purchase data available.", output) # For frequency
        self.assertIn("--- End of Report ---", output)


if __name__ == '__main__':
    unittest.main()
