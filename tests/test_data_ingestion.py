import unittest
import os
import tempfile
import csv
from data_ingestion import load_purchase_data_from_csv # Assuming data_ingestion.py is in the parent directory or PYTHONPATH is set

class TestDataIngestion(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory to store test CSV files
        self.temp_dir_context = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir_context.name

    def tearDown(self):
        # The TemporaryDirectory context manager automatically cleans up the directory
        self.temp_dir_context.cleanup()

    def _create_csv_file(self, filename, data_rows, header=None):
        """Helper function to create a CSV file in the temporary directory."""
        filepath = os.path.join(self.temp_dir_path, filename)
        if header is None:
            header = ['event_timestamp', 'user_id', 'order_id', 'product_id', 'product_name', 'category', 'price', 'quantity', 'payment_method']

        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            if header: # Write header only if it's not None (for testing cases like completely empty files)
                writer.writerow(header)
            writer.writerows(data_rows)
        return filepath

    def test_load_valid_data(self):
        """Test loading a CSV with valid data."""
        data = [
            ['2023-01-01T12:00:00Z', 'user1', 'order1', 'prodA', 'Product A', 'electronics', '100.00', '2', 'credit_card'],
            ['2023-01-01T12:05:00Z', 'user2', 'order2', 'prodB', 'Product B', 'books', '15.50', '1', 'upi']
        ]
        filepath = self._create_csv_file("valid.csv", data)
        loaded_data = load_purchase_data_from_csv(filepath)

        self.assertEqual(len(loaded_data), 2)
        self.assertEqual(loaded_data[0]['user_id'], 'user1')
        self.assertEqual(loaded_data[0]['price'], 100.00) # Check type conversion
        self.assertEqual(loaded_data[0]['quantity'], 2)   # Check type conversion
        self.assertEqual(loaded_data[1]['product_name'], 'Product B')
        self.assertEqual(loaded_data[1]['price'], 15.50)
        self.assertEqual(loaded_data[1]['quantity'], 1)

    def test_load_data_with_conversion_errors(self):
        """Test loading data where some rows have price/quantity conversion errors."""
        data = [
            ['2023-01-01T12:00:00Z', 'user1', 'order1', 'prodA', 'Product A', 'electronics', '100.00', '2', 'credit_card'],
            ['2023-01-01T12:05:00Z', 'user2', 'order2', 'prodB', 'Product B', 'books', 'INVALID_PRICE', '1', 'upi'], # Bad price
            ['2023-01-01T12:10:00Z', 'user3', 'order3', 'prodC', 'Product C', 'home', '25.00', 'XYZ', 'wallet'],   # Bad quantity
            ['2023-01-01T12:15:00Z', 'user4', 'order4', 'prodD', 'Product D', 'fashion', '50.75', '3', 'credit_card']
        ]
        filepath = self._create_csv_file("conversion_errors.csv", data)
        loaded_data = load_purchase_data_from_csv(filepath) # Assumes load_purchase_data_from_csv prints errors

        self.assertEqual(len(loaded_data), 2) # Only two rows should be valid
        self.assertEqual(loaded_data[0]['user_id'], 'user1')
        self.assertEqual(loaded_data[1]['user_id'], 'user4')
        self.assertEqual(loaded_data[1]['price'], 50.75)
        self.assertEqual(loaded_data[1]['quantity'], 3)

    def test_load_empty_csv_with_header(self):
        """Test loading an empty CSV file (only headers)."""
        filepath = self._create_csv_file("empty_with_header.csv", [])
        loaded_data = load_purchase_data_from_csv(filepath)
        self.assertEqual(len(loaded_data), 0)

    def test_load_completely_empty_csv(self):
        """Test loading a completely empty CSV file (no headers, no data)."""
        filepath = self._create_csv_file("completely_empty.csv", [], header=None) # No header
        loaded_data = load_purchase_data_from_csv(filepath)
        # DictReader with an empty file (and no fieldnames passed to constructor) will yield nothing.
        self.assertEqual(len(loaded_data), 0)

    def test_file_not_found(self):
        """Test that FileNotFoundError is raised for a non-existent file."""
        with self.assertRaises(FileNotFoundError):
            load_purchase_data_from_csv("non_existent_file.csv")

    def test_load_csv_with_fewer_columns_than_header(self):
        """Test CSV where a row has fewer columns than defined by header."""
        # DictReader will fill missing values with None
        header = ['col1', 'col2', 'col3']
        data = [
            ['val1a', 'val1b'] # Missing col3
        ]
        filepath = self._create_csv_file("fewer_cols.csv", data, header=header)
        # We need to adjust what load_purchase_data_from_csv expects for this test
        # For this specific test, we assume load_purchase_data_from_csv can handle Nones
        # and that 'price' and 'quantity' might be among the missing (None) values.
        # The current `load_purchase_data_from_csv` would try to convert None to float/int,
        # which would cause a TypeError, and the row would be skipped.

        # Redefine header for this specific test to match what load_purchase_data_from_csv expects
        test_header = ['event_timestamp', 'user_id', 'order_id', 'product_id', 'product_name', 'category', 'price', 'quantity', 'payment_method']
        test_data_row = [
            ['2023-01-01', 'user1', 'order1', 'prodA', 'NameA', 'catA', '10.0', '1'] # Missing payment_method
        ]
        filepath_specific = self._create_csv_file("fewer_cols_specific.csv", test_data_row, header=test_header)
        loaded_data = load_purchase_data_from_csv(filepath_specific)

        self.assertEqual(len(loaded_data), 1)
        self.assertIsNone(loaded_data[0].get('payment_method')) # DictReader fills with None
        self.assertEqual(loaded_data[0]['price'], 10.0) # Price is present and valid

    def test_load_csv_with_more_columns_than_header(self):
        """Test CSV where a row has more columns than defined by header."""
        # By default, DictReader includes extra columns under a None key if fieldnames are explicit,
        # or includes them under keys like '__extra_field_1' if fieldnames are from first row and data has more.
        # If fieldnames are explicitly set and data has more, the extra data is put into a list under the key None.
        # Our load_purchase_data_from_csv uses DictReader without explicit fieldnames, so it infers from header.
        header = ['col1', 'col2']
        data = [
            ['val1a', 'val1b', 'extra_val']
        ]
        filepath = self._create_csv_file("more_cols.csv", data, header=header)
        # The current load_purchase_data_from_csv will use col1, col2 as keys.
        # The 'extra_val' will be accessible if DictReader is used with restkey,
        # but our function doesn't do that. It will just pick up the header fields.
        # Price and quantity conversion will fail if col1/col2 are not them.

        # More relevant test: standard header, but data row has too many values
        std_header = ['event_timestamp', 'user_id', 'price', 'quantity'] # Simplified for this test
        std_data = [
             ['2023-01-01', 'user1', '10.0', '1', 'extra_col_value']
        ]
        filepath_std = self._create_csv_file("more_cols_std.csv", std_data, header=std_header)
        loaded_data = load_purchase_data_from_csv(filepath_std)

        self.assertEqual(len(loaded_data), 1)
        self.assertEqual(loaded_data[0]['user_id'], 'user1')
        self.assertEqual(loaded_data[0]['price'], 10.0)
        self.assertEqual(loaded_data[0]['quantity'], 1)
        # Check that extra column value is not directly part of the dict keys unless DictReader's behavior changes
        # or is explicitly handled (it's not in our current function).
        # We are testing that it doesn't break the parsing of known columns.
        # And we now acknowledge that DictReader puts extra values under a 'None' key.
        self.assertIn(None, loaded_data[0].keys(), "Extra columns should be placed under a 'None' key by DictReader")
        self.assertEqual(loaded_data[0][None], ['extra_col_value'])

    def test_csv_with_empty_lines(self):
        """Test CSV with empty lines interspersed with data."""
        filepath = os.path.join(self.temp_dir_path, "empty_lines.csv")
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['event_timestamp', 'user_id', 'price', 'quantity'])
            writer.writerow(['2023-01-01', 'user1', '10.0', '1'])
            writer.writerow([]) # Empty line
            writer.writerow(['2023-01-02', 'user2', '20.0', '2'])
            writer.writerow(['', '', '', '']) # Line with empty strings
            writer.writerow(['2023-01-03', 'user3', 'INVALID', '3']) # Invalid data

        loaded_data = load_purchase_data_from_csv(filepath)
        # csv.DictReader skips empty lines.
        # The line with empty strings will likely cause conversion errors or be missing keys.
        # 'INVALID' will cause conversion error.
        self.assertEqual(len(loaded_data), 2)
        self.assertEqual(loaded_data[0]['user_id'], 'user1')
        self.assertEqual(loaded_data[1]['user_id'], 'user2')

if __name__ == '__main__':
    unittest.main()
