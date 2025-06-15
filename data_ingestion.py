import csv

def load_purchase_data_from_csv(file_path: str) -> list[dict]:
    """
    Loads purchase data from a CSV file.

    Args:
        file_path: The path to the CSV file.

    Returns:
        A list of dictionaries, where each dictionary represents a row
        from the CSV file. Rows with conversion errors for 'price' or
        'quantity' are skipped.
    """
    data = []
    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                row['price'] = float(row['price'])
                row['quantity'] = int(row['quantity'])
                data.append(row)
            except ValueError:
                # Skip row if price or quantity conversion fails
                # In a real application, you might want to log this error
                print(f"Skipping row due to data conversion error: {row}")
                continue
    return data

if __name__ == '__main__':
    # Example usage:
    # Make sure 'sample_purchases.csv' is in the same directory or provide the correct path.
    sample_file = 'sample_purchases.csv'
    purchase_data = load_purchase_data_from_csv(sample_file)
    for record in purchase_data:
        print(record)

    print(f"\nTotal records loaded: {len(purchase_data)}")
    # Expected: 6 records (1 skipped due to "PRICE_ERROR")
    # Check if the known bad row was skipped by inspecting the output or len(purchase_data)

    # To test with a non-existent file:
    # try:
    #     load_purchase_data_from_csv("non_existent_file.csv")
    # except FileNotFoundError as e:
    #     print(f"Error: {e}")
