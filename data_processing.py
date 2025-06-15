ALLOWED_CATEGORIES = ["men", "women", "kids"]
ALLOWED_PAYMENT_METHODS = ["upi", "credit_card", "wallet"]

def clean_and_transform_data(raw_data: list[dict]) -> list[dict]:
    """
    Cleans and transforms raw purchase data.

    Args:
        raw_data: A list of dictionaries, where each dictionary represents
                  a raw purchase record. Assumes 'price' and 'quantity'
                  have been attempted to be converted to float/int respectively
                  during initial loading.

    Returns:
        A new list of dictionaries containing only valid and cleaned records.
    """
    cleaned_data = []
    if not isinstance(raw_data, list):
        # Or raise TypeError
        print("Error: Input must be a list of dictionaries.")
        return cleaned_data

    for record in raw_data:
        if not isinstance(record, dict):
            # Skip non-dictionary items in the list
            print(f"Skipping non-dictionary item: {record}")
            continue

        # 1. Filter out records with missing essential values
        if record.get('category') is None or record.get('category') == "":
            # print(f"Skipping record due to missing category: {record.get('order_id')}")
            continue
        if record.get('price') is None:
            # print(f"Skipping record due to missing price: {record.get('order_id')}")
            continue
        if record.get('quantity') is None:
            # print(f"Skipping record due to missing quantity: {record.get('order_id')}")
            continue

        # 2. Validate data types and values
        price = record.get('price')
        quantity = record.get('quantity')
        category = record.get('category')
        payment_method = record.get('payment_method')

        # Validate price
        try:
            price = float(price)
            if price <= 0:
                # print(f"Skipping record due to invalid price (not positive): {record.get('order_id')}")
                continue
        except (ValueError, TypeError):
            # print(f"Skipping record due to invalid price (conversion failed): {record.get('order_id')}")
            continue # Already handled by ingestion, but good for defense

        # Validate quantity
        try:
            quantity = int(quantity)
            if quantity <= 0:
                # print(f"Skipping record due to invalid quantity (not positive): {record.get('order_id')}")
                continue
        except (ValueError, TypeError):
            # print(f"Skipping record due to invalid quantity (conversion failed): {record.get('order_id')}")
            continue # Already handled by ingestion, but good for defense

        # Validate and normalize category
        if not isinstance(category, str):
            # print(f"Skipping record due to invalid category type: {record.get('order_id')}")
            continue
        normalized_category = category.lower()
        if normalized_category not in ALLOWED_CATEGORIES:
            # print(f"Skipping record due to invalid category value: {record.get('order_id')} - {category}")
            continue

        # Validate and normalize payment_method
        if not isinstance(payment_method, str):
            # print(f"Skipping record due to invalid payment_method type: {record.get('order_id')}")
            continue
        normalized_payment_method = payment_method.lower()
        if normalized_payment_method not in ALLOWED_PAYMENT_METHODS:
            # print(f"Skipping record due to invalid payment_method value: {record.get('order_id')} - {payment_method}")
            continue

        # If all checks pass, add a transformed copy to cleaned_data
        # Ensure we store the normalized and correctly typed values
        valid_record = record.copy()
        valid_record['price'] = price
        valid_record['quantity'] = quantity
        valid_record['category'] = normalized_category
        valid_record['payment_method'] = normalized_payment_method
        cleaned_data.append(valid_record)

    return cleaned_data

if __name__ == '__main__':
    # Example usage with the provided test data
    sample_raw_data = [
        {'event_timestamp': '2023-10-26T10:00:00Z', 'user_id': 'user123', 'order_id': 'order001', 'product_id': 'prodA', 'product_name': 'Laptop', 'category': 'electronics', 'price': 1200.00, 'quantity': 1, 'payment_method': 'credit_card'},
        {'event_timestamp': '2023-10-26T10:05:00Z', 'user_id': 'user456', 'order_id': 'order002', 'product_id': 'prodB', 'product_name': 'T-shirt', 'category': 'men', 'price': 25.00, 'quantity': 2, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:06:00Z', 'user_id': 'user123', 'order_id': 'order003', 'product_id': 'prodC', 'product_name': 'Coffee Maker', 'category': 'home', 'price': 75.50, 'quantity': 1, 'payment_method': 'wallet'},
        {'event_timestamp': '2023-10-26T10:10:00Z', 'user_id': 'user789', 'order_id': 'order004', 'product_id': 'prodD', 'product_name': 'Jeans', 'category': 'women', 'price': 0, 'quantity': 1, 'payment_method': 'credit_card'},
        {'event_timestamp': '2023-10-26T10:15:00Z', 'user_id': 'user456', 'order_id': 'order005', 'product_id': 'prodE', 'product_name': 'Toy Car', 'category': 'kids', 'price': 15.00, 'quantity': -3, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:15:00Z', 'user_id': 'user456', 'order_id': 'order006', 'product_id': 'prodF', 'product_name': 'Book', 'category': None, 'price': 10.00, 'quantity': 1, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:16:00Z', 'user_id': 'user123', 'order_id': 'order007', 'product_id': 'prodG', 'product_name': 'Sneakers', 'category': 'MEN', 'price': 90.00, 'quantity': 1, 'payment_method': 'Debit Card'},
        {'event_timestamp': '2023-10-26T10:17:00Z', 'user_id': 'user124', 'order_id': 'order008', 'product_id': 'prodH', 'product_name': 'Shorts', 'category': 'Women', 'price': 30.00, 'quantity': 2, 'payment_method': 'WALLET'}, # Valid
        {'event_timestamp': '2023-10-26T10:18:00Z', 'user_id': 'user125', 'order_id': 'order009', 'product_id': 'prodI', 'product_name': 'Cap', 'category': 'kids', 'price': 10.00, 'quantity': '1', 'payment_method': 'Upi'}, # quantity as string
        {'event_timestamp': '2023-10-26T10:19:00Z', 'user_id': 'user125', 'order_id': 'order010', 'product_id': 'prodJ', 'product_name': 'Socks', 'category': 'men', 'price': '10.invalid', 'quantity': 1, 'payment_method': 'upi'}, # invalid price string
        {'event_timestamp': '2023-10-26T10:20:00Z', 'user_id': 'user125', 'order_id': 'order011', 'product_id': 'prodK', 'product_name': 'Scarf', 'category': 'women', 'price': 10.0, 'quantity': 1, 'payment_method': 'unknown_pay'}, # invalid payment
        { 'user_id': 'user125', 'order_id': 'order012', 'product_id': 'prodL', 'product_name': 'Belt', 'category': '', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'}, # empty category
    ]

    print("Raw data:")
    for r in sample_raw_data:
        print(r)

    print("\nCleaned and transformed data:")
    cleaned_data_result = clean_and_transform_data(sample_raw_data)
    for r in cleaned_data_result:
        print(r)

    print(f"\nTotal raw records: {len(sample_raw_data)}")
    print(f"Total cleaned records: {len(cleaned_data_result)}")

    # Expected output for the provided example:
    # Should be 3 records if 'MEN' is handled and 'Debit Card' is invalid,
    # and 'Women' with 'WALLET' is valid, and 'kids' with 'Upi' and quantity '1' is valid.
    # 1. {'event_timestamp': '2023-10-26T10:05:00Z', ..., 'category': 'men', ..., 'payment_method': 'upi'}
    # 2. {'event_timestamp': '2023-10-26T10:17:00Z', ..., 'category': 'women', ..., 'payment_method': 'wallet'}
    # 3. {'event_timestamp': '2023-10-26T10:18:00Z', ..., 'category': 'kids', ..., 'payment_method': 'upi'}

    # Test with non-list input
    print("\nTesting with non-list input:")
    clean_and_transform_data("not a list")

    # Test with list containing non-dict item
    print("\nTesting with list containing non-dict item:")
    clean_and_transform_data([{"valid": "item"}, "not a dict"])

    # Test with empty list
    print("\nTesting with empty list input:")
    result_empty = clean_and_transform_data([])
    print(result_empty)
    print(f"Total cleaned records for empty input: {len(result_empty)}")

    # Test with list of only invalid data
    print("\nTesting with list of only invalid data:")
    invalid_only_data = [
        {'category': 'invalid', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'},
        {'category': 'men', 'price': -5.0, 'quantity': 1, 'payment_method': 'upi'},
    ]
    result_invalid_only = clean_and_transform_data(invalid_only_data)
    print(result_invalid_only)
    print(f"Total cleaned records for invalid only input: {len(result_invalid_only)}")


# --- KPI Calculation Functions ---
from collections import defaultdict

def calculate_top_selling_products(cleaned_data: list[dict], top_n: int = 5) -> list[dict]:
    """
    Calculates the top N selling products based on total quantity sold.

    Args:
        cleaned_data: A list of cleaned purchase data dictionaries.
        top_n: The number of top products to return. Defaults to 5.

    Returns:
        A list of dictionaries, each containing "product_id", "product_name",
        and "total_quantity_sold", sorted by total_quantity_sold in descending order.
        Returns an empty list if cleaned_data is empty.
    """
    if not cleaned_data:
        return []

    product_sales = defaultdict(lambda: {'total_quantity_sold': 0, 'product_name': ''})

    for record in cleaned_data:
        product_id = record['product_id']
        quantity = record['quantity']

        product_sales[product_id]['total_quantity_sold'] += quantity
        # Store product_name, if not already stored (picks the first one encountered)
        if not product_sales[product_id]['product_name']:
            product_sales[product_id]['product_name'] = record.get('product_name', 'Unknown Product')

    # Convert defaultdict to a list of dictionaries
    sorted_products = []
    for product_id, data in product_sales.items():
        sorted_products.append({
            "product_id": product_id,
            "product_name": data['product_name'],
            "total_quantity_sold": data['total_quantity_sold']
        })

    # Sort products by total_quantity_sold in descending order
    sorted_products.sort(key=lambda x: x['total_quantity_sold'], reverse=True)

    return sorted_products[:top_n]

def calculate_total_revenue_by_category(cleaned_data: list[dict]) -> dict:
    """
    Calculates the total revenue for each category.

    Args:
        cleaned_data: A list of cleaned purchase data dictionaries.

    Returns:
        A dictionary where keys are categories and values are their
        corresponding total revenues (float). All ALLOWED_CATEGORIES are
        guaranteed to be in the dictionary with a value of at least 0.0.
    """
    # Initialize revenues for all allowed categories to 0.0
    revenue_by_category = {category: 0.0 for category in ALLOWED_CATEGORIES}

    if not cleaned_data:
        return revenue_by_category

    for record in cleaned_data:
        category = record['category']
        price = record['price']
        quantity = record['quantity']

        # Ensure category is valid before accumulating (should be, due to cleaning)
        if category in revenue_by_category:
            revenue_by_category[category] += price * quantity
        # else:
            # This case should ideally not be hit if data is pre-cleaned
            # print(f"Warning: Record with unexpected category '{category}' found during revenue calculation.")

    return revenue_by_category


if __name__ == '__main__':
    # (Keep existing __main__ content for clean_and_transform_data)
    # ... (previous test code from line 71 to 154) ...

    print("\n--- Running clean_and_transform_data tests ---")
    sample_raw_data = [
        {'event_timestamp': '2023-10-26T10:00:00Z', 'user_id': 'user123', 'order_id': 'order001', 'product_id': 'prodA', 'product_name': 'Laptop', 'category': 'electronics', 'price': 1200.00, 'quantity': 1, 'payment_method': 'credit_card'},
        {'event_timestamp': '2023-10-26T10:05:00Z', 'user_id': 'user456', 'order_id': 'order002', 'product_id': 'prodB', 'product_name': 'T-shirt', 'category': 'men', 'price': 25.00, 'quantity': 2, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:06:00Z', 'user_id': 'user123', 'order_id': 'order003', 'product_id': 'prodC', 'product_name': 'Coffee Maker', 'category': 'home', 'price': 75.50, 'quantity': 1, 'payment_method': 'wallet'},
        {'event_timestamp': '2023-10-26T10:10:00Z', 'user_id': 'user789', 'order_id': 'order004', 'product_id': 'prodD', 'product_name': 'Jeans', 'category': 'women', 'price': 0, 'quantity': 1, 'payment_method': 'credit_card'},
        {'event_timestamp': '2023-10-26T10:15:00Z', 'user_id': 'user456', 'order_id': 'order005', 'product_id': 'prodE', 'product_name': 'Toy Car', 'category': 'kids', 'price': 15.00, 'quantity': -3, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:15:00Z', 'user_id': 'user456', 'order_id': 'order006', 'product_id': 'prodF', 'product_name': 'Book', 'category': None, 'price': 10.00, 'quantity': 1, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:16:00Z', 'user_id': 'user123', 'order_id': 'order007', 'product_id': 'prodG', 'product_name': 'Sneakers', 'category': 'MEN', 'price': 90.00, 'quantity': 1, 'payment_method': 'Debit Card'}, # Invalid PM
        {'event_timestamp': '2023-10-26T10:17:00Z', 'user_id': 'user124', 'order_id': 'order008', 'product_id': 'prodH', 'product_name': 'Shorts', 'category': 'Women', 'price': 30.00, 'quantity': 2, 'payment_method': 'WALLET'}, # Valid
        {'event_timestamp': '2023-10-26T10:18:00Z', 'user_id': 'user125', 'order_id': 'order009', 'product_id': 'prodI', 'product_name': 'Cap', 'category': 'kids', 'price': 10.00, 'quantity': '1', 'payment_method': 'Upi'}, # Valid (qty as str)
        {'event_timestamp': '2023-10-26T10:19:00Z', 'user_id': 'user125', 'order_id': 'order010', 'product_id': 'prodJ', 'product_name': 'Socks', 'category': 'men', 'price': '10.invalid', 'quantity': 1, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:20:00Z', 'user_id': 'user125', 'order_id': 'order011', 'product_id': 'prodK', 'product_name': 'Scarf', 'category': 'women', 'price': 10.0, 'quantity': 1, 'payment_method': 'unknown_pay'},
        { 'user_id': 'user125', 'order_id': 'order012', 'product_id': 'prodL', 'product_name': 'Belt', 'category': '', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'},
        # Add more data for robust KPI testing
        {'event_timestamp': '2023-10-27T11:00:00Z', 'user_id': 'user123', 'order_id': 'order013', 'product_id': 'prodB', 'product_name': 'T-shirt V-Neck', 'category': 'men', 'price': 28.00, 'quantity': 1, 'payment_method': 'credit_card'}, # another prodB
        {'event_timestamp': '2023-10-27T11:05:00Z', 'user_id': 'user456', 'order_id': 'order014', 'product_id': 'prodI', 'product_name': 'Baseball Cap', 'category': 'kids', 'price': 12.00, 'quantity': 2, 'payment_method': 'upi'}, # another prodI
        {'event_timestamp': '2023-10-27T11:10:00Z', 'user_id': 'user789', 'order_id': 'order015', 'product_id': 'prodH', 'product_name': 'Denim Shorts', 'category': 'women', 'price': 35.00, 'quantity': 1, 'payment_method': 'wallet'}, # another prodH
    ]

    print("Raw data for KPI tests:")
    # for r in sample_raw_data:
    #     print(r)

    cleaned_data_for_kpis = clean_and_transform_data(sample_raw_data)
    print("\nCleaned and transformed data for KPI tests:")
    for r in cleaned_data_for_kpis:
        print(r)
    print(f"Total cleaned records for KPI tests: {len(cleaned_data_for_kpis)}")
    # Expected:
    # order002 (men, prodB, qty 2, price 25)
    # order008 (women, prodH, qty 2, price 30)
    # order009 (kids, prodI, qty 1, price 10)
    # order013 (men, prodB, qty 1, price 28)
    # order014 (kids, prodI, qty 2, price 12)
    # order015 (women, prodH, qty 1, price 35)
    # Total 6 records

    print("\n--- Testing KPI Functions ---")

    # Test with empty data
    print("\nTesting KPIs with empty data:")
    empty_kpi_data = []
    top_products_empty = calculate_top_selling_products(empty_kpi_data)
    revenue_empty = calculate_total_revenue_by_category(empty_kpi_data)
    print(f"Top selling products (empty): {top_products_empty}")
    print(f"Total revenue by category (empty): {revenue_empty}")
    # Expected: [] and {'men': 0.0, 'women': 0.0, 'kids': 0.0}

    # Test with the cleaned data
    print("\nTesting KPIs with cleaned sample data:")
    top_products = calculate_top_selling_products(cleaned_data_for_kpis, top_n=3)
    revenue_by_cat = calculate_total_revenue_by_category(cleaned_data_for_kpis)

    print(f"\nTop 3 Selling Products:")
    for product in top_products:
        print(product)
    # Expected:
    # prodB (T-shirt): 2+1=3
    # prodI (Cap): 1+2=3
    # prodH (Shorts): 2+1=3
    # Order might vary for items with same quantity, which is fine.

    print(f"\nTotal Revenue by Category:")
    print(revenue_by_cat)
    # Expected:
    # men: (25*2) + (28*1) = 50 + 28 = 78
    # women: (30*2) + (35*1) = 60 + 35 = 95
    # kids: (10*1) + (12*2) = 10 + 24 = 34

    # Test top_n parameter
    top_1_product = calculate_top_selling_products(cleaned_data_for_kpis, top_n=1)
    print(f"\nTop 1 Selling Product:")
    for product in top_1_product:
        print(product)

    all_products_sorted = calculate_top_selling_products(cleaned_data_for_kpis, top_n=10) # top_n > actual products
    print(f"\nAll Selling Products (top_n=10):")
    for product in all_products_sorted:
        print(product)
    print(f"Number of unique products sold: {len(all_products_sorted)}")
    # Expected 3 unique products based on the cleaned data

    print("\n--- Previous clean_and_transform_data specific tests ---")
    # (This is to ensure the original tests for clean_and_transform_data still run if needed)
    # Re-run a small specific test for clean_and_transform_data if desired
    specific_test_data = [
        {'category': 'MEN', 'price': 90.00, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1', 'product_name': 'N1'},
        {'category': 'invalid', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p2', 'product_name': 'N2'},
    ]
    print(f"Specific test for clean_transform: {clean_and_transform_data(specific_test_data)}")
    # Expected: [{'category': 'men', 'price': 90.0, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1', 'product_name': 'N1'}]

    # Test with non-list input for clean_and_transform_data
    print("\nTesting clean_and_transform_data with non-list input:")
    clean_and_transform_data("not a list")

    # Test with list containing non-dict item for clean_and_transform_data
    print("\nTesting clean_and_transform_data with list containing non-dict item:")
    clean_and_transform_data([{"valid": "item"}, "not a dict"])

    # Test with empty list for clean_and_transform_data
    print("\nTesting clean_and_transform_data with empty list input:")
    result_empty_clean = clean_and_transform_data([])
    print(result_empty_clean)
    print(f"Total cleaned records for empty input (clean_and_transform_data): {len(result_empty_clean)}")

    # Test with list of only invalid data for clean_and_transform_data
    print("\nTesting clean_and_transform_data with list of only invalid data:")
    invalid_only_data_clean = [
        {'category': 'invalid', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'},
        {'category': 'men', 'price': -5.0, 'quantity': 1, 'payment_method': 'upi'},
    ]
    result_invalid_only_clean = clean_and_transform_data(invalid_only_data_clean)
    print(result_invalid_only_clean)
    print(f"Total cleaned records for invalid only input (clean_and_transform_data): {len(result_invalid_only_clean)}")


def calculate_preferred_payment_method_by_category(cleaned_data: list[dict]) -> dict:
    """
    Counts the occurrences of each payment_method for each category.

    Args:
        cleaned_data: A list of cleaned purchase data dictionaries.

    Returns:
        A nested dictionary: {category: {payment_method: count}}.
        Ensures all ALLOWED_CATEGORIES and ALLOWED_PAYMENT_METHODS are present
        with default counts of 0.
    """
    # Initialize with all categories and payment methods having 0 count
    preferred_methods = {
        cat: {pm: 0 for pm in ALLOWED_PAYMENT_METHODS} for cat in ALLOWED_CATEGORIES
    }

    if not cleaned_data:
        return preferred_methods

    for record in cleaned_data:
        category = record['category']
        payment_method = record['payment_method']

        if category in preferred_methods and payment_method in preferred_methods[category]:
            preferred_methods[category][payment_method] += 1
        # else:
            # This case should not be hit if data is pre-cleaned and constants are aligned
            # print(f"Warning: Record with unexpected category/payment_method found: {category}/{payment_method}")

    return preferred_methods

def calculate_purchase_frequency_per_user(cleaned_data: list[dict]) -> dict:
    """
    Counts how many items (rows in cleaned_data) are associated with each user_id.

    Args:
        cleaned_data: A list of cleaned purchase data dictionaries.

    Returns:
        A dictionary: {user_id: item_count}.
        Returns an empty dictionary if cleaned_data is empty.
    """
    if not cleaned_data:
        return {}

    user_frequency = defaultdict(int)
    for record in cleaned_data:
        user_id = record.get('user_id')
        if user_id: # Ensure user_id exists
            user_frequency[user_id] += 1
    return dict(user_frequency)

def calculate_average_order_value(cleaned_data: list[dict]) -> float:
    """
    Calculates the average value of orders.
    An order's value is the sum of (price * quantity) for all items with the same order_id.

    Args:
        cleaned_data: A list of cleaned purchase data dictionaries.

    Returns:
        A single float value representing the average order value.
        Returns 0.0 if there are no orders or cleaned_data is empty.
    """
    if not cleaned_data:
        return 0.0

    order_totals = defaultdict(float)
    for record in cleaned_data:
        order_id = record.get('order_id')
        price = record['price']
        quantity = record['quantity']
        if order_id: # Ensure order_id exists
            order_totals[order_id] += price * quantity

    if not order_totals:
        return 0.0

    average_value = sum(order_totals.values()) / len(order_totals)
    return average_value


if __name__ == '__main__':
    # (Existing __main__ content up to the previous KPI tests)
    # ...

    print("\n--- Running clean_and_transform_data tests ---")
    sample_raw_data = [
        # Base data from previous tests
        {'event_timestamp': '2023-10-26T10:00:00Z', 'user_id': 'user123', 'order_id': 'order001', 'product_id': 'prodA', 'product_name': 'Laptop', 'category': 'electronics', 'price': 1200.00, 'quantity': 1, 'payment_method': 'credit_card'},
        {'event_timestamp': '2023-10-26T10:05:00Z', 'user_id': 'user456', 'order_id': 'order002', 'product_id': 'prodB', 'product_name': 'T-shirt', 'category': 'men', 'price': 25.00, 'quantity': 2, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:06:00Z', 'user_id': 'user123', 'order_id': 'order003', 'product_id': 'prodC', 'product_name': 'Coffee Maker', 'category': 'home', 'price': 75.50, 'quantity': 1, 'payment_method': 'wallet'},
        {'event_timestamp': '2023-10-26T10:10:00Z', 'user_id': 'user789', 'order_id': 'order004', 'product_id': 'prodD', 'product_name': 'Jeans', 'category': 'women', 'price': 0, 'quantity': 1, 'payment_method': 'credit_card'},
        {'event_timestamp': '2023-10-26T10:15:00Z', 'user_id': 'user456', 'order_id': 'order005', 'product_id': 'prodE', 'product_name': 'Toy Car', 'category': 'kids', 'price': 15.00, 'quantity': -3, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:15:00Z', 'user_id': 'user456', 'order_id': 'order006', 'product_id': 'prodF', 'product_name': 'Book', 'category': None, 'price': 10.00, 'quantity': 1, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:16:00Z', 'user_id': 'user123', 'order_id': 'order007', 'product_id': 'prodG', 'product_name': 'Sneakers', 'category': 'MEN', 'price': 90.00, 'quantity': 1, 'payment_method': 'Debit Card'},
        {'event_timestamp': '2023-10-26T10:17:00Z', 'user_id': 'user124', 'order_id': 'order008', 'product_id': 'prodH', 'product_name': 'Shorts', 'category': 'Women', 'price': 30.00, 'quantity': 2, 'payment_method': 'WALLET'},
        {'event_timestamp': '2023-10-26T10:18:00Z', 'user_id': 'user125', 'order_id': 'order009', 'product_id': 'prodI', 'product_name': 'Cap', 'category': 'kids', 'price': 10.00, 'quantity': '1', 'payment_method': 'Upi'},
        {'event_timestamp': '2023-10-26T10:19:00Z', 'user_id': 'user125', 'order_id': 'order010', 'product_id': 'prodJ', 'product_name': 'Socks', 'category': 'men', 'price': '10.invalid', 'quantity': 1, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-26T10:20:00Z', 'user_id': 'user125', 'order_id': 'order011', 'product_id': 'prodK', 'product_name': 'Scarf', 'category': 'women', 'price': 10.0, 'quantity': 1, 'payment_method': 'unknown_pay'},
        {'user_id': 'user125', 'order_id': 'order012', 'product_id': 'prodL', 'product_name': 'Belt', 'category': '', 'price': 10.0, 'quantity': 1, 'payment_method': 'upi'},
        # Data for KPI testing (some are duplicates from previous tests, ensure they are handled by clean_and_transform_data)
        {'event_timestamp': '2023-10-27T11:00:00Z', 'user_id': 'user123', 'order_id': 'order013', 'product_id': 'prodB', 'product_name': 'T-shirt V-Neck', 'category': 'men', 'price': 28.00, 'quantity': 1, 'payment_method': 'credit_card'},
        {'event_timestamp': '2023-10-27T11:05:00Z', 'user_id': 'user456', 'order_id': 'order014', 'product_id': 'prodI', 'product_name': 'Baseball Cap', 'category': 'kids', 'price': 12.00, 'quantity': 2, 'payment_method': 'upi'},
        {'event_timestamp': '2023-10-27T11:10:00Z', 'user_id': 'user789', 'order_id': 'order015', 'product_id': 'prodH', 'product_name': 'Denim Shorts', 'category': 'women', 'price': 35.00, 'quantity': 1, 'payment_method': 'wallet'},
        # New data for AOV and frequency testing
        {'event_timestamp': '2023-10-28T12:00:00Z', 'user_id': 'user123', 'order_id': 'order016', 'product_id': 'prodX', 'product_name': 'Jacket', 'category': 'men', 'price': 120.00, 'quantity': 1, 'payment_method': 'credit_card'}, # user123, 2nd order
        {'event_timestamp': '2023-10-28T12:05:00Z', 'user_id': 'user124', 'order_id': 'order008', 'product_id': 'prodY', 'product_name': 'Scarflette', 'category': 'women', 'price': 20.00, 'quantity': 1, 'payment_method': 'wallet'}, # user124, 1st order (order008), 2nd item for order008
    ]

    print("Raw data for all KPI tests:")
    # for r in sample_raw_data:
    #     print(r)

    cleaned_data_all_kpis = clean_and_transform_data(sample_raw_data)
    print("\nCleaned and transformed data for all KPI tests:")
    for r in cleaned_data_all_kpis:
        print(r)
    print(f"Total cleaned records for all KPI tests: {len(cleaned_data_all_kpis)}")
    # Expected records from previous test: 6
    # New valid records:
    # order016 (user123, men, prodX, qty 1, price 120, credit_card)
    # order008 item 2 (user124, women, prodY, qty 1, price 20, wallet) - this is part of an existing order
    # Total = 6 + 1 new record from order016 + 1 new item for order008 = 8 records

    print("\n--- Testing Previous KPI Functions (Top Selling, Revenue by Cat) ---")
    # Test with empty data
    print("\nTesting Previous KPIs with empty data:")
    empty_kpi_data = []
    top_products_empty = calculate_top_selling_products(empty_kpi_data)
    revenue_empty = calculate_total_revenue_by_category(empty_kpi_data)
    print(f"Top selling products (empty): {top_products_empty}") # Expected: []
    print(f"Total revenue by category (empty): {revenue_empty}") # Expected: {'men': 0.0, 'women': 0.0, 'kids': 0.0}

    print("\nTesting Previous KPIs with all cleaned sample data:")
    top_3_products_all = calculate_top_selling_products(cleaned_data_all_kpis, top_n=3)
    revenue_by_cat_all = calculate_total_revenue_by_category(cleaned_data_all_kpis)
    print(f"Top 3 Selling Products (all data):")
    for p in top_3_products_all: print(p)
    print(f"Total Revenue by Category (all data): {revenue_by_cat_all}")
    # Expected revenue:
    # men: 78 (prev) + 120 (order016) = 198
    # women: 95 (prev) + 20 (order008 item 2) = 115
    # kids: 34 (prev) = 34
    # Expected top products (qty): prodB (3), prodH (3), prodI (3), prodX (1) -> top 3 are B, H, I

    print("\n--- Testing New KPI Functions ---")

    # Test with empty data for new functions
    print("\nTesting New KPIs with empty data:")
    preferred_pm_empty = calculate_preferred_payment_method_by_category(empty_kpi_data)
    purchase_freq_empty = calculate_purchase_frequency_per_user(empty_kpi_data)
    aov_empty = calculate_average_order_value(empty_kpi_data)
    print(f"Preferred Payment Method (empty): {preferred_pm_empty}")
    # Expected: {'men': {'upi': 0, ...}, 'women': {'upi': 0,...}, 'kids': {'upi': 0,...}}
    print(f"Purchase Frequency (empty): {purchase_freq_empty}") # Expected: {}
    print(f"Average Order Value (empty): {aov_empty}") # Expected: 0.0

    # Test new functions with the full cleaned dataset
    print("\nTesting New KPIs with all cleaned sample data:")
    preferred_pm = calculate_preferred_payment_method_by_category(cleaned_data_all_kpis)
    purchase_freq = calculate_purchase_frequency_per_user(cleaned_data_all_kpis)
    aov = calculate_average_order_value(cleaned_data_all_kpis)

    print(f"\nPreferred Payment Method by Category:")
    for cat, pms in preferred_pm.items():
        print(f"  {cat}: {pms}")
    # Expected based on cleaned_data_all_kpis (8 records):
    # men: user456,order002,prodB,upi | user123,order013,prodB,credit_card | user123,order016,prodX,credit_card
    #   => men: {'upi': 1, 'credit_card': 2, 'wallet': 0}
    # women: user124,order008,prodH,wallet | user789,order015,prodH,wallet | user124,order008,prodY,wallet
    #   => women: {'upi': 0, 'credit_card': 0, 'wallet': 3}
    # kids: user125,order009,prodI,upi | user456,order014,prodI,upi
    #   => kids: {'upi': 2, 'credit_card': 0, 'wallet': 0}

    print(f"\nPurchase Frequency Per User (item count):")
    print(purchase_freq)
    # Expected:
    # user456: order002 (1 item), order014 (1 item) => 2 items
    # user124: order008 (prodH), order008 (prodY) => 2 items
    # user125: order009 (1 item) => 1 item
    # user123: order013 (1 item), order016 (1 item) => 2 items
    # user789: order015 (1 item) => 1 item
    # {'user456': 2, 'user124': 2, 'user125': 1, 'user123': 2, 'user789': 1}

    print(f"\nAverage Order Value:")
    print(aov)
    # Orders and their values:
    # order002: 25*2 = 50
    # order008: (30*2) + (20*1) = 60 + 20 = 80  (prodH + prodY)
    # order009: 10*1 = 10
    # order013: 28*1 = 28
    # order014: 12*2 = 24
    # order015: 35*1 = 35
    # order016: 120*1 = 120
    # Total unique orders: 7
    # Sum of order values: 50 + 80 + 10 + 28 + 24 + 35 + 120 = 347
    # Average Order Value: 347 / 7 = 49.5714...

    print("\n--- Previous clean_and_transform_data specific tests (condensed) ---")
    specific_test_data = [{'category': 'MEN', 'price': 90.00, 'quantity': 1, 'payment_method': 'upi', 'product_id': 'p1', 'product_name': 'N1'}]
    print(f"Specific test for clean_transform: {clean_and_transform_data(specific_test_data)}")
    print(f"Test clean_transform with empty list: {clean_and_transform_data([])}")
