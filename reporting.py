from data_processing import (
    calculate_top_selling_products,
    calculate_total_revenue_by_category,
    calculate_preferred_payment_method_by_category,
    calculate_purchase_frequency_per_user,
    calculate_average_order_value,
    ALLOWED_CATEGORIES,
    ALLOWED_PAYMENT_METHODS
)

# --- Formatting and Printing Functions ---

def format_and_print_top_selling_products(top_products_data: list[dict]):
    """Prints formatted top selling products."""
    print("\n--- Top Selling Products ---")
    if not top_products_data:
        print("No products sold or data available.")
        return
    print(f"{'Product ID':<15} | {'Product Name':<25} | {'Total Quantity Sold':<20}")
    print("-" * 70)
    for product in top_products_data:
        print(f"{product.get('product_id', 'N/A'):<15} | {product.get('product_name', 'N/A'):<25} | {product.get('total_quantity_sold', 0):<20}")

def format_and_print_total_revenue_by_category(revenue_by_category_data: dict):
    """Prints formatted total revenue by category."""
    print("\n--- Total Revenue by Category ---")
    if not revenue_by_category_data:
        # This message can still be useful if the input dict is truly empty from a source
        # that doesn't guarantee the full structure like our calculate_ functions do.
        print("Note: Revenue data was empty or not fully structured; displaying default template.")
    print(f"{'Category':<15} | {'Total Revenue':<15}")
    print("-" * 35)
    for category in ALLOWED_CATEGORIES: # Ensure consistent order
        revenue = revenue_by_category_data.get(category, 0.0)
        print(f"{category.capitalize():<15} | ${revenue:,.2f}{'':<13}")

def format_and_print_preferred_payment_methods(preferred_payment_data: dict):
    """Prints formatted preferred payment methods by category."""
    print("\n--- Preferred Payment Methods by Category ---")
    if not preferred_payment_data:
        # Similar note as above
        print("Note: Payment method data was empty or not fully structured; displaying default template.")

    for category in ALLOWED_CATEGORIES: # Ensure consistent order for categories
        print(f"\nCategory: {category.capitalize()}")
        print(f"  {'Payment Method':<15} | {'Count':<10}")
        print("  " + "-" * 30)
        category_data = preferred_payment_data.get(category, {})
        if not category_data: # Should not happen if initialized correctly
            for pm in ALLOWED_PAYMENT_METHODS:
                 print(f"  {pm.replace('_', ' ').capitalize():<15} | {0:<10}")
            continue
        for pm in ALLOWED_PAYMENT_METHODS: # Ensure consistent order for payment methods
            count = category_data.get(pm, 0)
            print(f"  {pm.replace('_', ' ').capitalize():<15} | {count:<10}")

def format_and_print_purchase_frequency(purchase_frequency_data: dict, top_n_users: int = 5):
    """Prints formatted purchase frequency for top N users."""
    print(f"\n--- Purchase Frequency (Top {top_n_users} Users by Item Count) ---")
    if not purchase_frequency_data:
        print("No user purchase data available.")
        return

    # Sort users by purchase frequency (item count) in descending order
    sorted_users = sorted(purchase_frequency_data.items(), key=lambda item: item[1], reverse=True)

    print(f"{'User ID':<15} | {'Items Purchased':<15}")
    print("-" * 35)

    if not sorted_users: # Should be caught by the initial check, but good for safety
        print("No user purchase data after sorting.")
        return

    for i, (user_id, count) in enumerate(sorted_users):
        if i < top_n_users:
            print(f"{user_id:<15} | {count:<15}")
        else:
            break
    if len(sorted_users) < top_n_users and len(sorted_users) > 0:
        print(f"(All users shown as total users ({len(sorted_users)}) is less than top_n ({top_n_users}))")
    elif not sorted_users: # This case should ideally be caught by the outer if
         print("No user purchase data.")


def format_and_print_average_order_value(aov_data: float):
    """Prints formatted average order value."""
    print("\n--- Average Order Value ---")
    print(f"AOV: ${aov_data:,.2f}")

# --- Main Report Generation Function ---

def generate_kpi_report(cleaned_data: list[dict]):
    """
    Calculates all KPIs and prints them in a formatted report.
    """
    print("===================================")
    print("        Sales KPI Report           ")
    print("===================================")

    if not cleaned_data:
        print("\nNo data available to generate the report after cleaning.")
        # Print empty states for all KPIs
        format_and_print_top_selling_products([])
        format_and_print_total_revenue_by_category({cat: 0.0 for cat in ALLOWED_CATEGORIES})
        format_and_print_preferred_payment_methods(
            {cat: {pm: 0 for pm in ALLOWED_PAYMENT_METHODS} for cat in ALLOWED_CATEGORIES}
        )
        format_and_print_purchase_frequency({})
        format_and_print_average_order_value(0.0)
        print("\n--- End of Report ---")
        return

    # 1. Top Selling Products
    top_products = calculate_top_selling_products(cleaned_data, top_n=5)
    format_and_print_top_selling_products(top_products)

    # 2. Total Revenue by Category
    revenue_by_category = calculate_total_revenue_by_category(cleaned_data)
    format_and_print_total_revenue_by_category(revenue_by_category)

    # 3. Preferred Payment Method by Category
    preferred_payment = calculate_preferred_payment_method_by_category(cleaned_data)
    format_and_print_preferred_payment_methods(preferred_payment)

    # 4. Purchase Frequency Per User
    purchase_frequency = calculate_purchase_frequency_per_user(cleaned_data)
    format_and_print_purchase_frequency(purchase_frequency, top_n_users=5)

    # 5. Average Order Value
    aov = calculate_average_order_value(cleaned_data)
    format_and_print_average_order_value(aov)

    print("\n--- End of Report ---")


if __name__ == '__main__':
    from data_ingestion import load_purchase_data_from_csv
    from data_processing import clean_and_transform_data

    # Define the sample data directly for simplicity in this example,
    # or load from sample_purchases.csv if it's guaranteed to be in the right place
    # For robustness in testing, using the CSV is better.

    csv_file_path = 'sample_purchases.csv'
    print(f"Attempting to load data from: {csv_file_path}")

    raw_data = load_purchase_data_from_csv(csv_file_path)

    if not raw_data:
        print(f"No raw data loaded from {csv_file_path}. Make sure the file exists and is not empty.")
    else:
        print(f"Successfully loaded {len(raw_data)} raw records.")

        # Use the extended sample_raw_data from data_processing.py for more comprehensive test
        # This is a bit of a hack for the test; ideally, sample_purchases.csv would be comprehensive
        # For now, let's use the more comprehensive data used in data_processing.py's main block
        # to ensure the report shows meaningful numbers similar to what we tested there.

        # Re-defining a comprehensive dataset for reporting.py's __main__
        # This ensures this script is runnable and shows a good report independently.
        # This data matches the one used in the final tests of data_processing.py
        comprehensive_raw_data_for_report = [
            {'event_timestamp': '2023-10-26T10:05:00Z', 'user_id': 'user456', 'order_id': 'order002', 'product_id': 'prodB', 'product_name': 'T-shirt', 'category': 'men', 'price': 25.00, 'quantity': 2, 'payment_method': 'upi'},
            {'event_timestamp': '2023-10-26T10:17:00Z', 'user_id': 'user124', 'order_id': 'order008', 'product_id': 'prodH', 'product_name': 'Shorts', 'category': 'Women', 'price': 30.00, 'quantity': 2, 'payment_method': 'WALLET'},
            {'event_timestamp': '2023-10-26T10:18:00Z', 'user_id': 'user125', 'order_id': 'order009', 'product_id': 'prodI', 'product_name': 'Cap', 'category': 'kids', 'price': 10.00, 'quantity': '1', 'payment_method': 'Upi'},
            {'event_timestamp': '2023-10-27T11:00:00Z', 'user_id': 'user123', 'order_id': 'order013', 'product_id': 'prodB', 'product_name': 'T-shirt V-Neck', 'category': 'men', 'price': 28.00, 'quantity': 1, 'payment_method': 'credit_card'},
            {'event_timestamp': '2023-10-27T11:05:00Z', 'user_id': 'user456', 'order_id': 'order014', 'product_id': 'prodI', 'product_name': 'Baseball Cap', 'category': 'kids', 'price': 12.00, 'quantity': 2, 'payment_method': 'upi'},
            {'event_timestamp': '2023-10-27T11:10:00Z', 'user_id': 'user789', 'order_id': 'order015', 'product_id': 'prodH', 'product_name': 'Denim Shorts', 'category': 'women', 'price': 35.00, 'quantity': 1, 'payment_method': 'wallet'},
            {'event_timestamp': '2023-10-28T12:00:00Z', 'user_id': 'user123', 'order_id': 'order016', 'product_id': 'prodX', 'product_name': 'Jacket', 'category': 'men', 'price': 120.00, 'quantity': 1, 'payment_method': 'credit_card'},
            {'event_timestamp': '2023-10-28T12:05:00Z', 'user_id': 'user124', 'order_id': 'order008', 'product_id': 'prodY', 'product_name': 'Scarflette', 'category': 'women', 'price': 20.00, 'quantity': 1, 'payment_method': 'wallet'},
            # Add some invalid data to ensure cleaning works
            {'event_timestamp': '2023-10-29T10:00:00Z', 'user_id': 'user999', 'order_id': 'order999', 'product_id': 'prodZ', 'product_name': 'Invalid Cat', 'category': 'unknown', 'price': 10.00, 'quantity': 1, 'payment_method': 'upi'},
            {'event_timestamp': '2023-10-29T10:05:00Z', 'user_id': 'user998', 'order_id': 'order998', 'product_id': 'prodW', 'product_name': 'Invalid Price', 'category': 'men', 'price': -5.00, 'quantity': 1, 'payment_method': 'wallet'},
        ]
        print(f"Using a comprehensive inline dataset of {len(comprehensive_raw_data_for_report)} records for report generation.")

        cleaned_purchase_data = clean_and_transform_data(comprehensive_raw_data_for_report)

        if not cleaned_purchase_data:
            print("No data available after cleaning. Report will be empty.")
        else:
            print(f"Successfully cleaned data, {len(cleaned_purchase_data)} records remaining for reporting.")

        generate_kpi_report(cleaned_purchase_data)
