from sample_data_generator import generate_sample_data
from data_processor import (
    clean_data,
    transform_data,
    calculate_top_selling_products,
    calculate_total_revenue_by_category,
    calculate_total_revenue_by_payment_method,
    calculate_average_purchase_value,
    calculate_purchase_frequency_per_user
)

if __name__ == "__main__":
    # --- 1. Data Generation ---
    # Generate a dataset for processing and analysis.
    # Increased to 100 records for more meaningful KPI results.
    raw_sample_data = generate_sample_data(num_records=100)

    # Optional: Print a few raw records to inspect (commented out for cleaner KPI output)
    # print("\n--- Raw Sample Data (First 2 records for brevity) ---")
    # for i, record in enumerate(raw_sample_data[:2]):
    #     print(f"Record {i+1}: {record}")

    # --- 2. Data Cleaning ---
    # Clean the raw data, handling missing values and data type issues.
    cleaned_data = clean_data(raw_sample_data)

    # Optional: Print a few cleaned records to inspect (commented out)
    # print("\n--- Cleaned Data (First 2 records for brevity if available) ---")
    # for i, record in enumerate(cleaned_data[:2]):
    #     print(f"Record {i+1}: {record}")

    # --- 3. Data Transformation ---
    # Transform cleaned data, e.g., by calculating total purchase value.
    transformed_data = transform_data(cleaned_data)

    # --- 4. Display Sample of Transformed Data ---
    print("\n--- Sample of Transformed Data (First 2 records) ---")
    # Displaying a couple of records to show the structure after transformation.
    for i, record in enumerate(transformed_data[:2]): # Print only first 2 for brevity
        print("-" * 30)
        print(f"  Timestamp: {record.get('timestamp')}")
        print(f"  User ID: {record.get('user_id')}")
        print(f"  Product ID: {record.get('product_id')}")
        print(f"  Product Name: {record.get('product_name')}")
        print(f"  Category: {record.get('product_category')}")
        print(f"  Price: ${record.get('product_price', 0):.2f}")
        print(f"  Quantity: {record.get('quantity')}")
        print(f"  Payment Method: {record.get('payment_method')}")
        print(f"  Total Purchase Value: ${record.get('total_purchase_value', 0):.2f}")
    print("-" * 30)

    # --- 5. Calculate and Print Key Performance Indicators (KPIs) ---
    if transformed_data: # Ensure there's data to process before calculations
        print("\n--- Key Performance Indicators (KPIs) ---")

        top_qty, top_rev = calculate_top_selling_products(transformed_data, n=5)
        print("\nTop 5 Selling Products (by Quantity):")
        for item, value in top_qty:
            print(f"  - {item}: {value} units")

        print("\nTop 5 Selling Products (by Revenue):")
        for item, value in top_rev:
            print(f"  - {item}: ${value:.2f}")

        revenue_by_category = calculate_total_revenue_by_category(transformed_data)
        print("\nTotal Revenue by Category:")
        for category, total_revenue in revenue_by_category.items():
            print(f"  - {category.capitalize()}: ${total_revenue:.2f}")

        revenue_by_payment = calculate_total_revenue_by_payment_method(transformed_data)
        print("\nTotal Revenue by Payment Method:")
        for method, total_revenue in revenue_by_payment.items():
            print(f"  - {method.replace('_', ' ').capitalize()}: ${total_revenue:.2f}")

        avg_purchase_value = calculate_average_purchase_value(transformed_data)
        print(f"\nAverage Purchase Value: ${avg_purchase_value:.2f}")

        avg_purchase_freq = calculate_purchase_frequency_per_user(transformed_data)
        print(f"\nAverage Purchase Frequency per User: {avg_purchase_freq:.2f} purchases")

    else:
        print("\nNo data available after cleaning and transformation to calculate KPIs.")
