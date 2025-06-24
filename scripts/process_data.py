import pandas as pd

def load_and_clean_data(filepath="data/sample_purchases.csv"):
    """Loads, validates, and cleans the purchase data."""
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        print(f"Error: The file {filepath} was not found.")
        return None

    print("--- Initial Data Info ---")
    print(f"Shape: {df.shape}")
    print("\nData Types:")
    print(df.dtypes)
    print("\nFirst 5 rows:")
    print(df.head())

    # --- Missing Value Check ---
    print("\n--- Missing Values ---")
    missing_values = df.isnull().sum()
    print(missing_values[missing_values > 0])
    if df.isnull().any().any():
        print("\nHandling missing values (strategy: simple drop for now)...")
        # For simplicity, drop rows with any missing values.
        # In a real scenario, you might impute or investigate further.
        # df.dropna(inplace=True) # Example: df.dropna(subset=['user_id', 'product_id'], inplace=True)
        # For now, we'll just report as our generated data should be clean.
        print("Missing values found, but no action taken in this step for generated data.")


    # --- Data Type Conversion ---
    print("\n--- Data Type Conversion ---")
    if 'timestamp' in df.columns:
        print("Converting 'timestamp' column to datetime...")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        print("'timestamp' column type after conversion:", df['timestamp'].dtype)
    else:
        print("Warning: 'timestamp' column not found.")

    # Placeholder for more advanced cleaning or validation
    # e.g., validating category values, checking price/quantity ranges

    print("\n--- Cleaned Data Info ---")
    print(f"Shape after initial cleaning: {df.shape}")
    # print("\nFirst 5 rows of cleaned data:") # Moved to after transformations
    # print(df.head())

    return df

def transform_data(df):
    """Applies transformations to the DataFrame."""
    if df is None:
        return None

    print("\n--- Data Transformation ---")

    # Calculate total purchase amount
    if 'price' in df.columns and 'quantity' in df.columns:
        print("Calculating 'total_purchase_amount'...")
        df['total_purchase_amount'] = df['price'] * df['quantity']
        print("'total_purchase_amount' column added.")
    else:
        print("Warning: 'price' or 'quantity' column not found. Cannot calculate 'total_purchase_amount'.")

    # Future transformations can be added here
    # e.g., feature engineering, data normalization/scaling

    print("\n--- Transformed Data Info ---")
    print(f"Shape after transformation: {df.shape}")
    # print("\nFirst 5 rows of transformed data:") # Reduce verbose output for KPI step
    # print(df.head())

    return df

def calculate_kpis(df, top_n_products=5):
    """Calculates and prints various KPIs from the transformed data."""
    if df is None or 'total_purchase_amount' not in df.columns:
        print("Error: Transformed data with 'total_purchase_amount' is required for KPI calculation.")
        return None

    print("\n--- KPI Calculation ---")

    # 1. Total Revenue
    total_revenue = df['total_purchase_amount'].sum()
    print(f"\n1. Total Revenue: ${total_revenue:,.2f}")

    # 2. Total Revenue by Product Category
    revenue_by_category = df.groupby('category')['total_purchase_amount'].sum().sort_values(ascending=False)
    print("\n2. Total Revenue by Product Category:")
    print(revenue_by_category)

    # 3. Top N Selling Products (by total revenue)
    # For simplicity, assuming product_name is unique enough for this aggregation.
    # In a real scenario, product_id would be better.
    top_selling_products = df.groupby('product_name')['total_purchase_amount'].sum().nlargest(top_n_products)
    print(f"\n3. Top {top_n_products} Selling Products (by Revenue):")
    print(top_selling_products)

    # 4. Preferred Payment Methods (by transaction count)
    preferred_payment_methods = df['payment_method'].value_counts()
    print("\n4. Preferred Payment Methods (by Transaction Count):")
    print(preferred_payment_methods)

    # Store KPIs in a dictionary for potential later use (e.g., saving to a file or returning)
    # Convert pandas Series to dictionary for easier JSON serialization later
    kpis = {
        "total_revenue": total_revenue,
        "revenue_by_category": revenue_by_category.to_dict(),
        "top_selling_products": top_selling_products.to_dict(),
        "preferred_payment_methods": preferred_payment_methods.to_dict()
    }
    return kpis

def save_outputs(df, kpis, processed_df_path="data/processed_purchases.csv", kpis_path="data/kpis.json"):
    """Saves the processed DataFrame and KPIs to files."""
    if df is not None:
        try:
            df.to_csv(processed_df_path, index=False)
            print(f"\nProcessed data saved to {processed_df_path}")
        except Exception as e:
            print(f"Error saving processed data to CSV: {e}")

    if kpis is not None:
        import json
        try:
            with open(kpis_path, 'w') as f:
                json.dump(kpis, f, indent=4)
            print(f"KPIs saved to {kpis_path}")
        except Exception as e:
            print(f"Error saving KPIs to JSON: {e}")


if __name__ == "__main__":
    # Define file paths
    raw_data_filepath = "data/sample_purchases.csv"
    processed_output_filepath = "data/processed_purchases.csv"
    kpis_output_filepath = "data/kpis.json"

    # Main pipeline execution
    cleaned_df = load_and_clean_data(filepath=raw_data_filepath)
    transformed_df = transform_data(cleaned_df)

    if transformed_df is not None:
        print("\nData ingestion, cleaning, and transformation complete.")
        kpis_calculated = calculate_kpis(transformed_df, top_n_products=5)

        if kpis_calculated:
            print("\nKPI calculation complete.")
            save_outputs(transformed_df, kpis_calculated,
                         processed_df_path=processed_output_filepath,
                         kpis_path=kpis_output_filepath)
            print("\nPipeline processing finished. Outputs saved.")
        else:
            print("\nKPI calculation failed. Outputs not saved.")
    else:
        print("\nData processing failed. Outputs not saved.")
