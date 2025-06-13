import datetime

def clean_data(raw_data: list[dict]) -> list[dict]:
    """
    Cleans the raw purchase data.

    Args:
        raw_data: A list of raw purchase record dictionaries.

    Returns:
        A list of cleaned purchase record dictionaries.
    """
    cleaned_data = []
    for record in raw_data:
        # Missing Value Handling
        if 'product_price' not in record or record['product_price'] is None or \
           'quantity' not in record or record['quantity'] is None:
            print(f"Warning: Missing essential fields (product_price or quantity) in record: {record}. Skipping.")
            continue

        # Data Type Conversion
        try:
            record['product_price'] = float(record['product_price'])
        except ValueError:
            print(f"Warning: Invalid product_price format in record: {record}. Skipping.")
            continue

        try:
            record['quantity'] = int(record['quantity'])
        except ValueError:
            print(f"Warning: Invalid quantity format in record: {record}. Skipping.")
            continue

        # Assuming timestamp is already a datetime object as per our generator.
        # If it could be a string, conversion would be needed here:
        # if isinstance(record['timestamp'], str):
        #     try:
        #         record['timestamp'] = datetime.datetime.fromisoformat(record['timestamp']) # Or appropriate format
        #     except ValueError:
        #         print(f"Warning: Invalid timestamp format in record: {record}. Skipping.")
        #         continue
        # elif not isinstance(record['timestamp'], datetime.datetime):
        #     print(f"Warning: Invalid timestamp type in record: {record}. Skipping.")
        #     continue


        cleaned_data.append(record)
    return cleaned_data

def transform_data(cleaned_data: list[dict]) -> list[dict]:
    """
    Transforms cleaned purchase data by adding new calculated fields.

    Args:
        cleaned_data: A list of cleaned purchase record dictionaries.

    Returns:
        A list of transformed purchase record dictionaries.
    """
    transformed_data = []
    for record in cleaned_data:
        try:
            record['total_purchase_value'] = record['product_price'] * record['quantity']
            transformed_data.append(record)
        except KeyError as e:
            print(f"Error during transformation: Missing key {e} in record {record}. Skipping.")
        except TypeError as e:
            print(f"Error during transformation: Type error {e} for record {record}. Skipping.")

    return transformed_data

from collections import Counter, defaultdict

def calculate_top_selling_products(transformed_data: list[dict], n: int = 5) -> tuple[list, list]:
    """
    Calculates top N selling products by quantity and revenue.

    Args:
        transformed_data: List of transformed purchase records.
        n: Number of top products to return.

    Returns:
        A tuple containing two lists:
        - Top N products by quantity sold: [(product_name, total_quantity), ...]
        - Top N products by revenue generated: [(product_name, total_revenue), ...]
    """
    product_quantity = defaultdict(int)
    product_revenue = defaultdict(float)

    for record in transformed_data:
        name = record.get('product_name')
        quantity = record.get('quantity')
        revenue = record.get('total_purchase_value')

        if name and quantity is not None:
            product_quantity[name] += quantity
        if name and revenue is not None:
            product_revenue[name] += revenue

    top_by_quantity = Counter(product_quantity).most_common(n)
    top_by_revenue = Counter(product_revenue).most_common(n)

    return top_by_quantity, top_by_revenue

def calculate_total_revenue_by_category(transformed_data: list[dict]) -> dict:
    """
    Calculates total revenue for each product category.

    Args:
        transformed_data: List of transformed purchase records.

    Returns:
        A dictionary like {'category': total_revenue, ...}
    """
    category_revenue = defaultdict(float)
    for record in transformed_data:
        category = record.get('product_category')
        revenue = record.get('total_purchase_value')
        if category and revenue is not None:
            category_revenue[category] += revenue
    return dict(category_revenue)

def calculate_total_revenue_by_payment_method(transformed_data: list[dict]) -> dict:
    """
    Calculates total revenue for each payment method.

    Args:
        transformed_data: List of transformed purchase records.

    Returns:
        A dictionary like {'payment_method': total_revenue, ...}
    """
    payment_method_revenue = defaultdict(float)
    for record in transformed_data:
        method = record.get('payment_method')
        revenue = record.get('total_purchase_value')
        if method and revenue is not None:
            payment_method_revenue[method] += revenue
    return dict(payment_method_revenue)

def calculate_average_purchase_value(transformed_data: list[dict]) -> float:
    """
    Calculates the overall average purchase value.

    Args:
        transformed_data: List of transformed purchase records.

    Returns:
        The average purchase value as a float, or 0.0 if no data.
    """
    total_revenue = 0.0
    num_transactions = 0
    for record in transformed_data:
        revenue = record.get('total_purchase_value')
        if revenue is not None:
            total_revenue += revenue
            num_transactions += 1

    return total_revenue / num_transactions if num_transactions > 0 else 0.0

def calculate_purchase_frequency_per_user(transformed_data: list[dict]) -> float:
    """
    Calculates the average purchase frequency per user.

    Args:
        transformed_data: List of transformed purchase records.

    Returns:
        The average number of purchases per user as a float, or 0.0 if no data.
    """
    user_purchases = defaultdict(int)
    for record in transformed_data:
        user_id = record.get('user_id')
        if user_id is not None:
            user_purchases[user_id] += 1

    if not user_purchases:
        return 0.0

    total_purchases = sum(user_purchases.values())
    num_users = len(user_purchases)

    return total_purchases / num_users if num_users > 0 else 0.0
