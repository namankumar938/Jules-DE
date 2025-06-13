def create_purchase_record(timestamp, user_id, product_id, product_name, product_category, product_price, quantity, payment_method):
    """
    Creates a dictionary representing a purchase record.

    Args:
        timestamp: The timestamp of the purchase.
        user_id: The ID of the user who made the purchase.
        product_id: The ID of the product.
        product_name: The name of the product.
        product_category: The category of the product.
        product_price: The price of the product.
        quantity: The quantity of the product purchased.
        payment_method: The payment method used.

    Returns:
        A dictionary representing the purchase record.
    """
    return {
        "timestamp": timestamp,
        "user_id": user_id,
        "product_id": product_id,
        "product_name": product_name,
        "product_category": product_category,
        "product_price": product_price,
        "quantity": quantity,
        "payment_method": payment_method,
    }
