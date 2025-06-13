import datetime
import random
from data_structures import create_purchase_record

def generate_sample_data(num_records=100):
    """
    Generates a list of sample purchase records.

    Args:
        num_records: The number of records to generate.

    Returns:
        A list of purchase record dictionaries.
    """
    product_names = ["Laptop", "Mouse", "Keyboard", "Monitor", "Webcam", "Headphones", "Desk Chair", "Smartphone", "Tablet", "Charger"]
    categories = ["men", "women", "kids"]
    payment_methods = ["upi", "credit card", "wallet"]

    purchase_records = []

    current_time = datetime.datetime.now()

    for _ in range(num_records):
        # Generate random timestamp within the last year
        days_offset = random.randint(0, 365)
        hours_offset = random.randint(0, 23)
        minutes_offset = random.randint(0, 59)
        seconds_offset = random.randint(0, 59)

        timestamp = current_time - datetime.timedelta(days=days_offset, hours=hours_offset, minutes=minutes_offset, seconds=seconds_offset)

        user_id = random.randint(1000, 9999)
        product_id = random.randint(100, 999)
        product_name = random.choice(product_names)
        product_category = random.choice(categories)
        product_price = round(random.uniform(5.0, 200.0), 2)
        quantity = random.randint(1, 5)
        payment_method = random.choice(payment_methods)

        purchase_record = create_purchase_record(
            timestamp=timestamp,
            user_id=user_id,
            product_id=product_id,
            product_name=product_name,
            product_category=product_category,
            product_price=product_price,
            quantity=quantity,
            payment_method=payment_method
        )
        purchase_records.append(purchase_record)

    return purchase_records
