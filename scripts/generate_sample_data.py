import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Define categories and payment methods
PRODUCT_CATEGORIES = ["men", "women", "kids"]
PAYMENT_METHODS = ["upi", "credit_card", "wallet"]

# Define product names (simple examples)
PRODUCT_NAMES = {
    "men": ["Men's T-Shirt", "Men's Jeans", "Men's Watch", "Men's Shoes", "Men's Formal Shirt"],
    "women": ["Women's Dress", "Women's Handbag", "Women's Sandals", "Women's Scarf", "Women's Earrings"],
    "kids": ["Kid's Toy Car", "Kid's Story Book", "Kid's Building Blocks", "Kid's Tricycle", "Kid's Onesie"]
}

def generate_purchase_data(num_records=1000):
    """Generates sample purchase data."""
    data = []
    start_date = datetime(2023, 1, 1)

    for i in range(num_records):
        timestamp = start_date + timedelta(days=random.randint(0, 364),
                                           hours=random.randint(0, 23),
                                           minutes=random.randint(0, 59),
                                           seconds=random.randint(0, 59))
        user_id = fake.uuid4()
        category = random.choice(PRODUCT_CATEGORIES)
        product_name = random.choice(PRODUCT_NAMES[category])
        # Simulate some product IDs (could be more structured in a real scenario)
        product_id = f"P{random.randint(1000, 9999)}"
        price = round(random.uniform(5.0, 200.0), 2)
        quantity = random.randint(1, 5)
        payment_method = random.choice(PAYMENT_METHODS)

        data.append({
            "timestamp": timestamp,
            "user_id": user_id,
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "price": price,
            "quantity": quantity,
            "payment_method": payment_method
        })

    df = pd.DataFrame(data)
    # Ensure correct order of columns
    df = df[["timestamp", "user_id", "product_id", "product_name", "category", "price", "quantity", "payment_method"]]
    return df

if __name__ == "__main__":
    num_records_to_generate = 1000
    sample_df = generate_purchase_data(num_records_to_generate)

    # Save to data directory
    output_path = "data/sample_purchases.csv"
    sample_df.to_csv(output_path, index=False)

    print(f"{len(sample_df)} sample purchase records generated and saved to {output_path}")
