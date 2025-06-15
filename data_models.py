# Input Purchase Data Schema
# event_timestamp: str  # ISO format string or Unix timestamp
# user_id: str
# order_id: str
# product_id: str
# product_name: str
# category: str  # "men", "women", "kids"
# price: float
# quantity: int
# payment_method: str  # "upi", "credit_card", "wallet"

# --- Output KPI Structures ---

# Top Selling Products:
# List of dictionaries:
# [
#   { "product_id": str, "product_name": str, "total_quantity_sold": int },
#   ...
# ]

# Total Revenue by Category:
# Dictionary:
# {
#   "men": float,
#   "women": float,
#   "kids": float
# }

# Preferred Payment Method by Category:
# Dictionary:
# {
#   "men": { "upi": int, "credit_card": int, "wallet": int },
#   "women": { "upi": int, "credit_card": int, "wallet": int },
#   "kids": { "upi": int, "credit_card": int, "wallet": int }
# } # Values can be counts or percentages

# Purchase Frequency Per User:
# Dictionary:
# {
#   "user_id_1": int, # count of orders
#   "user_id_2": int,
#   ...
# }

# Average Order Value:
# float
