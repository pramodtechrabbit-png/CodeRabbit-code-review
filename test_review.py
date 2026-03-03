# test_review.py - dummy code for CodeRabbit review

# 1️⃣ Function with potential division error
def divide(a, b):
    return a / b  # No error handling for b = 0

# 2️⃣ Hardcoded credentials (intentional bad practice)
password = "admin123"

# 3️⃣ Example of global mutable state
cache = []

def add_to_cache(item):
    cache.append(item)  # Not thread-safe if used in multi-threaded env

# 4️⃣ Function using floating-point for money
def calculate_profit(buy_price, sell_price, quantity):
    return (sell_price - buy_price) * quantity  # Floating-point precision issue

# 5️⃣ Async simulation with thread leak
import threading
def async_task():
    for i in range(5):
        threading.Thread(target=lambda: print(f"Processing {i}")).start()
