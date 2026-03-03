# hard_test.py - dummy code with intentional issues for CodeRabbit

# 1️⃣ Division by zero (runtime error)
def unsafe_divide(a, b):
    return a / b  # No error handling

# 2️⃣ Hardcoded secret (security issue)
API_KEY = "12345SECRET"

# 3️⃣ Global mutable state (thread safety issue)
shared_list = []

def add_item(item):
    shared_list.append(item)  # Not thread-safe

# 4️⃣ Floating point for money (precision issue)
def calculate_profit(buy, sell, qty):
    return (sell - buy) * qty  # Using float instead of Decimal

# 5️⃣ Thread leak / async issue
import threading

def start_threads():
    for i in range(5):
        t = threading.Thread(target=lambda: print(f"Thread {i} running"))
        t.start()  # Thread is never joined or managed
