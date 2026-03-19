import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()
np.random.seed(42)
random.seed(42)

N = 10000
start_date = datetime(2022, 1, 1)
end_date = datetime(2024, 12, 31)

categories = {
    'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smart Watch', 'Camera', 'Speaker'],
    'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Dress', 'Sneakers', 'Boots', 'Hoodie'],
    'Home & Kitchen': ['Blender', 'Coffee Maker', 'Vacuum Cleaner', 'Bed Sheets', 'Cookware Set', 'Air Fryer'],
    'Books': ['Fiction Novel', 'Self-Help', 'Textbook', 'Biography', 'Science', 'History'],
    'Sports & Outdoors': ['Yoga Mat', 'Dumbbells', 'Running Shoes', 'Bicycle', 'Tent', 'Backpack'],
    'Beauty & Personal Care': ['Moisturizer', 'Shampoo', 'Perfume', 'Lipstick', 'Sunscreen', 'Face Wash']
}

price_ranges = {
    'Electronics': (50, 2500),
    'Clothing': (15, 300),
    'Home & Kitchen': (20, 500),
    'Books': (8, 80),
    'Sports & Outdoors': (10, 800),
    'Beauty & Personal Care': (5, 150)
}

regions = ['North', 'South', 'East', 'West', 'Central']
channels = ['Website', 'Mobile App', 'Marketplace', 'Social Media']
payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Crypto']
customer_segments = ['New', 'Returning', 'Premium', 'VIP']
statuses = ['Delivered', 'Returned', 'Cancelled', 'Pending']

rows = []
customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, 3001)]

for i in range(N):
    category = random.choice(list(categories.keys()))
    product = random.choice(categories[category])
    price_min, price_max = price_ranges[category]
    unit_price = round(random.uniform(price_min, price_max), 2)
    quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 12, 8, 5])[0]
    discount_pct = random.choices([0, 5, 10, 15, 20, 25, 30], weights=[40, 15, 15, 10, 10, 5, 5])[0]
    discount_amt = round(unit_price * quantity * discount_pct / 100, 2)
    revenue = round(unit_price * quantity - discount_amt, 2)
    shipping = round(random.uniform(0, 25), 2) if revenue < 100 else 0.0
    total = round(revenue + shipping, 2)

    order_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

    customer_id = random.choice(customer_ids)
    segment = random.choices(customer_segments, weights=[30, 40, 20, 10])[0]
    region = random.choice(regions)
    channel = random.choices(channels, weights=[40, 35, 15, 10])[0]
    payment = random.choice(payment_methods)
    status = random.choices(statuses, weights=[75, 10, 10, 5])[0]
    rating = round(random.uniform(3.0, 5.0), 1) if status == 'Delivered' else round(random.uniform(1.0, 3.5), 1)

    rows.append({
        'order_id': f"ORD{str(i+1).zfill(6)}",
        'order_date': order_date.strftime('%Y-%m-%d'),
        'customer_id': customer_id,
        'customer_segment': segment,
        'region': region,
        'sales_channel': channel,
        'category': category,
        'product_name': product,
        'unit_price': unit_price,
        'quantity': quantity,
        'discount_pct': discount_pct,
        'discount_amount': discount_amt,
        'revenue': revenue,
        'shipping_cost': shipping,
        'total_amount': total,
        'payment_method': payment,
        'order_status': status,
        'customer_rating': rating
    })

df = pd.DataFrame(rows)
df.to_csv('/home/claude/ecommerce-sales-analytics/data/raw/ecommerce_sales_raw.csv', index=False)
print(f"Generated {len(df)} rows")
print(df.dtypes)
