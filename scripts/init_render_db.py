"""
Initialize Render PostgreSQL database with schemas and seed data
Run this script after deploying to Render to set up the database

Usage:
  DATABASE_URL=<your-render-db-url> python scripts/init_render_db.py
"""
import os
import sys

# Add api directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))

import psycopg2
import json
import random
from datetime import datetime, timedelta

# Get database URL from environment
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable not set")
    print("Usage: DATABASE_URL=<your-render-db-url> python scripts/init_render_db.py")
    sys.exit(1)

# Fix Render's postgres:// URL
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

print(f"Connecting to database...")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Configuration
NUM_ORDERS = 1000
NUM_PRODUCTS = 100
NUM_CUSTOMERS = 20

END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=180)

def random_date():
    delta = END_DATE - START_DATE
    random_days = random.randint(0, delta.days)
    return START_DATE + timedelta(days=random_days)

def random_price(min_val=10, max_val=500):
    return round(random.uniform(min_val, max_val), 2)

# ============== CREATE SCHEMAS ==============
print("Creating schemas...")
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS intermediate;
    CREATE SCHEMA IF NOT EXISTS marts;
    CREATE SCHEMA IF NOT EXISTS public_staging;
    CREATE SCHEMA IF NOT EXISTS public_intermediate;
    CREATE SCHEMA IF NOT EXISTS public_marts;
""")
conn.commit()

# ============== CREATE TABLES ==============
print("Creating raw tables...")

# Shopify tables
cursor.execute("""
    DROP TABLE IF EXISTS raw.shopify_order_line_items CASCADE;
    DROP TABLE IF EXISTS raw.shopify_orders CASCADE;
    DROP TABLE IF EXISTS raw.shopify_products CASCADE;
    DROP TABLE IF EXISTS raw.shopify_customers CASCADE;
    
    CREATE TABLE raw.shopify_customers (
        id BIGINT PRIMARY KEY,
        email VARCHAR(255),
        phone VARCHAR(50),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        default_address JSONB,
        orders_count INT,
        total_spent DECIMAL(12,2),
        state VARCHAR(50),
        verified_email BOOLEAN,
        accepts_marketing BOOLEAN,
        tags TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    
    CREATE TABLE raw.shopify_products (
        id BIGINT PRIMARY KEY,
        title VARCHAR(500),
        handle VARCHAR(255),
        product_type VARCHAR(100),
        vendor VARCHAR(100),
        variants JSONB,
        status VARCHAR(50),
        published_at TIMESTAMP,
        body_html TEXT,
        tags TEXT,
        images JSONB,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    
    CREATE TABLE raw.shopify_orders (
        id BIGINT PRIMARY KEY,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        processed_at TIMESTAMP,
        cancelled_at TIMESTAMP,
        closed_at TIMESTAMP,
        customer_id BIGINT,
        email VARCHAR(255),
        total_price DECIMAL(12,2),
        subtotal_price DECIMAL(12,2),
        total_tax DECIMAL(12,2),
        total_discounts DECIMAL(12,2),
        currency VARCHAR(10),
        financial_status VARCHAR(50),
        fulfillment_status VARCHAR(50),
        cancel_reason VARCHAR(100),
        shipping_lines JSONB,
        line_items_count INT,
        source_name VARCHAR(100),
        tags TEXT,
        note TEXT
    );
    
    CREATE TABLE raw.shopify_order_line_items (
        id BIGINT PRIMARY KEY,
        order_id BIGINT,
        product_id BIGINT,
        variant_id BIGINT,
        title VARCHAR(500),
        variant_title VARCHAR(255),
        sku VARCHAR(100),
        quantity INT,
        price DECIMAL(12,2),
        total_discount DECIMAL(12,2),
        fulfillment_status VARCHAR(50),
        fulfillable_quantity INT,
        gift_card BOOLEAN,
        taxable BOOLEAN,
        requires_shipping BOOLEAN
    );
""")

# Amazon tables
cursor.execute("""
    DROP TABLE IF EXISTS raw.amazon_order_items CASCADE;
    DROP TABLE IF EXISTS raw.amazon_orders CASCADE;
    DROP TABLE IF EXISTS raw.amazon_catalog_items CASCADE;
    
    CREATE TABLE raw.amazon_orders (
        amazon_order_id VARCHAR(50) PRIMARY KEY,
        purchase_date TIMESTAMP,
        last_update_date TIMESTAMP,
        buyer_email VARCHAR(255),
        order_total JSONB,
        payment_method VARCHAR(50),
        order_status VARCHAR(50),
        shipping_address JSONB,
        number_of_items_shipped INT,
        number_of_items_unshipped INT,
        sales_channel VARCHAR(100)
    );
    
    CREATE TABLE raw.amazon_order_items (
        order_item_id VARCHAR(50) PRIMARY KEY,
        amazon_order_id VARCHAR(50),
        asin VARCHAR(20),
        title VARCHAR(500),
        seller_sku VARCHAR(100),
        quantity_ordered INT,
        quantity_shipped INT,
        item_price JSONB,
        promotion_discount JSONB
    );
    
    CREATE TABLE raw.amazon_catalog_items (
        asin VARCHAR(20) PRIMARY KEY,
        title VARCHAR(500),
        brand VARCHAR(100)
    );
""")

# Lazada tables
cursor.execute("""
    DROP TABLE IF EXISTS raw.lazada_order_items CASCADE;
    DROP TABLE IF EXISTS raw.lazada_orders CASCADE;
    DROP TABLE IF EXISTS raw.lazada_products CASCADE;
    
    CREATE TABLE raw.lazada_orders (
        order_id BIGINT PRIMARY KEY,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        customer_id BIGINT,
        buyer_email VARCHAR(255),
        price DECIMAL(12,2),
        items_count INT,
        voucher DECIMAL(12,2),
        payment_method VARCHAR(50),
        statuses VARCHAR(50),
        address_shipping JSONB,
        remarks TEXT
    );
    
    CREATE TABLE raw.lazada_order_items (
        order_item_id BIGINT PRIMARY KEY,
        order_id BIGINT,
        product_id BIGINT,
        name VARCHAR(500),
        variation VARCHAR(255),
        sku VARCHAR(100),
        paid_price DECIMAL(12,2),
        voucher_amount DECIMAL(12,2),
        status VARCHAR(50)
    );
    
    CREATE TABLE raw.lazada_products (
        item_id BIGINT PRIMARY KEY,
        name VARCHAR(500),
        sku_id VARCHAR(100)
    );
""")

# Shopee tables
cursor.execute("""
    DROP TABLE IF EXISTS raw.shopee_order_items CASCADE;
    DROP TABLE IF EXISTS raw.shopee_orders CASCADE;
    DROP TABLE IF EXISTS raw.shopee_products CASCADE;
    
    CREATE TABLE raw.shopee_orders (
        order_sn VARCHAR(50) PRIMARY KEY,
        create_time BIGINT,
        update_time BIGINT,
        pay_time BIGINT,
        buyer_user_id BIGINT,
        buyer_username VARCHAR(100),
        total_amount DECIMAL(12,2),
        estimated_shipping_fee DECIMAL(12,2),
        voucher_absorbed DECIMAL(12,2),
        currency VARCHAR(10),
        order_status VARCHAR(50),
        cancel_reason VARCHAR(255),
        recipient_address JSONB,
        item_list JSONB,
        message_to_seller TEXT
    );
    
    CREATE TABLE raw.shopee_order_items (
        id SERIAL PRIMARY KEY,
        order_sn VARCHAR(50),
        item_id BIGINT,
        item_name VARCHAR(500),
        model_id BIGINT,
        model_name VARCHAR(255),
        model_sku VARCHAR(100),
        model_quantity_purchased INT,
        model_original_price DECIMAL(12,2),
        model_discounted_price DECIMAL(12,2)
    );
    
    CREATE TABLE raw.shopee_products (
        item_id BIGINT PRIMARY KEY,
        item_name VARCHAR(500),
        item_sku VARCHAR(100)
    );
""")

conn.commit()
print("✓ Tables created")

# ============== SEED DATA ==============
print(f"\nSeeding data: {NUM_CUSTOMERS} customers, {NUM_PRODUCTS} products, {NUM_ORDERS} orders per platform...")

# Generate data (simplified version)
first_names = ['John', 'Jane', 'Mike', 'Sarah', 'David', 'Emily', 'Chris', 'Lisa', 'Tom', 'Anna',
               'James', 'Emma', 'Robert', 'Olivia', 'William', 'Sophia', 'Joseph', 'Ava', 'Daniel', 'Mia']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Manila', 'Cebu', 'Davao', 'Singapore', 'Kuala Lumpur']
countries = ['US', 'US', 'US', 'US', 'US', 'PH', 'PH', 'PH', 'SG', 'MY']
product_adjectives = ['Premium', 'Classic', 'Modern', 'Vintage', 'Ultra', 'Pro', 'Elite', 'Smart', 'Eco', 'Deluxe']
product_nouns = ['Widget', 'Gadget', 'Device', 'Tool', 'Kit', 'Set', 'Pack', 'Bundle', 'System', 'Unit']
product_categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Beauty', 'Toys', 'Books', 'Food', 'Health', 'Automotive']

# Insert customers
print("  Inserting customers...")
for i in range(NUM_CUSTOMERS):
    customer_id = 1000000 + i
    first_name = first_names[i % len(first_names)]
    last_name = last_names[i % len(last_names)]
    city_idx = i % len(cities)
    cursor.execute("""
        INSERT INTO raw.shopify_customers 
        (id, email, phone, first_name, last_name, default_address, orders_count, total_spent, 
         state, verified_email, accepts_marketing, tags, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (customer_id, f"{first_name.lower()}.{last_name.lower()}{i}@email.com", 
          f"+1555{random.randint(1000000, 9999999)}", first_name, last_name,
          json.dumps({'city': cities[city_idx], 'country': countries[city_idx], 'country_code': countries[city_idx]}),
          0, 0, 'enabled', True, random.choice([True, False]), '', random_date(), datetime.now()))

# Insert products
print("  Inserting products...")
products = []
for i in range(NUM_PRODUCTS):
    product_id = 2000000 + i
    adj = product_adjectives[i % len(product_adjectives)]
    noun = product_nouns[i % len(product_nouns)]
    category = product_categories[i % len(product_categories)]
    price = random_price(20, 300)
    products.append({'id': product_id, 'title': f"{adj} {noun} {i+1}", 'price': price, 'sku': f"SKU-{product_id}", 'vendor': f"Vendor {(i % 10) + 1}", 'category': category})
    
    cursor.execute("""
        INSERT INTO raw.shopify_products 
        (id, title, handle, product_type, vendor, variants, status, published_at, body_html, tags, images, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (product_id, f"{adj} {noun} {i+1}", f"{adj.lower()}-{noun.lower()}-{i+1}", category, f"Vendor {(i % 10) + 1}",
          json.dumps([{'price': str(price), 'compare_at_price': str(price * 1.2), 'sku': f"SKU-{product_id}", 'inventory_quantity': random.randint(10, 500)}]),
          'active', random_date(), f"Description", category, json.dumps([{'src': f"https://example.com/{product_id}.jpg"}]), random_date(), datetime.now()))
    
    cursor.execute("INSERT INTO raw.amazon_catalog_items (asin, title, brand) VALUES (%s, %s, %s)", 
                   (f"ASIN{product_id}", f"{adj} {noun} {i+1}", f"Vendor {(i % 10) + 1}"))
    cursor.execute("INSERT INTO raw.lazada_products (item_id, name, sku_id) VALUES (%s, %s, %s)", 
                   (product_id, f"{adj} {noun} {i+1}", f"SKU-{product_id}"))
    cursor.execute("INSERT INTO raw.shopee_products (item_id, item_name, item_sku) VALUES (%s, %s, %s)", 
                   (product_id, f"{adj} {noun} {i+1}", f"SKU-{product_id}"))

conn.commit()

# Insert orders
order_statuses_shopify = ['paid', 'pending', 'refunded']
fulfillment_statuses = ['fulfilled', 'partial', 'unfulfilled', None]
amazon_statuses = ['Shipped', 'Pending', 'Canceled', 'Unshipped']
lazada_statuses = ['delivered', 'shipped', 'pending', 'canceled']
shopee_statuses = ['COMPLETED', 'SHIPPED', 'READY_TO_SHIP', 'UNPAID', 'CANCELLED']

print("  Inserting orders...")
for platform in ['shopify', 'amazon', 'lazada', 'shopee']:
    for i in range(NUM_ORDERS):
        order_date = random_date()
        num_items = random.randint(1, 4)
        
        if platform == 'shopify':
            order_id = 3000000 + i
            subtotal = sum(random.choice(products)['price'] * random.randint(1, 2) for _ in range(num_items))
            total = round(subtotal * 1.08, 2)
            fin_status = random.choices(order_statuses_shopify, weights=[0.8, 0.15, 0.05])[0]
            ful_status = random.choices(fulfillment_statuses, weights=[0.7, 0.1, 0.15, 0.05])[0]
            cursor.execute("""
                INSERT INTO raw.shopify_orders 
                (id, created_at, updated_at, processed_at, customer_id, email, total_price, subtotal_price, 
                 total_tax, total_discounts, currency, financial_status, fulfillment_status, line_items_count, source_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (order_id, order_date, order_date, order_date, 1000000 + (i % NUM_CUSTOMERS), 
                  f"customer{i}@email.com", total, subtotal, round(subtotal * 0.08, 2), 0, 'USD', fin_status, ful_status, num_items, 'web'))
            for j in range(num_items):
                p = random.choice(products)
                qty = random.randint(1, 2)
                cursor.execute("""
                    INSERT INTO raw.shopify_order_line_items (id, order_id, product_id, variant_id, title, sku, quantity, price, total_discount, fulfillment_status, gift_card, taxable, requires_shipping)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (order_id * 100 + j, order_id, p['id'], p['id'], p['title'], p['sku'], qty, p['price'], 0, ful_status, False, True, True))
        
        elif platform == 'amazon':
            order_id = f"AMZ-{4000000 + i}"
            total = sum(random.choice(products)['price'] * random.randint(1, 2) for _ in range(num_items))
            status = random.choices(amazon_statuses, weights=[0.75, 0.1, 0.05, 0.1])[0]
            cursor.execute("""
                INSERT INTO raw.amazon_orders (amazon_order_id, purchase_date, last_update_date, buyer_email, order_total, payment_method, order_status, number_of_items_shipped, number_of_items_unshipped, sales_channel)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (order_id, order_date, order_date, f"buyer{i}@amazon.com", json.dumps({'Amount': str(round(total, 2)), 'CurrencyCode': 'USD'}),
                  'Credit Card', status, num_items if status == 'Shipped' else 0, 0 if status == 'Shipped' else num_items, 'Amazon.com'))
            for j in range(num_items):
                p = random.choice(products)
                qty = random.randint(1, 2)
                cursor.execute("""
                    INSERT INTO raw.amazon_order_items (order_item_id, amazon_order_id, asin, title, seller_sku, quantity_ordered, quantity_shipped, item_price, promotion_discount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (f"{order_id}-{j}", order_id, f"ASIN{p['id']}", p['title'], p['sku'], qty, qty if status == 'Shipped' else 0,
                      json.dumps({'Amount': str(p['price'] * qty)}), json.dumps({'Amount': '0'})))
        
        elif platform == 'lazada':
            order_id = 5000000 + i
            total = sum(random.choice(products)['price'] * 55 for _ in range(num_items))
            status = random.choices(lazada_statuses, weights=[0.7, 0.15, 0.1, 0.05])[0]
            cursor.execute("""
                INSERT INTO raw.lazada_orders (order_id, created_at, updated_at, customer_id, buyer_email, price, items_count, voucher, payment_method, statuses)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (order_id, order_date, order_date, 1000000 + (i % NUM_CUSTOMERS), f"buyer{i}@lazada.com", round(total, 2), num_items, 0, 'COD', status))
            for j in range(num_items):
                p = random.choice(products)
                cursor.execute("""
                    INSERT INTO raw.lazada_order_items (order_item_id, order_id, product_id, name, sku, paid_price, voucher_amount, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (order_id * 100 + j, order_id, p['id'], p['title'], p['sku'], round(p['price'] * 55, 2), 0, status))
        
        elif platform == 'shopee':
            order_sn = f"SHP{6000000 + i}"
            create_time = int(order_date.timestamp())
            total = sum(random.choice(products)['price'] * 55 for _ in range(num_items))
            status = random.choices(shopee_statuses, weights=[0.65, 0.15, 0.1, 0.05, 0.05])[0]
            cursor.execute("""
                INSERT INTO raw.shopee_orders (order_sn, create_time, update_time, pay_time, buyer_user_id, buyer_username, total_amount, estimated_shipping_fee, voucher_absorbed, currency, order_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (order_sn, create_time, create_time, create_time if status != 'UNPAID' else None, 1000000 + (i % NUM_CUSTOMERS), f"buyer{i}", round(total, 2), 50, 0, 'PHP', status))
            for j in range(num_items):
                p = random.choice(products)
                qty = random.randint(1, 2)
                disc_price = round(p['price'] * 55, 2)
                cursor.execute("""
                    INSERT INTO raw.shopee_order_items (order_sn, item_id, item_name, model_id, model_name, model_sku, model_quantity_purchased, model_original_price, model_discounted_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (order_sn, p['id'], p['title'], p['id'] * 10, 'Default', p['sku'], qty, round(disc_price * 1.1, 2), disc_price))
    
    conn.commit()
    print(f"    ✓ {platform}: {NUM_ORDERS} orders")

print("\n✓ Seed data complete!")
print("\nNow run: dbt run --profiles-dir . --target prod")
cursor.close()
conn.close()

