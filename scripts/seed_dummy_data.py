"""
Seed dummy data for DataPulse - 1K orders, 100 items, 20 customers per platform
"""
import psycopg2
import random
import json
from datetime import datetime, timedelta
from decimal import Decimal

# Database connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="datapulse",
    user="edgodalle"
)
cursor = conn.cursor()

# Configuration
NUM_ORDERS = 1000
NUM_PRODUCTS = 100
NUM_CUSTOMERS = 20
PLATFORMS = ['shopify', 'amazon', 'lazada', 'shopee']

# Date range for orders (last 6 months)
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=180)

def random_date():
    """Generate random date within range"""
    delta = END_DATE - START_DATE
    random_days = random.randint(0, delta.days)
    return START_DATE + timedelta(days=random_days)

def random_price(min_val=10, max_val=500):
    """Generate random price"""
    return round(random.uniform(min_val, max_val), 2)

# ============== CREATE TABLES ==============
print("Creating raw tables...")

# Shopify tables
cursor.execute("""
    DROP TABLE IF EXISTS raw.shopify_order_line_items CASCADE;
    DROP TABLE IF EXISTS raw.shopify_orders CASCADE;
    DROP TABLE IF EXISTS raw.shopify_products CASCADE;
    DROP TABLE IF EXISTS raw.shopify_customers CASCADE;
    DROP TABLE IF EXISTS raw.shopify_inventory_levels CASCADE;
    
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

# ============== GENERATE CUSTOMERS ==============
print(f"\nGenerating {NUM_CUSTOMERS} customers per platform...")

first_names = ['John', 'Jane', 'Mike', 'Sarah', 'David', 'Emily', 'Chris', 'Lisa', 'Tom', 'Anna',
               'James', 'Emma', 'Robert', 'Olivia', 'William', 'Sophia', 'Joseph', 'Ava', 'Daniel', 'Mia']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
              'Anderson', 'Taylor', 'Thomas', 'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White', 'Harris']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Manila', 'Cebu', 'Davao', 'Singapore', 'Kuala Lumpur']
countries = ['US', 'US', 'US', 'US', 'US', 'PH', 'PH', 'PH', 'SG', 'MY']

shopify_customers = []
for i in range(NUM_CUSTOMERS):
    customer_id = 1000000 + i
    first_name = first_names[i % len(first_names)]
    last_name = last_names[i % len(last_names)]
    city_idx = i % len(cities)
    
    shopify_customers.append({
        'id': customer_id,
        'email': f"{first_name.lower()}.{last_name.lower()}{i}@email.com",
        'phone': f"+1555{random.randint(1000000, 9999999)}",
        'first_name': first_name,
        'last_name': last_name,
        'default_address': json.dumps({
            'city': cities[city_idx],
            'province': 'State',
            'country': countries[city_idx],
            'country_code': countries[city_idx]
        }),
        'orders_count': 0,
        'total_spent': 0,
        'state': 'enabled',
        'verified_email': True,
        'accepts_marketing': random.choice([True, False]),
        'tags': '',
        'created_at': random_date(),
        'updated_at': datetime.now()
    })

for c in shopify_customers:
    cursor.execute("""
        INSERT INTO raw.shopify_customers 
        (id, email, phone, first_name, last_name, default_address, orders_count, total_spent, 
         state, verified_email, accepts_marketing, tags, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (c['id'], c['email'], c['phone'], c['first_name'], c['last_name'], c['default_address'],
          c['orders_count'], c['total_spent'], c['state'], c['verified_email'], c['accepts_marketing'],
          c['tags'], c['created_at'], c['updated_at']))

conn.commit()
print(f"✓ Shopify customers inserted")

# ============== GENERATE PRODUCTS ==============
print(f"\nGenerating {NUM_PRODUCTS} products per platform...")

product_categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Beauty', 'Toys', 'Books', 'Food', 'Health', 'Automotive']
product_adjectives = ['Premium', 'Classic', 'Modern', 'Vintage', 'Ultra', 'Pro', 'Elite', 'Smart', 'Eco', 'Deluxe']
product_nouns = ['Widget', 'Gadget', 'Device', 'Tool', 'Kit', 'Set', 'Pack', 'Bundle', 'System', 'Unit']

products = []
for i in range(NUM_PRODUCTS):
    product_id = 2000000 + i
    adj = product_adjectives[i % len(product_adjectives)]
    noun = product_nouns[i % len(product_nouns)]
    category = product_categories[i % len(product_categories)]
    price = random_price(20, 300)
    
    products.append({
        'id': product_id,
        'title': f"{adj} {noun} {i+1}",
        'handle': f"{adj.lower()}-{noun.lower()}-{i+1}",
        'product_type': category,
        'vendor': f"Vendor {(i % 10) + 1}",
        'price': price,
        'sku': f"SKU-{product_id}",
        'inventory': random.randint(10, 500)
    })

# Insert Shopify products
for p in products:
    cursor.execute("""
        INSERT INTO raw.shopify_products 
        (id, title, handle, product_type, vendor, variants, status, published_at, body_html, tags, images, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (p['id'], p['title'], p['handle'], p['product_type'], p['vendor'],
          json.dumps([{'price': str(p['price']), 'compare_at_price': str(p['price'] * 1.2), 'sku': p['sku'], 
                       'inventory_quantity': p['inventory'], 'inventory_policy': 'deny'}]),
          'active', random_date(), f"Description for {p['title']}", p['product_type'],
          json.dumps([{'src': f"https://example.com/img/{p['id']}.jpg"}]), random_date(), datetime.now()))

# Insert Amazon catalog items
for p in products:
    cursor.execute("""
        INSERT INTO raw.amazon_catalog_items (asin, title, brand)
        VALUES (%s, %s, %s)
    """, (f"ASIN{p['id']}", p['title'], p['vendor']))

# Insert Lazada products
for p in products:
    cursor.execute("""
        INSERT INTO raw.lazada_products (item_id, name, sku_id)
        VALUES (%s, %s, %s)
    """, (p['id'], p['title'], p['sku']))

# Insert Shopee products
for p in products:
    cursor.execute("""
        INSERT INTO raw.shopee_products (item_id, item_name, item_sku)
        VALUES (%s, %s, %s)
    """, (p['id'], p['title'], p['sku']))

conn.commit()
print(f"✓ Products inserted for all platforms")

# ============== GENERATE ORDERS ==============
print(f"\nGenerating {NUM_ORDERS} orders per platform...")

order_statuses_shopify = ['paid', 'pending', 'refunded', 'partially_refunded']
fulfillment_statuses = ['fulfilled', 'partial', 'unfulfilled', None]
amazon_statuses = ['Shipped', 'Pending', 'Canceled', 'Unshipped']
lazada_statuses = ['delivered', 'shipped', 'pending', 'canceled']
shopee_statuses = ['COMPLETED', 'SHIPPED', 'READY_TO_SHIP', 'UNPAID', 'CANCELLED']

currencies = {'shopify': 'USD', 'amazon': 'USD', 'lazada': 'PHP', 'shopee': 'PHP'}

# SHOPIFY ORDERS
print("  Inserting Shopify orders...")
for i in range(NUM_ORDERS):
    order_id = 3000000 + i
    customer = random.choice(shopify_customers)
    order_date = random_date()
    num_items = random.randint(1, 5)
    
    # Calculate totals
    subtotal = 0
    line_items = []
    for j in range(num_items):
        product = random.choice(products)
        qty = random.randint(1, 3)
        item_total = product['price'] * qty
        subtotal += item_total
        line_items.append({
            'id': order_id * 100 + j,
            'product_id': product['id'],
            'title': product['title'],
            'sku': product['sku'],
            'quantity': qty,
            'price': product['price']
        })
    
    discount = round(subtotal * random.uniform(0, 0.15), 2)
    tax = round((subtotal - discount) * 0.08, 2)
    total = round(subtotal - discount + tax, 2)
    
    fin_status = random.choices(order_statuses_shopify, weights=[0.8, 0.1, 0.05, 0.05])[0]
    ful_status = random.choices(fulfillment_statuses, weights=[0.7, 0.1, 0.15, 0.05])[0]
    
    cursor.execute("""
        INSERT INTO raw.shopify_orders 
        (id, created_at, updated_at, processed_at, cancelled_at, closed_at, customer_id, email,
         total_price, subtotal_price, total_tax, total_discounts, currency, financial_status,
         fulfillment_status, cancel_reason, shipping_lines, line_items_count, source_name, tags, note)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (order_id, order_date, order_date, order_date, None, 
          order_date + timedelta(days=random.randint(1, 7)) if ful_status == 'fulfilled' else None,
          customer['id'], customer['email'], total, subtotal, tax, discount, 'USD',
          fin_status, ful_status, None, json.dumps([]), num_items, 'web', '', None))
    
    # Insert line items
    for item in line_items:
        cursor.execute("""
            INSERT INTO raw.shopify_order_line_items
            (id, order_id, product_id, variant_id, title, variant_title, sku, quantity, price,
             total_discount, fulfillment_status, fulfillable_quantity, gift_card, taxable, requires_shipping)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (item['id'], order_id, item['product_id'], item['product_id'], item['title'], None, item['sku'],
              item['quantity'], item['price'], 0, ful_status, 0, False, True, True))

conn.commit()
print(f"  ✓ Shopify: {NUM_ORDERS} orders inserted")

# AMAZON ORDERS
print("  Inserting Amazon orders...")
for i in range(NUM_ORDERS):
    order_id = f"AMZ-{4000000 + i}"
    order_date = random_date()
    num_items = random.randint(1, 4)
    
    total = 0
    items = []
    for j in range(num_items):
        product = random.choice(products)
        qty = random.randint(1, 2)
        item_total = product['price'] * qty
        total += item_total
        items.append({
            'order_item_id': f"{order_id}-{j}",
            'asin': f"ASIN{product['id']}",
            'title': product['title'],
            'sku': product['sku'],
            'qty': qty,
            'price': item_total
        })
    
    status = random.choices(amazon_statuses, weights=[0.75, 0.1, 0.05, 0.1])[0]
    shipped = num_items if status == 'Shipped' else 0
    
    cursor.execute("""
        INSERT INTO raw.amazon_orders 
        (amazon_order_id, purchase_date, last_update_date, buyer_email, order_total, payment_method,
         order_status, shipping_address, number_of_items_shipped, number_of_items_unshipped, sales_channel)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (order_id, order_date, order_date, f"buyer{i}@amazon.com",
          json.dumps({'Amount': str(round(total, 2)), 'CurrencyCode': 'USD'}),
          'Credit Card', status, json.dumps({'city': 'New York', 'country': 'US'}),
          shipped, num_items - shipped, 'Amazon.com'))
    
    for item in items:
        cursor.execute("""
            INSERT INTO raw.amazon_order_items
            (order_item_id, amazon_order_id, asin, title, seller_sku, quantity_ordered, quantity_shipped, item_price, promotion_discount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (item['order_item_id'], order_id, item['asin'], item['title'], item['sku'],
              item['qty'], item['qty'] if status == 'Shipped' else 0,
              json.dumps({'Amount': str(item['price'])}), json.dumps({'Amount': '0'})))

conn.commit()
print(f"  ✓ Amazon: {NUM_ORDERS} orders inserted")

# LAZADA ORDERS
print("  Inserting Lazada orders...")
for i in range(NUM_ORDERS):
    order_id = 5000000 + i
    order_date = random_date()
    num_items = random.randint(1, 4)
    
    total = 0
    voucher = round(random.uniform(0, 50), 2)
    items = []
    for j in range(num_items):
        product = random.choice(products)
        price_php = product['price'] * 55  # Approx USD to PHP
        items.append({
            'order_item_id': order_id * 100 + j,
            'product_id': product['id'],
            'name': product['title'],
            'sku': product['sku'],
            'price': round(price_php, 2)
        })
        total += price_php
    
    status = random.choices(lazada_statuses, weights=[0.7, 0.15, 0.1, 0.05])[0]
    
    cursor.execute("""
        INSERT INTO raw.lazada_orders 
        (order_id, created_at, updated_at, customer_id, buyer_email, price, items_count, voucher,
         payment_method, statuses, address_shipping, remarks)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (order_id, order_date, order_date, 1000000 + (i % NUM_CUSTOMERS), f"buyer{i}@lazada.com",
          round(total, 2), num_items, voucher, 'COD', status,
          json.dumps({'city': 'Manila', 'country': 'PH'}), None))
    
    for item in items:
        cursor.execute("""
            INSERT INTO raw.lazada_order_items
            (order_item_id, order_id, product_id, name, variation, sku, paid_price, voucher_amount, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (item['order_item_id'], order_id, item['product_id'], item['name'], None, item['sku'],
              item['price'], 0, status))

conn.commit()
print(f"  ✓ Lazada: {NUM_ORDERS} orders inserted")

# SHOPEE ORDERS
print("  Inserting Shopee orders...")
for i in range(NUM_ORDERS):
    order_sn = f"SHP{6000000 + i}"
    order_date = random_date()
    create_time = int(order_date.timestamp())
    num_items = random.randint(1, 4)
    
    total = 0
    shipping_fee = round(random.uniform(30, 100), 2)
    voucher = round(random.uniform(0, 100), 2)
    items = []
    for j in range(num_items):
        product = random.choice(products)
        price_php = product['price'] * 55
        qty = random.randint(1, 2)
        orig_price = round(price_php * 1.1, 2)
        items.append({
            'item_id': product['id'],
            'item_name': product['title'],
            'model_id': product['id'] * 10,
            'model_name': 'Default',
            'model_sku': product['sku'],
            'qty': qty,
            'orig_price': orig_price,
            'disc_price': round(price_php, 2)
        })
        total += price_php * qty
    
    status = random.choices(shopee_statuses, weights=[0.65, 0.15, 0.1, 0.05, 0.05])[0]
    
    cursor.execute("""
        INSERT INTO raw.shopee_orders 
        (order_sn, create_time, update_time, pay_time, buyer_user_id, buyer_username, total_amount,
         estimated_shipping_fee, voucher_absorbed, currency, order_status, cancel_reason,
         recipient_address, item_list, message_to_seller)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (order_sn, create_time, create_time, create_time if status != 'UNPAID' else None,
          1000000 + (i % NUM_CUSTOMERS), f"buyer{i}", round(total + shipping_fee, 2),
          shipping_fee, voucher, 'PHP', status, None if status != 'CANCELLED' else 'Buyer request',
          json.dumps({'city': 'Manila', 'country': 'PH'}), json.dumps([]), None))
    
    for item in items:
        cursor.execute("""
            INSERT INTO raw.shopee_order_items
            (order_sn, item_id, item_name, model_id, model_name, model_sku, model_quantity_purchased,
             model_original_price, model_discounted_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (order_sn, item['item_id'], item['item_name'], item['model_id'], item['model_name'],
              item['model_sku'], item['qty'], item['orig_price'], item['disc_price']))

conn.commit()
print(f"  ✓ Shopee: {NUM_ORDERS} orders inserted")

# ============== SUMMARY ==============
print("\n" + "="*50)
print("SEED DATA COMPLETE")
print("="*50)

# Count records
cursor.execute("SELECT COUNT(*) FROM raw.shopify_customers")
print(f"Shopify Customers: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM raw.shopify_products")
print(f"Shopify Products: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM raw.shopify_orders")
print(f"Shopify Orders: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM raw.amazon_orders")
print(f"Amazon Orders: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM raw.lazada_orders")
print(f"Lazada Orders: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM raw.shopee_orders")
print(f"Shopee Orders: {cursor.fetchone()[0]}")

cursor.close()
conn.close()
print("\n✓ Database connection closed")

