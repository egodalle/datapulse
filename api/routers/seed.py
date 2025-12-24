"""
Database seeding endpoint - Run once to populate with dummy data
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import random
import json
from datetime import datetime, timedelta

from core.database import get_db

router = APIRouter()

NUM_ORDERS = 500  # Reduced for faster seeding
NUM_PRODUCTS = 100
NUM_CUSTOMERS = 20

def random_date():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

def random_price(min_val=10, max_val=500):
    return round(random.uniform(min_val, max_val), 2)

@router.post("/seed")
async def seed_database(db: AsyncSession = Depends(get_db)):
    """Seed the database with dummy data."""
    try:
        # Create schemas
        for schema in ['raw', 'staging', 'intermediate', 'marts', 'public_staging', 'public_intermediate', 'public_marts']:
            await db.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        await db.commit()

        # Drop existing tables
        tables_to_drop = [
            'raw.shopify_order_line_items', 'raw.shopify_orders', 'raw.shopify_products', 'raw.shopify_customers',
            'raw.amazon_order_items', 'raw.amazon_orders', 'raw.amazon_catalog_items',
            'raw.lazada_order_items', 'raw.lazada_orders', 'raw.lazada_products',
            'raw.shopee_order_items', 'raw.shopee_orders', 'raw.shopee_products'
        ]
        for table in tables_to_drop:
            await db.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
        await db.commit()

        # Create Shopify tables
        await db.execute(text("""CREATE TABLE raw.shopify_customers (
            id BIGINT PRIMARY KEY, email VARCHAR(255), phone VARCHAR(50), first_name VARCHAR(100), last_name VARCHAR(100),
            default_address JSONB, orders_count INT, total_spent DECIMAL(12,2), state VARCHAR(50), verified_email BOOLEAN,
            accepts_marketing BOOLEAN, tags TEXT, created_at TIMESTAMP, updated_at TIMESTAMP)"""))
        
        await db.execute(text("""CREATE TABLE raw.shopify_products (
            id BIGINT PRIMARY KEY, title VARCHAR(500), handle VARCHAR(255), product_type VARCHAR(100), vendor VARCHAR(100),
            variants JSONB, status VARCHAR(50), published_at TIMESTAMP, body_html TEXT, tags TEXT, images JSONB,
            created_at TIMESTAMP, updated_at TIMESTAMP)"""))
        
        await db.execute(text("""CREATE TABLE raw.shopify_orders (
            id BIGINT PRIMARY KEY, created_at TIMESTAMP, updated_at TIMESTAMP, processed_at TIMESTAMP, cancelled_at TIMESTAMP,
            closed_at TIMESTAMP, customer_id BIGINT, email VARCHAR(255), total_price DECIMAL(12,2), subtotal_price DECIMAL(12,2),
            total_tax DECIMAL(12,2), total_discounts DECIMAL(12,2), currency VARCHAR(10), financial_status VARCHAR(50),
            fulfillment_status VARCHAR(50), cancel_reason VARCHAR(100), shipping_lines JSONB, line_items_count INT,
            source_name VARCHAR(100), tags TEXT, note TEXT)"""))
        
        await db.execute(text("""CREATE TABLE raw.shopify_order_line_items (
            id BIGINT PRIMARY KEY, order_id BIGINT, product_id BIGINT, variant_id BIGINT, title VARCHAR(500),
            variant_title VARCHAR(255), sku VARCHAR(100), quantity INT, price DECIMAL(12,2), total_discount DECIMAL(12,2),
            fulfillment_status VARCHAR(50), fulfillable_quantity INT, gift_card BOOLEAN, taxable BOOLEAN, requires_shipping BOOLEAN)"""))

        # Create Amazon tables
        await db.execute(text("""CREATE TABLE raw.amazon_orders (
            amazon_order_id VARCHAR(50) PRIMARY KEY, purchase_date TIMESTAMP, last_update_date TIMESTAMP, buyer_email VARCHAR(255),
            order_total JSONB, payment_method VARCHAR(50), order_status VARCHAR(50), shipping_address JSONB,
            number_of_items_shipped INT, number_of_items_unshipped INT, sales_channel VARCHAR(100))"""))
        
        await db.execute(text("""CREATE TABLE raw.amazon_order_items (
            order_item_id VARCHAR(50) PRIMARY KEY, amazon_order_id VARCHAR(50), asin VARCHAR(20), title VARCHAR(500),
            seller_sku VARCHAR(100), quantity_ordered INT, quantity_shipped INT, item_price JSONB, promotion_discount JSONB)"""))
        
        await db.execute(text("""CREATE TABLE raw.amazon_catalog_items (asin VARCHAR(20) PRIMARY KEY, title VARCHAR(500), brand VARCHAR(100))"""))

        # Create Lazada tables
        await db.execute(text("""CREATE TABLE raw.lazada_orders (
            order_id BIGINT PRIMARY KEY, created_at TIMESTAMP, updated_at TIMESTAMP, customer_id BIGINT, buyer_email VARCHAR(255),
            price DECIMAL(12,2), items_count INT, voucher DECIMAL(12,2), payment_method VARCHAR(50), statuses VARCHAR(50),
            address_shipping JSONB, remarks TEXT)"""))
        
        await db.execute(text("""CREATE TABLE raw.lazada_order_items (
            order_item_id BIGINT PRIMARY KEY, order_id BIGINT, product_id BIGINT, name VARCHAR(500), variation VARCHAR(255),
            sku VARCHAR(100), paid_price DECIMAL(12,2), voucher_amount DECIMAL(12,2), status VARCHAR(50))"""))
        
        await db.execute(text("""CREATE TABLE raw.lazada_products (item_id BIGINT PRIMARY KEY, name VARCHAR(500), sku_id VARCHAR(100))"""))

        # Create Shopee tables
        await db.execute(text("""CREATE TABLE raw.shopee_orders (
            order_sn VARCHAR(50) PRIMARY KEY, create_time BIGINT, update_time BIGINT, pay_time BIGINT, buyer_user_id BIGINT,
            buyer_username VARCHAR(100), total_amount DECIMAL(12,2), estimated_shipping_fee DECIMAL(12,2), voucher_absorbed DECIMAL(12,2),
            currency VARCHAR(10), order_status VARCHAR(50), cancel_reason VARCHAR(255), recipient_address JSONB, item_list JSONB,
            message_to_seller TEXT)"""))
        
        await db.execute(text("""CREATE TABLE raw.shopee_order_items (
            id SERIAL PRIMARY KEY, order_sn VARCHAR(50), item_id BIGINT, item_name VARCHAR(500), model_id BIGINT,
            model_name VARCHAR(255), model_sku VARCHAR(100), model_quantity_purchased INT, model_original_price DECIMAL(12,2),
            model_discounted_price DECIMAL(12,2))"""))
        
        await db.execute(text("""CREATE TABLE raw.shopee_products (item_id BIGINT PRIMARY KEY, item_name VARCHAR(500), item_sku VARCHAR(100))"""))
        await db.commit()

        # Seed data
        first_names = ['John', 'Jane', 'Mike', 'Sarah', 'David', 'Emily', 'Chris', 'Lisa', 'Tom', 'Anna',
                       'James', 'Emma', 'Robert', 'Olivia', 'William', 'Sophia', 'Joseph', 'Ava', 'Daniel', 'Mia']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Manila', 'Cebu', 'Davao', 'Singapore', 'Kuala Lumpur']
        countries = ['US', 'US', 'US', 'US', 'US', 'PH', 'PH', 'PH', 'SG', 'MY']
        product_adjectives = ['Premium', 'Classic', 'Modern', 'Vintage', 'Ultra', 'Pro', 'Elite', 'Smart', 'Eco', 'Deluxe']
        product_nouns = ['Widget', 'Gadget', 'Device', 'Tool', 'Kit', 'Set', 'Pack', 'Bundle', 'System', 'Unit']
        product_categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Beauty']

        # Insert customers
        for i in range(NUM_CUSTOMERS):
            await db.execute(text("""INSERT INTO raw.shopify_customers 
                (id, email, phone, first_name, last_name, default_address, orders_count, total_spent, state, verified_email, accepts_marketing, tags, created_at, updated_at)
                VALUES (:id, :email, :phone, :first_name, :last_name, :address, 0, 0, 'enabled', true, :marketing, '', :created, :updated)"""),
                {"id": 1000000+i, "email": f"{first_names[i%20].lower()}.{last_names[i%10].lower()}{i}@email.com",
                 "phone": f"+1555{random.randint(1000000,9999999)}", "first_name": first_names[i%20], "last_name": last_names[i%10],
                 "address": json.dumps({'city': cities[i%10], 'country': countries[i%10]}), "marketing": random.choice([True, False]),
                 "created": random_date(), "updated": datetime.now()})
        await db.commit()

        # Insert products
        products = []
        for i in range(NUM_PRODUCTS):
            pid, price = 2000000+i, random_price(20, 300)
            title = f"{product_adjectives[i%10]} {product_nouns[i%10]} {i+1}"
            products.append({'id': pid, 'title': title, 'price': price, 'sku': f"SKU-{pid}"})
            
            await db.execute(text("""INSERT INTO raw.shopify_products (id, title, handle, product_type, vendor, variants, status, published_at, body_html, tags, images, created_at, updated_at)
                VALUES (:id, :title, :handle, :ptype, :vendor, :variants, 'active', :pub, 'Desc', :tags, :images, :created, :updated)"""),
                {"id": pid, "title": title, "handle": title.lower().replace(' ','-'), "ptype": product_categories[i%5], "vendor": f"Vendor{i%10}",
                 "variants": json.dumps([{'price': str(price), 'sku': f"SKU-{pid}", 'inventory_quantity': random.randint(10,500)}]),
                 "pub": random_date(), "tags": product_categories[i%5], "images": json.dumps([{'src': f"https://ex.com/{pid}.jpg"}]), "created": random_date(), "updated": datetime.now()})
            await db.execute(text("INSERT INTO raw.amazon_catalog_items (asin, title, brand) VALUES (:asin, :title, :brand)"),
                {"asin": f"ASIN{pid}", "title": title, "brand": f"Vendor{i%10}"})
            await db.execute(text("INSERT INTO raw.lazada_products (item_id, name, sku_id) VALUES (:id, :name, :sku)"),
                {"id": pid, "name": title, "sku": f"SKU-{pid}"})
            await db.execute(text("INSERT INTO raw.shopee_products (item_id, item_name, item_sku) VALUES (:id, :name, :sku)"),
                {"id": pid, "name": title, "sku": f"SKU-{pid}"})
        await db.commit()

        # Insert orders
        statuses = {'shopify': ['paid','pending','refunded'], 'amazon': ['Shipped','Pending','Canceled'], 
                   'lazada': ['delivered','shipped','pending'], 'shopee': ['COMPLETED','SHIPPED','READY_TO_SHIP','UNPAID']}
        
        for platform in ['shopify', 'amazon', 'lazada', 'shopee']:
            for i in range(NUM_ORDERS):
                order_date = random_date()
                num_items = random.randint(1, 3)
                status = random.choice(statuses[platform])
                
                if platform == 'shopify':
                    oid = 3000000 + i
                    total = sum(random.choice(products)['price'] * random.randint(1,2) for _ in range(num_items))
                    await db.execute(text("""INSERT INTO raw.shopify_orders (id, created_at, updated_at, processed_at, customer_id, email, total_price, subtotal_price, total_tax, total_discounts, currency, financial_status, fulfillment_status, line_items_count, source_name)
                        VALUES (:id, :date, :date, :date, :cid, :email, :total, :sub, :tax, 0, 'USD', :fin, :ful, :items, 'web')"""),
                        {"id": oid, "date": order_date, "cid": 1000000+(i%20), "email": f"c{i}@e.com", "total": round(total*1.08,2), "sub": round(total,2), "tax": round(total*0.08,2), "fin": status, "ful": 'fulfilled' if status=='paid' else None, "items": num_items})
                    for j in range(num_items):
                        p = random.choice(products)
                        await db.execute(text("""INSERT INTO raw.shopify_order_line_items (id, order_id, product_id, variant_id, title, sku, quantity, price, total_discount, fulfillment_status, gift_card, taxable, requires_shipping)
                            VALUES (:id, :oid, :pid, :pid, :title, :sku, :qty, :price, 0, :ful, false, true, true)"""),
                            {"id": oid*100+j, "oid": oid, "pid": p['id'], "title": p['title'], "sku": p['sku'], "qty": random.randint(1,2), "price": p['price'], "ful": 'fulfilled' if status=='paid' else None})
                
                elif platform == 'amazon':
                    oid = f"AMZ-{4000000+i}"
                    total = sum(random.choice(products)['price'] * random.randint(1,2) for _ in range(num_items))
                    await db.execute(text("""INSERT INTO raw.amazon_orders (amazon_order_id, purchase_date, last_update_date, buyer_email, order_total, payment_method, order_status, number_of_items_shipped, number_of_items_unshipped, sales_channel)
                        VALUES (:id, :date, :date, :email, :total, 'Card', :status, :shipped, :unshipped, 'Amazon')"""),
                        {"id": oid, "date": order_date, "email": f"b{i}@amz.com", "total": json.dumps({'Amount': str(round(total,2)), 'CurrencyCode': 'USD'}),
                         "status": status, "shipped": num_items if status=='Shipped' else 0, "unshipped": 0 if status=='Shipped' else num_items})
                    for j in range(num_items):
                        p = random.choice(products)
                        await db.execute(text("""INSERT INTO raw.amazon_order_items (order_item_id, amazon_order_id, asin, title, seller_sku, quantity_ordered, quantity_shipped, item_price, promotion_discount)
                            VALUES (:id, :oid, :asin, :title, :sku, :qty, :shipped, :price, :disc)"""),
                            {"id": f"{oid}-{j}", "oid": oid, "asin": f"ASIN{p['id']}", "title": p['title'], "sku": p['sku'], "qty": random.randint(1,2),
                             "shipped": random.randint(1,2) if status=='Shipped' else 0, "price": json.dumps({'Amount': str(p['price'])}), "disc": json.dumps({'Amount': '0'})})
                
                elif platform == 'lazada':
                    oid = 5000000 + i
                    total = sum(random.choice(products)['price'] * 55 for _ in range(num_items))
                    await db.execute(text("""INSERT INTO raw.lazada_orders (order_id, created_at, updated_at, customer_id, buyer_email, price, items_count, voucher, payment_method, statuses)
                        VALUES (:id, :date, :date, :cid, :email, :price, :items, 0, 'COD', :status)"""),
                        {"id": oid, "date": order_date, "cid": 1000000+(i%20), "email": f"b{i}@lzd.com", "price": round(total,2), "items": num_items, "status": status})
                    for j in range(num_items):
                        p = random.choice(products)
                        await db.execute(text("""INSERT INTO raw.lazada_order_items (order_item_id, order_id, product_id, name, sku, paid_price, voucher_amount, status)
                            VALUES (:id, :oid, :pid, :name, :sku, :price, 0, :status)"""),
                            {"id": oid*100+j, "oid": oid, "pid": p['id'], "name": p['title'], "sku": p['sku'], "price": round(p['price']*55,2), "status": status})
                
                elif platform == 'shopee':
                    osn = f"SHP{6000000+i}"
                    total = sum(random.choice(products)['price'] * 55 for _ in range(num_items))
                    await db.execute(text("""INSERT INTO raw.shopee_orders (order_sn, create_time, update_time, pay_time, buyer_user_id, buyer_username, total_amount, estimated_shipping_fee, voucher_absorbed, currency, order_status)
                        VALUES (:sn, :time, :time, :pay, :uid, :user, :total, 50, 0, 'PHP', :status)"""),
                        {"sn": osn, "time": int(order_date.timestamp()), "pay": int(order_date.timestamp()) if status!='UNPAID' else None,
                         "uid": 1000000+(i%20), "user": f"buyer{i}", "total": round(total,2), "status": status})
                    for j in range(num_items):
                        p = random.choice(products)
                        await db.execute(text("""INSERT INTO raw.shopee_order_items (order_sn, item_id, item_name, model_id, model_name, model_sku, model_quantity_purchased, model_original_price, model_discounted_price)
                            VALUES (:sn, :id, :name, :mid, 'Default', :sku, :qty, :orig, :disc)"""),
                            {"sn": osn, "id": p['id'], "name": p['title'], "mid": p['id']*10, "sku": p['sku'], "qty": random.randint(1,2), "orig": round(p['price']*55*1.1,2), "disc": round(p['price']*55,2)})
            await db.commit()

        return {"status": "success", "message": "Database seeded", "data": {"customers": NUM_CUSTOMERS, "products": NUM_PRODUCTS, "orders_per_platform": NUM_ORDERS, "total_orders": NUM_ORDERS*4}}

    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Seeding failed: {str(e)}")
