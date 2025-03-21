import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import pyodbc
import os
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file, forçando a sobrescrita de variáveis existentes
load_dotenv(override=True)

# Initialize Faker
fake = Faker()
Faker.seed(42)  # For reproducibility
random.seed(42)
np.random.seed(42)

# Database connection parameters
server = os.getenv('DB_SERVER', 'localhost,1433')
database = os.getenv('DB_NAME', 'interview_db')
username = os.getenv('DB_USER', 'sa')
password = os.getenv('DB_PASSWORD', 'YourStrongPassword123!')

# Function to create database connection
def create_connection():
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;'
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Function to generate client data
def generate_clients(num_clients=50, start_id=1):
    clients = []
    
    for i in range(start_id, start_id + num_clients):
        # Limit field lengths to avoid truncation
        company_name = fake.company()[:90]
        contact_name = fake.name()[:90]
        email = fake.email()[:90]
        phone = fake.phone_number()[:15]
        address = fake.street_address()[:190]
        city = fake.city()[:45]
        state = fake.state()[:45]
        zip_code = fake.zipcode()[:15]
        country = fake.country()[:45]
        
        # Generate random registration date in the past 5 years
        registration_date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
        
        # Randomly select status
        status = random.choice(['Active', 'Inactive', 'Pending', 'Suspended'])
        
        # Random credit limit between 1000 and 100000
        credit_limit = round(random.uniform(1000, 100000), 2)
        
        # Randomly set some fields to NULL (about 5-10% chance)
        if random.random() < 0.05:
            phone = None
        if random.random() < 0.08:
            address = None
        if random.random() < 0.05:
            zip_code = None
        if random.random() < 0.05:
            credit_limit = 0
        
        clients.append({
            'client_id': i,
            'company_name': company_name,
            'contact_name': contact_name,
            'email': email,
            'phone': phone,
            'address': address,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'country': country,
            'registration_date': registration_date,
            'status': status,
            'credit_limit': credit_limit,
            'last_update': pd.to_datetime('today').date()
        })
    
    return pd.DataFrame(clients)

# Function to generate customer data
def generate_customers(clients_df, num_customers=200, start_id=1):
    customers = []
    
    # Ensure we have clients to reference
    if len(clients_df) == 0:
        print("Warning: No clients available to generate customers. Generating customers without reference.")
        client_ids = [None] * num_customers
    else:
        # Get client IDs for reference
        client_ids = clients_df['client_id'].tolist()
    
    for i in range(start_id, start_id + num_customers):
        # Limit field lengths to avoid truncation
        first_name = fake.first_name()[:45]
        last_name = fake.last_name()[:45]
        email = fake.email()[:90]
        phone = fake.phone_number()[:15]
        address = fake.street_address()[:190]
        city = fake.city()[:45]
        state = fake.state()[:45]
        zip_code = fake.zipcode()[:15]
        country = fake.country()[:45]
        
        # Intentional issue: inconsistent date formats for birth date
        birth_date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y', '%Y/%m/%d']
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime(random.choice(birth_date_formats))
        
        # Intentional issue: inconsistent date formats for registration date
        reg_date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y', '%Y/%m/%d']
        registration_date = fake.date_between(start_date='-3y', end_date='today').strftime(random.choice(reg_date_formats))
        
        # Intentional issue: inconsistent datetime formats for last login
        login_date_formats = ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M', '%Y-%m-%d %I:%M %p', '%m-%d-%Y %H:%M']
        invalid_formats = ['%Y.%m.%d', 'YYYY-MM-DD', 'DD-MM-YY', 'MM/DD', 'INVALID DATE']
        
        if random.random() < 0.8:  # 80% valid dates with inconsistent formats
            last_login = fake.date_time_between(start_date='-1y', end_date='now').strftime(random.choice(login_date_formats))
        else:  # 20% completely invalid dates
            if random.random() < 0.5:
                # Completely invalid format
                last_login = random.choice(['Invalid date', 'N/A', '0000-00-00', '2023/13/45', '31/02/2022'])
            else:
                # Date with wrong format
                last_login = fake.date_time_between(start_date='-1y', end_date='now').strftime(random.choice(invalid_formats))
        
        # Intentional issue: missing client_id for some customers
        if random.random() < 0.05 and len(client_ids) > 0:  # 5% chance of missing client_id
            client_id = None
        else:
            # Ensure we have clients to reference
            if len(client_ids) > 0:
                client_id = random.choice(client_ids)
            else:
                client_id = None
        
        # Ensure client_id is an integer or None, not an empty string
        if client_id == "":
            client_id = None
        
        # Randomly set some fields to NULL (about 5-15% chance)
        if random.random() < 0.1:
            phone = None
        if random.random() < 0.05:
            birth_date = None
        if random.random() < 0.08:
            address = None
        if random.random() < 0.05:
            last_login = None
        if random.random() < 0.01:
            zip_code = None
            
        customers.append({
            'customer_id': i,
            'client_id': client_id,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': phone,
            'birth_date': birth_date,
            'address': address,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'country': country,
            'registration_date': registration_date,
            'last_login': last_login,
            'last_update': pd.to_datetime('today').date()
        })

    
    return pd.DataFrame(customers)

# Function to generate product data
def generate_products(num_products=100, start_id=1):
    products = []
    
    # Define categories and subcategories
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Toys', 'Books', 'Health & Beauty', 'Automotive', 'Grocery', 'Office']
    
    # Define subcategories for each category
    subcategories = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'TVs', 'Cameras', 'Audio', 'Wearables', 'Gaming'],
        'Clothing': ['Men\'s', 'Women\'s', 'Children\'s', 'Shoes', 'Accessories', 'Activewear', 'Formal', 'Casual'],
        'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Bathroom', 'Bedding', 'Garden', 'Tools', 'Lighting'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Winter Sports', 'Cycling', 'Golf', 'Running'],
        'Toys': ['Action Figures', 'Dolls', 'Educational', 'Games', 'Puzzles', 'Outdoor Toys', 'Building Sets', 'Remote Control'],
        'Books': ['Fiction', 'Non-Fiction', 'Children\'s', 'Textbooks', 'Comics', 'Biography', 'Self-Help', 'Cooking'],
        'Health & Beauty': ['Skincare', 'Makeup', 'Hair Care', 'Fragrance', 'Personal Care', 'Vitamins', 'Bath & Body', 'Men\'s Grooming'],
        'Automotive': ['Interior', 'Exterior', 'Parts', 'Tools', 'Electronics', 'Accessories', 'Tires', 'Oil & Fluids'],
        'Grocery': ['Beverages', 'Snacks', 'Canned Goods', 'Dairy', 'Meat', 'Produce', 'Bakery', 'Frozen'],
        'Office': ['Stationery', 'Furniture', 'Electronics', 'Paper', 'Writing Supplies', 'Organization', 'Printers', 'Ink & Toner']
    }
    
    # Define apparel categories and subcategories
    apparel_categories = ['Clothing']
    apparel_subcategories = ['Men\'s', 'Women\'s', 'Children\'s', 'Shoes', 'Accessories', 'Activewear', 'Formal', 'Casual']
    
    for i in range(start_id, start_id + num_products):
        # Select random category and subcategory
        category = random.choice(categories)
        sub_category = random.choice(subcategories[category])
        
        # Generate product name
        product_name = f"{fake.word().capitalize()} {sub_category} {fake.word().capitalize()}"[:95]
        
        # Generate description
        description = fake.paragraph(nb_sentences=3)[:495]
        
        # Generate supplier
        supplier = fake.company()[:90]
        
        # Generate prices
        cost_price = round(random.uniform(10, 500), 2)
        selling_price = round(cost_price * random.uniform(1.1, 2.0), 2)
        
        # Intentional issue: selling price lower than cost price (15% chance)
        if random.random() < 0.05:
            selling_price = round(cost_price * random.uniform(0.5, 0.9), 2)
        
        # Generate stock quantity and min stock level
        stock_quantity = random.randint(0, 500)
        min_stock_level = random.randint(10, 100)
        
        # Determine if the product is active
        is_active = random.choice([0, 1])
        
        # Generate dates
        created_at = fake.date_time_between(start_date='-2y', end_date='now')
        updated_at = fake.date_time_between(start_date=created_at, end_date='now')
        
        # Determine if the product is apparel based on category and subcategory
        is_apparel = 0
        if category in apparel_categories or sub_category in apparel_subcategories:
            if random.random() < 0.8:  # 90% chance for apparel categories to be marked as apparel
                is_apparel = 1
        else:
            if random.random() < 0.2:  # 10% chance for non-apparel categories to be marked as apparel
                is_apparel = 1
        
        # Randomly set some fields to NULL (about 5-10% chance)
        if random.random() < 0.05:
            description = None
        if random.random() < 0.01:
            stock_quantity = None
        if random.random() < 0.05:
            min_stock_level = None
        if random.random() < 0.05:
            supplier = None
            
        products.append({
            'product_id': i,
            'product_name': product_name,
            'description': description,
            'category': category,
            'sub_category': sub_category,
            'supplier': supplier,
            'cost_price': cost_price,
            'selling_price': selling_price,
            'stock_quantity': stock_quantity,
            'min_stock_level': min_stock_level,
            'is_active': is_active,
            'is_apparel': is_apparel,
            'created_at': created_at,
            'updated_at': updated_at,
            'last_update': pd.to_datetime('today').date()
        })

    
    return pd.DataFrame(products)

# Function to generate purchase data
def generate_purchases(clients_df, customers_df, products_df, num_purchases=500, start_id=1):
    purchases = []
    
    # Check if we have enough clients, products and customers
    if len(clients_df) == 0 or len(customers_df) == 0 or len(products_df) == 0:
        print("Warning: Not enough data to generate purchases. Skipping purchase generation.")
        return pd.DataFrame(columns=[
            'purchase_id', 'client_id', 'customer_id', 'product_id', 'purchase_date',
            'quantity', 'unit_price', 'total_amount', 'payment_method', 'payment_status',
            'shipping_address', 'shipping_city', 'shipping_state', 'shipping_zip',
            'shipping_country', 'shipping_date', 'delivery_date', 'notes'
        ])
    
    # Get client, customer, and product IDs for reference
    client_ids = clients_df['client_id'].tolist()
    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()
    
    # Select approximately 30% of clients that will have the product name issue
    problematic_clients = random.sample(client_ids, int(len(client_ids) * 0.3))
    
    # Create a dictionary to map product IDs to product names for easy lookup
    product_name_map = dict(zip(products_df['product_id'], products_df['product_name']))
    
    # Create a dictionary to map product IDs to prices for easy lookup
    product_price_map = dict(zip(products_df['product_id'], products_df['selling_price']))
    
    for i in range(start_id, start_id + num_purchases):
        # Select random client, customer, and product
        client_id = random.choice(client_ids)
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        
        # Get the actual product name for this product ID
        product_name = product_name_map.get(product_id, f"Unknown Product {product_id}")
        
        # Determine if this client has the product name issue
        if client_id in problematic_clients:
            # Client with issue - use product name instead of ID
            product_id_value = product_name
        else:
            # Normal client - just convert ID to string
            product_id_value = str(product_id)
        
        # Limit field lengths to avoid truncation
        shipping_address = fake.street_address()[:190]
        shipping_city = fake.city()[:45]
        shipping_state = fake.state()[:45]
        shipping_zip = fake.zipcode()[:15]
        shipping_country = fake.country()[:45]
        
        # Generate purchase date in the past 2 years
        purchase_date = fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
        
        # Generate shipping date 1-5 days after purchase
        shipping_date_obj = datetime.strptime(purchase_date, '%Y-%m-%d') + timedelta(days=random.randint(1, 5))
        shipping_date = shipping_date_obj.strftime('%Y-%m-%d')
        
        # Generate delivery date 1-10 days after shipping
        delivery_date_obj = shipping_date_obj + timedelta(days=random.randint(1, 10))
        delivery_date = delivery_date_obj.strftime('%Y-%m-%d')
        
        # Generate quantity between 1 and 10 - Garantindo que seja um inteiro positivo
        quantity = max(1, random.randint(1, 10))
        
        # Get the price from the product
        unit_price = product_price_map.get(product_id, random.uniform(5, 1000))
        
        # Calculate total amount
        total_amount = round(quantity * unit_price, 2)
        
        # Select payment method
        payment_method = random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Cash on Delivery', 'Gift Card'])
        
        # Select payment status
        payment_status = random.choice(['Paid', 'Pending', 'Failed', 'Refunded', 'Partially Paid'])
        
        # Generate notes
        notes = fake.sentence()[:195] if random.random() < 0.3 else None
        
        # Randomly set some fields to NULL (about 5-15% chance)
        if random.random() < 0.02:
            shipping_address = None
        if random.random() < 0.1:
            shipping_zip = None
        if random.random() < 0.05:
            delivery_date = None
        if random.random() < 0.15:
            notes = None
        if random.random() < 0.02:
            payment_method = None
            
        purchases.append({
            'purchase_id': i,
            'client_id': client_id,
            'customer_id': customer_id,
            'product_id': product_id_value,
            'purchase_date': purchase_date,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'payment_method': payment_method,
            'payment_status': payment_status,
            'shipping_address': shipping_address,
            'shipping_city': shipping_city,
            'shipping_state': shipping_state,
            'shipping_zip': shipping_zip,
            'shipping_country': shipping_country,
            'shipping_date': shipping_date,
            'delivery_date': delivery_date,
            'notes': notes,
            'last_update': pd.to_datetime('today').date()
        })
    
    return pd.DataFrame(purchases)

# Function to generate return data
def generate_returns(purchases_df, clients_df, customers_df, products_df, num_returns=100, start_id=1):
    returns = []
    
    # Check if we have enough purchases
    if len(purchases_df) == 0:
        print("Warning: No purchases available to generate returns. Skipping return generation.")
        return pd.DataFrame(columns=[
            'return_id', 'purchase_id', 'client_id', 'customer_id', 'product_id',
            'return_date', 'quantity', 'reason', 'status', 'refund_amount', 'notes'
        ])
    
    # Get purchase IDs for reference
    purchase_ids = purchases_df['purchase_id'].tolist()
    
    # Ensure we don't try to generate more returns than we have purchases
    num_returns = min(num_returns, len(purchase_ids))
    
    # Randomly select purchases to be returned (without replacement to avoid duplicates)
    selected_purchase_ids = random.sample(purchase_ids, num_returns)
    
    # Verificar quais colunas existem no DataFrame de compras
    required_columns = ['client_id', 'customer_id', 'product_id', 'purchase_date', 'quantity', 'total_amount']
    missing_columns = [col for col in required_columns if col not in purchases_df.columns]
    
    if missing_columns:
        print(f"Warning: Missing columns in purchases data: {missing_columns}")
        # Adicionar colunas faltantes com valores padrão
        for col in missing_columns:
            if col == 'quantity':
                purchases_df[col] = 1  # Valor padrão para quantity
            elif col == 'total_amount':
                purchases_df[col] = 100.0  # Valor padrão para total_amount
            else:
                purchases_df[col] = None
    
    # Create dictionaries to map purchase IDs to other fields for easy lookup
    purchase_client_map = dict(zip(purchases_df['purchase_id'], purchases_df['client_id']))
    purchase_customer_map = dict(zip(purchases_df['purchase_id'], purchases_df['customer_id']))
    purchase_product_map = dict(zip(purchases_df['purchase_id'], purchases_df['product_id']))
    
    # Garantir que quantity e total_amount existam e tenham valores válidos
    if 'quantity' in purchases_df.columns:
        purchase_quantity_map = dict(zip(purchases_df['purchase_id'], purchases_df['quantity']))
    else:
        # Se a coluna não existir, usar valor padrão 1
        purchase_quantity_map = {pid: 1 for pid in purchases_df['purchase_id']}
        
    if 'total_amount' in purchases_df.columns:
        purchase_amount_map = dict(zip(purchases_df['purchase_id'], purchases_df['total_amount']))
    else:
        # Se a coluna não existir, usar valor padrão 100.0
        purchase_amount_map = {pid: 100.0 for pid in purchases_df['purchase_id']}
        
    purchase_date_map = dict(zip(purchases_df['purchase_id'], purchases_df['purchase_date']))
    
    # Get the list of problematic clients from the memory
    problematic_clients = []
    if len(clients_df) > 0:
        client_ids = clients_df['client_id'].tolist()
        problematic_clients = random.sample(client_ids, int(len(client_ids) * 0.3))
    
    for i, purchase_id in enumerate(selected_purchase_ids, start=start_id):
        # Get related fields from the purchase
        client_id = purchase_client_map.get(purchase_id)
        customer_id = purchase_customer_map.get(purchase_id)
        product_id = purchase_product_map.get(purchase_id)
        purchase_quantity = purchase_quantity_map.get(purchase_id, 1)
        purchase_amount = purchase_amount_map.get(purchase_id, 0)
        purchase_date = purchase_date_map.get(purchase_id)
        
        # Generate return date 1-30 days after purchase date
        if purchase_date:
            try:
                purchase_date_obj = datetime.strptime(purchase_date, '%Y-%m-%d')
                return_date_obj = purchase_date_obj + timedelta(days=random.randint(1, 30))
                return_date = return_date_obj.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                # Handle invalid purchase date
                return_date = fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
        else:
            # Handle missing purchase date
            return_date = fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
        
        # Generate return quantity (usually less than or equal to purchase quantity)
        # Garantindo que seja um inteiro positivo
        if isinstance(purchase_quantity, (int, float)) and purchase_quantity > 0:
            return_quantity = max(1, random.randint(1, max(1, int(purchase_quantity))))
        else:
            return_quantity = 1
        
        # Calculate refund amount (proportional to return quantity)
        refund_amount = round((return_quantity / purchase_quantity) * float(purchase_amount), 2) if purchase_quantity > 0 else 0
        
        # Select reason for return
        reason = random.choice([
            'Defective product', 'Wrong item received', 'Not as described',
            'Arrived too late', 'No longer needed', 'Better price found elsewhere',
            'Missing parts', 'Damaged during shipping', 'Changed mind', 'Ordered by mistake'
        ])
        
        # Select return status
        status = random.choice(['Approved', 'Pending', 'Rejected', 'Completed', 'Processing'])
        
        # Generate notes
        notes = fake.sentence()[:195] if random.random() < 0.3 else None
        
        # Randomly set some fields to NULL (about 5-15% chance)
        if random.random() < 0.1:
            reason = None
        if random.random() < 0.01:
            refund_amount = None
        if random.random() < 0.15:
            notes = None
            
        returns.append({
            'return_id': i,
            'purchase_id': purchase_id,
            'client_id': client_id,
            'customer_id': customer_id,
            'product_id': product_id,  # Mantém o mesmo valor de product_id da compra (nome ou ID)
            'return_date': return_date,
            'quantity': return_quantity,
            'reason': reason,
            'status': status,
            'refund_amount': refund_amount,
            'notes': notes,
            'last_update': pd.to_datetime('today').date()
        })
    
    return pd.DataFrame(returns)

# Function to insert data into database
def insert_data(conn, df, table_name):
    cursor = conn.cursor()
    
    # Get column names
    columns = df.columns.tolist()
    
    # Remove internal columns that start with underscore
    columns = [col for col in columns if not col.startswith('_')]
    
    # Prepare placeholders for parameterized query
    placeholders = ', '.join(['?' for _ in columns])
    
    # Prepare insert query
    query = f"INSERT INTO dbo.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    # Insert data row by row
    successful_inserts = 0
    for _, row in df.iterrows():
        values = [row[col] for col in columns]
        
        # Process values to ensure they match expected data types
        for i, val in enumerate(values):
            # Handle text values - limit length to avoid truncation
            if isinstance(val, str) and len(val) > 4000:
                values[i] = val[:4000]
            
            # Handle empty strings for numeric columns
            if val == "" and columns[i] in ['client_id', 'customer_id', 'purchase_id', 'return_id', 'quantity', 'price', 'total_amount', 'cost_price', 'selling_price', 'stock_quantity', 'min_stock_level', 'refund_amount', 'credit_limit']:
                values[i] = None
                
            # Ensure float values are valid
            if columns[i] in ['price', 'total_amount', 'credit_limit', 'cost_price', 'selling_price', 'refund_amount'] and val is not None:
                try:
                    values[i] = float(val) if val != "" else None
                except (ValueError, TypeError):
                    values[i] = None
                    
            # Ensure integer values are valid - EXCEPT for product_id which may contain product names
            if columns[i] in ['client_id', 'customer_id', 'purchase_id', 'return_id', 'stock_quantity', 'min_stock_level', 'is_active', 'is_apparel'] and val is not None:
                try:
                    values[i] = int(val) if val != "" else None
                except (ValueError, TypeError):
                    values[i] = None

            # Special handling for quantity field to ensure it's always a positive integer
            if columns[i] == 'quantity' and val is not None:
                try:
                    # Converter para inteiro e garantir que seja positivo
                    quantity_val = int(val) if val != "" else 1
                    values[i] = max(1, quantity_val)  # Garantir que seja pelo menos 1
                except (ValueError, TypeError):
                    values[i] = 1  # Default to 1 if conversion fails

            # Special handling for product_id - only convert to int if it's a numeric string
            if columns[i] == 'product_id' and val is not None and val != "":
                if isinstance(val, str) and val.isdigit():
                    # It's a numeric string, convert to int
                    values[i] = int(val)
                # Otherwise, keep it as is (product name for problematic clients)
        
        try:
            cursor.execute(query, values)
            successful_inserts += 1
        except pyodbc.Error as e:
            print(f"Error inserting into {table_name}: {e}")
            print(f"Row data: {dict(zip(columns, values))}")
            continue
    
    conn.commit()
    print(f"Inserted {successful_inserts} rows into {table_name}")

# Function to generate and insert all data
def main(append_only=False, num_clients=50, num_customers=200, num_products=100, num_purchases=500, num_returns=100):
    print(f"Running in {'append-only' if append_only else 'recreate tables'} mode")
    
    # Check if tables exist and have data
    conn = create_connection()
    if conn is None:
        print("Failed to connect to database. Exiting.")
        return
    
    cursor = conn.cursor()
    
    # Check if we're in append mode and tables already have data
    if append_only:
        try:
            cursor.execute("SELECT COUNT(*) FROM client")
            client_count = cursor.fetchone()[0]
            
            if client_count > 0:
                print(f"Found {client_count} existing clients. Will generate new data with higher IDs.")
                
                # Get the highest IDs from each table to avoid conflicts
                cursor.execute("SELECT MAX(client_id) FROM client")
                max_client_id = cursor.fetchone()[0] or 0
                
                cursor.execute("SELECT MAX(customer_id) FROM customer")
                max_customer_id = cursor.fetchone()[0] or 0
                
                cursor.execute("SELECT MAX(product_id) FROM products")
                max_product_id = cursor.fetchone()[0] or 0
                
                cursor.execute("SELECT MAX(purchase_id) FROM purchases")
                max_purchase_id = cursor.fetchone()[0] or 0
                
                cursor.execute("SELECT MAX(return_id) FROM returns")
                max_return_id = cursor.fetchone()[0] or 0
                
                # Adjust starting IDs for new data
                start_client_id = max_client_id + 1
                start_customer_id = max_customer_id + 1
                start_product_id = max_product_id + 1
                start_purchase_id = max_purchase_id + 1
                start_return_id = max_return_id + 1
            else:
                # No existing data, use default starting IDs
                start_client_id = 1
                start_customer_id = 1
                start_product_id = 1
                start_purchase_id = 1
                start_return_id = 1
        except Exception as e:
            print(f"Error checking existing data: {e}")
            print("Will use default starting IDs")
            start_client_id = 1
            start_customer_id = 1
            start_product_id = 1
            start_purchase_id = 1
            start_return_id = 1
    else:
        # Not in append mode, use default starting IDs
        start_client_id = 1
        start_customer_id = 1
        start_product_id = 1
        start_purchase_id = 1
        start_return_id = 1
    
    # Generate data
    print("Generating client data...")
    clients_df = generate_clients(num_clients, start_id=start_client_id)
    
    print("Inserting client data...")
    insert_data(conn, clients_df, 'client')
    
    print("Generating customer data...")
    customers_df = generate_customers(clients_df, num_customers, start_id=start_customer_id)
    
    print("Inserting customer data...")
    insert_data(conn, customers_df, 'customer')
    
    print("Generating product data...")
    products_df = generate_products(num_products, start_id=start_product_id)
    
    print("Inserting product data...")
    insert_data(conn, products_df, 'products')
    
    print("Generating purchase data...")
    purchases_df = generate_purchases(clients_df, customers_df, products_df, num_purchases, start_id=start_purchase_id)
    
    print("Inserting purchase data...")
    insert_data(conn, purchases_df, 'purchases')
    
    # After inserting purchases, retrieve the actual purchases from the database
    print("Retrieving actual purchases from database...")
    try:
        # Tentar obter todos os campos necessários
        cursor.execute("SELECT purchase_id, client_id, customer_id, product_id, purchase_date, quantity, total_amount FROM purchases")
        actual_purchases = []
        for row in cursor.fetchall():
            actual_purchases.append({
                'purchase_id': row[0],
                'client_id': row[1],
                'customer_id': row[2],
                'product_id': row[3],
                'purchase_date': row[4],
                'quantity': row[5],
                'total_amount': row[6]
            })
    except Exception as e:
        print(f"Error retrieving purchases with all fields: {e}")
        print("Trying with fewer fields...")
        
        # Tentar com menos campos se a primeira tentativa falhar
        try:
            cursor.execute("SELECT purchase_id, client_id, customer_id, product_id, purchase_date FROM purchases")
            actual_purchases = []
            for row in cursor.fetchall():
                actual_purchases.append({
                    'purchase_id': row[0],
                    'client_id': row[1],
                    'customer_id': row[2],
                    'product_id': row[3],
                    'purchase_date': row[4],
                    'quantity': 1,  # Valor padrão para quantity
                    'total_amount': 100.0  # Valor padrão para total_amount
                })
        except Exception as e:
            print(f"Error retrieving purchases with fewer fields: {e}")
            actual_purchases = []
    
    if not actual_purchases:
        print("Warning: No purchases available in the database to generate returns.")
    else:
        print(f"Found {len(actual_purchases)} purchases in database for generating returns.")
        
        # Convert to DataFrame
        actual_purchases_df = pd.DataFrame(actual_purchases)
        
        print("Generating return data based on actual purchases...")
        returns_df = generate_returns(actual_purchases_df, clients_df, customers_df, products_df, 
                                      num_returns=min(num_returns, len(actual_purchases)), 
                                      start_id=start_return_id)
        
        print("Inserting return data...")
        insert_data(conn, returns_df, 'returns')
    
    # Close connection
    conn.close()
    
    print("Data insertion completed successfully!")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate and insert data into SQL Server database')
    parser.add_argument('--append', action='store_true', help='Append data to existing tables instead of recreating them')
    parser.add_argument('--clients', type=int, default=50, help='Number of clients to generate')
    parser.add_argument('--customers', type=int, default=200, help='Number of customers to generate')
    parser.add_argument('--products', type=int, default=100, help='Number of products to generate')
    parser.add_argument('--purchases', type=int, default=500, help='Number of purchases to generate')
    parser.add_argument('--returns', type=int, default=100, help='Number of returns to generate')
    
    args = parser.parse_args()
    
    main(
        append_only=args.append,
        num_clients=args.clients,
        num_customers=args.customers,
        num_products=args.products,
        num_purchases=args.purchases,
        num_returns=args.returns
    )
