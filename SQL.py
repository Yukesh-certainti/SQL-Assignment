import psycopg2
import pandas as pd

# Database connection details
DB_NAME = "ShopDetails"
DB_USER = "postgres"
DB_PASSWORD = "yukesh8585"
DB_HOST = "localhost"
DB_PORT = "5432"
                                                # -- Task 1: Schema Creation 
                                                # --Part 1: Database Design & Schema Creation

# Function to connect to the database
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("✅ Database connected successfully!")
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None

# Function to create tables (IF NOT EXISTS)
def create_tables(conn):
    try:
        cur = conn.cursor()

        # Creating the tables 
        # -- 1. Stores Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stores (
                store_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                location VARCHAR(255) NOT NULL,
                manager_id INT
            );
        """)
        # -- 2. Employees Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                employee_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                role VARCHAR(100) NOT NULL,
                store_id INT REFERENCES stores(store_id) ON DELETE SET NULL,
                salary DECIMAL(10,2) CHECK (salary >= 0),
                manager_id INT REFERENCES employees(employee_id) ON DELETE SET NULL
            );
        """)
        # -- 3. Customers Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                phone VARCHAR(20) UNIQUE NOT NULL,
                city VARCHAR(255) NOT NULL
            );
        """)
        # -- 4. Suppliers Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                contact_person VARCHAR(255) NOT NULL,
                phone VARCHAR(20) UNIQUE NOT NULL,
                city VARCHAR(255) NOT NULL
            );
        """)
        # -- 5. Products Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) CHECK (price >= 0),
                stock INT CHECK (stock >= 0),
                supplier_id INT REFERENCES suppliers(supplier_id) ON DELETE CASCADE
            );
        """)
        # -- 6. Orders Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INT PRIMARY KEY,
                customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE,
                store_id INT REFERENCES stores(store_id) ON DELETE SET NULL,
                order_date DATE NOT NULL DEFAULT CURRENT_DATE,
                total_amount DECIMAL(10,2) CHECK (total_amount >= 0)
            );
        """)
        # -- 7. Order_Items Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                order_item_id INT PRIMARY KEY,
                order_id INT REFERENCES orders(order_id) ON DELETE CASCADE,
                product_id INT REFERENCES products(product_id) ON DELETE CASCADE,
                quantity INT CHECK (quantity > 0),
                price DECIMAL(10,2) CHECK (price >= 0)
            );
        """)
        # -- 8. Payments Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                payment_id INT PRIMARY KEY,
                order_id INT REFERENCES orders(order_id) ON DELETE CASCADE,
                amount DECIMAL(10,2) CHECK (amount >= 0),
                payment_method VARCHAR(50) NOT NULL,
                payment_date DATE NOT NULL DEFAULT CURRENT_DATE
            );
        """)

        conn.commit()
        cur.close()
        print("✅ Tables created successfully (if not already existing)!")

    except Exception as e:
        print(f"❌ Error creating tables: {e}")

                                            # -- Part 2: Indexing for Performance Optimization 

# -- Task 2: Create Indexes 

def create_indexes(conn):
    try:
        cur = conn.cursor()

        # -- Index on product name for faster search
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_product_name
            ON products (name);
        """)

        # -- Composite index on (customer_id, order_date) for fast order retrieval
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_customer_order_date
            ON orders (customer_id, order_date);
        """)

        conn.commit()
        cur.close()
        print("✅ Indexes created successfully!")

    except Exception as e:
        print(f"❌ Error creating indexes: {e}")


                                                # -- Part 3: Views and Triggers 

# -- Task 3: Create Views
def create_views(conn):
    try:
        cur = conn.cursor()

        # View for top-selling products based on total quantity sold
        cur.execute("""
            CREATE OR REPLACE VIEW top_selling_products AS
            SELECT 
                p.product_id,
                p.name AS product_name,
                SUM(oi.quantity) AS total_quantity_sold
            FROM order_items oi
            JOIN products p ON oi.product_id = p.product_id
            GROUP BY p.product_id, p.name
            ORDER BY total_quantity_sold DESC;
        """)

        # View for total revenue per store
        cur.execute("""
            CREATE OR REPLACE VIEW store_revenue AS
            SELECT 
                s.store_id,
                s.name AS store_name,
                COALESCE(SUM(o.total_amount), 0) AS total_revenue
            FROM stores s
            LEFT JOIN orders o ON s.store_id = o.store_id
            GROUP BY s.store_id, s.name
            ORDER BY total_revenue DESC;
        """)

        conn.commit()
        cur.close()
        print("✅ Views created successfully!")

    except Exception as e:
        print(f"❌ Error creating views: {e}")


                                        # -- Task 4: Create Triggers 
#  -- trigger to prevent orders for out-of-stock products.      
# Function to create triggers
def create_triggers(conn):
    try:
        cur = conn.cursor()

        # 1️ Trigger to prevent orders for out-of-stock products
        cur.execute("""
            CREATE OR REPLACE FUNCTION prevent_out_of_stock_orders()
            RETURNS TRIGGER AS $$
            BEGIN
                -- Check if there is enough stock
                IF (SELECT stock FROM products WHERE product_id = NEW.product_id) < NEW.quantity THEN
                    RAISE EXCEPTION 'Cannot place order: Not enough stock for product ID %', NEW.product_id;
                END IF;

                -- Reduce stock after successful order placement
                UPDATE products
                SET stock = stock - NEW.quantity
                WHERE product_id = NEW.product_id;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        cur.execute("""
            CREATE OR REPLACE TRIGGER check_stock_before_order
            BEFORE INSERT ON order_items
            FOR EACH ROW
            EXECUTE FUNCTION prevent_out_of_stock_orders();
        """)

        #  Create an audit table for deleted employees
        cur.execute("""
            CREATE TABLE IF NOT EXISTS employee_audit (
                audit_id SERIAL PRIMARY KEY,
                employee_id INT,
                name VARCHAR(255),
                role VARCHAR(255),
                store_id INT,
                salary DECIMAL(10,2),
                manager_id INT,
                deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 2 Trigger to log deleted employees before deletion
        cur.execute("""
            CREATE OR REPLACE FUNCTION log_deleted_employee()
            RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO employee_audit (employee_id, name, role, store_id, salary, manager_id)
                VALUES (OLD.employee_id, OLD.name, OLD.role, OLD.store_id, OLD.salary, OLD.manager_id);
                RETURN OLD;
            END;
            $$ LANGUAGE plpgsql;
        """)

        cur.execute("""
            CREATE OR REPLACE TRIGGER log_employee_deletion
            BEFORE DELETE ON employees
            FOR EACH ROW
            EXECUTE FUNCTION log_deleted_employee();
        """)

        conn.commit()
        cur.close()
        print("✅ Triggers created successfully!")

    except Exception as e:
        print(f"❌ Error creating triggers: {e}")


                                    # -- Part 4: Data Insertion & Importing from Files 
                                        #   Task 5: Insert Data Manually 
                                    
# Function to insert sample data
def insert_sample_data(conn):
    try:
        cur = conn.cursor()

        # Insert Stores(5)
        cur.execute("""
            INSERT INTO stores (store_id, name, location, manager_id) VALUES
            (1, 'ShopEase Mart - 1', 'Tiruppur', 1),
            (2, 'ShopEase Mart - 2', 'Coimbatore', 2),
            (3, 'ShopEase Mart - 3', 'Erode', 3),
            (4, 'ShopEase Mart - 4', 'Ooty', 4),
            (5, 'ShopEase Mart - 5', 'Namakkal', 5)
            ON CONFLICT (store_id) DO NOTHING;
        """)

        # Insert Employees(10)
        cur.execute("""
            INSERT INTO employees (employee_id, name, role, store_id, salary, manager_id) VALUES
            (1, 'Yukesh', 'CEO', NULL, 150000, NULL),
            (2, 'Vishwa', 'Manager', 1, 70000, 1),
            (3, 'Vinayagam', 'Cashier', 1, 30000, 2),
            (4, 'Varsha', 'Salesperson', 1, 40000, 2), 
            (5, 'Sreena', 'Manager', 2, 75000, 1),
            (6, 'Thiyanesh', 'Cashier', 2, 32000, 5), 
            (7, 'Elamathy', 'Salesperson', 2, 35000, 5), 
            (8, 'Atheshwar', 'Manager', 3, 72000, 1),
            (9, 'Sriharan', 'Cashier', 3, 29000, 8), 
            (10, 'Jai', 'Salesperson', 3, 38000, 8) 
            ON CONFLICT (employee_id) DO NOTHING;
        """)

        # Insert Suppliers(10)
        cur.execute("""
            INSERT INTO suppliers (supplier_id, name, contact_person, phone, city) VALUES
            (1, 'Indian Textiles Ltd', 'Ramesh Kumar', '9876543210', 'Surat, Gujarat'),
            (2, 'Mumbai Electronics Corp', 'Anita Desai', '9988776655', 'Mumbai, Maharashtra'),
            (3, 'Delhi Footwear Co', 'Sandeep Sharma', '9898989898', 'Delhi, Delhi'),
            (4, 'Bangalore Home Appliances', 'Prakash Rao', '9765432109', 'Bangalore, Karnataka'),
            (5, 'Kolkata Book Depot', 'Sumitra Banerjee', '9977889900', 'Kolkata, West Bengal'),
            (6, 'Hyderabad IT Products', 'Krishna Reddy', '9878787878', 'Hyderabad, Telangana'),
            (7, 'Pune Fashion House', 'Meena Joshi', '9966554433', 'Pune, Maharashtra'),
            (8, 'Ahmedabad Dairy Products', 'Gopal Patel', '9897979797', 'Ahmedabad, Gujarat'),
            (9, 'Jaipur Handicrafts', 'Rajesh Meena', '9789898989', 'Jaipur, Rajasthan'),
            (10, 'Lucknow Stationery Mart', 'Kamal Singh', '9987878787', 'Lucknow, Uttar Pradesh')
            ON CONFLICT (supplier_id) DO NOTHING;
        """)

        # Insert Customers(20)
        cur.execute("""
            INSERT INTO customers (customer_id, name, email, phone, city) VALUES
            (1, 'Aishwarya Iyer', 'aishwarya@example.com', '9876543211', 'Chennai, Tamil Nadu'),
            (2, 'Rahul Desai', 'rahul@example.com', '9988776656', 'Mumbai, Maharashtra'),
            (3, 'Priya Sharma', 'priya@example.com', '9898989899', 'Delhi, Delhi'),
            (4, 'Vikram Rao', 'vikram@example.com', '9765432110', 'Bangalore, Karnataka'),
            (5, 'Sneha Banerjee', 'sneha@example.com', '9977889901', 'Kolkata, West Bengal'),
            (6, 'Krishna Reddy', 'krishna@example.com', '9878787879', 'Hyderabad, Telangana'),
            (7, 'Meena Joshi', 'meena@example.com', '9966554434', 'Pune, Maharashtra'),
            (8, 'Gopal Patel', 'gopal@example.com', '9897979798', 'Ahmedabad, Gujarat'),
            (9, 'Rajesh Meena', 'rajesh@example.com', '9789898990', 'Jaipur, Rajasthan'),
            (10, 'Kamal Singh', 'kamal@example.com', '9987878788', 'Lucknow, Uttar Pradesh'),
            (11, 'Divya Gupta', 'divya@example.com', '9876543212', 'Chandigarh, Punjab'),
            (12, 'Rohit Agarwal', 'rohit@example.com', '9988776657', 'Indore, Madhya Pradesh'),
            (13, 'Kavita Mishra', 'kavita@example.com', '9898989900', 'Bhopal, Madhya Pradesh'),
            (14, 'Manish Tiwari', 'manish@example.com', '9765432111', 'Visakhapatnam, Andhra Pradesh'),
            (15, 'Pooja Pandey', 'pooja@example.com', '9977889902', 'Surat, Gujarat'),
            (16, 'Sandeep Kumar', 'sandeep@example.com', '9878787880', 'Nagpur, Maharashtra'),
            (17, 'Anjali Singh', 'anjali@example.com', '9966554435', 'Patna, Bihar'),
            (18, 'Rakesh Yadav', 'rakesh@example.com', '9897979799', 'Vadodara, Gujarat'),
            (19, 'Sunita Verma', 'sunita@example.com', '9789898991', 'Ludhiana, Punjab'),
            (20, 'Amit Chauhan', 'amit@example.com', '9987878789', 'Kanpur, Uttar Pradesh')
            ON CONFLICT (customer_id) DO NOTHING;
        """)

        # Insert Products(50)
        cur.execute("""
            INSERT INTO products (product_id, name, category, price, stock, supplier_id) VALUES
            (1, 'Cotton Kurta', 'Clothing', 800.00, 100, 1),
            (2, 'Silk Saree', 'Clothing', 2500.00, 50, 1),
            (3, 'Mens Formal Shirt', 'Clothing', 1200.00, 80, 1),
            (4, 'Women Leggings', 'Clothing', 500.00, 150, 1),
            (5, 'Leather Sandals', 'Footwear', 900.00, 60, 3),
            (6, 'Sports Shoes', 'Footwear', 1500.00, 40, 3),
            (7, 'Formal Shoes', 'Footwear', 2000.00, 30, 3),
            (8, 'Handmade Juttis', 'Footwear', 700.00, 90, 3),
            (9, 'Pressure Cooker', 'Kitchen Appliances', 1800.00, 25, 4),
            (10, 'Mixer Grinder', 'Kitchen Appliances', 2200.00, 20, 4),
            (11, 'Induction Cooktop', 'Kitchen Appliances', 2500.00, 15, 4),
            (12, 'Water Purifier', 'Kitchen Appliances', 3000.00, 10, 4),
            (13, 'Bhagavad Gita', 'Books', 300.00, 200, 5),
            (14, 'Indian Cookbooks', 'Books', 500.00, 150, 5),
            (15, 'Fiction Novels', 'Books', 400.00, 180, 5),
            (16, 'Childrens Storybooks', 'Books', 250.00, 250, 5),
            (17, 'School Notebooks', 'Stationery', 50.00, 500, 10),
            (18, 'Ballpoint Pens', 'Stationery', 20.00, 1000, 10),
            (19, 'Geometry Box', 'Stationery', 150.00, 300, 10),
            (20, 'Art Supplies', 'Stationery', 300.00, 200, 10),
            (21, 'Smart LED TV', 'Electronics', 35000.00, 15, 2),
            (22, 'Refrigerator', 'Electronics', 28000.00, 10, 2),
            (23, 'Washing Machine', 'Electronics', 25000.00, 12, 2),
            (24, 'Mobile Phone', 'Electronics', 18000.00, 30, 2),
            (25, 'Laptop Computer', 'Electronics', 45000.00, 8, 2),
            (26, 'Gold Bangles', 'Jewelry', 20000.00, 5, NULL),
            (27, 'Silver Earrings', 'Jewelry', 5000.00, 10, NULL),
            (28, 'Diamond Necklace', 'Jewelry', 50000.00, 2, NULL),
            (29, 'Artificial Jewelry Set', 'Jewelry', 1000.00, 20, NULL),
            (30, 'Wooden Toys', 'Toys', 800.00, 50, NULL),
            (31, 'Board Games', 'Toys', 1200.00, 30, NULL),
            (32, 'Soft Toys', 'Toys', 500.00, 80, NULL),
            (33, 'Remote Control Cars', 'Toys', 1800.00, 20, NULL),
            (34, 'Yoga Mat', 'Sports & Fitness', 600.00, 100, NULL),
            (35, 'Cricket Bat', 'Sports & Fitness', 1500.00, 40, NULL),
            (36, 'Badminton Racket', 'Sports & Fitness', 1000.00, 60, NULL),
            (37, 'Dumbbell Set', 'Sports & Fitness', 2500.00, 25, NULL),
            (38, 'Herbal Tea', 'Grocery', 200.00, 200, 8),
            (39, 'Indian Spices', 'Grocery', 300.00, 150, 8),
            (40, 'Basmati Rice', 'Grocery', 800.00, 100, 8),
            (41, 'Organic Pulses', 'Grocery', 400.00, 120, 8),
            (42, 'Handloom Shawl', 'Handicrafts', 1200.00, 30, 9),
            (43, 'Brass Idols', 'Handicrafts', 1800.00, 20, 9),
            (44, 'Rajasthani Paintings', 'Handicrafts', 2500.00, 15, 9),
            (45, 'Pottery Items', 'Handicrafts', 500.00, 50, 9),
            (46, 'Ayurvedic Shampoo', 'Personal Care', 300.00, 100, NULL),
            (47, 'Natural Soaps', 'Personal Care', 200.00, 120, NULL),
            (48, 'Essential Oils', 'Personal Care', 500.00, 80, NULL),
            (49, 'Herbal Toothpaste', 'Personal Care', 150.00, 150, NULL),
            (50, 'Wall Clock', 'Home Decor', 700.00, 60, NULL)
            ON CONFLICT (product_id) DO NOTHING;
        """)

        # Insert Orders(100)
        cur.execute("""
            INSERT INTO orders (order_id, customer_id, store_id, order_date, total_amount) VALUES
            (1, 1, 1, '2023-10-26', 50000.00),
            (2, 2, 2, '2023-10-26', 1000.00),
            (3, 3, 3, '2023-10-26', 350.00),
            (4, 4, 4, '2023-10-26', 25000.00),
            (5, 5, 5, '2023-10-26', 500.00),
            (6, 6, 1, '2023-10-27', 30000.00),
            (7, 7, 2, '2023-10-27', 1500.00),
            (8, 8, 3, '2023-10-27', 50.00),
            (9, 9, 4, '2023-10-27', 800.00),
            (10, 10, 5, '2023-10-27', 600.00),
            (11, 11, 1, '2023-10-28', 20000.00),
            (12, 12, 2, '2023-10-28', 2000.00),
            (13, 13, 3, '2023-10-28', 150.00),
            (14, 14, 4, '2023-10-28', 1200.00),
            (15, 15, 5, '2023-10-28', 400.00),
            (16, 16, 1, '2023-10-29', 2000.00),
            (17, 17, 2, '2023-10-29', 2500.00),
            (18, 18, 3, '2023-10-29', 40.00),
            (19, 19, 4, '2023-10-29', 600.00),
            (20, 20, 5, '2023-10-29', 300.00),
            (21, 1, 2, '2024-10-30', 10000.00),
            (22, 2, 3, '2024-10-30', 1800.00),
            (23, 3, 4, '2024-10-30', 100.00),
            (24, 4, 5, '2024-10-30', 700.00),
            (25, 5, 1, '2024-10-30', 700.00),
            (26, 6, 2, '2024-10-31', 40000.00),
            (27, 7, 3, '2024-10-31', 3000.00),
            (28, 8, 4, '2024-10-31', 20.00),
            (29, 9, 5, '2024-10-31', 400.00),
            (30, 10, 1, '2024-10-31', 800.00),
            (31, 11, 2, '2024-11-01', 8000.00),
            (32, 12, 3, '2024-11-01', 800.00),
            (33, 13, 4, '2024-11-01', 120.00),
            (34, 14, 5, '2024-11-01', 900.00),
            (35, 15, 1, '2024-11-01', 900.00),
            (36, 16, 2, '2024-11-02', 1500.00),
            (37, 17, 3, '2024-11-02', 300.00),
            (38, 18, 4, '2024-11-02', 500.00),
            (39, 19, 5, '2024-11-02', 200.00),
            (40, 20, 1, '2024-11-02', 1200.00),
            (41, 1, 3, '2025-01-03', 800.00),
            (42, 2, 4, '2025-01-03', 700.00),
            (43, 3, 5, '2025-01-03', 80.00),
            (44, 4, 1, '2025-01-03', 1000.00),
            (45, 5, 2, '2025-01-03', 1000.00),
            (46, 6, 3, '2025-01-04', 500.00),
            (47, 7, 4, '2025-01-04', 200.00),
            (48, 8, 5, '2025-01-04', 60.00),
            (49, 9, 1, '2025-01-04', 1500.00),
            (50, 10, 2, '2025-01-04', 2500.00),
            (51, 11, 3, '2025-01-05', 5000.00),
            (52, 12, 4, '2025-01-05', 1000.00),
            (53, 13, 5, '2025-01-05', 350.00),
            (54, 14, 1, '2025-01-05', 25000.00),
            (55, 15, 2, '2025-01-05', 500.00),
            (56, 16, 3, '2025-01-06', 30000.00),
            (57, 17, 4, '2025-01-06', 1500.00),
            (58, 18, 5, '2025-01-06', 50.00),
            (59, 19, 1, '2025-01-06', 800.00),
            (60, 20, 2, '2025-01-06', 600.00),
            (61, 1, 4, '2025-02-07', 20000.00),
            (62, 2, 5, '2025-02-07', 2000.00),
            (63, 3, 1, '2025-02-07', 150.00),
            (64, 4, 2, '2025-02-07', 1200.00),
            (65, 5, 3, '2025-02-07', 400.00),
            (66, 6, 4, '2025-02-08', 2000.00),
            (67, 7, 5, '2025-02-08', 2500.00),
            (68, 8, 1, '2025-02-08', 40.00),
            (69, 9, 2, '2025-02-08', 600.00),
            (70, 10, 3, '2025-02-08', 300.00),
            (71, 11, 4, '2025-02-09', 10000.00),
            (72, 12, 5, '2025-02-09', 1800.00),
            (73, 13, 1, '2025-02-09', 100.00),
            (74, 14, 2, '2025-02-09', 700.00),
            (75, 15, 3, '2025-02-09', 700.00),
            (76, 16, 4, '2025-02-10', 40000.00),
            (77, 17, 5, '2025-02-10', 3000.00),
            (78, 18, 1, '2025-02-10', 20.00),
            (79, 19, 2, '2025-02-10', 400.00),
            (80, 20, 3, '2025-02-10', 800.00),
            (81, 1, 5, '2025-02-11', 8000.00),  
            (82, 2, 1, '2025-02-11', 800.00),  
            (83, 3, 2, '2025-02-11', 120.00),  
            (84, 4, 3, '2025-02-11', 900.00),  
            (85, 5, 4, '2025-02-11', 900.00),  
            (86, 6, 5, '2025-02-12', 1500.00),  
            (87, 7, 1, '2025-02-12', 300.00),  
            (88, 8, 2, '2025-02-12', 500.00),  
            (89, 9, 3, '2025-02-12', 200.00),  
            (90, 10, 4, '2025-02-12', 1200.00),  
            (91, 11, 5, '2025-02-13', 800.00),  
            (92, 12, 1, '2025-02-13', 700.00),  
            (93, 13, 2, '2025-02-13', 80.00),  
            (94, 14, 3, '2025-02-13', 1000.00),  
            (95, 15, 4, '2025-02-13', 1000.00),  
            (96, 16, 5, '2025-02-14', 500.00),  
            (97, 17, 1, '2025-02-14', 200.00),  
            (98, 18, 2, '2025-02-14', 60.00),  
            (99, 19, 3, '2025-02-14', 1500.00),  
            (100, 20, 4, '2025-02-14', 2500.00) 
            ON CONFLICT (order_id) DO NOTHING;        
        """)

        conn.commit()
        cur.close()
        print("✅ Sample data inserted successfully!")
    except Exception as e:
        print(f"❌ Error inserting sample data: {e}")


                                            # Task 6: Import Data from CSV/XLSX
def load_xlsx_to_db(file_path, table_name, conn):
    cursor = conn.cursor()
    try:
        df = pd.read_excel(file_path)
        for _, row in df.iterrows():
            columns = ', '.join(df.columns)
            values = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        print(f"Data loaded successfully into {table_name} from {file_path}")
    except Exception as e:
        print(f"Error loading XLSX: {e}")
    finally:
        cursor.close()

                            # Part 5: Advanced Querying 

                            # Task 7: Recursive Query (Common Table Expressions - CTE) 

def display_employee_hierarchy(conn):
    """
    Displays the hierarchical reporting structure of employees using a recursive CTE.
    """
    try:
        cur = conn.cursor()

        #  -- Recursive CTE to get the employee hierarchy (USER PROVIDED QUERY)
        cur.execute("""
            WITH RECURSIVE EmployeeHierarchy AS (
                -- Anchor member: Select the CEO (manager_id is NULL)
                SELECT employee_id, name, role, manager_id, 0 AS level
                FROM Employees
                WHERE manager_id IS NULL

                UNION ALL

                -- Recursive member: Join with Employees to find subordinates
                SELECT e.employee_id, e.name, e.role, e.manager_id, eh.level + 1 AS level
                FROM Employees e
                JOIN EmployeeHierarchy eh ON e.manager_id = eh.employee_id
            )
            SELECT *
            FROM EmployeeHierarchy
            ORDER BY level, employee_id;
        """)

        results = cur.fetchall()

        if not results:
            print("No employees found or hierarchy could not be determined.")
            return

        print("\nEmployee Hierarchy:")
        print("--------------------")
        for row in results:
            employee_id, name, role, manager_id, level = row # unpack all columns
            indent = "  " * level  # Indentation for hierarchy level
            print(f"{indent}Level {level}: Employee ID: {employee_id}, Name: {name}, Role: {role}, Manager ID: {manager_id}") # Print all columns

        cur.close()

    except Exception as e:
        print(f"❌ Error displaying employee hierarchy: {e}")
    finally:
        if cur:
            cur.close()

                                                           # Task 8: Pivot (Transpose Data) 
def display_monthly_sales_pivot_crosstab(conn):
    """
    Displays monthly sales data per store in pivot table format using PostgreSQL's crosstab function.
    """
    try:
        cur = conn.cursor()

        # --- Setup: Create extension and table if they don't exist ---
        cur.execute("CREATE EXTENSION IF NOT EXISTS tablefunc;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS monthly_sales (
                store_id INT REFERENCES stores(store_id),
                month TEXT,
                total_sales DECIMAL(10,2)
            );
        """)
        conn.commit() # Commit after schema changes

        # --- Insert sample data (clearing existing data first for demonstration) ---
        cur.execute("DELETE FROM monthly_sales;") # Clear existing data for consistent output
        cur.executemany("""
            INSERT INTO monthly_sales (store_id, month, total_sales) VALUES (%s, %s, %s);
        """, [
            (1, 'Jan', 5000.00), (1, 'Feb', 7000.00), (1, 'Mar', 8000.00),
            (2, 'Jan', 6000.00), (2, 'Feb', 7500.00), (2, 'Mar', 9000.00)
        ])
        conn.commit() # Commit inserted data

        # --- Execute crosstab query ---
        cur.execute("""
            SELECT * FROM crosstab(
                'SELECT store_id, month, total_sales FROM monthly_sales ORDER BY store_id, month',
                'SELECT DISTINCT month FROM monthly_sales ORDER BY month'
            )
            AS pivot_table (
                store_id INT, Jan DECIMAL(10,2), Feb DECIMAL(10,2), Mar DECIMAL(10,2)
            );
        """)

        pivot_results = cur.fetchall()

        if not pivot_results:
            print("No pivoted sales data to display.")
            return

        # --- Print the pivoted table ---
        print("\nMonthly Sales per Store (Pivot Table using crosstab):")
        print("----------------------------------------------------")

        # Header row
        header_row = ["Store ID", "Jan", "Feb", "Mar"]
        print("| " + " | ".join(header_row) + " |")
        print("-" * (len(header_row) * 12 + 5)) # Adjust separator length

        # Data rows
        for row in pivot_results:
            store_id, jan_sales, feb_sales, mar_sales = row
            data_row = [str(store_id), f"{jan_sales:.2f}" if jan_sales else '0.00',
                        f"{feb_sales:.2f}" if feb_sales else '0.00',
                        f"{mar_sales:.2f}" if mar_sales else '0.00'] # Handle NULL sales and format
            print("| " + " | ".join(data_row) + " |")

        cur.close()

    except Exception as e:
        print(f"❌ Error displaying pivoted sales data using crosstab: {e}")
    finally:
        if cur:
            cur.close()


                            # Task 9: Query Data using Joins 
def demonstrate_joins(conn):
    """
    Demonstrates different types of JOINs: INNER, LEFT, RIGHT, FULL, and SELF JOIN.
    """
    try:
        cur = conn.cursor()

        # 1. INNER JOIN – Customers who have placed orders
        cur.execute("""
            SELECT customers.customer_id, customers.name, orders.order_id, orders.order_date
            FROM customers
            INNER JOIN orders ON customers.customer_id = orders.customer_id;
        """)
        inner_join_results = cur.fetchall()
        print("\n1. INNER JOIN – Customers who have placed orders:")
        print("--------------------------------------------------")
        if inner_join_results:
            for row in inner_join_results:
                customer_id, customer_name, order_id, order_date = row
                print(f"Customer ID: {customer_id}, Name: {customer_name}, Order ID: {order_id}, Order Date: {order_date}")
        else:
            print("No customers found with orders using INNER JOIN.")

        # 2. LEFT JOIN – All customers and their orders (including customers with no orders)
        cur.execute("""
            SELECT customers.customer_id, customers.name, orders.order_id, orders.order_date
            FROM customers
            LEFT JOIN orders ON customers.customer_id = orders.customer_id;
        """)
        left_join_results = cur.fetchall()
        print("\n2. LEFT JOIN – All customers and their orders (including customers with no orders):")
        print("--------------------------------------------------------------------------------")
        if left_join_results:
            for row in left_join_results:
                customer_id, customer_name, order_id, order_date = row
                print(f"Customer ID: {customer_id}, Name: {customer_name}, Order ID: {order_id}, Order Date: {order_date}")
        else:
            print("No customers found using LEFT JOIN.")

        # 3. RIGHT JOIN – All orders and their respective customers (even if customer details are missing)
        cur.execute("""
            SELECT customers.customer_id, customers.name, orders.order_id, orders.order_date
            FROM customers
            RIGHT JOIN orders ON customers.customer_id = orders.customer_id;
        """)
        right_join_results = cur.fetchall()
        print("\n3. RIGHT JOIN – All orders and their respective customers (even if customer details are missing):")
        print("-----------------------------------------------------------------------------------------------")
        if right_join_results:
            for row in right_join_results:
                customer_id, customer_name, order_id, order_date = row
                print(f"Customer ID: {customer_id}, Name: {customer_name}, Order ID: {order_id}, Order Date: {order_date}")
        else:
            print("No orders found using RIGHT JOIN.")

        # 4. FULL JOIN – Combine customer and order information while including unmatched records from both sides
        cur.execute("""
            SELECT customers.customer_id, customers.name, orders.order_id, orders.order_date
            FROM customers
            FULL JOIN orders ON customers.customer_id = orders.customer_id;
        """)
        full_join_results = cur.fetchall()
        print("\n4. FULL JOIN – Combine customer and order information while including unmatched records from both sides:")
        print("--------------------------------------------------------------------------------------------------")
        if full_join_results:
            for row in full_join_results:
                customer_id, customer_name, order_id, order_date = row
                print(f"Customer ID: {customer_id}, Name: {customer_name}, Order ID: {order_id}, Order Date: {order_date}")
        else:
            print("No data found using FULL JOIN.")

        # 5. SELF JOIN – Find employees working under the same manager
        cur.execute("""
            SELECT E1.employee_id AS Employee, E1.name AS EmployeeName,
                    E2.employee_id AS Manager, E2.name AS ManagerName
            FROM employees E1
            JOIN employees E2 ON E1.manager_id = E2.employee_id;
        """)
        self_join_results = cur.fetchall()
        print("\n5. SELF JOIN – Find employees working under the same manager:")
        print("----------------------------------------------------------")
        if self_join_results:
            for row in self_join_results:
                employee_id, employee_name, manager_id, manager_name = row
                print(f"Employee ID: {employee_id}, Employee Name: {employee_name}, Manager ID: {manager_id}, Manager Name: {manager_name}")
        else:
            print("No employees found working under managers using SELF JOIN.")

        cur.close()

    except Exception as e:
        print(f"❌ Error demonstrating JOIN operations: {e}")
    finally:
        if cur:
            cur.close()

                            # Task 10: UNION and UNION ALL 

def demonstrate_union_union_all(conn):
    """
    Demonstrates UNION and UNION ALL set operations.
    """
    try:
        cur = conn.cursor()

        # 1. UNION: Retrieve all customers and employees in a single list
        cur.execute("""
            SELECT name, 'Customer' AS person_type FROM customers
            UNION
            SELECT name, 'Employee' AS person_type FROM employees;
        """)
        union_results = cur.fetchall()
        print("\n1. UNION: Retrieve all customers and employees in a single list:")
        print("------------------------------------------------------------")
        if union_results:
            for row in union_results:
                name, person_type = row
                print(f"Name: {name}, Type: {person_type}")
        else:
            print("No customers or employees found for UNION operation.")

        # 2. UNION ALL: Retrieve all recent and older orders (example splitting by year)
        cur.execute("""
            SELECT order_id, order_date, 'Recent Order' AS order_category FROM orders WHERE EXTRACT(YEAR FROM order_date) = EXTRACT(YEAR FROM CURRENT_DATE)
            UNION ALL
            SELECT order_id, order_date, 'Older Order' AS order_category FROM orders WHERE EXTRACT(YEAR FROM order_date) < EXTRACT(YEAR FROM CURRENT_DATE);
        """)
        union_all_results = cur.fetchall()
        print("\n2. UNION ALL: Retrieve all recent and older orders in a single result set:")
        print("-----------------------------------------------------------------------")
        if union_all_results:
            for row in union_all_results:
                order_id, order_date, order_category = row
                print(f"Order ID: {order_id}, Order Date: {order_date}, Category: {order_category}")
        else:
            print("No orders found for UNION ALL operation.")

        cur.close()

    except Exception as e:
        print(f"❌ Error demonstrating UNION and UNION ALL operations: {e}")
    finally:
        if cur:
            cur.close()

                            # Part 6: Data Modification & Deletion 

                            # Task 11: Update Data 

def demonstrate_data_updates(conn):
    """
    Demonstrates data update operations: price increase, salary update, stock update.
    """
    try:
        cur = conn.cursor()

        # --- 1. Increase the price of all products in the Electronics category by 10%. ---
        print("\n--- 1. Increase Electronics Product Prices by 10% ---")

        # Before update: Retrieve current prices for Electronics category
        cur.execute("SELECT product_id, name, price FROM products WHERE category = 'Electronics';")
        electronics_products_before = cur.fetchall()
        print("\nElectronics products prices before update:")
        for row in electronics_products_before:
            print(f"Product ID: {row[0]}, Name: {row[1]}, Price: {row[2]}")

        # Perform the price update
        cur.execute("""
            UPDATE products
            SET price = price * 1.10
            WHERE category = 'Electronics';
        """)
        conn.commit()
        print("✅ Electronics product prices increased by 10%.")

        # After update: Retrieve updated prices for Electronics category
        cur.execute("SELECT product_id, name, price FROM products WHERE category = 'Electronics';")
        electronics_products_after = cur.fetchall()
        print("\nElectronics products prices after update:")
        for row in electronics_products_after:
            print(f"Product ID: {row[0]}, Name: {row[1]}, Price: {row[2]}")


        # --- 2. Update the salary of employees working for more than 5 years by 5%. ---
        print("\n--- 2. Update Employee Salaries (5% increase for > 5 years service) ---")

        # Add hire_date column if it doesn't exist
        cur.execute("ALTER TABLE Employees ADD COLUMN IF NOT EXISTS hire_date DATE;")
        conn.commit()

        # Update hire_date for employees if it's currently NULL (example dates - adjust as needed)
        cur.execute("UPDATE Employees SET hire_date = '2016-01-15' WHERE employee_id = 1 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2021-03-20' WHERE employee_id = 2 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2022-05-10' WHERE employee_id = 3 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2019-11-01' WHERE employee_id = 4 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2023-01-05' WHERE employee_id = 5 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2018-09-22' WHERE employee_id = 6 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2022-12-12' WHERE employee_id = 7 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2017-07-08' WHERE employee_id = 8 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2021-08-18' WHERE employee_id = 9 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2020-05-03' WHERE employee_id = 10 AND hire_date IS NULL;")
        conn.commit()


        # Before update: Retrieve salaries of eligible employees
        cur.execute("""
            SELECT employee_id, name, salary, hire_date
            FROM Employees
            WHERE hire_date < CURRENT_DATE - INTERVAL '5 years';
        """)
        eligible_employees_before = cur.fetchall()
        print("\nEligible employees for salary increase before update:")
        for row in eligible_employees_before:
            print(f"Employee ID: {row[0]}, Name: {row[1]}, Salary: {row[2]}, Hire Date: {row[3]}")

        # Perform the salary update
        cur.execute("""
            UPDATE Employees
            SET salary = salary * 1.05
            WHERE hire_date < CURRENT_DATE - INTERVAL '5 years';
        """)
        conn.commit()
        print("✅ Salaries updated for employees working more than 5 years.")

        # After update: Retrieve salaries of updated employees
        cur.execute("""
            SELECT employee_id, name, salary, hire_date
            FROM Employees
            WHERE hire_date < CURRENT_DATE - INTERVAL '5 years';
        """)
        eligible_employees_after = cur.fetchall()
        print("\nEligible employees for salary increase after update:")
        for row in eligible_employees_after:
            print(f"Employee ID: {row[0]}, Name: {row[1]}, Salary: {row[2]}, Hire Date: {row[3]}")


        # --- 3. Update product stock based on new supplier shipments. ---
        print("\n--- 3. Update Product Stock based on Shipments ---")

        # Create Shipments table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Shipments (
                shipment_id INT PRIMARY KEY,
                product_id INT REFERENCES Products(product_id),
                quantity INT NOT NULL,
                shipment_date DATE DEFAULT CURRENT_DATE
            );
        """)
        conn.commit()

        # Insert sample shipments (clearing existing data for demonstration)
        cur.execute("DELETE FROM Shipments;") # Clear existing shipment data
        cur.executemany("""
            INSERT INTO Shipments (shipment_id, product_id, quantity) VALUES (%s, %s, %s);
        """, [
            (101, 1, 50),   # 50 more Cotton Kurtas
            (102, 10, 20),  # 20 more Mixer Grinders
            (103, 21, 10),  # 10 more Smart LED TVs
            (104, 30, 100), # 100 more Wooden Toys
            (105, 40, 25)   # 25 more Basmati Rice
        ])
        conn.commit()
        print("✅ Sample shipments data inserted.")


        # Before update: Retrieve stock levels of affected products
        product_ids_to_check = [1, 10, 21, 30, 40]
        placeholders = ','.join(['%s'] * len(product_ids_to_check)) # Create placeholders for IN clause
        cur.execute(f"""
            SELECT product_id, name, stock FROM products WHERE product_id IN ({placeholders});
        """, product_ids_to_check) # Pass product_ids as parameters
        products_stock_before = cur.fetchall()
        print("\nProduct stock levels before shipment update:")
        for row in products_stock_before:
            print(f"Product ID: {row[0]}, Name: {row[1]}, Stock: {row[2]}")


        # Perform the stock update using JOIN
        cur.execute("""
            UPDATE Products
            SET stock = Products.stock + Shipments.quantity
            FROM Shipments
            WHERE Products.product_id = Shipments.product_id;
        """)
        conn.commit()
        print("✅ Product stock levels updated based on shipments.")

        # After update: Retrieve updated stock levels of affected products
        cur.execute(f"""
            SELECT product_id, name, stock FROM products WHERE product_id IN ({placeholders});
        """, product_ids_to_check) # Re-use the placeholders and product_ids
        products_stock_after = cur.fetchall()
        print("\nProduct stock levels after shipment update:")
        for row in products_stock_after:
            print(f"Product ID: {row[0]}, Name: {row[1]}, Stock: {row[2]}")


        cur.close()

    except Exception as e:
        print(f"❌ Error demonstrating data update operations: {e}")
    finally:
        if cur:
            cur.close()
 
                            # Task 12: Delete Data  

def demonstrate_data_deletion(conn):
    """
    Demonstrates data deletion operations: delete inactive customers, delete order with items, truncate audit table.
    """
    try:
        cur = conn.cursor()

        # --- 1. Delete inactive customers (who haven’t ordered in the last 2 years). ---
        print("\n--- 1. Delete Inactive Customers (No orders in last 2 years) ---")

        # Before deletion: Count of inactive customers
        cur.execute("""
            SELECT COUNT(*)
            FROM customers
            WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '2 years');
        """)
        inactive_customers_count_before = cur.fetchone()[0]
        print(f"Inactive customers count before deletion: {inactive_customers_count_before}")

        # Retrieve inactive customer IDs and Names before deletion for display
        cur.execute("""
            SELECT customer_id, name
            FROM customers
            WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '2 years');
        """)
        inactive_customers_before = cur.fetchall()
        print("\nInactive customers to be deleted:")
        for row in inactive_customers_before:
            print(f"Customer ID: {row[0]}, Name: {row[1]}")


        # Perform the deletion of inactive customers
        cur.execute("""
            DELETE FROM customers
            WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '2 years');
        """)
        deleted_customer_count = cur.rowcount
        conn.commit()
        print(f"✅ {deleted_customer_count} inactive customers deleted.")

        # After deletion: Count of inactive customers
        cur.execute("""
            SELECT COUNT(*)
            FROM customers
            WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '2 years');
        """)
        inactive_customers_count_after = cur.fetchone()[0]
        print(f"Inactive customers count after deletion: {inactive_customers_count_after}")


        # --- 2. Delete an order and ensure order items are also deleted (Cascading Delete). ---
        print("\n--- 2. Delete Order (and Cascade Delete Order Items) ---")

        order_id_to_delete = 1  # Choose an order ID to delete (e.g., Order ID 1)

        # Before deletion: Count of order items for the order to be deleted
        cur.execute("SELECT COUNT(*) FROM order_items WHERE order_id = %s;", (order_id_to_delete,))
        order_items_count_before = cur.fetchone()[0]
        print(f"Order items count for Order ID {order_id_to_delete} before deletion: {order_items_count_before}")

        # Before deletion: Check if order exists
        cur.execute("SELECT order_id FROM orders WHERE order_id = %s;", (order_id_to_delete,))
        order_exists_before = cur.fetchone()
        print(f"Order ID {order_id_to_delete} exists before deletion: {'Yes' if order_exists_before else 'No'}")


        # Perform order deletion
        cur.execute("DELETE FROM orders WHERE order_id = %s;", (order_id_to_delete,))
        deleted_order_count = cur.rowcount # Should be 1 if order existed
        conn.commit()
        print(f"✅ Order ID {order_id_to_delete} deleted.")

        # After deletion: Count of order items for the deleted order (should be 0 due to CASCADE DELETE)
        cur.execute("SELECT COUNT(*) FROM order_items WHERE order_id = %s;", (order_id_to_delete,))
        order_items_count_after = cur.fetchone()[0]
        print(f"Order items count for Order ID {order_id_to_delete} after deletion: {order_items_count_after} (Cascade Delete verified)")

        # After deletion: Check if order still exists
        cur.execute("SELECT order_id FROM orders WHERE order_id = %s;", (order_id_to_delete,))
        order_exists_after = cur.fetchone()
        print(f"Order ID {order_id_to_delete} exists after deletion: {'Yes' if order_exists_after else 'No'}")


        # --- 3. Truncate the audit table to reset logs. ---
        print("\n--- 3. Truncate Employee Audit Table ---")

        # Before truncation: Count of records in audit table
        cur.execute("SELECT COUNT(*) FROM employee_audit;")
        audit_table_count_before = cur.fetchone()[0]
        print(f"Records in employee_audit table before truncation: {audit_table_count_before}")

        # Perform truncation
        cur.execute("TRUNCATE TABLE employee_audit;")
        conn.commit()
        print("✅ employee_audit table truncated (logs reset).")

        # After truncation: Count of records in audit table (should be 0)
        cur.execute("SELECT COUNT(*) FROM employee_audit;")
        audit_table_count_after = cur.fetchone()[0]
        print(f"Records in employee_audit table after truncation: {audit_table_count_after}")


        cur.close()

    except Exception as e:
        print(f"❌ Error demonstrating data deletion operations: {e}")
    finally:
        if cur:
            cur.close()

                                # -- Part 7: Stored Procedures for CRUD Operations 

                                # -- Task 13: Create Stored Procedures 


def create_stored_procedures(conn):
    """
    Creates stored procedures for CRUD operations on the database.
    """
    try:
        cur = conn.cursor()

        # 1. sp_AddCustomer: Add a new customer
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_AddCustomer (
                p_name VARCHAR(255),
                p_email VARCHAR(255),
                p_phone VARCHAR(20),
                p_city VARCHAR(255)
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                INSERT INTO customers (customer_id, name, email, phone, city)
                VALUES (nextval('customers_customer_id_seq'), p_name, p_email, p_phone, p_city); -- Assuming you have a sequence for customer_id
                COMMIT;
            END;
            $$;
        """)

        # 2. sp_UpdateCustomer: Update customer details
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_UpdateCustomer (
                p_customer_id INT,
                p_name VARCHAR(255),
                p_email VARCHAR(255),
                p_phone VARCHAR(20),
                p_city VARCHAR(255)
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                UPDATE customers
                SET name = p_name,
                    email = p_email,
                    phone = p_phone,
                    city = p_city
                WHERE customer_id = p_customer_id;
                COMMIT;
            END;
            $$;
        """)

        # 3. sp_DeleteCustomer: Delete a customer if they have no orders
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_DeleteCustomer (
                p_customer_id INT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM orders WHERE customer_id = p_customer_id) THEN
                    DELETE FROM customers WHERE customer_id = p_customer_id;
                    COMMIT;
                ELSE
                    RAISE EXCEPTION 'Customer cannot be deleted because they have existing orders.';
                END IF;
            END;
            $$;
        """)

        # 4. sp_GetCustomerOrders: Retrieve all orders by a customer
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_GetCustomerOrders (
                p_customer_id INT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                SELECT *
                FROM orders
                WHERE customer_id = p_customer_id;
            END;
            $$;
        """)

        # 5. sp_AddProductStock: Increase product stock
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_AddProductStock (
                p_product_id INT,
                p_quantity INT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                UPDATE products
                SET stock = stock + p_quantity
                WHERE product_id = p_product_id;
                COMMIT;
            END;
            $$;
        """)

        # 6. sp_GenerateSalesReport: Generate monthly sales report per store
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_GenerateSalesReport (
                p_year INT,
                p_month INT
            )
            LANGUAGE plpgsql
            AS $$
            DECLARE
                report_cursor CURSOR FOR
                    SELECT
                        s.name AS store_name,
                        SUM(oi.price * oi.quantity) AS monthly_revenue
                    FROM stores s
                    JOIN orders o ON s.store_id = o.store_id
                    JOIN order_items oi ON o.order_id = oi.order_id
                    WHERE EXTRACT(YEAR FROM o.order_date) = p_year AND EXTRACT(MONTH FROM o.order_date) = p_month
                    GROUP BY s.name
                    ORDER BY monthly_revenue DESC;
                report_record RECORD;
            BEGIN
                OPEN report_cursor;
                LOOP
                    FETCH NEXT FROM report_cursor INTO report_record;
                    EXIT WHEN NOT FOUND;
                    -- You can process or display the report_record here, for example:
                    RAISE NOTICE 'Store: %, Revenue: %', report_record.store_name, report_record.monthly_revenue;
                END LOOP;
                CLOSE report_cursor;
            END;
            $$;
        """)


        conn.commit()
        cur.close()
        print("✅ Stored procedures created successfully!")

    except Exception as e:
        print(f"❌ Error creating stored procedures: {e}")
    finally:
        if cur:
            cur.close()


                                                    #             Part 8: Data Export 

                                                    # Task 14: Extract Results to CSV/XLSX 

def export_monthly_revenue_to_file(conn, file_format="CSV"):
    """
    Exports monthly revenue per store to a CSV or XLSX file.
    """
    try:
        cur = conn.cursor()

        # SQL query to get monthly revenue per store
        cur.execute("""
            SELECT
                s.name AS store_name,
                EXTRACT(MONTH FROM o.order_date) AS month,
                EXTRACT(YEAR FROM o.order_date) AS year,
                SUM(oi.price * oi.quantity) AS monthly_revenue
            FROM stores s
            JOIN orders o ON s.store_id = o.store_id
            JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY s.name, EXTRACT(MONTH FROM o.order_date), EXTRACT(YEAR FROM o.order_date)
            ORDER BY s.name, year, month;
        """)

        results = cur.fetchall()
        if not results:
            print("No monthly revenue data to export.")
            return

        # Convert to DataFrame for easy export
        df = pd.DataFrame(results, columns=['Store Name', 'Month', 'Year', 'Monthly Revenue'])
        month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                       7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        df['Month'] = df['Month'].map(month_names) # Convert month number to month name
        df['Monthly Revenue'] = df['Monthly Revenue'].round(2) # Round revenue to 2 decimal places


        if file_format.upper() == "CSV":
            filename = "monthly_store_revenue.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Monthly store revenue exported to: {filename}")
        elif file_format.upper() == "XLSX":
            filename = "monthly_store_revenue.xlsx"
            df.to_excel(filename, index=False, sheet_name='Monthly Revenue')
            print(f"✅ Monthly store revenue exported to: {filename}")
        else:
            print("❌ Invalid file format specified. Please choose CSV or XLSX.")

        cur.close()

    except Exception as e:
        print(f"❌ Error exporting monthly revenue data: {e}")
    finally:
        if cur:
            cur.close()


def export_customer_spending_to_file(conn, file_format="CSV"):
    """
    Exports a list of customers and their total spending to a CSV or XLSX file.
    """
    try:
        cur = conn.cursor()

        # SQL query to get customer spending
        cur.execute("""
            SELECT
                c.name AS customer_name,
                SUM(oi.price * oi.quantity) AS total_spending
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY c.name
            ORDER BY total_spending DESC;
        """)

        results = cur.fetchall()
        if not results:
            print("No customer spending data to export.")
            return

        # Convert to DataFrame
        df = pd.DataFrame(results, columns=['Customer Name', 'Total Spending'])
        df['Total Spending'] = df['Total Spending'].round(2) # Round spending to 2 decimal places


        if file_format.upper() == "CSV":
            filename = "customer_total_spending.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Customer total spending exported to: {filename}")
        elif file_format.upper() == "XLSX":
            filename = "customer_total_spending.xlsx"
            df.to_excel(filename, index=False, sheet_name='Customer Spending')
            print(f"✅ Customer total spending exported to: {filename}")
        else:
            print("❌ Invalid file format specified. Please choose CSV or XLSX.")

        cur.close()

    except Exception as e:
        print(f"❌ Error exporting customer spending data: {e}")
    finally:
        if cur:
            cur.close()


# Establish connection and create tables
conn = connect_db()
inp = int(input("""1. Create tables
2. Create indexes 
3. Create Views  
4. Create Triggers 
5. Insert Values into Table 
6. Load Xlsx to DB 
7. Common Table Expressions - (CTE)- Employee hierarchy   
8. Query Data using Joins(INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, SELF JOIN)    
9. UNION and UNION ALL   
10. Update Data  
11. Pivot (Transpose Data) 
12. Delete Data        
13. CRUD Operations    
14. Data Export                                                                                                                                     
-> """))
if conn:
    if inp == 1:
        create_tables(conn)
    elif inp == 2:
        create_indexes(conn)
    elif inp == 3:
        create_views(conn)  
    elif inp == 4:
        create_triggers(conn)
    elif inp == 5:
        insert_sample_data(conn)
    elif inp == 6:
        file_path = input("Enter XLSX file path: ")
        table_name = input("Enter table name: ")
        load_xlsx_to_db(file_path, table_name, conn)  
    elif inp == 7:
        display_employee_hierarchy(conn) 
    elif inp == 8:
            demonstrate_joins(conn)  
    elif inp == 9:
            demonstrate_union_union_all(conn)  
    elif inp == 10:
            demonstrate_data_updates(conn)   
    elif inp == 11:
            display_monthly_sales_pivot_crosstab(conn) 
    elif inp == 12:
            demonstrate_data_deletion(conn) 
    elif inp == 13:
            create_stored_procedures(conn)  
    elif inp == 14:  
                    export_type = int(input("""1. Export monthly revenue per store
2. Export customer spending

-> """))
                    file_format = input("Enter export file format (CSV or XLSX): ").strip()
                    if export_type == 1:
                        export_monthly_revenue_to_file(conn, file_format)
                    elif export_type == 2:
                        export_customer_spending_to_file(conn, file_format)
                    else:
                        print("❌ Invalid export type selected.")               

    else:
        print("Invalid input!")
    conn.close()


