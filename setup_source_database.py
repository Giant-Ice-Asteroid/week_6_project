import mysql.connector
import pandas as pd
from config import host, user, password
def setup_source_database():
    """
    This function sets up the source database (ProductDB) by creating it
    and its tables, then loading data from the CSV files.
    """

    print("STEP 1: Setting up source database (ProductDB)...")
    
    try:
        # Connect to the MySQL server (without specifying a database)
        conn = mysql.connector.connect(
            host=host,
            user=user,  
            password=password
        )
        
        # Create a cursor to execute SQL commands
        cursor = conn.cursor()
        
        # Create the ProductDB database (like in etl_db_setup.py)
        print("Creating ProductDB database...")
        cursor.execute("DROP DATABASE IF EXISTS ProductDB")
        cursor.execute("CREATE DATABASE ProductDB")
        
        # Create user and grant privileges (from etl_db_setup.py)
        try:
            cursor.execute("CREATE USER 'curseist'@'localhost' IDENTIFIED BY 'curseword'")
            cursor.execute("GRANT ALL PRIVILEGES ON *.* TO 'curseist'@'%'")
            print("User 'curseist' created with privileges.")
        except mysql.connector.Error as e:
            # User might already exist, which is fine
            print(f"Note: {e}")
        
        # Switch to using the ProductDB database
        cursor.execute("USE ProductDB")
        
        # Create tables based on the structure in the SQL dump file
        
        # Create brands table
        print("Creating brands table...")
        cursor.execute("""
        CREATE TABLE brands (
            brand_id INT NOT NULL AUTO_INCREMENT,
            brand_name VARCHAR(255) NOT NULL,
            PRIMARY KEY (brand_id)
        )
        """)
        
        # Create categories table
        print("Creating categories table...")
        cursor.execute("""
        CREATE TABLE categories (
            category_id INT NOT NULL AUTO_INCREMENT,
            category_name VARCHAR(255) NOT NULL,
            PRIMARY KEY (category_id)
        )
        """)
        
        # Create products table
        print("Creating products table...")
        cursor.execute("""
        CREATE TABLE products (
            product_id INT NOT NULL AUTO_INCREMENT,
            product_name VARCHAR(255) NOT NULL,
            brand_id INT,
            category_id INT,
            model_year INT,
            list_price FLOAT,
            PRIMARY KEY (product_id),
            FOREIGN KEY (brand_id) REFERENCES brands(brand_id),
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        )
        """)
        
        # Create stocks table
        print("Creating stocks table...")
        cursor.execute("""
        CREATE TABLE stocks (
            store_name VARCHAR(255),
            product_id INT,
            quantity INT,
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
        """)
        
        # Commit these table creations
        conn.commit()
        print("Tables created successfully in ProductDB.")
        
        # Now load data from CSV files
        print("\nLoading data from CSV files into ProductDB...")
        
        # Load brands data
        try:
            brands_df = pd.read_csv('data\\brands.csv')
            for _, row in brands_df.iterrows():
                cursor.execute(
                    "INSERT INTO brands (brand_id, brand_name) VALUES (%s, %s)",
                    (row['brand_id'], row['brand_name'])
                )
            print(f"Loaded {len(brands_df)} records into brands table")
        except Exception as e:
            print(f"Error loading brands data: {e}")
        
        # Load categories data
        try:
            categories_df = pd.read_csv('data\\categories.csv')
            for _, row in categories_df.iterrows():
                cursor.execute(
                    "INSERT INTO categories (category_id, category_name) VALUES (%s, %s)",
                    (row['category_id'], row['category_name'])
                )
            print(f"Loaded {len(categories_df)} records into categories table")
        except Exception as e:
            print(f"Error loading categories data: {e}")
        
        # Load products data
        try:
            products_df = pd.read_csv('data\\products.csv')
            for _, row in products_df.iterrows():
                cursor.execute(
                    "INSERT INTO products (product_id, product_name, brand_id, category_id, model_year, list_price) VALUES (%s, %s, %s, %s, %s, %s)",
                    (row['product_id'], row['product_name'], row['brand_id'], row['category_id'], row['model_year'], row['list_price'])
                )
            print(f"Loaded {len(products_df)} records into products table")
        except Exception as e:
            print(f"Error loading products data: {e}")
        
        # Load stocks data
        try:
            stocks_df = pd.read_csv('data\\stocks.csv')
            for _, row in stocks_df.iterrows():
                cursor.execute(
                    "INSERT INTO stocks (store_name, product_id, quantity) VALUES (%s, %s, %s)",
                    (row['store_name'], row['product_id'], row['quantity'])
                )
            print(f"Loaded {len(stocks_df)} records into stocks table")
        except Exception as e:
            print(f"Error loading stocks data: {e}")
        
        # Commit all the data insertions
        conn.commit()
        print("All data loaded successfully into ProductDB.")
        
        # Close the connection and cursor
        cursor.close()
        conn.close()
        
        return True
        
    except mysql.connector.Error as e:
        print(f"Error setting up ProductDB: {e}")
        # If we already created the connection, try to close it
        if 'conn' in locals():
            conn.close()
        return False

# This allows the script to be run directly
if __name__ == "__main__":
    success = setup_source_database()
    if success:
        print("\nSTEP 1 COMPLETE: Source database (ProductDB) is set up and ready.")
    else:
        print("\nSTEP 1 FAILED: Could not set up source database.")