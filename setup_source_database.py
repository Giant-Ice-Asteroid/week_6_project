import mysql.connector
import pandas as pd
import json



def setup_source_database():
    """
    This function sets up the source database (ProductDB) by creating it
    and its tables, then loading data from the CSV files..
    """

    print("Setting up source database (ProductDB)...")
    
    try:        
        with open("cred_info.json") as f:
            content = f.read()
            json_content = json.loads(content)
        #connect to the MySQL server (note: without specifying a database)
        conn = mysql.connector.connect(
            host = json_content["host"],
            user = json_content["user"],
            password = json_content["password"]
            )
            
        
        # Create a cursor to execute SQL commands
        cursor = conn.cursor()
        
        # Create the ProductDB database (like in etl_db_setup.py)
        print("Creating ProductDB database...")
        cursor.execute("DROP DATABASE IF EXISTS ProductDB")
        cursor.execute("CREATE DATABASE ProductDB")        
        
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
        
        # commit these table creations
        conn.commit()
        print("Tables created successfully in ProductDB.")
        
        # Nnext load data from CSV files
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
        
        # load products data
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
        
        # load stocks data
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
        
        # commit all the data insertions
        conn.commit()
        print("All data loaded successfully into ProductDB")
        
        # close the connection and cursor
        cursor.close()
        conn.close()
        
        return True
        
    except mysql.connector.Error as e:
        print(f"Error setting up ProductDB: {e}")
        # If we already created the connection, try to close it
        if 'conn' in locals():
            conn.close()
        return False

#  allows the script to be run directly
if __name__ == "__main__":
    success = setup_source_database()
    if success:
        print("\nSuccess: Source database (ProductDB) is set up and ready.")
    else:
        print("\nFailure: Could not set up source database.")