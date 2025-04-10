import mysql.connector
import json

def create_bikecorp_db():
    """
    Function that sets up the taget database (BikeCorpDB) where all the consolidated data from the different sources will be stored
    When run successfully, the BikeCorpDb database will be created with the following tables:

    """

    print("Attempting to set up BikeCorpDB")

    # first attempt to connect to the mySQL server itself:
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

        #creates cursor to execute sql commands
        cursor = conn.cursor()

        #drops BikeCorpDB if it already exists to ensure a clean slate
        print("Dropping BikeCorpDB in case it already exists..")
        cursor.execute("DROP DATABASE IF EXISTS BikeCorpDB")

        #next create a fresh BikeCorpDB database
        print("Creating BikeCorpDB database..")
        cursor.execute("CREATE DATABASE BikeCorpDB")

        #Ensures that this is the database to be used for the subsequent table creation steps with USE command
        cursor.execute("USE BikeCorpDB")

        """
        --> Table creation step <--
        Be mindful of the order of table creation to ensure correct key relationships
        "Parent" tables must be created before "child" tables that reference them..
        """

        # BRANDS table (based on ProductDB data)
        print("Creating the Brands table..")
        cursor.execute("""
        CREATE TABLE brands (
        brand_id INT PRIMARY KEY,
        brand_name VARCHAR(255) NOT NULL
        ) COMMENT 'Stores bike brand information, sourced from ProductDB'
        """)


        # CATEGORIES table (from ProductDB)
        print("Creating Categories table..")
        cursor.execute("""
        CREATE TABLE categories (
            category_id INT PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL
        ) COMMENT 'Stores bike category information soruced from ProductDB'
        """)

        # STORES table (based on flat CSV files)
        # we create a new column (new as not in the source csv file) called store_id and use AUTO_Increment to create a unique store_id

        print("Creating Stores table..")
        cursor.execute("""
        CREATE TABLE stores(
            store_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            phone VARCHAR(255),
            email VARCHAR(255),
            street VARCHAR(255),
            city VARCHAR (255),
            state VARCHAR (255),
            zip_code int
        ) COMMENT 'Stores information about store locations sourced from flat CSV file'
        """)

        # PRODUCTS table (from ProductDB data)
        # Similar to a fact table, it references brands and categories table with foreign keys

        print("Creating Products table..")
        cursor.execute("""
        CREATE TABLE products (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            brand_id INT,
            category_id INT,
            model_year INT,
            list_price DECIMAL(10, 2),
            FOREIGN KEY (brand_id) REFERENCES brands(brand_id),
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        ) COMMENT 'Stores product information sourced from ProductDB'
        """)

        # STAFFS table (CSV flat file origin)
        # first we create a new column, staff_id (not in the origin csv data)
        # Has a "self-referencing" foreign key (manager_id to staff_id)
        # -> this allows a row in the table to be related to another row in the same table
        # Also references the stores table with a foreign key

        print("Creating Staffs table..")
        cursor.execute("""
        CREATE TABLE staffs (
            staff_id INT AUTO_INCREMENT PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            email VARCHAR(255),
            phone VARCHAR(25),
            active TINYINT DEFAULT 1,
            store_id INT,
            manager_id INT,
            FOREIGN KEY (store_id) REFERENCES stores(store_id),
            FOREIGN KEY (manager_id) REFERENCES staffs(staff_id)
        ) COMMENT 'Stores staff information sourced from flat CSV file'
        """)

        # STOCKS table (Product DB data)
        #this table contains a composite primary key (store_id, product_id)
        # -> the composite key combines two or more columns to ensure uniqueness 
        # thus the combination of store and product must be unique in the table
        # -> in this case it prevents duplicate stock records, by forcing update the existing record
        # Also references these tables with foreign keys

        print("creating Stocks table..")
        cursor.execute("""
        CREATE TABLE stocks (
            store_id INT,
            product_id INT,
            quantity INT NOT NULL,
            PRIMARY KEY(store_id, product_id),
            FOREIGN KEY (store_id) REFERENCES stores(store_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        ) COMMENT 'Stores inventory information soruced from ProductDB'
        """)

        # CUSTOMER table (API)
        print("Creating Customers table...")
        cursor.execute("""
        CREATE TABLE customers (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            phone VARCHAR(25),
            email VARCHAR(255),
            street VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(10),
            zip_code INT
        ) COMMENT 'Stores customer information sourced from API'
        """)

        # ORDERS table (API)
        # foreign key references the customers, stores and staffs tables
        print("Creating Orders table..")
        cursor.execute("""
        CREATE TABLE orders (
            order_id INT PRIMARY KEY,
            customer_id INT,
            order_status TINYINT NOT NULL,
            order_date DATE NOT NULL,
            required_date DATE NOT NULL,
            shipped_date DATE,
            store_id INT,
            staff_id INT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (store_id) REFERENCES stores(store_id),
            FOREIGN KEY (staff_id) REFERENCES staffs(staff_id)
        ) COMMENT 'Stores order information sourced from API'
        """)

        # ORDER_ITEMS table (API)
        # also has a composite primary key (order_id, item_id)
        # references orders and products tables
        print("Creating order_items table...")
        cursor.execute("""
        CREATE TABLE order_items (
            order_id INT,
            item_id INT,
            product_id INT,
            quantity INT NOT NULL,
            list_price DECIMAL(10, 2) NOT NULL,
            discount DECIMAL(4, 2) NOT NULL DEFAULT 0,
            PRIMARY KEY (order_id, item_id),
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        ) COMMENT 'Stores order line items from API'
        """)

        # commits all these changes to make them permanent
        conn.commit()
        print("All tables created successfully in BikeCorpDB.")
        
        # returns the connection and cursor for possible later use
        return conn, cursor
    
    except mysql.connector.Error as e:
        print(f"Error creating BikeCorpDB: {e}")
        # if connection has been created already, try to close it
        if 'conn' in locals():
            conn.close()
        return None, None

# allows the script to be run directly
if __name__ == "__main__":
    conn, cursor = create_bikecorp_db()
    
    if conn and cursor:
        print("\nSuccess: Target database (BikeCorpDB) has been created successfully...!")
        #summary of the created tables for documentation
        print("\nDatabase Schema Summary:")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"Total tables created: {len(tables)}")
        for table in tables:
            table_name = table[0]
            print(f"  - {table_name}")
            #prints column information for this table
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            for column in columns:
                print(f"      {column[0]}: {column[1]}")
        
        # close the connection and cursor when done
        cursor.close()
        conn.close()
    else:
        print("\nFAILURE: Could not create target database :<")