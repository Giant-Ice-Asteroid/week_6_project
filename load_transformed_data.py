import mysql.connector
import pandas as pd
import json

def load_data_to_bikecorpdb():
    
    """
    Function that loads data from the transformed_data dir into our BikeCorpDB MySQL server
    
    """
    print("Final step!!!! Loading the transformed data into the database!!!")
    
    with open("cred_info.json") as f:
            content = f.read()
            json_content = json.loads(content)     
    conn = mysql.connector.connect(
        host = json_content["host"],
        user = json_content["user"],
        password = json_content["password"],
        database = "BikeCorpDB"
            )
    cursor = conn.cursor()
    print("Successfully connected to the BikeCropDB database")
    
    # ensure tables to load in the proper order. Making sure not to load tables with the dependencies before the tables they refer to
    tables = [
        'brands', 'categories', 'stores', 'products', 'staffs',
        'customers', 'orders', 'stocks', 'order_items'
    ]
    
    # Disable foreign key checks
    # to avoid errors when loading due to foreign key restraints..
    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    
    # Loading each table in the order defined above
    for table in tables:
        print(f"Loading {table}...")
        
        df = pd.read_csv(f"transformed_data/{table}.csv")
        
        # Handling null values by replacing pandas' NaN values with python None (which mySQL can properly recognise as null) # fix due to errors prev
        df = df.astype(object).where(pd.notnull(df), None)
        
        # creating the SQL command (=insert statement) which will insert data from the df into the db
        
        # takes all column names from our df and joins them into a single string with commas between them:
        columns = ", ".join(df.columns)
        
        # next creates a list with one %s for each column and combines these into a single string with commas (% provided later)
        # the _ is a convention in python that means "I need a variable here but won't use its value" 
        placeholders = ", ".join(["%s" for _ in df.columns])
        
        #combines the table name, column names, and placeholders into a complete SQL command:
        insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        # converting value from the df into tuples to match sql format..
        # df.to_numpy() converts the df to a NumPy array (like a list of lists), which is then turned into a list of tuples
        values = [tuple(x) for x in df.to_numpy()]
        
        # executemany() to run the same sql insert statement for multiple rows of data at once
        # saves with commit
        cursor.executemany(insert_query, values)
        conn.commit()
        
        print(f"Loaded {len(df)} records into {table}")
    
    # turn on foreign key checks again
    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
    
    # aaand close connection
    cursor.close()
    conn.close()
    print("Data loading complete!")
    return True

if __name__ == "__main__":
    load_data_to_bikecorpdb()