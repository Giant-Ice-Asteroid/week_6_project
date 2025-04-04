import mysql.connector
import pandas as pd
import os

def extract_from_productdb():
    """
    Function which extracts data from the source database (ProductDB)
    The extracted data is then saved as CSV files in a newly created directory for later transformation
    """

    print("Extracting data from ProductDB")

    # creates a directory/folder wherever the terminal or prompt is pointed when script is run (=current working directory)
    # in this case it should be created in the root folder of this VS code project
    if not os.path.exists("extracted_data"):
        os.makedirs("extracted_data")
        print("The 'extracted_data' directory has been created succesfully")

    #next we need to connect to the source database, ProductDB
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Velkommen25",
            database="ProductDB"
        )

        #creates cursor, here dictionary=true return the results as a dict which is easier to work with later
        cursor = conn.cursor(dictionary=True)

        # the tables to be extracted from the db
        tables = ["brands", "categories", "products", "stocks"]

        # attempt to extract each table and then save it as a CSV file in the newly created directory
        for table in tables:
            print("Beginning to extract data from {table} table..")
            cursor.execute(f"SELECT * FROM {table}") # grabs all with *
            results = cursor.fetchall() # fetchall method retrieves all the rows in the result set of a query 

            #checking if we got any data..
            if not results:
                print(f"No data found in {table} table :<")
                continue
                       
            print(f"Found {len(results)} records in {table} table")

            #next step is to convert the results just generated to a pandas dataframe
            # makes it easier to work with and to save as CSV file
            df = pd.DataFrame(results)

            #can then save the dataframe created to a CSV file in the new extracted_data dir
            # index=False is important, as it prevents pandas from adding a rownumber column on its own
            output_file = f"extracted_data/{table}_from_db.csv"
            df.to_csv(output_file, index=False)

            print(f"Saved data to {output_file}!")

        # close cursor and connection to wrap up
        cursor.close()
        conn.close()
        print("Connection to ProductDB closed")

        
        return True
    
    # error handling in case connection or extraction fails
    except mysql.connector.Error as e:
        print(f"Oh no, error when attempting to extarct data from ProductDB: {e}")
        # if connection was created nontheless, try to close it down:
        if "conn" in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection to Database closed")
        return False
    
# allows the script to be run directly
if __name__ == "__main__":
    success = extract_from_productdb()
    if success:
        print("\nSuccess: All data from ProductDB has been extracted!")
    else:
        print("\nFailure: Could not extract data from ProductDB ;___;")
