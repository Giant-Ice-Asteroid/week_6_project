import pandas as pd
import os


def transform_sales_data(): 
    """
    function that transform the sales related data set CUSTOMERS, ORDERS and ORDER_ITEMS
    loads previously transformed data for reference and validation
    
    NB to be run as the last transformation script
    
    """
    
    print("Initiating tranformation of sales data (customers, orders and order_items)..")
    
    #  ensure dir
    if not os.path.exists("transformed_data"):
        os.makedirs("transformed_data")
        print("Created 'transformed_data' directory")
        
    #loading previously transformed data for validation of IDs etc
    try:
        products_df = pd.read_csv("transformed_data/products.csv")
        stores_df = pd.read_csv("transformed_data/stores.csv")
        staffs_df = pd.read_csv("transformed_data/staffs.csv")
        
        print(f"Loaded previously transformed data for validation purposes: {len(products_df)} products data, {len(stores_df)} stores data, and {len(staffs_df)} staffs data")
    except Exception as e:
        print("Error when loading previously transformed data: {e}")
        return False
    
##################################### CUSTOMERS ################################


    print("\n Transforming customers data set --->")

    try:
        customers_df = pd.read_csv("extracted_data/customers_from_api.csv")
        print(f"Successfully loaded {len(customers_df)} rows of data from customers data set")
    except Exception as e:
        print("Error when loading customers data: {e}")
        return False
    
    # copy ok ok
    transformed_customers_df = customers_df.copy()
    
    # data type conversion for customers data set columns
    
    transformed_customers_df["customer_id"] = transformed_customers_df["customer_id"].astype(int) #customer_id -> int (primary key)
    print("converted customer_id to integer")

    for col in ['first_name', 'last_name', 'phone', 'email', 'street', 'city', 'state']: # -> all strings
        if col in transformed_customers_df.columns:
            transformed_customers_df[col] = transformed_customers_df[col].fillna('').astype(str)
    print("Converted 'first_name', 'last_name', 'phone', 'email', 'street', 'city', 'state' to string values and converted NaN to empty strings")
    
    if "zip_code" in transformed_customers_df.columns:
        transformed_customers_df["zip_code"] = pd.to_numeric(transformed_customers_df["zip_code"], errors="coerce") # zip_code -> numeric first
        transformed_customers_df["zip_code"] = transformed_customers_df["zip_code"].fillna(0).astype(int) # NaN are replaced ith 0 and zip_code -> int
        print("Zip codes are converted to numeric, NaN are replaced with 0, and zip codes are finally converted to integers")
        
    # Save it aaaall
    transformed_customers_df.to_csv("transformed_data/customers.csv", index=False)
    print(f"Transformed and saved {len(transformed_customers_df)} rows of customers data")
    
    
    #################### ORDERS ##############################
    
    print("Initiating transformation of ORDERS data")
    
    try:
        orders_df = pd.read_csv("extracted_data/orders_from_api.csv")
        print(f"Loaded {len(orders_df)} rows of orders data for tranformation")
    except Exception as e:
        print(f"Error when loading orders data: {e}")
        return False
    
    # copy copy copy
    transformed_orders_df = orders_df.copy()
    
    # data type conversions
    transformed_orders_df["order_id"] = transformed_orders_df["order_id"].astype(int) # order_id -> int
    transformed_orders_df["customer_id"] = transformed_orders_df["customer_id"].astype(int) # customer_id -> int
    transformed_orders_df["order_status"] = transformed_orders_df["order_status"].astype(int) # order_status -> int
    print("converted order_id, customer_id, and order_status to integer")
    
    # data type conversion cont... Dates <____<
    # converting string dates into DATETIME objects with pandas
    transformed_orders_df["order_date"] = pd.to_datetime(transformed_orders_df["order_date"], format="%d/%m/%Y", errors="coerce") #order_date -> datetime 
    transformed_orders_df["required_date"] = pd.to_datetime(transformed_orders_df["required_date"],format="%d/%m/%Y", errors="coerce") #required_date -> datetime
    transformed_orders_df["shipped_date"] = pd.to_datetime(transformed_orders_df["shipped_date"], format="%d/%m/%Y", errors="coerce") #shipped_date -> datetime
    print("converted order_date, required_date, and shipped_date to datetime data types. Note that shipped_date values may Null values (not shipped yet)")
    
    # Next, changing store names to store IDs (and thus creation of relationship with stores table)
    if "store" in transformed_orders_df.columns:
        store_name_to_id = dict(zip(stores_df["name"], stores_df["store_id"]))
        transformed_orders_df["store_id"] = transformed_orders_df["store"].map(store_name_to_id)
        transformed_orders_df = transformed_orders_df.drop(columns=["store"])
        print("Converted store names to store_id referencing staffs table")
        
    # changing staff_name to staff_id. note that staff_name in orders corresponds to first_name in our staffs data set
    if "staff_name" in transformed_orders_df.columns:
        staff_name_to_id = dict(zip(staffs_df["first_name"], staffs_df["staff_id"]))
        transformed_orders_df["staff_id"] = transformed_orders_df["staff_name"].map(staff_name_to_id)
        transformed_orders_df = transformed_orders_df.drop(columns=["staff_name"])
        print("converted staff names to staff_id referencing staffs table")
        
    # lastly, validating customer_id's, ensuring that all orders are referencing customers that exist
    # OPting to setting potential orders with invalid customer_id to NULL to keep the data
    valid_customer_ids = set(transformed_customers_df["customer_id"])
    invalid_customer_mask = ~transformed_orders_df["customer_id"].isin(valid_customer_ids)
    
    if invalid_customer_mask.any():
        invalid_count = invalid_customer_mask.sum()
        transformed_orders_df.loc[invalid_customer_mask, "customer_id"] = None
        print(f"Attention: encountered {invalid_count} orders where customer_id is invalid! Where applicable, customer_id set as NULL")
    else:
        print("No issues encountered when validating customer_id in orders data set")
        
    # save it all
    transformed_orders_df.to_csv("transformed_data/orders.csv", index=False)
    print(f"Saved {len(transformed_orders_df)} transformed rows of orders data")
    
    
    ############################# ORDER_ITEMS ##################################################
    
    print("Initiating transformation of order_items data set ---->")
    
    try:
        order_items_df = pd.read_csv("extracted_data/order_items_from_api.csv")
        print(f"loaded {len(order_items_df)} rows of order_items from the extracted order_items data set")
    except Exception as e:
        print(f"Error encounted when attempting to load the extracted order_items data set: {e}")
        return False
    
    # copy dataframe
    transformed_order_items_df = order_items_df.copy()
    
    # conversion of datatypes
    transformed_order_items_df["order_id"] = transformed_order_items_df["order_id"].astype(int) #order_id -> int
    transformed_order_items_df["product_id"] = transformed_order_items_df["product_id"].astype(int) # product_id -> int
    transformed_order_items_df["quantity"] = transformed_order_items_df["quantity"].astype(int) # quantity -> int
    print("Converted order_id, item_id, product_id, and quantity to integers")    
    transformed_order_items_df["list_price"] = pd.to_numeric(transformed_order_items_df["list_price"], errors="coerce") #list_price -> numeric (to allow decimals -> float)
    transformed_order_items_df["discount"] = pd.to_numeric(transformed_order_items_df["discount"], errors="coerce") #discount -> numeric (ditto)
    print("Converted list_price and discount to numeric (-> float) values")
    
    #next up, validating order_id against the orders data set, ensuring that the ordered items refer to actual orders
    valid_order_ids = set(transformed_orders_df["order_id"])
    invalid_order_mask = ~transformed_order_items_df["order_id"].isin(valid_order_ids)
    
    if invalid_order_mask.any():
        invalid_count = invalid_order_mask.sum()
        transformed_order_items_df = transformed_order_items_df[~invalid_order_mask] # deletes the bad rows
        print(f"Warning!! Found {invalid_count} rows of order_items data with invalid order_ids - These rows have been removed from the transformed order_items data")
    else:
        print("Wow, all order items reference valid order_id - Nice data")
        
    # same thing with product_id's - ensuring that all products in order_items reference actual products in the products table
    valid_product_ids = set(products_df["product_id"])
    invalid_product_mask = ~transformed_order_items_df["product_id"].isin(valid_product_ids)
    
    if invalid_product_mask.any():
        invalid_count = invalid_product_mask.sum()
        transformed_order_items_df.loc[invalid_order_mask, "product_id"] = None # opting to set these as NULL rather than delete
        print(f"Warning!! Found {invalid_count} rows of order_items data with invalid product_id's - these set as NULL values")
    else:
        print("Yay, all order items reference valid product_id - Nice data")
        
    # ensuring all quantities are positive 
    negative_qty_mask = transformed_order_items_df["quantity"] <= 0
    if negative_qty_mask.any():
        negative_count = negative_qty_mask.sum()                   
        transformed_order_items_df.loc[negative_qty_mask, 'quantity'] = 1 # Fixing the issue by setting to a minimum value of 1
        print(f"Oops! Found {negative_count} order items with zero or negative quantities, that doens't make sense. Correctly the affected rows by setting val as 1")

    # likewise, ensure that all discounts are between 0 and 1 (=0% to 100%)
    invalid_discount_mask = (transformed_order_items_df["discount"] < 0) | (transformed_order_items_df["discount"] > 1)
    if invalid_discount_mask.any():
        invalid_count = invalid_discount_mask.sum()
        transformed_order_items_df.loc[transformed_order_items_df["discount"] < 0, "discount"] = 0 # if negative, set to 0
        transformed_order_items_df.loc[transformed_order_items_df["discount"] > 1, "discount"] = 1 # if > 1 set to 1
        print(f"Warning: Found {invalid_count} order items with invalid discount values.. Vals > 1 set to 1, vals < 0 set to 0 ")
        
    # FINALLY, saving the transformed order items data..
    transformed_order_items_df.to_csv("transformed_data/order_items.csv", index=False)
    print(f"Saved {len(transformed_order_items_df)} rows of transformed order_item records")
    
    # Summarize the overall transformation
    print("\nSales data transformation complete!")
    print(f"Transformed {len(transformed_customers_df)} rows of customers, {len(transformed_orders_df)} rows of orders, and {len(transformed_order_items_df)} rows order_items data")
    return True

# This allows the script to be run directly
if __name__ == "__main__":
    success = transform_sales_data()
    if success:
        print("Final step of transformation complete: Sales data transformation successful.")
    else:
        print("NooooOoooooo, couldn't transform sales data")