import pandas as pd
import os

def transform_product_data():
    """
    loads previously transformed data for referencing/validation
    ensures correct data types
    validates brand_id, category_id (products) and product_id (stocks)
    changed store_name to store_id in stocks
    NB shouldn't be run untill AFTER the location and reference transformation functions have run (their df are referenced here) 
    """

    print("Initiating transformation of product data (PRODUCTS and STOCKS)...")

    # again creates a directory to store the transformed data if it doesn't exist already (though it ought to)
    if not os.path.exists("transformed_data"):
        os.makedirs("transformed_data")
        print("Created 'transformed_data' directory")  

    # FIRST we have to load some of previously transformed data - brands, categories and stores
    # this is because IDs from the brands and categories will be used to validate our data later
    try:
        brands_df = pd.read_csv("transformed_data/brands.csv")
        categories_df = pd.read_csv("transformed_data/categories.csv")
        stores_df = pd.read_csv("transformed_data/stores.csv")    
        print(f"Loaded previously transformed data: {len(brands_df)} rwos from brands, {len(categories_df)} rows from categories, and {len(stores_df)} rows from stores ")
    except Exception as e:
        print("Error when attempting to load previously transformed data - Ensure that the transformation scripts for location and reference data has been run")
        return False
    
#################### PRODUCTS #################

    print("Initiating transformation of the products data set --->")
    
    # loading the extracted data into a df
    try:
        products_df = pd.read_csv("extracted_data/products_from_db.csv")
        print(f"Loaded {len(products_df)} rows of products records")
    except Exception as e:
        print(f"Error when loading products data: {e}")
        return False
    
    #copying the df
    transformed_products_df = products_df.copy()
    
    # beginning the tranformation by ensuring correct data types for all columsn in products
    
    transformed_products_df["product_id"] = transformed_products_df["product_id"].astype(int) #product_id -> int (primary key)
    print("converted product_id to integers")
    transformed_products_df["product_name"] = transformed_products_df["product_name"].astype(str) #product_name -> string
    print("Converted product_name to string type")
    transformed_products_df["brand_id"] = pd.to_numeric(transformed_products_df["brand_id"], errors="coerce") #brand_id -> num. using pd.to_num which allows handling of NaN
    print("Converted brand_id to numeric")
    transformed_products_df["category_id"] = pd.to_numeric(transformed_products_df["category_id"], errors="coerce") #category_id -> numeric (might encounter NaN)
    print("Converted category_id to numeric")
    transformed_products_df["model_year"] = transformed_products_df["model_year"].astype(int) #model_year -> int
    print("converted model_year to integers")
    transformed_products_df["list_price"] = pd.to_numeric(transformed_products_df["list_price"], errors="coerce") #list_price -> numeric (to be float)
    print("Converted list_price to numeric (float)")
    
    # validating the brand IDs in products by comparing to brands
    # first extracting the brand_id column from the brands df resulting in a list 
    # the list is converted to a set to eliminate potential duplicates and for optimisation
    valid_brand_ids = set(brands_df["brand_id"])
    
    #next a we create a mask for catching invaLid brand IDs:
    # a list of all brand ID in products is checked against the set of valid IDs created above and returns a boolen
    # ~ operator inverts those booleans value, so that True becomes False and vice versa
    # the invalid_brand_mask series of bools now has the value of True for the rows(if nay) that need fixing
    invalid_brand_mask = ~transformed_products_df["brand_id"].isin(valid_brand_ids)
    
    # here then any() return True if at least one value in the mask is True
    # potentential invalid ID are then counted with .sum (True is 1 and False is 0)
    # where invalid ID are encountered, they're changed to NULL at the affected rows 
    if invalid_brand_mask.any():
        invalid_count = invalid_count.sum()
        print(f"Attention: Located {invalid_count} products with invalid brand_id values..!")
        transformed_products_df.loc[invalid_brand_mask, "brand_id"] = None
        print("Invalid brand_id values changed to NULL")
    else:
        print("All good - No invalid brand_id values identified!")
        
    #repeating the procedure for category_id values against category data set..
    
    valid_category_ids = set(categories_df["category_id"])
    invalid_category_mask = ~transformed_products_df["category_id"].isin(valid_category_ids)
    
    if invalid_category_mask.any():
        invalid_count = invalid_category_mask.sum()
        print(f"Attention: Located {invalid_count} products with invalid category_id values..!")
        transformed_products_df.loc[invalid_brand_mask, "category_id"] = None
        print("Invalid categoryd_id values changed to NULL")
    else:
        print("All the category_id values are valid - good data quality!")
    
    # we can then save the transformed products data
    transformed_products_df.to_csv("transformed_data/products.csv", index=False)
    print(f"Saved {len(transformed_products_df)} transformed product records")
    
    ##################### STOCKS #####################
    
    print("Initiating transformation of the stocks data set --->")
    
    #first, loading the stocks data..
    try:
        stocks_df = pd.read_csv("extracted_data/stocks_from_db.csv")
        print(f"Loaded {len(stocks_df)} rows from the extracted stocks data set")
    except Exception as e:
        print(f"Encounted error when loeading stokcs data: {e}")
        return False
    
    #copy time
    transformed_stocks_df = stocks_df.copy()
    
    # beginning the transforming of stocks data by converting store_name to store_id
    # doing this in order to be able to establish relationships between tables later
    # first a dict is created to map store names to store IDs using the store df
    # then each store "name" is replaced by the corresponding "store_id" with .map
    if "store_name" in transformed_stocks_df.columns:
        store_name_to_id = dict(zip(stores_df["name"], stores_df["store_id"]))
        transformed_stocks_df["store_id"] = transformed_stocks_df["store_name"].map(store_name_to_id)
        print("converted store names to store IDs in stocks data set")
        # can then remove the store_name columns which is now redundant 
        transformed_stocks_df = transformed_stocks_df.drop(columns=["store_name"])
        print("Removed store_name column in stocks data set")
    else:
        print("store_name column not found")
    
    #moving on to data type conversions:
    transformed_stocks_df["product_id"] = transformed_stocks_df["product_id"].astype(int) # product_id -> int
    print("Converted product_id to integers")
    transformed_stocks_df["quantity"] = transformed_stocks_df["quantity"].astype(int) # quantity -> int
    print("converted quantity to integer")
    
    #lastly, validation that product_id values in the stocks data exist in the products data 
    valid_product_ids = set(transformed_products_df["product_id"])
    invalid_product_mask = ~transformed_stocks_df["product_id"].isin(valid_product_ids)
    
    if invalid_product_mask.any():
        invalid_count = invalid_product_mask.sum()
        print(f"Warning: Encountered {invalid_count} rows in stocks data set with invalid product IDs")

        #opting to delete any rows in stocks with invalid product ID since it represents non-existing product
        transformed_stocks_df = transformed_stocks_df[~invalid_product_mask]
        print(f"Removed {invalid_count} stocks rows with invalid product IDs")
    else:
        print("All inventory in stock has a valid product ID - Yay!")
        
    #save the transformed stocks data
    transformed_stocks_df.to_csv("transformed_data/stocks.csv", index=False)
    print(f"Saved {len(transformed_stocks_df)} rows of stocks records")
    return True

#  allows the script to be run directly
if __name__ == "__main__":
    success = transform_product_data()
    if success:
        print("Succes: Product data transformation successful.")
    else:
        print("Oooh nooo: Could not transform product data.")