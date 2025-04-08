import pandas as pd
import os

def transform_reference_data():
    """
    Function that handles the BRANDS and CATEGORIES data set
    - loads the extracted data from the "extracted_data" dir
    - creates a copy to keep the original intact
    - ensures correct data types where applicable
    - removes duplicates
    - saved the transformed data in a new transformed_data dir
    """

    print("Initiating transformation of reference data (brands and categories..)")

    # first, create a new directory for the newly transformed data 
    if not os.path.exists("transformed_data"):
        os.makedirs("transformed_data")
        print("Created new directory: transformed_data")

    ############ BRANDS ###############

    print("Part 1 of 2: Initiating transformation of the brands data set --->")

    # load the extracted brands csv data into a pandas df
    try:
        brands_df = pd.read_csv("extracted_data/brands_from_db.csv")
        print(f"Loaded {len(brands_df)} records from previously extracted brands data set")
    except Exception as e:
        print(f"Error loading brands data set: {e}")
        return False
    
    # then the dataframe is copied to avoid modifying the original data
    transformed_brands_df = brands_df.copy()

    # first step of tranformation -> data types
    # brand_id type is set to integer,brand_name data type as string 
    transformed_brands_df["brand_id"] = transformed_brands_df["brand_id"].astype(int)
    transformed_brands_df["brand_name"] = transformed_brands_df["brand_name"].astype(str)

    # since data set is small, no need to check for duplicates etc.

    # lastly, save the transformed_brands_df as a csv file in the new dir
    transformed_brands_df.to_csv(("transformed_data/brands.csv"), index=False)
    print(f"The extracted BRANDS data set has been transformed and {len(transformed_brands_df)} reocrds have been saved to the transformed_data directory")

    ################# CATEGORIES ###############

    print("Initiating transformation of the categories data set --->")

    # load the categories data
    try:
        categories_df = pd.read_csv("extracted_data/categories_from_db.csv")
        print(f"Loaded {len(categories_df)} records from the previously extracted category records")
    except Exception as e:
        print(f"Error loading categories data: {e}")
        return False
    
    # Copying the DataFrame as before
    transformed_categories_df = categories_df.copy()
    
    # data types: category_id is set as int; category_name as string
    transformed_categories_df['category_id'] = transformed_categories_df['category_id'].astype(int)
    transformed_categories_df['category_name'] = transformed_categories_df['category_name'].astype(str)
    
    
    # Save the transformed categories data
    transformed_categories_df.to_csv("transformed_data/categories.csv", index=False)
    print(f"Saved {len(transformed_categories_df)} transformed category records")
    
    print("\nTransformation complete for BRANDS and CATEGORIES data!")
    return True

# allows the script to be run directly
if __name__ == "__main__":
    success = transform_reference_data()
    if success:
        print("Script ran successfully")
    else: # such as in case of exception
        print("Failure: Could not transform reference data.. ")