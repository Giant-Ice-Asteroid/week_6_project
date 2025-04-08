import pandas as pd
import os

def transform_location_data():
    """
    Function that handles the STORES and STAFFS data set
    - loads the extracted data from the "extracted_data" dir
    - creates a copy to keep the original intact
    - ensures correct data types where applicable
    - adds a store_id to the STORES data set as a primary key
    - adds a staff_id to the STAFFS data set as a primary key
    - standardises name columns in STAFFS
    - creates relationship between the STORES and STAFFS tables by changing "store_name" in STAFFS to "store_id" (as in STORES)
    - saves the transformed data in the transformed_data dir
    """

### 
    print("Initiating transformation of location data (stores and staffs)...")
    
    # as before, creates a directory to store the transformed data if it doesn't exist already
    if not os.path.exists("transformed_data"):
        os.makedirs("transformed_data")
        print("Created 'transformed_data' directory")
    
    ###### STORES ############
    
    print("\nPart 1 of 2: Transforming stores data...")
    
    # load stores data
    try:
        stores_df = pd.read_csv("extracted_data/stores_from_csv.csv")
        print(f"Loaded {len(stores_df)} records from previously extracted stores data set")
    except Exception as e:
        print(f"Error loading stores data: {e}")
        return False
    
    # copy the DataFrame 
    transformed_stores_df = stores_df.copy()
    
    # first: adding store_id column if it doesn't exist (to be used as primary key)
    if 'store_id' not in transformed_stores_df.columns:
        # we do this by creating incrementing IDs starting from 1
        transformed_stores_df["store_id"] = range(1, len(transformed_stores_df) + 1)
        print("Added a new store_id column (primary key)")
    
    # next, loop through each column in the df and check if datatype = object (string in pandas)
    # for those, missing values (na/NaN) is replaced with an empty string (" ") and each column is set as string type
    # doesn't seem super necessary in this set, but doing it for consistency
    for col in transformed_stores_df.columns:
        if transformed_stores_df[col].dtype == "object":
            transformed_stores_df[col] = transformed_stores_df[col].astype(str)    

    # making sure that zip_code can be treated as intergers
    transformed_stores_df["zip_code"] = transformed_stores_df["zip_code"].astype(int)
    print("Converted zip_code to integer")

    # lastly, saving the newly transformed stores data in its target dir
    transformed_stores_df.to_csv("transformed_data/stores.csv", index=False)
    print(f"Saved {len(transformed_stores_df)} transformed STORES records")
    
    ############### STAFFS #####################

    print("\nInitiating transformation of staffs data...")
    
    # load staffs data
    try:
        staffs_df = pd.read_csv("extracted_data/staffs_from_csv.csv")
        print(f"Loaded {len(staffs_df)} records from previously extracted staffs data set")
    except Exception as e:
        print(f"Error when loading staffs data: {e}")
        return False
    
    # again, copy the df
    transformed_staffs_df = staffs_df.copy()
    
    # starting by renaming  the "name" column to "first_name" for clarity
    if "name" in transformed_staffs_df.columns:
        transformed_staffs_df = transformed_staffs_df.rename(columns={"name": "first_name"})
        print("Renamed 'name' column to 'first_name'")
    
    # then adding a NEW staff_id column if it doesn't exist already (to be used as primary key)
    if "staff_id" not in transformed_staffs_df.columns:
        transformed_staffs_df["staff_id"] = range(1, len(transformed_staffs_df) + 1)
        print("Added a new column: staff_id, to be used as primary key")
    
    # convert store_name to store_id and map it using the transformed stores df just created (where store_id was added)
    if "store_name" in transformed_staffs_df.columns:

        # zip() pairs the two columns in tuples, and then into a dict (e.g {'Santa Cruz Bikes': 1, 'Baldwin Bikes': 2, 'Rowlett Bikes': 3})
        # this dict can then be used to look up ID for store name
        store_name_to_id = dict(zip(transformed_stores_df['name'], transformed_stores_df['store_id']))

        # then we can create a new "store_id" column by replacing each store name with its corresponding store_id:
        # for each staff member .map takes each store_name and looks up its corresponding store_id in the dict
        # the IDs are then assigned to a new column on the left called store_id
        # lastly we the store_name column is dropped(deleted)
        transformed_staffs_df["store_id"] = transformed_staffs_df["store_name"].map(store_name_to_id)
        transformed_staffs_df = transformed_staffs_df.drop(columns=["store_name"])
        print("Converted store_name to store_id in a new store_id column and dropped store_name column")
    
    # next we need to handle manager_id because first row is empty
    if "manager_id" in transformed_staffs_df.columns:

        # Convert manager_id to floats in pandas first
        # this is because, in pandas, int columns cant store null/NaN values, but float columns can
        # errors="coerce" tells pandas to convert any values that can't be interpreted as numbers (like stirngs) to NaN rather than raise error
        transformed_staffs_df["manager_id"] = pd.to_numeric(transformed_staffs_df["manager_id"], errors="coerce")     

        # got to keep manager_id as NaN for top manager and as the rest as integers..
        # to do this, we create a boolean array (=mask) where rows where manager_id is not Nan == True
        # the .loc method allows us to select only specified rows, choosing only the rows where mask is True in the manager_id coloumn
        # these are then converted to integer type (though will prob come out as float in the csv due to pandas shenanigans)
        mask = transformed_staffs_df["manager_id"].notna()
        transformed_staffs_df.loc[mask, "manager_id"] = transformed_staffs_df.loc[mask, "manager_id"].astype(int)
        
        print("Processed manager_id values: kept NaN for top manager, converted others to integers")

    # also ensure that "active" column is an integer type (0 or 1)
    if "active" in transformed_staffs_df.columns:
        transformed_staffs_df["active"] = transformed_staffs_df["active"].astype(int)
        print("Converted values in 'active' column to integers")
       

    # as before standardise remaining columns
    for col in transformed_staffs_df.columns:
        if transformed_staffs_df[col].dtype == "object":  # string columns
            transformed_staffs_df[col] = transformed_staffs_df[col].fillna('').astype(str)
 

    # then save the transformed staffs data to its dir
    transformed_staffs_df.to_csv("transformed_data/staffs.csv", index=False)
    print(f"Saved {len(transformed_staffs_df)} transformed staff records")

    return True

# This allows the script to be run directly
if __name__ == "__main__":
    success = transform_location_data()
    if success:
        print("Script ran successfully")
    else: # such as in case of exception
        print("Failure: Could not transform location data.. ")