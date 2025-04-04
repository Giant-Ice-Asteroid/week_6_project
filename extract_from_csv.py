import pandas as pd
import os


def extract_from_csv_files():
    """
    Function that axtracts data from local CSV files
    the data is then saved to the extracted_data directory
    """

    print("Attempting to extract data from local CSV files..")

    # as before, create a directory to store the extracted data if it doesn't exist already
    if not os.path.exists("extracted_data"):
        os.makedirs("extracted_data")
        print("Created 'extracted_data' directory")

    #defining what files to be extracted
    # the path.join specifies the path to the data directory 
    csv_files = {
        "staffs": os.path.join("data","staffs.csv"),
        "stores": os.path.join("data", "stores.csv")
    }

    #going through and extracting each CSV file..
    for table_name, file_name in csv_files.items():
        try:
            print(f"\nExtracting data from {file_name}..")

            # checking if the file exists
            if not os.path.exists(file_name):
                print(f"Error: File {file_name} not found")
                continue

            #next we read the CSV file into a pandas df
            # pandas should automatically detect headers and data types from the CSV
            df = pd.read_csv(file_name)

            # then saving the data to the extraction dir
            output_file = f"extracted_data/{table_name}_from_csv.csv"
            df.to_csv(output_file, index=False)
            print(f"\nSaved data to {output_file}..!")
        
        except Exception as e:
            print(f"Sorry, error when attempting to extract {file_name: {e}}")
    
    return True

# allows the script to be run directly
if __name__ == "__main__":
    success = extract_from_csv_files()
    if success:
        print("\nSuccess: Data from local CSV files has been extracted")
    else:
        print("\nFailure: Could not extract data from local CSV files :<")