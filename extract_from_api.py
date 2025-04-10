import requests #used for making HTTP reuqests to the API
import pandas as pd
import os
import json

def extract_from_api():
    """
    this function extracts data from an fastAPI server
    NB: it requires the API to be running already
    --> the API server can be started by running the main.py script
    """

    #print("Beginning process of extracting data from API")

    # creates the extracted_data directory if it doesn't exist already
    if not os.path.exists("extracted_data"):
        os.makedirs("extracted_data")
        print("Created 'extracted_data' directory")

    #next, defines the endpoints(=data sources available from the API)
    endpoints = ["customers", "orders", "order_items"]
    # defines the address where the API is running
    # "http://192.168.20.171:8000"
    base_url = "http://localhost:8000" 
    
    # iterating through each endpoint..
    for endpoint in endpoints:
       
        try:
            
            #making a varible that contains the full url address for each endpoint
            full_url = f"{base_url}/{endpoint}"
            
            #requests.get() sends an HTTP GET request to the newly created url
            # the API server receives the request and sends back data
            # the response variable below contains everything the server sends back (data, status codes, headers)
            response = requests.get(full_url)

            response_text = response.text
            # checks if the request was successful (=HTTP status code 200)
            if response.status_code == 200:

                response_text = json.loads(response_text)
                data = json.loads(response_text)
                #can then parse the response (which is in the JSON file format) into a python data structure:
                #data = response.json()

                #then converts it to a pandas df 
                df = pd.DataFrame(data)

                #defining where the output file goes:
                output_file = f"extracted_data/{endpoint}_from_api.csv"

                #next saving it as a CSV file
                df.to_csv(output_file, index=False)
                print(f"Saved {len(df)} records to {output_file}")

            else:
                #error handling
                print(f"Error when accessing {endpoint}: Status code {response.status_code}")
                print(f"Response text: {response.text}")

        except Exception as e:
            print(f"Error when processing {endpoint}: {e}")

    return True

if __name__ == "__main__":
    success = extract_from_api()
    if success:
        print("\nSuccess: Data from API server has been extracted")
    else:
        print("\nFailure: Could not extract data from API :<")

    

