# Bike Corp ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for processing bicycle shop data from multiple sources into a unified database structure.

## Project Overview
The ETL pipeline processes data through three sequential phases:

Extraction: Pulls data from different sources
Transformation: Cleans and standardizes the data
Loading: Inserts the transformed data into the target database

Each phase is implemented through separate, independent scripts.

## Project Structure

### Extraction Scripts

extract_from_source_database.py: Extracts data from the ProductDB MySQL database

Extracts brands, categories, products, and stocks tables
Uses credentials from cred_info.json
Saves extracted data as CSV files in the extracted_data directory


extract_from_csv.py: Processes local CSV files

Extracts data from staffs.csv and stores.csv
Saves extracted data in the extracted_data directory


extract_from_api.py: Fetches data from the local API

Connects to endpoints: customers, orders, and order_items
Requires the API to be running separately (via fastapi run main.py)
Saves extracted data as CSV files in the extracted_data directory

### Transformation Scripts

transform_location_data.py: Processes extracted stores and staffs data
transform_product_data.py: Processes extracted products and stocks data
transform_sales_data.py: Processes extracted customers, orders and order_item data
transform_reference_data.py: Processes extracted brands and categories data


Each transformation script:

Reads CSV files from the extracted_data directory
Performs data type conversions, cleaning, and validation
Saves transformed data to the transformed_data directory

### Loading Script

load_transformed_data.py: Loads all transformed data into the target database

Connects to BikeCorpDB using credentials from cred_info.json
Reads transformed CSV files
Inserts data into corresponding database tables



## Setup and Installation

Clone the repository
Install required dependencies:
pip install pandas mysql-connector-python requests fastapi uvicorn

Ensure your database credentials are stored in a cred_info.json file:
json{
  "host": "your_host",
  "user": "your_username",
  "password": "your_password"
}


## Usage
Running the ETL Process
The ETL process must be executed in sequence:

### Run extraction scripts:
python extract_from_db.py
python extract_from_csv.py
python extract_from_api.py
Note: For the API extraction, you need to start the API server first in a separate terminal:
fastapi run main.py

Run transformation scripts in this specific order:
python transform_location_data.py
python transform_reference_data.py
python transform_product_data.py
python transform_sales_data.py

### Run loading script:
python load_transformed_data.py


## Data Sources

ProductDB Database: Contains brands, categories, products, and stocks data
CSV Files: Contains staffs and stores information
API: Provides customers, orders, and order_items data

## Directory Structure
project/
├── cred_info.json              # Database credentials
├── data/                       # Original CSV data files
│   ├── staffs.csv
│   └── stores.csv
├── extracted_data/             # Storage for extracted data
├── transformed_data/           # Storage for transformed data
├── extract_from_db.py          # Database extraction script
├── extract_from_csv.py         # CSV extraction script
├── extract_from_api.py         # API extraction script
├── transform_*.py              # Transformation scripts
└── load_to_db.py               # Database loading script

## Requirements

Python 3.6+
pandas
mysql-connector-python
requests
FastAPI (for the API server)
uvicorn (for running the API server)
