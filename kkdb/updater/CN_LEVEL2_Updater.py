import os
import pickle
import pandas as pd
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from concurrent.futures import ThreadPoolExecutor

# Constants for database and collection names
DB_NAME = 'cn_stock_level_2'
COLLECTION_ORDERBOOK = 'order'
COLLECTION_TRADES = 'trader'

# MongoDB Client Initialization
client = MongoClient('mongodb://10.201.8.215:27017/')
db = client[DB_NAME]

# Function to create time-series collections
def create_timeseries_collection(collection_name, time_field):
    if collection_name not in db.list_collection_names():
        db.create_collection(
            collection_name,
            timeseries={
                'timeField': time_field,
                'metaField': 'metadata',
                'granularity': 'seconds'
            },
        )
        db[collection_name].create_index([(time_field, ASCENDING)])

# Create collections
create_timeseries_collection(COLLECTION_ORDERBOOK, 'timestamp')
create_timeseries_collection(COLLECTION_TRADES, 'timestamp')

# Function to load and insert data from a single file
def load_data_from_file(file_path, collection_name):
    try:
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        if isinstance(data, pd.DataFrame):
            # Extract orderbook_id from filename
            filename = os.path.basename(file_path)
            orderbook_id = filename.split('_')[1].replace('.pkl', '')
            data["orderbook_id"] = orderbook_id

            # Convert datetime fields, replace NaT with Unix epoch
            for col in data.select_dtypes(include=['datetime']):
                data[col] = pd.to_datetime(data[col], errors='coerce')
                data[col] = data[col].fillna(pd.Timestamp('1970-01-01'))
            data_json = data.to_dict('records')
            db[collection_name].insert_many(data_json)
            print(f"Inserted {len(data_json)} records from {file_path} into {collection_name}")
    except BulkWriteError as bwe:
        print(bwe.details)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")

# Function to load data from a directory
def load_data(directory_path, collection_name):
    with ThreadPoolExecutor() as executor:
        futures = []
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.pkl'):
                    file_path = os.path.join(root, file)
                    futures.append(executor.submit(load_data_from_file, file_path, collection_name))
        for future in futures:
            future.result()  # Wait for all futures to complete

# Directory paths for the data
order_directory_path = 'M:\\order_2023-01-03_2023-02-06'
trade_directory_path = 'M:\\trade_2023-01-03_2023-02-06'

# Load data into MongoDB for both collections
load_data(order_directory_path, COLLECTION_ORDERBOOK)
load_data(trade_directory_path, COLLECTION_TRADES)

print("Data loading complete.")
