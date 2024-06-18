import pandas as pd
from pymongo import MongoClient
import glob

# MongoDB connection setup
client = MongoClient('mongodb://10.201.8.215:27017/')
db = client['CN_INDEX_AND_ETF']

def create_timeseries_collection(db, collection_name):
    """Create a time-series collection if it doesn't exist."""
    if collection_name not in db.list_collection_names():
        db.create_collection(
            collection_name,
            timeseries={
                'timeField': 'date',
                'metaField': 'metadata',
                'granularity': 'seconds'
            }
        )
        print(f"Created timeseries collection: {collection_name}")
    else:
        print(f"Collection {collection_name} already exists.")

def load_csv_to_mongodb(file_path, collection_name):
    """Load a CSV file into MongoDB."""
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')  # Ensure the date format matches your CSV files
    df.rename(columns={'symbol': 'orderbook_id'}, inplace=True)

    # Convert DataFrame to dictionary
    data_json = df.to_dict('records')

    # Insert data into MongoDB
    db[collection_name].insert_many(data_json)
    print(f"Inserted {len(data_json)} records into {collection_name} from {file_path}")

def process_all_files(directory_path, collection_name):
    """Process all CSV files in the given directory."""
    create_timeseries_collection(db, collection_name)
    for file_path in glob.glob(directory_path + '/*.csv'):
        load_csv_to_mongodb(file_path, collection_name)

if __name__ == "__main__":
    directory_path = r'C:\Users\Simon\Downloads\Quantlab-4.2.1\lab\data\quotes'  # Windows path
    collection_name = 'kline-1D'  # Collection name for all CSV data
    process_all_files(directory_path, collection_name)
