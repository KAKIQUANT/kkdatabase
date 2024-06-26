import pymongo
from kkdb.utils.check_root_base import find_and_add_project_root
import pandas as pd
import yaml

cfg = yaml.load(open(find_and_add_project_root() + "/configs/db/mongodb.yaml"), Loader=yaml.FullLoader)
db_str = cfg["str"]
def get_client(db_type: str = "mongodb"):
    if db_type == "mongodb":
        return pymongo.MongoClient(db_str)
    # Add condition for other database types if needed
    if db_type == "mysql":
        pass
    if db_type == "dolphindb":
        pass


def get_client_str(db_type: str = "mongodb"):
    if db_type == "mongodb":
        return db_str
    # Add condition for other database types if needed
    if db_type == "mysql":
        raise NotImplementedError("MySQL not implemented yet.")
    if db_type == "dolphindb":
        raise NotImplementedError("DolphinDB not implemented yet.")


async def insert_data_to_mongodb(collection, data: pd.DataFrame) -> None:
    if not data.empty:
        data_dict = data.to_dict("records")
        await collection.insert_many(data_dict)  # type: ignore


def mongodb_general_info(client) -> dict:
    """
    Returns general information about the MongoDB database.
    """
    db_list = list(
        set(client.list_database_names())
        - set(["admin", "config", "local", "crypto_db"])
    )
    db_info = {}
    for db_name in db_list:
        db = client[db_name]
        collection_list = db.list_collection_names()
        db_info[db_name] = collection_list
    return db_info


def get_collection_date_range(collection, instId, bar):
    """
    Returns the earliest (start) and latest (end) timestamps for a specific instId and bar size
    in the specified MongoDB collection.

    Parameters:
    - collection: A pymongo collection object.
    - instId: The instrument ID to filter documents by.
    - bar: The bar size to filter documents by.

    Returns:
    - A tuple containing the start and end timestamps.
    """
    # Filter documents by instId and bar, then aggregate to find min and max timestamps
    pipeline = [
        {"$match": {"instId": instId, "bar": bar}},
        {
            "$group": {
                "_id": None,
                "start_date": {"$min": "$timestamp"},
                "end_date": {"$max": "$timestamp"},
            }
        },
    ]

    result = list(collection.aggregate(pipeline))

    if result:
        start_date = result[0]["start_date"]
        end_date = result[0]["end_date"]
        return start_date, end_date
    else:
        return None, None


def get_collection_pair_lists(collection):
    pipeline = []


if __name__ == "__main__":
    client = get_client(db_type="mongodb")
    db = client["crypto"]
    bar = "1m"
    collection = db[f"kline-{bar}"]

    instId = "BTC-USDT-SWAP"  # Example instrument ID

    start_date, end_date = get_collection_date_range(collection, instId, bar)
    if start_date and end_date:
        print(f"The earliest document for {instId} {bar} is from: {start_date}")
        print(f"The latest document for {instId} {bar} is from: {end_date}")
    else:
        print(f"No documents found in the collection for {instId} {bar}.")

    print(mongodb_general_info(client))
