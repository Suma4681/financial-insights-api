# app/db.py
import os
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure

load_dotenv()
def get_collection():
    return collection

client = MongoClient(os.environ["MONGODB_URI"])
db = client[os.environ["MONGODB_DB"]]
collection = db[os.environ["MONGODB_COLLECTION"]]

def ensure_indexes():
    try:
        collection.create_index(
            [("business_id", ASCENDING), ("provider_txn_id", ASCENDING)],
            unique=True,
            name="uniq_business_provider_txn"
        )
    except OperationFailure as e:
        # If it already exists under some other name, ignore
        if "IndexOptionsConflict" in str(e) or "already exists" in str(e):
            return
        raise
