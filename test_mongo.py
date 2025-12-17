import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

uri = os.environ["MONGODB_URI"]
client = MongoClient(uri)
db = client[os.environ["MONGODB_DB"]]
collection = db[os.environ["MONGODB_COLLECTION"]]

client.admin.command("ping")
print("✅ MongoDB Atlas connected")
