from pymongo import MongoClient
from bson import Timestamp
from datetime import datetime

MONGO_URL = "mongodb+srv://ilian:Password.@m0.takskf9.mongodb.net"
database_name = "mydb"
collection_name = "foo"
change_stream_token_collection = f"{collection_name}.changeStreamToken"

# Connect to MongoDB
client = MongoClient(MONGO_URL)
db = client[database_name]

def setup(capped_token_coll, db):
    collections = db.list_collection_names()
    if capped_token_coll not in collections:
        db.create_collection(capped_token_coll, capped=True, size=16000, max=1)
        print(f"Collection '{capped_token_coll}' created.")
    else:
        print(f"Collection '{capped_token_coll}' already exists.")

setup(change_stream_token_collection, db)

# Set desired date and convert it to a Timestamp
desired_date = datetime(2023, 11, 23, 15, 30, 0)
desired_timestamp = Timestamp(int(desired_date.timestamp()), 0)
resume_by_timestamp = False

# Use a small and fast capped collection to store last token processed
resume_by_last_processed = True
resume_after_data = db[change_stream_token_collection].find_one()

# Pipeline for the change stream
pipeline = [
    {
        '$match': {
            'operationType': {'$in': ['insert', 'update', 'delete']}
        }
    }
]

# Set options
options = {}

if resume_by_last_processed and resume_after_data and '_id' in resume_after_data:
    # Pass the resume token directly as a string
    options['resume_after'] = resume_after_data['_id']

if resume_by_timestamp:
    options['start_at_operation_time'] = desired_timestamp

# Create and start the change stream
change_stream = db[collection_name].watch(pipeline, **options)

print("Change Stream started, awaiting for events...")
for change in change_stream:
    print(change)
    # Store the resume token
    if '_id' in change:
        resume_token = change['_id']
        db[change_stream_token_collection].replace_one({'_id': resume_token}, {'_id': resume_token}, upsert=True)
