from glob import iglob
import os.path
import pymongo
import json
from pymongo import MongoClient

connection = MongoClient()
db = connection['abc']
videos = db.videos

for fname in iglob(os.path.expanduser('/docs_and_data/test/*.json')):
    print (fname)
    with open(fname) as fin:
        videos = json.load(fin)
        print(videos)
        db.videos.insert(videos)
