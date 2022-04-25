import os
import sys
import json
from pymongo import MongoClient
import urllib
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))

from utils.db import connection_database

db = connection_database({
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "khoaluan",
    'db_auth': "admin",
})
col_raw_block = db['raw_blocks']
col_txns = db['txns']


with open("raw_blocks.json") as infile:
    # block_index = 0
    for line in infile:
        block = json.loads(line)
        txns = block.get("tx")
        del block["tx"]
        col_raw_block.replace_one(
            {'block_index': block.get("block_index")}, block, upsert=True)

        for txn in txns:
            db["txns"].replace_one({"hash": txn.get("hash")}, txn, upsert=True)
        print(block["height"])
