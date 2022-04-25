from audioop import add
from re import I
import signal
from pymongo import ReturnDocument
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import connection_database


config = {
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "local",
    'db_auth': "admin",
}

# Normal
db = connection_database(config=config, maxPoolSize=50)

# collection nay luu nhung group thuoc cung 1 nhom
col_map_group = db["map_group"]

# collection nay luu address nao thuoc group nao (tuy nhien luc nay 1 address co the thuoc nhieu group)
col_address = db["address"]

# collection nay luu dang parse toi group nao 
col_group_index = db["group_index"]

# luu txns
col_txns = db["txns"]

# luu lai block nao toi group nao 
col_block_group_index = db["block_group"]

received_signal = False

def signal_handler(signal, frame):
    global received_signal
    print("signal received", signal)
    received_signal = True

def handler_sigkill(signal, frame):
    print("KILL")

# signal.signal(signal.SIGTSTP, signal_handler)
# signal.signal(signal.SIGINT, handler_sigkill)


# create "txns" collection from block, log current blocks which is already processed into collection "parse_block_done"
def flat_txn(from_block, to_block):
    for i in range(from_block, to_block+1):
        block = db["raw_blocks"].find_one({"block_index": i})
        txns = block.get("tx")
        for txn in txns:
            db["txns"].replace_one({"hash": txn.get("hash")}, txn, upsert=True)
        print(block.get("block_index"))
        db["parse_block_done"].update_one({"block_index": block.get("block_index")}, { "$set" : { "done": True}}, upsert=True)

# current to here
flat_txn(451000, 451850)


