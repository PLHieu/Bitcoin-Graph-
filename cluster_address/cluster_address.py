from audioop import add
from re import I
import signal
from pymongo import ReturnDocument
import time
import sys
import os
import threading

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import connection_database
from mgoqueue import queue

config = {
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "khoaluan",
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

# def handler_sigkill(signal, frame):
#     print("KILL")

# signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# push block into cluster_address queue to process
def init_queue(start_block, end_block):
    for i in range (start_block, end_block):
        if received_signal:
            return
        queue.push_mgo_queue("cluster_address", {"block": i}, f"{i}", [i])
        print(i)


# process each transaction inside 1 block
# for each transaction -> loop over inputs and outpust -> save into inoutflow 
def cluster_address(start_block, end_block):
    global received_signal

    for i in range (start_block, end_block):
        if received_signal:
            return 

        col_block_group_index.update_one({"block": i,},{"$set": {"start_group": col_group_index.find_one({"status": "current"}).get("index")}}, upsert=True)
        offset = 0
        limit = 100
        while True:
            filter = {"block_index": i}
            txns = list(db["txns"].find(filter).limit(limit).skip(offset))

            if len(txns) == 0:
                break

            for txn in txns:
                process_one_txn(txn)
            print(f"Txns in block: {i}, offset: {offset}")
            offset += limit
        col_block_group_index.update_one({"block": i,},{"$set": {"end_group": col_group_index.find_one({"status": "current"}).get("index")}}, upsert=True)

        print(f"---------------------DONE BLOCK {i}---------------------------")

def process_cluster_address(data):
    i = data.get("block")

    offset = 0
    limit = 100
    while True:
        filter = {"block_index": i}
        txns = list(db["txns"].find(filter).limit(limit).skip(offset))

        if len(txns) == 0:
            break

        for txn in txns:
            process_one_txn(txn)
        print(f"Txns in block: {i}, offset: {offset}")
        offset += limit

    print(f"---------------------DONE BLOCK {i}---------------------------")

def process_one_txn(txn):
    if not check_coin_base_txn(txn):
        inputs = txn.get("inputs")
        outputs = txn.get("out")
        process_inputs(txn, inputs)
        process_outputs(txn, outputs)


# each address in these inputs will be in the same group
def process_inputs(txn, inputs):
    new_group_id = get_new_group_id()

    # insert this group input address first
    for i in inputs:
        address = i.get("prev_out").get("addr", "")
        
        # find for this address that already exist
        res = col_address.find({"address": address, "group": {"$ne": new_group_id}})
        for r in res:
            map(r.get("group"), new_group_id)

        # add group for this address
        col_address.update_one({"address": address, "group": new_group_id}, {"$set": {}}, upsert=True)


def process_outputs(txn, outputs):
    def func(i):
        address = i.get("addr", "")

        if address == "":
                return

        #get new group id
        new_group_id = get_new_group_id()

         # find for this address that already exist
        res = col_address.find({"address": address, "group": {"$ne": new_group_id}})
        for r in res:
            map(r.get("group"), new_group_id)

        # add group for this address
        col_address.update_one({"address": address, "group": new_group_id}, {"$set": {}}, upsert=True)
    # check outputs

    # list_threads = []

    for i in outputs:
        # t = threading.Thread(target=func, kwargs={"i": i})
        # list_threads.append(t)
    
    # for t in list_threads:
    #     t.start()
    # for t in list_threads:
    #     t.join()
        address = i.get("addr", "")

        if address == "":
                continue

        #get new group id
        new_group_id = get_new_group_id()

         # find for this address that already exist
        res = col_address.find({"address": address, "group": {"$ne": new_group_id}})
        for r in res:
            map(r.get("group"), new_group_id)

        # add group for this address
        col_address.update_one({"address": address, "group": new_group_id}, {"$set": {}}, upsert=True)

def get_new_group_id():
    current = db["group_index"].find_one_and_update({"status": "current"}, {"$inc": {"index": 1}}, return_document=ReturnDocument.AFTER, upsert=True)
    return current.get("index")

def check_coin_base_txn(txn):
    input_address = txn.get("inputs")[0].get("prev_out").get("addr", "")
    if input_address == "":
        print("--Coinbase---")
        # Group all output address into one group 
        new_group_id_outputs = get_new_group_id()
        outputs = txn.get("out")
        for i in outputs:        
            address = i.get("addr", "")
            if address == "":
                continue
             # find for this address that already exist
            res = col_address.find({"address": address, "group": {"$ne": new_group_id_outputs}})
            for r in res:
                map(r.get("group"), new_group_id_outputs)
            col_address.update_one({"address": address, "group": new_group_id_outputs}, {"$set": {}}, upsert=True)
        return True
    return False


def map(group_from, group_to):
    l = [group_from, group_to]
    l.sort()

    col_map_group.update_one({"group_from": l[0], "group_to": l[1]}, {"$set": {}}, upsert=True)

queue.consume_mgo_queue("cluster_address", process_cluster_address)

