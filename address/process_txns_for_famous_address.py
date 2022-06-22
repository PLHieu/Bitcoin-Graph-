import os
import sys
import json
from pymongo import MongoClient
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))

from utils.db import db
from mgoqueue import queue
import requests
from datetime import datetime
import time
import http.client
import urllib.parse
import pytz
import redis
import copy

r = redis.Redis(host='localhost', port=6379, db=0)

col_famous_address = db['famous_address']
col_famous_inoutflow_v2 = db['famous_inoutflow_v2']
col_famous_address_txns_v2 = db['famous_address_txns_v2']

def make_requests(method, url, retry_times, sleeptime, retry_lists):
    for i in range(0, retry_times):        
        if method == "get":
            response = requests.get(url)
        elif method == "post":
            response = requests.get(url)
        if response.status_code in retry_lists:
            if i == retry_times - 1:
                return None, False
            time.sleep(sleeptime)
        else:
            return response, True

def get_list_txns_blockchair(add):
    offset = 0
    limit = 10000
    while True:
        url = f"https://api.blockchair.com/bitcoin/dashboards/address/{add}?limit={limit}&offset={offset}"
        response, succces = make_requests("get", url, 3, 1, [429,500,400])
        if not succces:
            return False
        result =  json.loads(response.text)
        txns = result.get("data").get(add).get("transactions",[])
        if len(txns) == 0:
            break
        for txn in txns:
            txn = txn.lower()
            queue.push_mgo_queue("queue_address_txns", {"address": add, "txn": txn}, f"{add}-{txn}", [add, txn])
        offset = offset + limit
        time.sleep(1.5)
    return True

def get_list_txns_chain_so(add):
    url = f"https://chain.so/api/v2/address/btc/{add}"
    response, succces = make_requests("get", url, 3, 1, [429,500,400])
    if not succces:
        return False
    result =  json.loads(response.text)
    txns = result.get("data").get("txs",[])
    for item in txns:
        txn = item.get("txid").lower()
        queue.push_mgo_queue("queue_address_txns", {"address": add, "txn": txn}, f"{add}-{txn}", [add, txn])
    return True

def is_famous_address(address):
    return r.sismember("famous_address", address)

def process_txn_v2(txn, list_txns, list_ios):
    # get time of this transaction 
    time = datetime.fromtimestamp(txn.get("time"), pytz.UTC)

    # is_txn_inserted = False
    result_txn = copy.deepcopy(txn)
    existed_famous_address = set()

    # process inouflow
    for input in txn.get("inputs",[]):
        address = input.get("prev_out").get("addr", None)

        if not address:
            continue

        if not is_famous_address(address):
            continue

        # # update this txn document in db to inclue this address
        # if is_txn_inserted == False:
        #     col_famous_address_txns_v2.insert_one(txn)
        #     is_txn_inserted = True

        ## col_famous_address_txns_v2.update_one({"hash": txn.get("hash","")}, {"$push": {"famous_address": address}})

        existed_famous_address.add(address)

        # # this is outflow
        # row = {
        #     "amount": input.get("prev_out").get("value", 0),
        #     "type": "OUTFLOW",
        #     "address": address,
        #     "txn_hash": txn.get("hash"),
        #     "block_index": txn.get("block_index"),
        #     "time": time,
        #     "timestamp": txn.get("time"),
        # }
        # filter = {
        #     "type": "OUTFLOW",
        #     "address": address,
        #     "txn_hash": txn.get("hash"),
        # }
        # col_famous_inoutflow_v2.replace_one(filter,row, upsert=True)
        
        item = {
            "amount": input.get("prev_out").get("value", 0),
            "type": "OUTFLOW",
            "address": address,
            "txn_hash": txn.get("hash"),
            "block_index": txn.get("block_index"),
            "time": time,
            "timestamp": txn.get("time"),
        }
        list_ios.append(item)
    
    for output in txn.get("out"):
        address = output.get("addr",None)

        if not address:
            continue
            
        if not is_famous_address(address):
            continue

        # # update this txn document in db to inclue this address
        # if is_txn_inserted == False:
        #     col_famous_address_txns_v2.insert_one(txn)
        #     is_txn_inserted = True

        # start = datetime.now(pytz.UTC)
        # col_famous_address_txns_v2.update_one({"hash": txn.get("hash","")}, {"$push": {"famous_address": address}})
        # end = datetime.now(pytz.UTC)
        # print("col_famous_address_txns_v2.update_one", (end-start).total_seconds())

        existed_famous_address.add(address)

        # # this is inflow
        # row = {
        #     "amount": output.get("value", ""),
        #     "type": "INFLOW",
        #     "address": address,
        #     "txn_hash": txn.get("hash"),
        #     "block_index": txn.get("block_index"),
        #     "time": time,
        #     "timestamp": txn.get("time"),
        # }
        # filter = {
        #     "type": "INFLOW",
        #     "address": address,
        #     "txn_hash": txn.get("hash"),
        # }
        # start = datetime.now(pytz.UTC)
        # col_famous_inoutflow_v2.replace_one(filter,row, upsert=True)
        # end = datetime.now(pytz.UTC)
        # print("col_famous_inoutflow_v2.replace_one", (end-start).total_seconds())
        item = {
            "amount": output.get("value", ""),
            "type": "INFLOW",
            "address": address,
            "txn_hash": txn.get("hash"),
            "block_index": txn.get("block_index"),
            "time": time,
            "timestamp": txn.get("time"),
        }
        list_ios.append(item)

    if len(existed_famous_address) > 0:
        result_txn["famous_addresses"] = list(existed_famous_address)
        list_txns.append(result_txn)

def process_block(data):
    block = data['block']
    response = requests.get(f"https://blockchain.info/rawblock/{block}")
    result =  json.loads(response.text)
    list_txns = []
    list_ios = []

    for txn in result.get("tx", []):
        process_txn_v2(txn, list_txns, list_ios)

    
    if len(list_txns) > 0:
        col_famous_address_txns_v2.insert_many(list_txns)

    if len(list_ios) > 0:
        col_famous_inoutflow_v2.insert_many(list_ios)

def get_txn_inoutflow_address(s, e):
    for i in range(s, e+1):
        process_block({"block": i})

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', help="start block")
    parser.add_argument('-e', '--end', help="end block")
    args = parser.parse_args()


    if args.start and args.end:
        get_txn_inoutflow_address(args.start, args.end)
    else:
        raise Exception("Arguments is invalid")


# queue.consume_mgo_queue("queue_raw_block", process_block)