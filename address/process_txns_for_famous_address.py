import os
import sys
import json
from pymongo import MongoClient
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))

from utils.db import connection_database
from mgoqueue import queue
import requests
from datetime import datetime
import time
import http.client
import urllib.parse
import pytz

conn = http.client.HTTPSConnection("api.scrapingant.com")

headers = {
    'x-api-key': "44f80c6530024aeb969ad4d29816f1a6"
}

db = connection_database({
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "khoaluan",
    'db_auth': "admin",
})
col_famous_address = db['famous_address']
col_famous_address_txns = db['famous_address_txns']
col_famous_inoutflow = db['famous_inoutflow']


# push address into famous_address queue to process
def init_queue():
    offset = 0
    limit = 2000
    while True:
        list_address = list(col_famous_address.find({}).limit(limit).skip(offset))
        if len(list_address) == 0:
            break
        for address in list_address:
            add = address.get("address")
            resp = queue.push_mgo_queue("queue_famous_address", {"address": add}, f"{add}", [add])
            if resp.get("message") == "Item existed in queue":
                print(add, "exists")
        offset = offset + limit

def process_famous_address(data):
    time.sleep(0.5)
    address = data.get("address","")

    offset = 0
    limit = 50

    while True:
        url = f"https://blockchain.info/rawaddr/{address}?offset={offset}&limit={limit}"
        url_parse_quote = urllib.parse.quote(url, safe="")
        conn.request("GET", f"/v1/general?url={url_parse_quote}&browser=false", headers=headers)
        string_response = conn.getresponse().read().decode('utf-8')
        result_string = json.loads(string_response).get("content","")
        result = json.loads(result_string)

        txns = result.get("txs", [])
        if len(txns) == 0:
            break
        
        for tx in txns:
            process_txn(tx, address)

        offset += limit
        time.sleep(0.2)


def process_txn(txn, famous_address):
    txn["famous_address"] = famous_address
    txn["hash"] = txn.get("hash","")
    col_famous_address_txns.replace_one({"hash": txn.get("hash")}, txn, upsert=True)

    # get time of this transaction 
    time = datetime.fromtimestamp(txn.get("time"), pytz.UTC)

    # process inouflow
    for input in txn.get("inputs",[]):
        address = input.get("prev_out").get("addr", "")
        if address == famous_address:
            # this is outflow
            row = {
                "amount": input.get("prev_out").get("value", 0),
                "type": "OUTFLOW",
                "address": address,
                "txn_hash": txn.get("hash"),
                "block_index": txn.get("block_index"),
                "time": time,
                "timestamp": txn.get("time"),
            }
            filter = {
                "type": "OUTFLOW",
                "address": address,
                "txn_hash": txn.get("hash"),
            }
            col_famous_inoutflow.replace_one(filter,row, upsert=True)
    
    for output in txn.get("out"):
        address = output.get("addr","")
        if address == famous_address:
            # this is inflow
            row = {
                "amount": output.get("value", ""),
                "type": "INFLOW",
                "address": address,
                "txn_hash": txn.get("hash"),
                "block_index": txn.get("block_index"),
                "time": time,
                "timestamp": txn.get("time"),
            }
            filter = {
                "type": "INFLOW",
                "address": address,
                "txn_hash": txn.get("hash"),
            }
            col_famous_inoutflow.replace_one(filter,row, upsert=True)


#-------------------------------------------------------------------


def get_list_txns_blockstream_io(add):
    response = requests.get(f"http://blockstream.info/api/address/{address}/txs", )


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

def test():
    process_famous_address({"address": "1BNRxRGV7CfRwGHRkz5NFEbYeaZvT58Cxj"})

# test()
queue.consume_mgo_queue("queue_famous_address", process_famous_address)
