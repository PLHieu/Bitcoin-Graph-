import os
import sys
import json

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

col_famous_address = db['famous_address']
col_famous_address_txns_v2 = db['famous_address_txns_v2']
col_edge = db['edge']

# col_extracted_group = db['extracted_graph']
# col_map_graph = db['map_graph']

# last time to 2360000
def extract_edge():
    offset = 12440000
    limit = 5000
    group = 0

    # init start id
    start_item = list(col_famous_address_txns_v2.find({}).limit(1).skip(offset))[0]
    last_id = start_item.get("_id")

    while True:
        list_txns = list(col_famous_address_txns_v2.find({"_id": {"$gt": last_id}}).limit(limit))

        if len(list_txns) == 0:
            break
        for txn in list_txns:
            process_txn(txn)
        print(offset)
        print(last_id)
        last_id = list_txns[len(list_txns)-1].get("_id")
        offset = offset + limit
    

def process_txn(txn):
    list_famous_adds = txn.get("famous_address",[])
    add_ins = []
    for input in txn.get("inputs",[]):
        address = input.get("prev_out").get("addr", "")
        if address in list_famous_adds:
            res = col_famous_address.find_one({"address": address})
            exchange_in = res.get("exchange")
            add_ins.append({"address": address, "exchange": exchange_in})


    add_outs = []
    for output in txn.get("out",[]):
        address = output.get("addr","")
        if address in list_famous_adds:
            res = col_famous_address.find_one({"address": address})
            exchange_out = res.get("exchange")
            add_outs.append({"address": address, "exchange": exchange_out})

    if len(add_ins)>0 and len(add_outs) > 0:
        items = []
        for adin in add_ins:
            address_in = adin.get("address")
            exchange_in = adin.get("exchange")
            for adout in add_outs:
                address_out = adout.get("address")
                exchange_out = adout.get("exchange")
                items.append({"address_in": address_in, "address_out": address_out, "exchange_in": exchange_in, "exchange_out":exchange_out })
        col_edge.insert_many(items)

extract_edge()