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
col_map_add_group = db['map_add_group']

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

# --------------------------------------
# label -> belong to which address 
cache_map_label_address = {}
# add -> belong to which label 
cache_map_add_label = {}
# add label -> belong to which group 
cache_map_add_group = {}

set_edges = set()
list_edges = []

def extract_edge_v2(start_block, end_block):
    for i in range(start_block, end_block+1):
        offset = 0
        limit = 5000

        while True:
            list_txns = list(col_famous_address_txns_v2.find({"block_index": i}).skip(offset).limit(limit))

            if len(list_txns) == 0:
                break
            for txn in list_txns:
                process_txn_v2(txn)
            offset = offset + limit
        
        print(i)

def process_txn_v2(txn):
    global list_edges
    list_famous_adds = txn.get("famous_address",[])
    group_ins = []
    for input in txn.get("inputs",[]):
        address = input.get("prev_out").get("addr", "")
        if address in list_famous_adds:
            # get label of this address
            label = get_label_by_address(address)
            # get group of this label
            gr = get_group_by_address_label(label)
            group_ins.append({"address_from": address, "label_from": label,  "group_from": gr})


    group_outs = []
    for output in txn.get("out",[]):
        address = output.get("addr","")
        if address in list_famous_adds:
            # get label of this address
            label = get_label_by_address(address)
            # get group of this label
            gr = get_group_by_address_label(label)
            group_outs.append({"address_to": address, "label_to": label,  "group_to": gr})

    if len(group_ins)>0 and len(group_outs) > 0:
        items = []
        for gin in group_ins:
            for gout in group_outs:
                list_edges.append(gin | gout)

def get_address_by_label(label):
    global cache_map_label_address
    a = cache_map_label_address.get(label)
    if a:
        return a
    
    # query database 
    res = col_famous_address.find_one({"label": label})
    cache_map_label_address[label] = res.get("address")
    return res.get("address")

def get_label_by_address(add):
    global cache_map_add_label
    a = cache_map_add_label.get(add)
    if a:
        return a
    
    # query database 
    res = col_famous_address.find_one({"address": add})
    cache_map_add_label[add] = res.get("label")
    return res.get("label")


def get_group_by_address_label(label):
    global cache_map_add_group
    a = cache_map_add_group.get(label)
    if a:
        return a
    
    # query database 
    gr = col_map_add_group.find_one({"address": label}).get("group")
    cache_map_add_group[label] = gr
    return gr

# extract_edge_v2(119891,331938)

# print("start writing list_edges.json")
# with open('list_edges.json','a') as file:
#     for list_edges in list_edges:
#         file.write(json.dumps(edge))
#         file.write("\n")
# print("done writing list_edges.json")

with open("list_edges.json") as infile:
    # block_index = 0
    for line in infile:
        edge = json.loads(line)
        list_edges.append(edge)

for edge in list_edges:
    l = [edge.get("group_from"), edge.get("group_to")]
    l.sort()
    set_edges.add(f"{l[0]}-{l[1]}")

print("start writing set_edges.csv")
with open('set_edges.csv','a') as file:
    for edge in set_edges:
        pair_grs = edge.split('-')
        int_pair_grs = [ int(i) for i in pair_grs]
        file.write(f"{int_pair_grs[0]},{int_pair_grs[1]}\n")
print("done writing set_edges.csv")