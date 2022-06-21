import os
import sys
import json
import signal
import snap
from pymongo import ReturnDocument
import redis


sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import connection_database
from mgoqueue import queue

r = redis.Redis(host='localhost', port=6379, db=0)

# received_signal = False

# def signal_handler(signal, frame):
#     global received_signal
#     print("signal received", signal)
#     received_signal = True

# def handler_sigkill(signal, frame):
#     print("KILL")

# signal.signal(signal.SIGTSTP, signal_handler)
# signal.signal(signal.SIGINT, handler_sigkill)

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
col_edges_grouping_graph_v2 = db['edges_grouping_graph_v2']


def process_block(data):
    block_index = data.get("block")

    offset = 0
    limit = 10000
    
    filter = {"block_index": block_index}
    last_id = None
    list_edges = []

    while True:
        txns = list(col_famous_address_txns_v2.find(filter).limit(limit))
        if len(txns) == 0:
            break
        
        for txn in txns:
            #get list of famous adds
            list_famous_adds = txn.get("famous_addresses",[])

            if len(list_famous_adds) == 0:
                list_famous_adds = txn.get("famous_address",[])

            # process_inputs(txn, list_famous_adds, list_edges)
            # process_outputs(txn, list_famous_adds, list_edges)
            
            inputs = txn.get("inputs")
            list_famous_adds_inputs = []
            for i in inputs:
                address = i.get("prev_out").get("addr", "")
                if address not in list_famous_adds:
                    continue
                label = get_label_address(address)
                list_famous_adds_inputs.append(label)
            
            outputs = txn.get("out")
            list_famous_adds_outputs = []
            for i in outputs:
                address = i.get("addr", "")
                if address not in list_famous_adds:
                    continue
                label = get_label_address(address)
                list_famous_adds_outputs.append(label)

            for fr in list_famous_adds_inputs:
                for to in list_famous_adds_outputs:
                    list_edges.append({"from_node": fr, "to_node": to})

          
        # print(offset)
        offset += limit
        
        last_id = txns[len(txns)-1].get("_id")
        filter["_id"] = {"$gt": last_id}
    
    if len(list_edges) > 0:
        col_edges_grouping_graph_v2.insert_many(list_edges)


# process inputs
def process_inputs(txn, list_famous_adds,  list_edges):
    inputs = txn.get("inputs")
    list_famous_adds_inputs = []

    for i in inputs:
        address = i.get("prev_out").get("addr", "")

        if address not in list_famous_adds:
            continue
        
        label = get_label_address(address)

        list_famous_adds_inputs.append(label)
    
    for add in list_famous_adds_inputs:
        list_edges.append({"add_from":list_famous_adds_inputs[0], "add_to": add })

def process_outputs(txn, list_famous_adds, list_edges):
    outputs = txn.get("out")
    list_famous_adds_outputs = []

    for i in outputs:
        address = i.get("addr", "")

        if address not in list_famous_adds:
            continue

        label = get_label_address(address)
        list_famous_adds_outputs.append(label)
    
    for add in list_famous_adds_outputs:
        list_edges.append({"add_from":add, "add_to": add })


def push_extract_graph_new_strategy_queue():
    list_block = col_famous_address_txns_v2.distinct("block_index")

    for bl in list_block:
        queue.push_mgo_queue("extract_graph_new_strategy_queue", {"block": bl}, f"{bl}", [bl])
        print(bl)

def get_label_address(add):
    # query database 
    res = col_famous_address.find_one({"address": add})
    return res.get("label")




# queue.consume_mgo_queue("extract_graph_new_strategy_queue", process_block)
# push_extract_graph_new_strategy_queue()
# process_block(549999)
# test_group_with_snap()

def load_saved():
    res = db["saved"].find({})
    result = []
    for item in res:
        result.append(item.get("label"))
    
    return result

def load_processed_edge():
    with open('processed_set_edges.csv','r') as file:
        set_address = set()
        # saved = load_saved()
        for row in file:
            row = row.strip('\n') 
            pair_grs = row.split(',')
            int_pair_grs = [ int(i) for i in pair_grs]
            
            # print(int_pair_grs)
            # r.sadd('address_nodes', int_pair_grs[0])
            # r.sadd('address_nodes', int_pair_grs[1])

            set_address.add(int_pair_grs[0])
            set_address.add(int_pair_grs[1])
        

        for node in set_address:

            # if node in saved:
                # continue

            # get address of this label 
            res = col_famous_address.find_one({"label": node})
            data = {
                "address": res.get("address"),
                "label": node
            }
            queue.push_mgo_queue("queue_gget_node_label", data, node, [node])



