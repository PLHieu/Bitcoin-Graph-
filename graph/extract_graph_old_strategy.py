import os
import sys
import json
import signal
from pymongo import ReturnDocument

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import connection_database
from mgoqueue import queue
received_signal = False

def signal_handler(signal, frame):
    global received_signal
    print("signal received", signal)
    received_signal = True

# def handler_sigkill(signal, frame):
#     print("KILL")

# signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
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
col_map_add_group = db['map_add_group']
# col_map_group_group = db['map_group_group']
col_group_index = db['group_index']

current_group_id = 1

# add -> belong to which label 
cache_map_add_label = {}

# add -> belong to which group
map_add_group = {}

# group -> contains which addresses ?
map_group_addresses = {}

# map_group_group = set()


def push_process_old_strategy_queue():
    list_block = col_famous_address_txns_v2.distinct("block_index")

    for bl in list_block:
        queue.push_mgo_queue("process_old_strategy_queue", {"block": bl}, f"{bl}", [bl])
        print(bl)

def process_old_strategy(block):
    # block = data.get("block")

    offset = 0
    limit = 10000
    
    filter = {"block_index": block}
    last_id = None

    while True:
        txns = list(col_famous_address_txns_v2.find(filter).limit(limit))
        if len(txns) == 0:
            break
        
        for txn in txns:
            process_one_txn(txn)
        
        # print(offset)
        offset += limit
        
        last_id = txns[len(txns)-1].get("_id")
        filter["_id"] = {"$gt": last_id}

def process_one_txn(txn):
    # if not check_coin_base_txn(txn):
    inputs = txn.get("inputs")
    outputs = txn.get("out")

    list_famous_adds = txn.get("famous_address",[])

    process_inputs(list_famous_adds, inputs)
    process_outputs(list_famous_adds, outputs)


# each address in these inputs will be in the same group
def process_inputs(list_famous_adds, inputs):
    global current_group_id 
    global map_add_group 
    global cache_map_add_label 
    global map_group_addresses 
    
    is_already_get_new_gr_id = False


    # new_group_id = get_new_group_id()

    # insert this group input address first
    for i in inputs:
        address = i.get("prev_out").get("addr", "")

        if address not in list_famous_adds:
            continue

        labeled_address = get_label_address(address)

        if not is_already_get_new_gr_id:
            new_group_id = current_group_id

            current_group_id += 1
            is_already_get_new_gr_id = True

            # init new group
            map_group_addresses[new_group_id] = set()


        
        # find for this address that already exist
        # res = col_map_add_group.find({"address": address})
        # for r in res:
        #     map(r.get("group"), new_group_id)

        # add group for this address
        # col_map_add_group.update_one({"address": address, "group": new_group_id}, {"$set": {}}, upsert=True)

        # --------------------------------------------------------------V2
        # set_group = map_add_group.get(address, set())
        # for gr in set_group:
        #     # if gr == current_group_id:
        #     l = [gr, current_group_id]
        #     l.sort()
        #     map_group_group.add(f"{l[0]}-{l[1]}")
        # if not map_add_group.get(address, set()):
        #     map_add_group[address] = set()
        # map_add_group[address].add(new_group_id)
        
        # --------------------------------------------------------------V3
        # get current group of this address
        gr = map_add_group.get(labeled_address)
        if not gr: 
            map_add_group[labeled_address] = new_group_id
            map_group_addresses[new_group_id].add(labeled_address)
        else:
            if gr == new_group_id:
                continue
            # get all address belong to that group
            set_adds = map_group_addresses.get(gr)
            # add these addresses into new group
            map_group_addresses[new_group_id] = map_group_addresses[new_group_id].union(set_adds)
            # update the group of these addresses
            for add in set_adds:
                map_add_group[add] = new_group_id

            # delete the old group
            del map_group_addresses[gr]



def process_outputs(list_famous_adds, outputs):
    global current_group_id 
    global map_add_group 
    global cache_map_add_label 
    global map_group_addresses  

    for i in outputs:
        # t = threading.Thread(target=func, kwargs={"i": i})
        # list_threads.append(t)
    
    # for t in list_threads:
    #     t.start()
    # for t in list_threads:
    #     t.join()
        address = i.get("addr", "")

        if address not in list_famous_adds:
            continue

        if address == "":
            continue

        labeled_address = get_label_address(address)
        
        # -------------------------------------------------------------V2
        #get new group id
        # new_group_id = current_group_id
        # current_group_id += 1
        # new_group_id = get_new_group_id()

         # find for this address that already exist
        # res = col_map_add_group.find({"address": address, "group": {"$ne": new_group_id}})
        # for r in res:
        #     map(r.get("group"), new_group_id)

        # # add group for this address
        # col_map_add_group.update_one({"address": address, "group": new_group_id}, {"$set": {}}, upsert=True)
        # set_group = map_add_group.get(address, set())
        # for gr in set_group:
        #     # if gr == current_group_id:
        #     l = [gr, current_group_id]
        #     l.sort()
        #     map_group_group.add(f"{l[0]}-{l[1]}")
        # if not map_add_group.get(address, set()):
        #     map_add_group[address] = set()
        # map_add_group[address].add(new_group_id)
        
        # -------------------------------------------------------------V3
        # if this address already has the group -> skip it
        if map_add_group.get(labeled_address):
            continue
        else:
            new_group_id = current_group_id
            current_group_id += 1
            #set new group for this address
            map_add_group[labeled_address] = new_group_id
            map_group_addresses[new_group_id] = {labeled_address}



def get_new_group_id():
    current = col_group_index.find_one_and_update({"status": "current"}, {"$inc": {"index": 1}}, return_document=ReturnDocument.AFTER, upsert=True)
    return current.get("index")

# def check_coin_base_txn(txn):
#     input_address = txn.get("inputs")[0].get("prev_out").get("addr", "")
#     if input_address == "":
#         print("--Coinbase---")
#         # Group all output address into one group 
#         new_group_id_outputs = get_new_group_id()
#         outputs = txn.get("out")
#         for i in outputs:        
#             address = i.get("addr", "")
#             if address == "":
#                 continue
#              # find for this address that already exist
#             res = col_address.find({"address": address, "group": {"$ne": new_group_id_outputs}})
#             for r in res:
#                 map(r.get("group"), new_group_id_outputs)
#             col_address.update_one({"address": address, "group": new_group_id_outputs}, {"$set": {}}, upsert=True)
#         return True
#     return False


# def map(group_from, group_to):
#     l = [group_from, group_to]
#     l.sort()

#     col_map_group_group.update_one({"group_from": l[0], "group_to": l[1]}, {"$set": {}}, upsert=True)


def get_label_address(add):
    global cache_map_add_label
    a = cache_map_add_label.get(add)
    if a:
        return a
    
    # query database 
    res = col_famous_address.find_one({"address": add})
    cache_map_add_label[add] = res.get("label")
    return res.get("label")


# queue.consume_mgo_queue("process_old_strategy_queue", process_old_strategy)
# push_process_old_strategy_queue()

#2:47pm 5/15 -> 331938
for i in range(119891, 428383):
    if received_signal:
        break
    process_old_strategy(i)
    print(i)


print("start inserting map_group_addresses")
for item in map_group_addresses.items():
    gr = item[0]
    lis_adds = item[1]
    for add in lis_adds:
        col_map_add_group.insert_one({"address": add, "group": gr})
print("done inserting map_group_addresses")


# print("start inserting map group group")
# for item in map_group_group:
#     pair_grs = item.split('-')
#     int_pair_grs = [ int(i) for i in pair_grs]
#     col_map_group_group.insert_one({"pair_0": int_pair_grs[0], "pair_1": int_pair_grs[1]})
# print("done inserting map group group")

