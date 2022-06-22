import os
import sys
import json
import signal
from pymongo import ReturnDocument
from datetime import datetime
import copy
import threading
from bson.objectid import ObjectId

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import db
from mgoqueue import queue
# received_signal = False

# def signal_handler(signal, frame):
#     global received_signal
#     print("signal received", signal)
#     received_signal = True

# def handler_sigkill(signal, frame):
#     print("KILL")

# signal.signal(signal.SIGTERM, signal_handler)
# signal.signal(signal.SIGINT, signal_handler)


col_famous_address = db['famous_address']
col_map_add_group = db['map_add_group']
col_famous_inoutflow_v2 = db['famous_inoutflow_v2']
# col_agg_famous_inoutflow_v2 = db['agg_famous_inoutflow_v2']
col_agg_famous_inoutflow_v3 = db['agg_famous_inoutflow_v3']


def process_item(item):
    address = item.get("address")
    node = item.get("label")
    list_agg_io = []

    results = {
        "INFLOW" : dict(),
        "OUTFLOW": dict()
    }

    offset = 0
    limit = 5000

    while True:
        list_io = list(col_famous_inoutflow_v2.find({"address": address}).skip(offset).limit(limit))
        
        if len(list_io) == 0:
            break
        
        for io in list_io:  
            time = io.get("time")
            # date = time.strftime("%m/%d/%Y")
            date = datetime(time.year,time.month,time.day )
            date_string = date.strftime("%m/%d/%Y")
            
            if results[io.get("type")].get(date_string) is None:
                results[io.get("type")][date_string] = {"value": 0, "date":date }

            results[io.get("type")][date_string]["value"] += io.get("amount")
            results[io.get("type")][date_string]["date"] = date
            # col_agg_famous_inoutflow_v2.update_one({"node": node, "type": io.get("type"), "date": date, "address": address},{"$inc": {"value": io.get("amount")}}, upsert=True)

        offset += limit

    for type_io, item in results.items():
        for d_string, res in item.items():
            list_agg_io.append({"node": node, "type": io.get("type"), "date": res.get("date"), "address": address, "value": res.get("value")})

    if len(list_agg_io) == 0 :
        raise "ew this wid is empty in ioflow"

    col_agg_famous_inoutflow_v3.insert_many(list_agg_io)

# offset = 0
# limit = 10000

# while True:

#     if received_signal:
#         break

#     res = list(col_map_add_group.find().skip(offset).limit(limit))
    
#     if len(res) == 0:
#         break

#     for item in res:
#         if not item.get("checked"):
#             queue.push_mgo_queue("queue_agg_io", item, f"{item.get('address')}-{item.get('group')}", [""])
#             col_map_add_group.update_one({"_id": item.get("_id")}, {"$set": {"checked": True}})
    
#     offset += limit
#     print(offset)

# queue.consume_mgo_queue("queue_agg_io", process_item)


def process_item_block(data):
    block = data.get("block")
    list_agg_io = []
    list_io = []

    results = dict()

    offset = 0
    limit = 5000

    while True:
        list_io = list(col_famous_inoutflow_v2.find({"block_index": block}).skip(offset).limit(limit))
        
        if len(list_io) == 0:
            break
        
        for io in list_io:  
            time = io.get("time")
            type_io = io.get("type")
            amount = io.get("amount")
            address = io.get("address")
            # date = time.strftime("%m/%d/%Y")
            date = datetime(time.year,time.month,time.day )
            date_string = date.strftime("%m/%d/%Y")
            
            if results.get(address) is None:
                results[address] = {
                    "INFLOW": dict(),
                    "OUTFLOW": dict()
                }
            
            if results[address][type_io].get(date_string) is None:
                results[address][type_io][date_string] = {"amount": 0, "date":date }

            results[address][type_io][date_string]["amount"] += amount
            results[address][type_io][date_string]["date"] = date
            # col_agg_famous_inoutflow_v2.update_one({"node": node, "type": io.get("type"), "date": date, "address": address},{"$inc": {"value": io.get("amount")}}, upsert=True)

        offset += limit

    for address, item_1 in results.items():
        for type_io, item_2 in item_1.items():
            for d_string, item_3 in item_2.items():
                list_agg_io.append({"type": type_io, "date": item_3.get("date"), "address": address, "amount": item_3.get("amount")})

    if len(list_agg_io) == 0 :
        return 
        # raise "ew list_agg_io is empty"

    col_agg_famous_inoutflow_v3.insert_many(list_agg_io)



# for i in range(122569, 550000):
#     data = {'block': i}
#     queue.push_mgo_queue("queue_agg_io_v2_with_blocks", data, f'{i}', [i])
#     print(i)    

def load_pre():
    res = db["queue_agg_io_consumed"].find()
    saved = []
    for item in res:
        data = item.get("data")
        label = data.get("label")
        saved.append({"label": label})
    db["saved"].insert_many(saved)
    

# process_item_block(450000)

# queue.consume_mgo_queue("queue_agg_io_v2_with_blocks", process_item_block)


def clean_col_agg_famous_inoutflow_v2():
    i = 1
    cursor = col_agg_famous_inoutflow_v3.find()
    while True:
        io = cursor.next()
        if io is None:
            break
        print(i)
        current_id = io.get("_id")
        current_amount = io.get("amount")
        dt = io.get("date")
        type_io = io.get("type")
        address = io.get("address")

        filter = {
            "address": address, "date": dt, "type": type_io, "_id": {"$gt": current_id}
        }
        res = list(col_agg_famous_inoutflow_v3.find(filter))
        for item in res:
            current_amount += item.get("amount")

        if len(res) > 0:
            # update current amount
            col_agg_famous_inoutflow_v3.update_one({"_id": current_id}, {"$set": {"amount": current_amount}})
            # delete redundant records
            col_agg_famous_inoutflow_v3.delete_many(filter)
        
        i=i+1

def clean_col_agg_famous_inoutflow_v2_with_set_python():
    offset = 0
    limit = 2000000

    while True:
        setItems = set()
        setItemsNeedClean = set()

        res = list(col_agg_famous_inoutflow_v3.find().limit(limit).skip(offset))
        if len(res) == 0:
            break

        for i in range(0, len(res)):
            item = res[i]
            key = f'{item.get("address")}-{item.get("type")}-{item.get("date").strftime("%m/%d/%Y")}'
            if key not in setItems:
                setItems.add(key)
            else:
                setItemsNeedClean.add(key)
                # print(key)
        
        list_item_need_clean = []
        for key in setItemsNeedClean:
            l = key.split('-')
            address = l[0]
            type_io = l[1]
            d = datetime.strptime(l[2], "%m/%d/%Y")
            obj = {"address": address, "type": type_io, "date": d}
            list_item_need_clean.append(obj)

        if len(list_item_need_clean) > 0:
            db["need_clean"].insert_many(list_item_need_clean)

        print(offset)
        offset += limit
    
    # list_item_need_clean = []
    # for key in setItemsNeedClean:
    #     l = key.split('-')
    #     address = l[0]
    #     type_io = l[1]
    #     d = datetime.strptime(l[2], "%m/%d/%Y")
    #     obj = {"address": address, "type": type_io, "date": d}
    #     list_item_need_clean.append(obj)

    # db["need_clean"].insert_many(list_item_need_clean)

def clean_XXX():
    offset = 0
    limit = 300000

    while True:
        if received_signal:
            print("Shutdown")
            break
        starttime = datetime.now()
        res = list(db["need_clean"].find().skip(offset).limit(limit))
        if len(res) == 0:
            break

        list_inserted = []
        list_filter_deleted_agg_io = []
        list_id_deleted_need_clean = []
        set_checked = set()
        dict_data = dict()
        
        list_threads = []
        # preprare the data
        for item in res:
            list_id_deleted_need_clean.append(item.get("_id"))
            
            dt = item.get("date")
            type_io = item.get("type")
            address = item.get("address")

            key = f'{address}-{type_io}-{dt.strftime("%m/%d/%Y")}'
            
            if key in set_checked:
                continue

            set_checked.add(key)
            
            filter_io = {
                "address": address, "date": dt, "type": type_io
            }
            dict_data[key] = {
                "filter":filter_io
            }
            
            t = threading.Thread(target=prepare_data, args=[filter_io, dict_data[key]])
            list_threads.append(t)


        batch_size = 10000
        chunk_list_threads=[list_threads[i:i + batch_size] for i in range(0, len(list_threads), batch_size)]    
        for l in chunk_list_threads:
            for t in l:
                t.start()

            for t in l:
                t.join()

            print("Done", l)

        
        # start processing
        for _, item in dict_data.items():
            filter_io = item.get("filter")

            all_dups = item.get("data",[])
            if len(all_dups) <= 1:
                continue
            else:
                current_amount = 0
                for io in all_dups:
                    current_amount += io.get("amount")
                
                list_filter_deleted_agg_io.append(filter_io)
                
                # reinsert
                new_item = copy.deepcopy(filter_io)
                new_item["amount"] = current_amount
                list_inserted.append(new_item)

        print("start delete all filter")
        # firstly we need to delete all filter
        list_threads = []
        for f in list_filter_deleted_agg_io:
            t =threading.Thread(target=col_agg_famous_inoutflow_v3.delete_many, args=[f])
            list_threads.append(t)
        batch_size = 10000
        chunk_list_threads=[list_threads[i:i + batch_size] for i in range(0, len(list_threads), batch_size)]    
        for l in chunk_list_threads:
            for t in l:
                t.start()

            for t in l:
                t.join()

            print("Done", l)
        
        print("start insert again into agg io ")
        if len(list_inserted)>0:
            # insert again into agg io 
            col_agg_famous_inoutflow_v3.insert_many(list_inserted)

        print("start delete in need clean ")
        if len(list_id_deleted_need_clean)>0:
            # delete in need clean
            db["need_clean"].delete_many({"_id": {"$in": list_id_deleted_need_clean}})

        print(offset)
        offset += limit
        endtime = datetime.now()
        print("total: ", (endtime-starttime).total_seconds())


def prepare_data(filter_io, dict_data):
    all_dups = list(col_agg_famous_inoutflow_v3.find(filter_io))
    dict_data["data"] = all_dups

# def process_clean(item):
#     dt = item.get("date")
#     type_io = item.get("type")
#     address = item.get("address")
#     filter_io = {
#         "address": address, "date": dt, "type": type_io
#     }
#     all_dups = list(col_agg_famous_inoutflow_v3.find(filter_io))
#     if len(all_dups) <= 1:
#         db["need_clean"].delete_one({"_id": item.get("_id")})
#         return 
#     else:
#         current_amount = 0
#         for io in all_dups:
#             current_amount += io.get("amount")
#         #delete all 
#         col_agg_famous_inoutflow_v3.delete_many(filter_io)
#         # reinsert
#         filter_io["amount"] = current_amount
#         col_agg_famous_inoutflow_v3.insert_one(filter_io)
#         db["need_clean"].delete_one({"_id": item.get("_id")})

# clean_XXX()
# queue.consume_mgo_queue("queue_clean_agg_io", process_clean)
# clean_col_agg_famous_inoutflow_v2_with_set_python()



def load_processed_edge():
    with open('processed_set_edges.csv','r') as file:
        set_address = set()
        # saved = load_saved()
        for row in file:
            row = row.strip('\n') 
            pair_grs = row.split(',')
            int_pair_grs = [ int(i) for i in pair_grs]

            set_address.add(int_pair_grs[0])
            set_address.add(int_pair_grs[1])

        print("Done loading node")
        
        set_address = list(set_address)
        batch_size = 100000
        chunk_list_threads=[set_address[i:i + batch_size] for i in range(0, len(set_address), batch_size)]
        for l in chunk_list_threads:
            f(l)


def f(set_address):
    map_add_node = dict()
    list_threads = []
    for node in set_address:
        t = threading.Thread(target=get_node_of_address, args=[node, map_add_node])
        list_threads.append(t)

    # start getting node of addresses
    batch_size = 10000
    chunk_list_threads=[list_threads[i:i + batch_size] for i in range(0, len(list_threads), batch_size)]    
    for i in range(len(chunk_list_threads)):
        l = chunk_list_threads[i]
        for t in l:
            t.start()

        for t in l:
            t.join()
        print("Done getting node of addresses batch", i)


    # start updating node of address
    list_threads=[]
    for k, v in map_add_node.items():
        t = threading.Thread(target=col_agg_famous_inoutflow_v3.update_many, args=[{"address": k}, {"$set": {"node": v}}])
        list_threads.append(t)

    batch_size = 10000
    chunk_list_threads=[list_threads[i:i + batch_size] for i in range(0, len(list_threads), batch_size)]    
    for i in range(len(chunk_list_threads)):
        l = chunk_list_threads[i]
        for t in l:
            t.start()

        for t in l:
            t.join()
        print("Done updating node of address batch", i)

def get_node_of_address(node, m):
    res = col_famous_address.find_one({"label": node})
    if res:     
        m[res.get("address","")] = node

# load_processed_edge()

# def process_get_label(data):
#     address = data.get("address")
#     node = data.get("label")

#     col_agg_famous_inoutflow_v3.update_many({"address": address}, {"$set": {"node": node}})


# queue.consume_mgo_queue("queue_gget_node_label", process_get_label)


def load_all_label():
    offset = 0
    limit = 1000000
    result = dict()

    while True:
        res = list(col_famous_address.find().skip(offset).limit(limit))

        if len(res) == 0:
            break

        for item in res:
            if not result.get(item.get("address")):
                result[item.get("address")] = item.get("label")
        
        print("Done loading label offset", offset)
        offset += limit
    
    return result


def write_csv():
    # firstly, load all label of address
    map_add_label = load_all_label()


    # load processed_address fro db and write into csv file
    with open('agg_inoutflow.csv','a') as file:
        offset = 0
        limit = 1000000

        while True:
            res = list(col_agg_famous_inoutflow_v3.find({"_id": {"$gte": ObjectId("629a2e4b81c42094c0eaafdc")}}).skip(offset).limit(limit))

            if len(res) == 0:
                break
            
            for item in res:
                date_string = item.get("date").strftime("%m/%d/%Y")
                txt = '%s,%d,%s,%s,%d\n' % (item.get("address", ""), map_add_label.get(item.get("address", "")), item.get("type", ""), date_string, item.get("amount", ""))
                file.write(txt)

            print(offset)
            # print(last_id)
            offset = offset + limit

# write_csv()

def agg_all_inoutflow():
    with open('processed_set_edges.csv','r') as file:
        set_address = set()
        # saved = load_saved()
        for row in file:
            row = row.strip('\n') 
            pair_grs = row.split(',')
            int_pair_grs = [ int(i) for i in pair_grs]

            set_address.add(int_pair_grs[0])
            set_address.add(int_pair_grs[1])


        for label in set_address:
            res = col_famous_address.find_one({"label": label})
            add = res.get("address")
            process_item({"address": add, "label": label})

agg_all_inoutflow()