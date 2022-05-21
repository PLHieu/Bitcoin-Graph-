import os
import sys
import json
import signal
from pymongo import ReturnDocument
from datetime import datetime

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import connection_database
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
db = connection_database({
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "khoaluan",
    'db_auth': "admin",
})

col_famous_address = db['famous_address']
col_map_add_group = db['map_add_group']
col_famous_inoutflow_v2 = db['famous_inoutflow_v2']
col_agg_famous_inoutflow_v2 = db['agg_famous_inoutflow_v2']


def process_item(item):
    address_labeled = item.get("address")
    group = item.get("group")

    address = col_famous_address.find_one({"label": address_labeled}).get("address")

    offset = 0
    limit = 500

    while True:
        list_io = list(col_famous_inoutflow_v2.find({"address": address}).skip(offset).limit(limit))
        
        if len(list_io) == 0:
            break
        
        for io in list_io:  
            time = io.get("time")
            # date = time.strftime("%m/%d/%Y")
            date = datetime(time.year,time.month,time.day )
            col_agg_famous_inoutflow_v2.update_one({"group": group, "type": io.get("type"), "date": date},{"$inc": {"value": io.get("amount")}}, upsert=True)

        offset += limit

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

queue.consume_mgo_queue("queue_agg_io", process_item)

