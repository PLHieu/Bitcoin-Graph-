from datetime import datetime
import pytz
import signal
import pymongo
import sys
import os
import time

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import connection_database
from mgoqueue import queue

db = connection_database({
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "khoaluan",
    'db_auth': "admin",
})
col_famous_address = db['famous_address']
col_famous_inoutflow_v2 = db['famous_inoutflow_v2']
col_famous_address_txns_v2 = db['famous_address_txns_v2']
col_agg_inoutflow_v2 = db['agg_inoutflow_v2']

# received_signal = False

# def signal_handler(signal, frame):
#     global received_signal
#     print("signal received", signal)
#     received_signal = True

# def handler_sigkill(signal, frame):
#     print("KILL")

# signal.signal(signal.SIGTSTP, signal_handler)
# signal.signal(signal.SIGINT, handler_sigkill)


def init_queue():
    start_block=134900
    end_block=346522

    for i in range(start_block, end_block+1):
        data = {"block": i}
        queue.push_mgo_queue("queue_aggregate_inoutflow", data, f"{i}", [ i])
        print(f"{i}")            



def generate_agg_inoutflow(block):
    offset = 0
    limit = 100

    while True:
        filter = {"block_index": block}
        items = list(col_famous_inoutflow_v2.find(filter).limit(limit).skip(offset))
        if len(items) == 0:
            break
        for item in items:
            # get group of this address
            res = col_famous_address.find_one({"address": item.get("address")})
            if res is None:
                continue

            group = res.get("group")
            country = res.get("country","")
            exchange = res.get("exchange","")

            time = item.get("time")
            date_io = datetime(year=time.year, month=time.month, day=time.day)
            col_agg_inoutflow_v2.update_one({"group": group, "type": item.get("type"), "date": date_io},{"$inc": {"value": item.get("amount")}}, upsert=True)
        offset = offset + limit


def process_agg_inoutflow(data):
    block = data.get("block")
    generate_agg_inoutflow(block)
    time.sleep(0.01)
    print(f"Done {block}")

queue.consume_mgo_queue("queue_aggregate_inoutflow", process_agg_inoutflow)
# init_queue()
# generate_agg_inoutflow(119891)
