from datetime import datetime
import pytz
import signal
import pymongo
import sys
import os

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import db_local

col_inoutflow = db_local["inoutflow"]
col_agg_inoutflow = db_local["agg_inoutflow"]
col_agg_all_group = db_local["agg_all_group"]

# received_signal = False

# def signal_handler(signal, frame):
#     global received_signal
#     print("signal received", signal)
#     received_signal = True

# def handler_sigkill(signal, frame):
#     print("KILL")

# signal.signal(signal.SIGTSTP, signal_handler)
# signal.signal(signal.SIGINT, handler_sigkill)


def get_all_group_and_save_db():
    list_group = col_inoutflow.distinct("group")
    for gr in list_group:
        col_agg_all_group.update_one({"group": gr}, {"$set": {}}, upsert = True)
        print(f"---------------------DONE GROUP {gr}---------------------------")

def run():
    # global received_signal

    offset = 0
    limit = 100

    # get the latest date in inoutflow
    # col_inoutflow.find({}).sort([("time", pymongo.DESCENDING)]).limit(1)
    # get the oldest date in inoutflow
    # col_inoutflow.find({}).sort([("time", pymongo.AS)]).limit(1)

    while True:
        # if received_signal:
        #     return 
        
        list_groups = list(col_agg_all_group.find({}).limit(limit).skip(offset))
        if len(list_groups) == 0:
            break

        for gr in list_groups:
            generate_agg_inoutflow(gr.get("group"))
            print(f"---------------------DONE Group {gr.get('group')}---------------------------")
        offset = offset + limit

def generate_agg_inoutflow(group):
    offset = 0
    limit = 100

    while True:
        filter = {"group": group}
        items = list(col_inoutflow.find(filter).limit(limit).skip(offset))
        if len(items) == 0:
            break
        for item in items:
            time = item.get("time")
            date = time.strftime("%m/%d/%Y")
            col_agg_inoutflow.update_one({"group": group, "type": item.get("type")},{"$inc": {date: item.get("amount")}}, upsert=True)
        offset = offset + limit


run()