import os
import sys
import json
import signal
import pymongo
import numpy

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
col_famous_address_txns_v2 = db['famous_address_txns_v2']
col_map_add_group = db['map_add_group']
col_famous_inoutflow_v2 = db['famous_inoutflow_v2']
col_feature_with_time = db['feature_with_time']



# get feature for 1 group and return the dictionary feature 
def get_feature_group(group):
    list_wallets = []
    features = {}
    
    # number of wallets in this group
    num_wallets = 0

    # map from exchange to list address
    map_exchange_list_address = {}

    # analytics the exchange in this group
    
    
    # get the first txn
    first_txn = list(col_famous_address_txns_v2.find({"label": {"$in": list_wallets}}).sort([{"block_index": pymongo.ASCENDING}]).limit(1))[0]

    # get the last txn 
    last_txn = list(col_famous_address_txns_v2.find({"label": {"$in": list_wallets}}).sort([{"block_index": pymongo.DESCENDING}]).limit(1))[0]


    # analytics the largest in flow among dates

    # analytics the largest in flow among weeks

    # analytics the largest out flow among dates

    # analytics the largest out flow among weeks

    # analytics the first date txns
    features["first_date_txn"] = first_txn.get("time")

    # analytics the last date txns
    features["last_date_txn"] = last_txn.get("time")

    # analytics the week which has the most trading volume

    # analytics the date which has the most trading volume

    # total number of transaction
    features["total_txns"] = col_famous_address_txns_v2.count_documents({"label": {"$in": list_wallets}})

    # analytics the first block txns
    features["first_block_txn"] = first_txn.get("block_index")

    # analytics the last block txns
    features["last_block_txn"] = last_txn.get("block_index")


# get feature for 1 address and return the dictionary feature 
def get_feature_address(address):
    features = {}
    list_io = []
    list_inflow = []
    list_outflow = []

    list_io = list(col_famous_inoutflow_v2.find({"address": address}))
    len_io = len(list_io)

    for io in list_io:
        if io.get("type") == "INFLOW":
            list_inflow.append(io.get("amount")) 
        elif io.get("type") == "OUTFLOW":
            list_outflow.append(io.get("amount")) 

    # firstly we sort according to time
    list_io_sorted_time = sorted(list_io, key=lambda item: item.get("timestamp"))
    
    # get the date of the first txn
    # res = col_famous_address.find({"address": address})
    # features["first_date_txn"] = res.get("date_first_tx")
    features["first_date_txn"] = list_io_sorted_time[0].get("time")

    # get the date of the last txn
    features["last_date_txn"] = list_io_sorted_time[len_io-1].get("time") 

    # get the first txn_hash
    first_txn_hash = list_io_sorted_time[0].get("txn_hash")

    # get the last txn_hash
    last_txn_hash = list_io_sorted_time[len_io-1].get("txn_hash")

    # secondly we sort according to value
    list_io_sorted_value = sorted(list_io, key=lambda item: item.get("amount"))

    # get the ath outflow
    for i in reversed(range(len_io)):
        if list_io_sorted_value[i].get("type") == "OUTFLOW":
            features["ath_outflow"] = list_io_sorted_value[i].get("amount")
            break

    # get the lowest outflow
    for i in range(len_io):
        if list_io_sorted_value[i].get("type") == "OUTFLOW":
            features["lowest_outflow"] = list_io_sorted_value[i].get("amount")
            break

    # get the ath inflow
    for i in reversed(range(len_io)):
        if list_io_sorted_value[i].get("type") == "INFLOW":
            features["ath_inflow"] = list_io_sorted_value[i].get("amount")
            break

    # get the lowest inflow
    for i in range(len_io):
        if list_io_sorted_value[i].get("type") == "INFLOW":
            features["lowest_inflow"] = list_io_sorted_value[i].get("amount")
            break

    # get the number of txns
    set_txns = set()
    for io in list_io:
        set_txns.add(io.get("txn_hash"))
    features["num_txns"] = len(set_txns)
     
    # get the mean of in flow
    mean_inflow = numpy.mean(list_inflow)
    features["mean_inflow"] = mean_inflow

    # get the mean of outflow
    mean_outflow = numpy.mean(list_outflow)
    features["mean_outflow"] = mean_outflow

    return features

def process_get_feature(data):
    address = data.get("address")
    label = data.get("label")

    feature = get_feature_address(address)

    feature["address"] = address
    feature["label"] = label
    col_feature_with_time.insert_one(feature)


queue.consume_mgo_queue("queue_extract_feature", process_get_feature)