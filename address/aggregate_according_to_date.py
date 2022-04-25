from datetime import datetime
import pytz
import signal
import pymongo
import sys
import os

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
col_famous_address_txns = db['famous_address_txns']
col_famous_inoutflow = db['famous_inoutflow']

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
    list_country = col_famous_address.distinct("country")
    list_exchange = col_famous_address.distinct("exchange")
    for exchange in list_exchange:
        for country in list_country:
            data = {
                "country": country,
                "exchange": exchange,
            }
            queue.push_mgo_queue("country_exchange", data, f"{exchange}-{country}", [exchange, country])
            print(f"{exchange}-{country}")


def process_group(data):
    exchange = data.get("exchange")
    country = data.get("country")
    print(f"---------------------Starting processing ({exchange}-{country})---------------------------")
    offset = 0
    limit = 100
    while True:
        adds = list(col_famous_address.find({"exchange": exchange, "country": country}).limit(limit).skip(offset))
        if len(adds) == 0:
            break
        for add in adds:
            generate_agg_inoutflow(exchange, country, add)
            print(f"---------------------Done address {add}---------------------------")
        offset = offset + limit
    print(f"---------------------Finish processing ({exchange}-{country})---------------------------")


def generate_agg_inoutflow(exchange, country, add):
    offset = 0
    limit = 100

    while True:
        filter = {"address": add}
        items = list(col_famous_inoutflow.find(filter).limit(limit).skip(offset))
        if len(items) == 0:
            break
        for item in items:
            time = item.get("time")
            date = time.strftime("%m/%d/%Y")
            col_agg_inoutflow.update_one({"exchange": exchange, "country": country, "type": item.get("type")},{"$inc": {date: item.get("amount")}}, upsert=True)
        offset = offset + limit

