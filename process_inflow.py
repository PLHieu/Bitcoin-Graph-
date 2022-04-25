from datetime import datetime
import pytz
import signal
from db import db_local

col_graph = db_local["graph"]
col_txns = db_local["txns"]
col_inoutflow = db_local["inoutflow"]
col_processed_address = db_local["processed_address"]

received_signal = False

def signal_handler(signal, frame):
    global received_signal
    print("signal received", signal)
    received_signal = True

def handler_sigkill(signal, frame):
    print("KILL")

signal.signal(signal.SIGTSTP, signal_handler)
signal.signal(signal.SIGINT, handler_sigkill)

def run():
    global received_signal
    start_block = 450000
    end_block = 450006

    for i in range (start_block, end_block):
        if received_signal:
            return 

        offset = 0
        limit = 100
        while True:
            filter = {"block_index": i}
            txns = list(col_txns.find(filter).limit(limit).skip(offset))

            if len(txns) == 0:
                break

            for txn in txns:
                process_one_txn(txn)
            print(f"Txns in block: {i}, offset: {offset}")
            offset += limit

        print(f"---------------------DONE BLOCK {i}---------------------------")

def process_one_txn(txn):
    inputs = txn.get("inputs")
    outputs = txn.get("out")
            
    time = datetime.fromtimestamp(txn.get("time"), pytz.UTC)
    is_coin_base_txn = txn.get("inputs")[0].get("prev_out").get("addr", "") == ""
    
    updater = {
        "block_index": txn.get("block_index"),
        "time": time,
        "timestamp": txn.get("time"),
    }

    if not is_coin_base_txn:
        for input in inputs:
            # get group of this input
            address = input.get("prev_out").get("addr", "")
            res_group = col_processed_address.find_one({"address": address})

            if not res_group:
                continue 
            
            group = res_group.get("group")

            filter = {
                "group": group,
                "type": "OUTFLOW",
                "address": address,
                "txn_hash": txn.get("hash"),
            }

            updater["amount"] =  input.get("prev_out").get("value", "")

            col_inoutflow.update_one(filter, {"$set": updater}, upsert=True)



    for output in outputs:
            # get group of this output
            address = output.get("addr","")
            res_group = col_processed_address.find_one({"address": address})
            
            if not res_group:
                continue 
            
            group = res_group.get("group")
            
            updater["amount"] = output.get("value", "")
            filter = {
                "group": group,
                "amount": output.get("value", ""),
                "type": "INFLOW",
                "address": address,
                "txn_hash": txn.get("hash"),
            }
            col_inoutflow.update_one(filter, {"$set": updater}, upsert=True)

run()