from csv import DictReader
import time
from datetime import datetime
import os
import sys

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import db_khoaluan

col_famous_address = db_khoaluan['famous_address']

type_map = {
    "address/Exchanges_full_detailed.csv": "exchange",
    "address/Gambling_full_detailed.csv": "gambling",
    "address/Historic_full_detailed.csv": "historic",
    "address/Mining_full_detailed.csv": "mining",
    "address/Services_full_detailed.csv": "service",
}

def run():
    for path,type_add in type_map.items():
        # open file in read mode
        with open(path, 'r') as read_obj:
            # pass the file object to DictReader() to get the DictReader object
            csv_dict_reader = DictReader(read_obj)
            print(f"Start processing for {path}")
            i = 1
            # iterate over each line as a ordered dictionary
            for row in csv_dict_reader:
                process_famous_address(row, type_add)
                print(i)
                i = i +1 
            
                


def process_famous_address(item, type_add):
    row = dict()
    row["address"] = item.get("hashAdd")
    row["type"] = type_add

    date_time_str = item.get("date_first_tx")
    if date_time_str:
        date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
        row["date_first_tx"] = date_time_obj

    # for exchange
    row["country"] = item.get("country")
    row["exchange"] = item.get("exchange")

    # for mining 
    row["mining"] = item.get("mining")

    # for Gambling
    row["gambling"] = item.get("gambling")

    #for historic
    row["historic"] = item.get("historic")

    #for service
    row["service"] = item.get("service")

    filter = {"address":  row["address"], "type": type_add}
    col_famous_address.insert_one(row)
    # col_famous_address.replace_one(filter, row, upsert=True)

def test():
    a = list(col_famous_address.find({}).limit(100))
    print(a)

run()