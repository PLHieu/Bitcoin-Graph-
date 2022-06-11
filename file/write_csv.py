import json
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

col = db['feature_with_time']


# load processed_address fro db and write into csv file
with open('feature.csv','a') as file:
    # write header
    file.write("first_date_txn,last_date_txn,ath_outflow,lowest_outflow,ath_inflow,lowest_inflow,num_txns,mean_inflow,mean_outflow,address,label\n")

    offset = 0
    limit = 50000

    while True:
        res = list(col.find().skip(offset).limit(limit))

        if len(res) == 0:
            break
        
        for item in res:
            date_string = item.get("date").strftime("%m/%d/%Y")
            txt = '%s,%s,%s,%d\n' % (item.get("address", ""), item.get("type", ""), date_string, item.get("amount", ""))
            file.write(txt)

        print(offset)
        # print(last_id)
        offset = offset + limit
