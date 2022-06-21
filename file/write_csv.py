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

col = db['feature_partition']


# load processed_address fro db and write into csv file
with open('feature_partition.csv','a') as file:
    # write header
    file.write("num_node,num_internal_edge,num_txns,ath_outflow,ath_inflow,lowest_outflow,lowest_inflow,total_out,total_in,mean_mean_out,mean_mean_in,first_date_txn,last_date_txn,mean_all_in_among_node,mean_all_out_among_node\n")

    offset = 0
    limit = 50000

    while True:
        res = list(col.find().skip(offset).limit(limit))

        if len(res) == 0:
            break
        
        for item in res:
            last_date_string = item.get("last_date_txn").strftime("%m/%d/%Y")
            first_date_string = item.get("first_date_txn").strftime("%m/%d/%Y")
            txt=f'{item.get("num_node")},{item.get("num_internal_edge")},{item.get("num_txns")},{item.get("ath_outflow")},{item.get("ath_inflow")},{item.get("lowest_outflow")},{item.get("lowest_inflow")},{item.get("total_out")},{item.get("total_in")},{item.get("mean_mean_out")},{item.get("mean_mean_in")},{first_date_string},{last_date_string},{item.get("mean_all_in_among_node")},{item.get("mean_all_out_among_node")}\n'
            file.write(txt)

        print(offset)
        # print(last_id)
        offset = offset + limit
