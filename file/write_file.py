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

col_agg_famous_inoutflow_v2 = db["agg_famous_inoutflow_v2"]


# load processed_address fro db and write into csv file
with open('inoutflow.csv','a') as file:
    offset = 0
    limit = 5000
    group = 0

    # init start id
    start_item = list(col_agg_famous_inoutflow_v2.find({}).limit(1).skip(offset))[0]
    last_id = start_item.get("_id")

    while True:
        edges = list(col_agg_famous_inoutflow_v2.find({"_id": {"$gt": last_id}}).limit(limit))

        if len(edges) == 0:
            break
        last_id = edges[len(edges)-1].get("_id")
        for edge in edges:
            # del edge["_id"]
            # file.write(json.dumps(edge))
            file.write(f"{edge.get('group')},{edge.get('type')},{edge.get('date')},{edge.get('value')}")
            file.write('\n')
        print(offset)
        print(last_id)
        offset = offset + limit
