import os
import sys
import json

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
col_famous_address_txns_v2 = db['famous_address_txns_v2']
col_edge = db['edge']


def lable_address_from_start():
    offset = 0
    limit = 5000
    label = 1

    while True:
        list_address = list(
            col_famous_address.find().skip(offset).limit(limit))

        if len(list_address) == 0:
            break

        for add in list_address:
            col_famous_address.find_one_and_update(
                {"_id": add.get("_id")}, {"$set": {"label": label}})
            label = label + 1

        print(offset)

        offset += limit


def lable_address_from_edge_collection():
  with open('edges.csv','a') as file:
    offset = 0
    limit = 5000
    group = 0

    label = 1
    map_add_lable = dict()

    # init start id
    start_item = list(col_edge.find({}).limit(1).skip(offset))[0]
    last_id = start_item.get("_id")

    while True:

      if offset > 3000000:
          break

      edges = list(col_edge.find({"_id": {"$gte": last_id}}).limit(limit))

      if len(edges) == 0:
          break
      last_id = edges[len(edges)-1].get("_id")
      for edge in edges:
        #get from node
        from_node = edge.get("address_in", "")
        if from_node not in map_add_lable:
          map_add_lable[from_node] = label
          label = label + 1
    
        #get target node
        to_node = edge.get("address_out", "")
        if to_node not in map_add_lable:
          map_add_lable[to_node] = label
          label = label + 1

        from_label = map_add_lable[from_node]
        to_label = map_add_lable[to_node]

        txt = '%d,%d\n' % (from_label, to_label)
        file.write(txt)

      print(offset)
      # print(last_id)
      offset = offset + limit


lable_address_from_start()
