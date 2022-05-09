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
col_extracted_group = db['extracted_graph']
col_map_graph = db['map_graph']

def extract_group():
	offset = 0
	limit = 5000
	group = 0
	while True:
		list_address = list(col_famous_address.find({}).limit(limit).skip(offset))

		if len(list_address) == 0:
			break

		for add in list_address:
			country = add.get("country","")
			exchange = add.get("exchange", "")

			res = col_extracted_group.find_one({"country": country, "exchange": exchange})
			if res is not None:
				continue

			col_extracted_group.insert_one({"country": country, "exchange": exchange, "group": group})
			# update all group into col_famous_address
			col_famous_address.update_many({"country": country, "exchange": exchange}, {"$set": {"group": group}})

			group = group + 1
		print(offset)
		offset = offset + limit

def map_graph():
	list_group = list(col_extracted_group.find({}))
	for gr_from in list_group:
		for gr_to in list_group:
			if gr_from.get("_id") == gr_to.get("_id"):
				continue

			# map 2 group if they come from the same country
			country_from = gr_from.get("country")
			country_to = gr_to.get("country")

			l = [gr_from.get("group"),  gr_to.get("group")]
			l.sort()

			if country_from == country_to:
				print("Found same in country")
				col_map_graph.update_one({"group_from": l[0],"group_to": l[1]}, {"$set": {"same_in_country": country_from}}, upsert=True)

			# map 2 group if they come from the same exchange
			exchange_from = gr_from.get("exchange")
			exchange_to = gr_to.get("exchange")

			if exchange_from == exchange_to:
				print("Found same in exchange")
				col_map_graph.update_one({"group_from": l[0],"group_to": l[1]}, {"$set": {"same_in_exchange": exchange_from}}, upsert=True)


# function to create adge between graph
def extract_edge():
	offset = 0
	limit = 5000
	group = 0
	while True:
		list_address = list(col_famous_address.find({}).limit(limit).skip(offset))

		if len(list_address) == 0:
			break

		for add in list_address:
			country = add.get("country","")
			exchange = add.get("exchange", "")

			res = col_extracted_group.find_one({"country": country, "exchange": exchange})
			if res is not None:
				continue

			col_extracted_group.insert_one({"country": country, "exchange": exchange, "group": group})
			# update all group into col_famous_address
			col_famous_address.update_many({"country": country, "exchange": exchange}, {"$set": {"group": group}})

			group = group + 1
		print(offset)
		offset = offset + limit

