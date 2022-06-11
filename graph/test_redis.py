import os
import sys
import json
from pymongo import MongoClient
import requests
from datetime import datetime
import time
import http.client
import urllib.parse
import pytz
import redis

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import connection_database
# from mgoqueue import queue



db = connection_database({
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "khoaluan",
    'db_auth': "admin",
})
col_famous_address = db['famous_address']

r = redis.Redis(host='localhost', port=6379, db=0)

def add_all_famous_address():
	offset = 0
	limit = 100000
	while True:
		list_address = list(col_famous_address.find({}).limit(limit).skip(offset))

		if len(list_address) == 0:
			break

		for add in list_address:
			# r.sadd("famous_address", add.get("address"))
			r.set(name=add.get("address"), value=add.get("label"))
		print(offset)
		offset = offset + limit


# is_memeber = r.sismember("famous_address", "1HqwgAJqqneUcxUUWKbhGiRADNWXwY9yYG")
num_items = r.scard("famous_address")

print(num_items)
# add_all_famous_address()

# print(r.get("1LDbBEqndip1dhPMszjT6n3CzuD768R5aR"))
