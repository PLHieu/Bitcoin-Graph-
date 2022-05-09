from db import db_local
import json
import pymongo

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

col_group_mapping = 


# load processed_address fro db and write into csv file
with open('group_mapping.json','a') as file:
	total = 0
	limit = 200
	while True:
		list_blocks = list(db_local["processed_address"].find(filter={}, limit=limit).sort([("group", pymongo.ASCENDING)]).skip(total))
		if len(list_blocks) == 0:
			break
		for line in list_blocks:
			del line["_id"]
			file.write(json.dumps(line))
			file.write('\n')
			print("Got ", line.get("address"))
		total += limit
