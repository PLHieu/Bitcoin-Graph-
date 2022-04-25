from db import db_local
import json
import pymongo


# load processed_address fro db and write into csv file
with open('clusters_address.json','a') as file:
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
