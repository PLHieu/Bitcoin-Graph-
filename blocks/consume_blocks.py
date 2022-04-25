import requests
import json

from db import *
from mgoqueue import queue

def process_block(data):
    block = data['block']
    response = requests.get(f"https://blockchain.info/rawblock/{block}")
    result =  json.loads(response.text)
    res = col_raw_block.replace_one({'block_index': result.get("block_index")}, result, upsert=True)
    print("Done", block)


queue.consume_mgo_queue("raw_block", process_block)

