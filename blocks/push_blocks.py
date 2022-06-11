import sys
import os

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))

from mgoqueue import queue


start_block = 450000
end_block = 550000

for i in range(start_block, end_block):
    queue.push_mgo_queue("queue_raw_block", {"block": i}, f"{i}", [i])
    print(i)



