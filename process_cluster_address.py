from re import S
from tokenize import group
from pymongo import ReturnDocument


sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import connection_database


config = {
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "local",
    'db_auth': "admin",
}

# Normal
db = connection_database(config=config, maxPoolSize=50)
col_map_group = db["map_group"]
col_address = db["address"]
col_processed_address = db["processed_address"]
col_group_index = db["group_index"]
col_txns = db["txns"]
col_visited_group = db["visited_group"]



def run():
    offset = 0
    limit = 1000
    while True:
        list_address = list(col_address.find({}).limit(limit).skip(offset))
        if len(list_address) == 0:
            break
        for add in list_address:
            group_id = add.get("group")
            if check_visited(group_id):
                continue
            print("DFS ", group_id)
            dfs(group_id)

        offset = offset + limit

# def find_all_group(address, group_id):
#     process_from(address, group_id)
#     process_to(address, group_id)



# def process_to(address_check, group_id):
#     #todo offset limit
#     res = list(col_map_group.find({"group_from": group_id}))
#     for r in res:
#         group_to = r.get("group_to")
#         # find all address belong to group_to
#         #todo offset limit
#         list_address_to = list(col_address.find({"group": group_to}))
#         for add in list_address_to:
#             address = add.get("address")
#             if address == address_check:
#                 continue
#             find_all_group(address, group_id)

# def process_from(address_check, group_id):
#     #todo offset limit
#     res = list(col_map_group.find({"group_to": group_id}))
#     for r in res:
#         group_from = r.get("group_from")
#         # find all address belong to group_from
#         #todo offset limit
#         list_address_from = list(col_address.find({"group": group_from}))
#         for add in list_address_from:
#             address = add.get("address")
#             if address == address_check:
#                 continue
#             find_all_group(address, group_id)



def dfs(group_id):
    stack = []
    stack.append(group_id)

    while len(stack) != 0:
        current = stack.pop()

        if not check_visited(current):
            mark_visited(current, group_id)
        
        neighbors = get_neighbor(current)
        for n in neighbors:
            if not check_visited(n):
                stack.append(n)

def check_visited(group_id):
    res = col_visited_group.find_one({"group": group_id})
    if res == None:
        return res
    return res.get("checked",None)

def mark_visited(group_id, original_id):
    col_visited_group.update_one({"group": group_id}, {"$set": {"checked": True}}, upsert=True)
    # get all address in this group
    list_address = list(col_address.find({"group": group_id}))
    # Mark all of these addresses into process address
    for add in list_address:
        col_processed_address.update_one({"address": add.get("address")}, {"$set": {"group": original_id}},upsert=True)

def get_neighbor(group_id):
    s = set()
    add_from = list(col_map_group.find({"group_from": group_id}))
    for ad in add_from:
        s.add(ad.get("group_to"))
    
    add_to = list(col_map_group.find({"group_to": group_id}))
    for ad in add_to:
        s.add(ad.get("group_from"))
    
    return list(s)

run()