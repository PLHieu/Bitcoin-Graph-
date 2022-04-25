import signal
import socket
import traceback
from time import sleep
from datetime import datetime
from pymongo import errors
from utils.db import connection_database
from copy import deepcopy

received_signal = False


def signal_handler(signal, frame):
    global received_signal
    print("signal received", signal)
    received_signal = True


signal.signal(signal.SIGINT, signal_handler)

config = {
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "khoaluan",
    'db_auth': "admin",
}

# Normal
db = connection_database(config=config, maxPoolSize=50)
hostname = socket.gethostname()
listDb = {}


def init_queue(db_name):
    if db_name in listDb:
        return listDb[db_name]['queue'], listDb[db_name]['queue_consumed']
    queue = db[db_name]
    queue.create_index('keys')
    queue.create_index('unique_key', unique=True)

    queue_consumed = db[db_name + "_consumed"]
    queue_consumed.create_index('keys')
    queue_consumed.create_index("created_time", expireAfterSeconds=604800)

    listDb[db_name] = {
        'queue': queue,
        'queue_consumed': queue_consumed
    }

    return queue, queue_consumed


def push_mgo_queue_multiple(db_name, params):
    try:
        queue, _ = init_queue(db_name)
    except:
        raise Exception('Something went wrong with the db')

    now = datetime.now()
    items = []
    for data, unique_key, keys in params:
        item = {
            'process_by': 'NONE',
            'data': deepcopy(data),
            'unique_key': unique_key,
            'created_time': now,
            'updated_time': now,
        }

        if len(keys) > 0:
            item['keys'] = keys

        items.append(deepcopy(item))

    try:
        queue.insert_many(items)
        return {
            'status': True,
            'message': 'success'
        }
    except errors.DuplicateKeyError:
        return {
            'status': True,
            'message': 'Item existed in queue'
        }
    except Exception as e:
        return {
            'status': False,
            'message': e
        }


def push_mgo_queue(db_name, data, unique_key, keys):
    try:
        queue, _ = init_queue(db_name)
    except:
        raise Exception('Something went wrong with the db')

    now = datetime.now()
    item = {
        'process_by': 'NONE',
        'data': data,
        'unique_key': unique_key,
        'created_time': now,
        'updated_time': now,
    }

    if len(keys) > 0:
        item['keys'] = keys

    try:
        queue.insert_one(item)
        return {
            'status': True,
            'message': 'success'
        }
    except errors.DuplicateKeyError:
        return {
            'status': True,
            'message': 'Item existed in queue'
        }
    except Exception as e:
        return {
            'status': False,
            'message': e
        }


def consume_mgo_queue(queue_name, callback_function, on_process_done=None, on_process_failed=None):
    try:
        queue, queue_consumed = init_queue(queue_name)
    except Exception as ex:
        raise ex

    while True:
        if received_signal:
            return
        item = queue.find_one_and_update({'process_by': 'NONE'},
                                         {'$set': {'process_by': hostname, 'updated_time': datetime.now()}})

        if item is None:
            sleep(0.05)
        else:
            print(f"processing item {item}")
            count_time = datetime.now()
            data = item['data']
            logs = item.get("logs")
            if logs is not None and len(logs) > 5:
                queue.update_one(
                    {'_id': item['_id']},
                    {'$set': {'process_by': "PROCESS TOO MANY TIMES", 'updated_time': datetime.now()}}
                )
                continue
            # do func
            try:
                callback_function(data)
            except:
                queue.update_one({'_id': item['_id']},
                                 {
                                     '$set': {
                                         'process_by': 'NONE',
                                         'updated_time': datetime.now(),
                                     },
                                     "$push": {
                                         "logs": traceback.format_exc()
                                     }
                                 })
                if on_process_failed is not None:
                    _id = item['_id']
                    queue.delete_one({'_id': _id})
                    on_process_failed(data)
                sleep(0.5)
                continue

            now = datetime.now()
            time_cost = now - count_time
            _id = item['_id']
            item['process_by'] = hostname
            item["process_time_ms"] = time_cost.total_seconds()
            item["created_time"] = now
            queue_consumed.insert_one(item)
            queue.delete_one({'_id': _id})
            if on_process_done is not None:
                on_process_done(data)
