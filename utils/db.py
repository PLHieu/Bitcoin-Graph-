import urllib
from pymongo import MongoClient


def connection_database(config, collection=None, maxPoolSize=200):
    usr = config.get('db_user', "")
    pwd = config.get('db_pass', "")
    host = config.get('db_host')[0]
    port = config.get('port', '27017')
    database_name = config.get('db_name', '')
    auth_source = config.get('db_auth', '')
    pwd = urllib.parse.quote_plus(pwd)
    # check if no auth_source connect to local DB
    if auth_source:
        link = f'mongodb://{usr}:{pwd}@{host}:{port}/?authSource={auth_source}'
    else:
        link = f'mongodb://{host}:{port}/?directConnection=true'
    db = MongoClient(link, maxPoolSize=maxPoolSize).get_database(database_name)
    return db if collection is None else db[collection]


db_khoaluan = connection_database({
    'db_user': "hieu",
    'db_pass': "password",
    'db_host': ["127.0.0.1"],
    'port': "27017",
    'db_name': "khoaluan",
    'db_auth': "admin",
})
