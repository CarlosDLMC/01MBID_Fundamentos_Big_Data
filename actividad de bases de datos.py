import sqlite3
from pandas import read_sql_query
from pymongo import MongoClient
from time import time

def read_sqlite(dbfile):
    with sqlite3.connect(dbfile) as dbcon:
        tables = list(read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", dbcon)['name'])
        out = {tbl : read_sql_query(f"SELECT * from {tbl}", dbcon) for tbl in tables}
    return out

def object_dict(table, id):
    res = dict()
    for field in table:
        res[field] = table[field][id]
    return res

def db_to_mongo(db_name):
    client = MongoClient('mongodb://localhost:27017/')
    old_db = read_sqlite(db_name)
    mongo_db_name = db_name.replace('.db', '')
    new_db = client[mongo_db_name]
    for table in old_db:
        table_dict = old_db[table].to_dict()
        objects = [object_dict(table_dict, id) for id in range(len(table_dict[list(table_dict.keys())[0]]))]
        collection = new_db[table]
        collection.insert_many(objects)

t1 = time()
db_to_mongo('love4pets.db')
t2 = time()
print(t2 - t1)
