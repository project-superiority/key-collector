import argparse
import datetime
from dateutil import parser
from pymongo import MongoClient
import pymongo

arg_parser = argparse.ArgumentParser()

arg_parser.add_argument('filename')

args = arg_parser.parse_args()

set_to_delete = set()
with open(args.filename, "r") as f:
    while (niche := f.readline()):
        set_to_delete.add(niche.replace("\n", ""))

client = MongoClient('localhost', 27017)
db = client['Superiority']
requests = db['requests']
db_anomalies = db['db_anomalies']

for niche in set_to_delete:
    requests.delete_many({'nameniche': niche})
    db_anomalies.delete_many({'nameniche': niche})
    print(niche, ': удалено')