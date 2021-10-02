from pymongo import MongoClient
import pymongo

client = MongoClient('localhost', 27017)
db = client['Superiority']
requests = db['requests']
db_anomalies = db['db_anomalies']

array_requests = requests.find()

set_niches = {request['nameniche'] for request in array_requests}
i = 0
for niche in set_niches:
    i += 1
    pos_anomalies = 0
    neg_anomalies = 0
    array_anomalies = db_anomalies.find({'nameniche': niche})
    for anomaly in array_anomalies:
        if anomaly['anomalies'][0]['growth'] > 0:
            pos_anomalies += 1
        else:
            neg_anomalies += 1

    db.requests.update_many({'nameniche': niche}, {'$set': {'posAnomalies': pos_anomalies, 'negAnomalies': neg_anomalies}})
    # print(niche, ':', pos_anomalies, '/', neg_anomalies)
    print(i * 100 // len(set_niches), '%', end='\r')


