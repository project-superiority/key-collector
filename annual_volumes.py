from pymongo import MongoClient
import pymongo

client = MongoClient('localhost', 27017)
db = client['Superiority']
requests = db['requests']
db_anomalies = db['db_anomalies']

array_anomalies = db_anomalies.find()
total_anomalies = 11454

count = 0
for anomaly in array_anomalies:
    count += 1
    request = requests.find_one({'nameniche': anomaly['nameniche'], 'namereg': anomaly['namereg']})
#    volumes = [int(i['volume']) for i in request['volumes'][:12]]
    if (annual_volume := sum(request['volumes'][:12])) < 100:
        print(anomaly['nameniche'], anomaly['namereg'], annual_volume)

    print(count * 100 //  total_anomalies, '%', end = '\r')
