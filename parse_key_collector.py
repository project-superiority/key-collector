import argparse
import datetime
from dateutil import parser
from pymongo import MongoClient
import pymongo

def trend(array, lastmonth):
    averageYear = sum(array[:12]) / 12

    seasonpeek = []
    for i in range(12):
        if array[i] > averageYear * 1.2:
            if lastmonth > i:
                seasonpeek.append(lastmonth - i)
            else:
                seasonpeek.append(lastmonth - i + 12)

    return ', '.join([str(i) for i in sorted(seasonpeek)])

arg_parser = argparse.ArgumentParser()

arg_parser.add_argument('namereg')
arg_parser.add_argument('filename')

args = arg_parser.parse_args()

print ('Имя региона:', args.namereg)
print ('Файл:', args.filename)

with open(args.filename, "r") as f:
    lastperiod = parser.parse(f.readline().replace('"', '').split(';')[24])

    count_requests = 0
    count_anomalies = 0
    array_requests = []
    array_anomalies = []
    error_list = []
    while (line := f.readline()):
        row_parsed = line.replace('"', '').split(';')
        if len(row_parsed) < 25:
            error_list.append(row_parsed[0] + '\n')
            print('Ошибка парсинга: ', row_parsed[0])
            continue

        #* Подготовка к заполнению requests
        volumes = [int(row_parsed[i]) for i in range(24, 0, -1)]
        dict_request = dict()
        dict_request['nameniche'] = row_parsed[0]
        dict_request['namereg'] = args.namereg
        dict_request['lastperiod'] = lastperiod
        dict_request['volumes'] = volumes
        dict_request['trend'] = trend(dict_request['volumes'], lastperiod.month)

        if sum(volumes[:12]) > 100:
            array_requests.append(dict_request)
            count_requests += 1

        #* Подготовка к заполнению db_anomalies
        volume = volumes[0]
        if (volume_year_ago := dict_request['volumes'][12] > 0):
            growth = dict_request['volumes'][0] / dict_request['volumes'][12] - 1
        else:
            growth = 10

        dict_anomalies = dict()
        dict_anomalies['nameniche'] = dict_request['nameniche']
        dict_anomalies['namereg'] = dict_request['namereg']
        dict_anomalies['trend'] = dict_request['trend']
        dict_anomalies['anomalies'] = [{'volume': volume, 'growth': growth}]

        if  ( sum(volumes[:12]) > 100 and 
            volume > 100 and 
            (growth > 0.5 or growth < -0.5)
            ):
            array_anomalies.append(dict_anomalies)
            count_anomalies += 1

#* Заполнение БД
client = MongoClient('localhost', 27017)
db = client['Superiority']
requests = db['requests']
db_anomalies = db['db_anomalies']

requests.insert_many(array_requests)
db_anomalies.insert_many(array_anomalies)
        
with open("error.log", "a") as f:
    f.writelines(error_list)

print('Всего requests:', count_requests)
print('Всего anomalies:', count_anomalies)
