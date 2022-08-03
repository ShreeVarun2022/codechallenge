import pymongo, re
from loguru import logger
import datetime
from glob import glob
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm
from flask import Flask
from flask import request

conn_str = "mongodb://localhost/Test?retryWrites=true&w=majority"
mongoclient = pymongo.MongoClient(conn_str, port=27017, serverSelectionTimeoutMS=5000)
try:
    logger.opt(colors=True).info("<green>Connection to MongoDB successful</green>")
except Exception:
    logger.opt(colors=True).info("<green>Unable to connect to Mongo</green>")

mongo_db = mongoclient["Test"]
weathercollection = mongo_db["WeatherData"]
yieldcollection = mongo_db["YieldData"]

app = Flask(__name__)
 

def getStats(year, station):
    date_regex = re.compile(year, re.IGNORECASE)
    station_regex = re.compile(station, re.IGNORECASE)
    min_temps = []
    max_temps = []
    precipation_list = []
    for document in list(weathercollection.find({"weather_station": station_regex, "date": date_regex})):
        max_temps.append((document['max_temp']))
        min_temps.append((document['min_temp']))
        precipation_list.append((document['precipitation']))
    min_temps = [x for x in min_temps if x != -999.9]
    max_temps = [x for x in max_temps if x != -999.9]
    precipation_list = [x for x in precipation_list if x != -999.9]
    responseObj = defaultdict()
    responseObj = { 
        "Average Max": sum(max_temps)/len(max_temps),
        "Average Min": sum(min_temps)/len(min_temps),
        "Average Precipation": sum(precipation_list)/len(precipation_list)
    }
    return responseObj

def getWeather(year='', station=''):
    date_regex = re.compile(f"^{year}", re.IGNORECASE)
    station_regex = re.compile(station, re.IGNORECASE)
    responseObj = list(weathercollection.find({"weather_station": station_regex, "date": date_regex}))
    return responseObj

def getYield(year=''):
    date_regex = re.compile(f"^{year}", re.IGNORECASE)
    responseObj = list(yieldcollection.find({"_id": date_regex}))
    return responseObj
    

def bar():
    print ("Inside bar")

@app.route('/api/weather')
def weather():
    year = request.args.get('year')
    station = request.args.get('station')
    if (year!=None or station!=None):
        if year!=None:
            a = year
        else:
            a = ''
        if station!=None:
            b = station
        else:
            b = ''
        obj = getWeather(a, b)
    else:
        obj = getWeather()
    return obj

@app.route('/api/yield')
def yield_():
    year = request.args.get('year')
    if year!=None:
        a = year
    else:
        a = ''
    return getYield(a)
    
@app.route('/api/weather/stats')
def stats():
    year = request.args.get('year')
    station = request.args.get('station')
    if (year!=None or station!=None):
        if year!=None:
            a = year
        else:
            a = ''
        if station!=None:
            b = station
        else:
            b = ''
        obj = getStats(a, b)
    else:
        bar()
    return obj

if __name__ == '__main__':
    app.run()