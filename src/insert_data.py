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

weather_data_dir = '../wx_data'
yield_data_dir = '../yld_data'

weather_data_files = glob(f'{weather_data_dir}/*.txt')

for file in tqdm(weather_data_files, desc="Files Completed"):
    file_name = Path(file).stem
    with open(file, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
            l = line.split('\t')
            date, max_temp, min_temp, precipitation = l[0], float(l[1])/10, float(l[2])/10, float(l[3])/10
            distinct_key = file_name+date
            weather_record = defaultdict()
            weather_record = {
                '_id': distinct_key,
                'weather_station': file_name,
                'date': date,
                'max_temp': max_temp,
                'min_temp': min_temp,
                'precipitation': precipitation
            }
            if not weathercollection.find_one({"_id": distinct_key}):
                x = weathercollection.insert_one(weather_record)

yield_data_files = glob(f'{yield_data_dir}/*.txt')

for file in tqdm(yield_data_files, desc="Files Completed"):
    file_name = Path(file).stem
    with open(file, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
            l = line.split('\t')
            year, crop_yield = l[0], float(l[1])
            distinct_key = file_name+date
            yield_record = defaultdict()
            yield_record = {
                '_id': year,
                'crop_yield': crop_yield
            }
            if not yieldcollection.find_one({"_id": distinct_key}):
                x = yieldcollection.insert_one(yield_record)