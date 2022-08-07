from loguru import logger
import json
from glob import glob
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm
from flask import Flask
from flask import request
import sqlite3
from sqlite3 import Error

weather_data_dir = 'wx_data'
yield_data_dir = 'yld_data'

weather_data_files = glob(f'{weather_data_dir}/*.txt')

def insert_record(record, table_name, conn):
    columns = ', '.join(record.keys())
    keySet = list(record.keys())
    values = ', '.join([f"'{str(record[key])}'" for key in keySet])
    sql = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'
    conn.execute(sql)

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

conn = sqlite3.connect('weather.db', check_same_thread=False)
conn.execute('''CREATE TABLE IF NOT EXISTS weather
         (id TEXT PRIMARY   KEY     NOT NULL,
         station            TEXT    NOT NULL,
         date               INT     NOT NULL,
         year               INT     NOT NULL,
         max_temp           REAL    NOT NULL,
         min_temp           REAL    NOT NULL,
         precipitation      REAL    NOT NULL);''')

for file in tqdm(weather_data_files, desc="Inserting Weather Records"):
    file_name = Path(file).stem
    with open(file, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
            l = line.split('\t')
            date, max_temp, min_temp, precipitation = l[0], float(l[1])/10, float(l[2])/10, float(l[3])/10
            if (max_temp == -999.9):
                max_temp = 0.0
            if (min_temp == -999.9):
                min_temp = 0.0
            if (precipitation == -999.9):
                precipitation = 0.0
            distinct_key = file_name+date
            weather_record = defaultdict()
            weather_record = {
                'id': distinct_key,
                'station': file_name,
                'date': date,
                'year': date[:4],
                'max_temp': max_temp,
                'min_temp': min_temp,
                'precipitation': precipitation
            }
            try:
                insert_record(weather_record, 'weather', conn)
            except:
                pass
conn.commit()
conn.close()

yield_data_files = glob(f'{yield_data_dir}/*.txt')

conn = sqlite3.connect('yield.db', check_same_thread=False)
conn.execute('''CREATE TABLE IF NOT EXISTS yield
         (year TEXT PRIMARY   KEY     NOT NULL,
         crop_yield           TEXT    NOT NULL);''')
for file in tqdm(yield_data_files, desc="Inserting Yield Records"):
    file_name = Path(file).stem
    with open(file, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
            l = line.split('\t')
            year, crop_yield = l[0], float(l[1])
            yield_record = defaultdict()
            yield_record = {
                'year': year,
                'crop_yield': crop_yield
            }
            try:
                insert_record(yield_record, 'yield', conn)
            except:
                pass
conn.commit()
conn.close()

app = Flask(__name__)
 
def getStats(year='', station=''):
    conn = sqlite3.connect('weather.db', check_same_thread=False)
    conn.row_factory = dict_factory
    sql = ""
    if len(year) > 0 and len(station) > 0:
        sql = f"SELECT year, AVG(max_temp), AVG(min_temp), AVG(precipitation) FROM weather WHERE date LIKE '{year}%' AND station like '{station}'"
    elif len(year) and len(station)==0:
        sql = f"SELECT year, AVG(max_temp), AVG(min_temp), AVG(precipitation) FROM weather WHERE date LIKE '{year}%'"
    elif len(station) and len(year)==0:
        sql = f"SELECT year, AVG(max_temp), AVG(min_temp), AVG(precipitation) FROM weather WHERE station LIKE '{station}'"
    else:
        sql = f"SELECT year, AVG(max_temp), AVG(min_temp), AVG(precipitation) FROM weather GROUP BY year"
    cursor = conn.execute(sql)
    data = cursor.fetchall()
    responseObj = json.dumps(data)
    conn.close()
    return responseObj

def getWeather(year='', station=''):
    conn = sqlite3.connect('weather.db', check_same_thread=False)
    conn.row_factory = dict_factory
    sql = ""
    if len(year) > 0 and len(station) > 0:
        sql = f"SELECT * FROM weather WHERE date LIKE '{year}%' AND station like '{station}'"
    elif len(year) and len(station)==0:
        sql = f"SELECT * FROM weather WHERE date LIKE '{year}%'"
    elif len(station) and len(year)==0:
        sql = f"SELECT * FROM weather WHERE station LIKE '{station}'"
    else:
        sql = f"SELECT * FROM weather"
    cursor = conn.execute(sql)
    data = cursor.fetchall()
    responseObj = json.dumps(data)
    conn.close()
    return responseObj

def getYield(year=''):
    conn = sqlite3.connect('yield.db', check_same_thread=False)
    conn.row_factory = dict_factory
    sql = ""
    if len(year)>0:
        sql = f"SELECT * FROM yield WHERE year LIKE '{year}%'"
    else:
        sql = f"SELECT * FROM yield"
    cursor = conn.execute(sql)
    data = cursor.fetchall()
    responseObj = json.dumps(data)
    conn.close()
    return responseObj
    
@app.route('/api/weather')
def weather():
    year = request.args.get('year')
    station = request.args.get('station')
    if year!=None:
        a = year
    else:
        a = ''
    if station!=None:
        b = station
    else:
        b = ''
    obj = getWeather(a, b)
    return obj

@app.route('/api/yield')
def yield_():
    year = request.args.get('year')
    logger.info(year)
    if year!=None:
        a = year
    else:
        a = ''
    return getYield(a)
    
@app.route('/api/weather/stats')
def stats():
    year = request.args.get('year')
    station = request.args.get('station')
    if year!=None:
        a = year
    else:
        a = ''
    if station!=None:
        b = station
    else:
        b = ''
    obj = getStats(a, b)
    return obj

if __name__ == '__main__':
    conn = sqlite3.connect('test.db')
    app.run()



