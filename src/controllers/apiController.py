from os import stat
from services.service import getYield, getWeather, getStats

def index():
    return {'status': 'OK',
            'localhost:5000/api/yeild': 'Desc',
            'localhost:5000/api/weather': 'Desc',
            'localhost:5000/api/weather/stats': 'Desc'}

def weather(year, station):
    return getWeather(year, station)

def yield_(year):
    return getYield(year) 

def stats(year, station):
    return getStats(year, station)