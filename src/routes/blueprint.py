from flask import Blueprint
from flask import request
from controllers.apiController import yield_, weather, stats, index

blueprint = Blueprint('blueprint', __name__)

blueprint.route('/', methods=['GET'])(index)

@blueprint.route('/weather/stats', methods=['GET'])
def stats_response():
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
    return stats(a, b)

@blueprint.route('/weather', methods=['GET'])
def weather_response():
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
    return weather(a, b)

@blueprint.route('/yield', methods=['GET'])
def yield_response():
    year = request.args.get('year')
    if year!=None:
        a = year
    else:
        a = ''
    return yield_(a)