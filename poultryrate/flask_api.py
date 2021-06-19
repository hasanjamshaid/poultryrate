"""
poultryrate.entry_points.py
~~~~~~~~~~~~~~~~~~~~~~

This module contains the entry-point functions for the py_pkg module,
that are referenced in setup.py.
"""

from os import remove
from poultryrate.data_model import data_model
from poultryrate.tweet_classifier import tweet_classifier
from poultryrate.csv_reader import csv_reader
from flask import request, jsonify
from flask_sslify import SSLify
from sys import argv
from zipfile import ZipFile
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import configparser

import requests
import time
import twint
import schedule
import os
import datetime
import flask
import pandas as pd
from sys import platform

os.environ['build_mode'] = 'release' ### debug or release

#db_host="hasanjamshaid.mysql.pythonanywhere-services.com"
#db_username="hasanjamshaid"
#db_password="kNvHn434dQDaWgJe"
#db_name="hasanjamshaid$poultry_rates_db"


    
#db_host="smartfarmdb.cldvyav9mexp.us-east-1.rds.amazonaws.com"
#db_username="admin"
#db_password="4w0pqsMWkZsXi4Piuk4L"
#db_name="twint" 
 
#db_host="127.0.0.1"
#db_username="root"
#db_password="root"
#db_name="twint"

app = flask.Flask(__name__)

def main() -> None:
    parser = configparser.ConfigParser()

    #ini_path = os.path.join(os.getcwd(), 'poultryrate','poultryrate.cfg')
    ini_path = os.path.join('poultryrate','poultryrate.cfg')
    
    print(ini_path)
    parser.read(ini_path)

    print ("os detected ", platform)

    if platform == "linux" or platform == "linux2":
        # linux
        os.environ['data_files_path']=parser['DEFAULT_LINUX']['data_files_path']
        os.environ['config_path']=parser['DEFAULT_LINUX']['config_path']
        os.environ['db_host']=parser['DEFAULT_LINUX']['db_host']
        os.environ['db_port']=parser['DEFAULT_LINUX']['db_port']
        os.environ['db_username']=parser['DEFAULT_LINUX']['db_username']
        os.environ['db_password']=parser['DEFAULT_LINUX']['db_password']
        os.environ['host']=parser['DEFAULT_LINUX']['host']
        os.environ['port']=parser['DEFAULT_LINUX']['port']

    elif platform == "darwin":
        # OS X
        os.environ['data_path']=parser['DARWIN_DEFAULT']['data_path']
        os.environ['config_path']=parser['DARWIN_DEFAULT']['config_path']        
    elif platform == "win32":
        # Windows...
        os.environ['data_files_path']=parser['DEFAULT_WINDOWS']['data_files_path']
        os.environ['config_path']=parser['DEFAULT_WINDOWS']['config_path']
        os.environ['db_host']=parser['DEFAULT_WINDOWS']['db_host']
        os.environ['db_port']=parser['DEFAULT_WINDOWS']['db_port']
        os.environ['db_username']=parser['DEFAULT_WINDOWS']['db_username']
        os.environ['db_password']=parser['DEFAULT_WINDOWS']['db_password']
        os.environ['host']=parser['DEFAULT_WINDOWS']['host']
        os.environ['port']=parser['DEFAULT_WINDOWS']['port']

    os.environ['db_name']=parser['DEFAULT']['db_name']
    os.environ['data_files_pattern']=parser['DEFAULT']['data_files_pattern']

    print("data_files_path ", os.environ['data_files_path'])
    print("data_file_pattern ", os.environ['data_files_pattern'])

    print("config_path ", os.environ['config_path'])
    print("db_host ", os.environ['db_host'])
    print("db_port ", os.environ['db_port'])
    print("db_name ", os.environ['db_name'])

        
    '''Main package entry point.

    Delegates to other functions based on user input.
    '''

    #job_classify_tweet()


    try:
            app.run(host=os.environ['host'], port=os.environ['port'])
    except IndexError:
        RuntimeError('please supply a command for py_pkg - e.g. install.')
    return None


    #if __name__ == "poultryrate.entry_points":
    print("name ", __name__)
    
    
    return None


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Poultry rates api</h1>
<p>A prototype API for fetching poultry rate.</p>'''

@app.route('/api/v1/resources/tweets/all', methods=['GET'])
def fetch_unlabel_tweet():
    tweets = pd.DataFrame()
    sqlEngine = create_engine(db_url, pool_recycle=3600)
    dbConnection = sqlEngine.connect()

    try:
        tweets = pd.read_sql("SELECT * FROM tweets_table "+
        " where label='doc_rate' limit 1", dbConnection)
    except ValueError as vx:
        print(vx)
    except Exception as ex:   
        print(ex)
    else:
        print("SELECT * FROM tweets_table where label='' limit 1")   
    finally:
        dbConnection.close()
    tweets.set_index("id",inplace=True)

    return tweets.to_json(orient='index', index=True)


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


@app.route('/api/v1/resources/tweets', methods=['GET'])
def api_tweets():
    query_parameters = request.args

    page = int(query_parameters.get('page'))

    page_size = 20
    start_limit = (page-1) * page_size
    end_limit= start_limit + page_size
    
    query = "SELECT id, created_at, tweet, link, translate_english, translate_urdu, label, cities " + \
            " FROM tweets_table WHERE processed=1 order by created_at desc limit "+ str(start_limit) + "," + str(end_limit)
    
    sqlEngine = create_engine(db_url, pool_recycle=3600)
    dbConnection = sqlEngine.connect()

    tweets = pd.DataFrame()
    try:
        tweets = pd.read_sql(query, dbConnection)
    except ValueError as vx:
        print(vx)
    except Exception as ex:   
        print(ex)
    else:
        print(query)   
    finally:
        dbConnection.close()
    #tweets.set_index("id",inplace=True)
    return tweets.to_json(orient='records', date_format='iso')


@app.route('/api/v1/resources/<method>', methods=['GET'])
def api_generic_filter(method):
    #url = request.path
    query_parameters = request.args

    id = query_parameters.get('id')
    city = query_parameters.get('city')
    start_date = query_parameters.get('start_date')
    end_date = query_parameters.get('end_date')
    tweet_id = query_parameters.get('tweet_id')
    days_ago = query_parameters.get('days_ago')
    
    data_model_obj=data_model()
    tweets=data_model_obj.fetch_rates(method,id,city,start_date,end_date,tweet_id,days_ago)

    if tweets is None or len(tweets) == 0:
        return page_not_found(404)
        
    tweets["duration"]=tweets["date"]-pd.Timestamp.now()

    tweets.set_index("id",inplace=True)
    #return tweets.to_json(orient='index', index=True, date_format='iso')
    return tweets.to_json(orient='records', date_format='iso')



