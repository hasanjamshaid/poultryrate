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


def main() -> None:

    parser = configparser.ConfigParser()
    print('Hello World')	
    #ini_path = os.path.join(os.getcwd(), 'poultryrate','poultryrate.cfg')
    ini_path = os.path.join('app','poultryrate.cfg')
    
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
        os.environ['build_mode']=parser['DEFAULT_LINUX']['mode']
        os.environ['TWINT_DEBUG']=parser['DEFAULT_LINUX']['mode']     

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
        os.environ['build_mode']=parser['DEFAULT_WINDOWS']['mode']
        os.environ['TWINT_DEBUG']=parser['DEFAULT_WINDOWS']['mode']    

    os.environ['db_name']=parser['DEFAULT']['db_name']
    os.environ['data_files_pattern']=parser['DEFAULT']['data_files_pattern']

    print("data_files_path ", os.environ['data_files_path'])
    print("data_file_pattern ", os.environ['data_files_pattern'])

    print("config_path ", os.environ['config_path'])
    print("db_host ", os.environ['db_host'])
    print("db_port ", os.environ['db_port'])
    print("db_name ", os.environ['db_name'])


    print("logging mode ", os.environ['build_mode'])

        
    '''Main package entry point.

    Delegates to other functions based on user input.
    '''

    #job_classify_tweet()


    try:
            schedule.every(1).minutes.do(job_fetch_tweet_using_twint) ## not below 1 minute
            schedule.every(0.1).minutes.do(job_read_tweet_csv)
            schedule.every(0.1).minutes.do(job_classify_tweet)
            schedule.every(0.1).minutes.do(job_translate_tweets)
            
            while True:
                schedule.run_pending()
                time.sleep(1)
    except IndexError:
        RuntimeError('please supply a command for py_pkg - e.g. install.')


    return None

def job_fetch_tweet_using_twint():
    
    time_since=(datetime.datetime.now() - datetime.timedelta(days=0, minutes = 10))
    time_until=datetime.datetime.now()
    print("running job ", time_since)
    print("current time",datetime.datetime.now())
    current_timestamp = time_since.strftime(f"%Y%m%d%H%M%S")
    c = twint.Config()
    #c.Username = "Shahzadsaeed240"
    c.User_id = "3723347053"
    c.Limit = 1000
    c.Store_csv = True
    c.Output = os.environ['data_files_path']+ "file"+current_timestamp+".csv"
    c.Since = time_since.strftime(f"%Y-%m-%d %H:%M:%S")
	#c.Since = "2021-01-20 00:00:00"
    c.Until = time_until.strftime(f"%Y-%m-%d %H:%M:%S")
    #c.Show_hashtags = True
    #c.Show_cashtags = True
    #c.Stats = True
    #c.Debug = True
    #c.Pandas = True
    try:
        twint.run.Search(c)
    except:
        print("Error has occured in fetching tweet")

def job_classify_tweet():
    print("job_classify_tweet")
    tweet_classifier_obj = tweet_classifier()
    tweet_classifier_obj.label_tweet()

def job_read_tweet_csv():
    print("job_read_tweets")
    
    csv_reader_obj=csv_reader()
    csv_reader_obj.read_tweet_csv_file()

def job_translate_tweets():
    print("job_translate_tweets")
    data_model_obj=data_model()
    data_model_obj.translate_unprocessed_tweet()


