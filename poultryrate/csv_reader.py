import schedule
import time
import os
import datetime

#Step 2: All tweets in file are stored in temporary table and ‘new’ tweets are moved to tweets table

import pandas as pd

from glob import glob 
from poultryrate.data_model import data_model

class csv_reader():
    
    def read_tweet_csv_file(self):
        # csvs will contain all CSV files names ends with .csv in a list
        #if os.path.isdir('poultryrate\\poultryrate') :
        #    os.chdir(os.getcwd()+"\\data_files")
        #    print("csv_reader 1st current directory", os.getcwd())
        #elif os.path.isdir('poultryrate') :
        #    os.chdir(os.getcwd()+"\\poultryrate\\data_files")
        #    print("csv_reader 2nd current directory", os.getcwd())

        csvs = glob(os.environ['data_files_path']+os.environ['data_files_pattern'])

        if len(csvs)==0 :
            print(os.environ['data_files_path'], os.environ['data_files_pattern']," CSV not found")
            return
        print("file "+csvs[0]+" is being processed")

        latest_tweets=pd.read_csv(csvs[0], index_col="id", sep=',')
        #latest_tweets=pd.read_excel('file2years.xlsx', index_col="id", sep=',')
        #latest_tweets.set_index("id",drop=True, inplace=True)
        latest_tweets["label"]=""
        latest_tweets["sub_label"]=""
        latest_tweets["processed"]=""
        latest_tweets["translate_english"]=""
        latest_tweets["translate_urdu"]=""
        latest_tweets["cities"]=""
        latest_tweets

        data_model_obj=data_model()
        data_model_obj.store_latest_tweets_into_db(latest_tweets)
        os.rename(csvs[0], csvs[0]+"_processed")

        path_parent = os.path.dirname(os.getcwd())
        os.chdir(path_parent)
        #print(os.getcwd())    
